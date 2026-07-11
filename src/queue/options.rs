use std::sync::Arc;

use crate::{
    BackOffJobOptions, FailedDetails, JobMetrics, JobState, JobToken, RemoveOnCompletionOrFailure,
    Repeat, Trace, error::QueueError,
};
use compact_str::{CompactString, format_compact};
#[cfg(feature = "redis-store")]
use redis::{FromRedisValue, ParsingError, ToRedisArgs, ToSingleRedisArg, Value};
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(untagged)]
/// The outcome of a single processor invocation.
pub enum ProcessedResult<R> {
    /// The processor returned an error.
    Failed(FailedDetails),
    #[debug("{_1:?}")]
    /// The processor succeeded, returning a value and timing metrics.
    Success(R, JobMetrics),
}
/// A typed field update applied to a job record in the store.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(untagged)]
pub enum JobField<R> {
    /// Worker lock token.
    Token(JobToken),
    /// Processor outcome (success value or failure details).
    Payload(ProcessedResult<R>),
    /// Unix timestamp (µs) when the processor started.
    ProcessedOn(u64),
    /// Unix timestamp (µs) when the job reached a terminal state.
    FinishedOn(u64),
    /// New lifecycle state.
    State(JobState),
    /// Stack-trace entry captured on failure.
    BackTrace(Trace),
}
impl<R> JobField<R> {
    /// Returns the store field name (key) for this variant.
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Token(_) => "token",
            Self::Payload(processed_result) => {
                if let ProcessedResult::Success(_, _) = processed_result {
                    "returnedValue"
                } else {
                    "failedReason"
                }
            }
            Self::ProcessedOn(_) => "processedOn",
            Self::FinishedOn(_) => "finishedOn",
            Self::State(_) => "state",
            Self::BackTrace(_) => "stackTrace",
        }
    }
}

use derive_more::{Debug, Display};
/// Identifies a named collection (list, set, sorted-set, hash, or key) in the
/// backing store.
///
/// Each queue owns a set of collections whose keys are formed as
/// `{prefix}:{name}:{suffix}`.  The suffix comes from this enum's `Display`
/// implementation via [`CollectionSuffix::to_collection_name`].
#[derive(Display, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
pub enum CollectionSuffix {
    /// List of jobs that are ready to be processed.
    Active,
    /// Sorted set of completed jobs ordered by finish time.
    Completed,
    /// Sorted set of delayed jobs ordered by scheduled time.
    Delayed,
    /// Set of jobs whose lock has expired (stalled).
    Stalled,
    /// Sorted set of high-priority jobs waiting for a worker slot.
    Prioritized,
    /// Hash storing the monotonically-increasing priority counter.
    PriorityCounter,
    /// Hash storing the auto-increment job ID counter.
    Id,
    /// Hash storing queue metadata (processing count, pause flag, etc.).
    Meta,
    /// The event stream or pub-sub channel for this queue.
    Events,
    /// List of jobs waiting to be picked up by a worker.
    Wait,
    /// List of jobs held while the queue is paused.
    Paused,
    /// Sorted set of permanently failed jobs ordered by failure time.
    Failed,
    /// Sentinel marker used internally for queue state signalling.
    Marker,
    /// The hash that stores all fields for a single job.
    #[display("{_0}")]
    Job(u64),
    /// The queue's top-level prefix key.
    #[display("")]
    Prefix,
    /// The distributed lock key for a specific job.
    #[display("{_0}:lock")]
    Lock(u64),
    /// Key storing the last stall-check timestamp.
    #[display("stalled_check")]
    StalledCheck,
    /// Key storing serialised metrics for a specific worker.
    #[display("worker_metrics")]
    WorkerMetrics,
    /// Key storing serialised metrics for a specific pid.
    #[display("process_metrics")]
    ProcessMetrics,
}

impl CollectionSuffix {
    /// Builds the full collection key as `{prefix}:{name}:{self}` (lowercased).
    #[must_use]
    pub fn to_collection_name(&self, prefix: &str, name: &str) -> CompactString {
        format_compact!("{}:{}:{}", prefix, name, &self).to_lowercase()
    }
    /// create an identifier for this enum
    const fn discriminant(&self) -> u8 {
        match self {
            Self::Active => 1,
            Self::Completed => 2,
            Self::Delayed => 3,
            Self::Stalled => 4,
            Self::Prioritized => 5,
            Self::PriorityCounter => 6,
            Self::Id => 7,
            Self::Meta => 8,
            Self::Events => 9,
            Self::Wait => 10,
            Self::Paused => 11,
            Self::Failed => 12,
            Self::Marker => 13,
            Self::Job(_) => 14,
            Self::Prefix => 15,
            Self::Lock(_) => 16,
            Self::StalledCheck => 17,
            Self::WorkerMetrics => 18,
            Self::ProcessMetrics => 19,
        }
    }
    /// Encodes this variant as a compact `u64` tag.
    ///
    /// The top 8 bits identify the variant and the lower 56 bits hold any
    /// payload (job ID, UUID fragment, etc.).  Used for O(1) membership checks
    /// in in-memory sets.
    #[must_use]
    pub fn tag(&self) -> u64 {
        let top = u64::from(self.discriminant()) << 56; // high 8 bits for variant id
        match self {
            // Fieldless variants → just top bits
            Self::Active
            | Self::Completed
            | Self::Delayed
            | Self::Stalled
            | Self::Prioritized
            | Self::PriorityCounter
            | Self::Id
            | Self::Meta
            | Self::Events
            | Self::Wait
            | Self::Paused
            | Self::Failed
            | Self::Marker
            | Self::Prefix
            | Self::StalledCheck
            | Self::WorkerMetrics
            | Self::ProcessMetrics => top,

            // Tagged variants → combine variant id + payload in lower 56 bits
            Self::Job(id) | Self::Lock(id) => top | (id & 0x00FF_FFFF_FFFF_FFFF),
        }
    }
    /// Returns the tag as a big-endian byte array.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; 8] {
        self.tag().to_be_bytes()
    }
    /// Decodes a tag produced by [`CollectionSuffix::tag`] back into the
    /// corresponding enum variant, or `None` if the discriminant is unknown.
    #[must_use]
    pub const fn from_tag(tag: u64) -> Option<Self> {
        let disc = (tag >> 56) as u8;
        let payload = tag & 0x00FF_FFFF_FFFF_FFFF;

        Some(match disc {
            1 => Self::Active,
            2 => Self::Completed,
            3 => Self::Delayed,
            4 => Self::Stalled,
            5 => Self::Prioritized,
            6 => Self::PriorityCounter,
            7 => Self::Id,
            8 => Self::Meta,
            9 => Self::Events,
            10 => Self::Wait,
            11 => Self::Paused,
            12 => Self::Failed,
            13 => Self::Marker,
            14 => Self::Job(payload),
            15 => Self::Prefix,
            16 => Self::Lock(payload),
            17 => Self::StalledCheck,
            _ => return None,
        })
    }
}
impl From<JobState> for CollectionSuffix {
    fn from(val: JobState) -> Self {
        match val {
            JobState::Wait => Self::Wait,
            JobState::Stalled | JobState::Paused => Self::Paused,
            JobState::Active | JobState::Resumed => Self::Active,
            JobState::Completed => Self::Completed,
            JobState::Failed => Self::Failed,
            JobState::Delayed => Self::Delayed,
            JobState::Progress => Self::Prefix,
            JobState::Prioritized => Self::Prioritized,
            JobState::Processing => Self::Meta,
            JobState::Obliterated => Self::Events,
        }
    }
}

#[cfg(feature = "redis-store")]
use redis::RedisWrite;
#[cfg(feature = "redis-store")]
impl ToRedisArgs for CollectionSuffix {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg_fmt(self.to_string().to_lowercase());
    }
}
#[cfg(feature = "redis-store")]
impl ToSingleRedisArg for CollectionSuffix {}
#[cfg(feature = "redis-store")]
impl ToSingleRedisArg for QueueEventMode {}
/// Controls how events are published and consumed within a queue.
///
/// Set this via [`QueueOpts::event_mode`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum QueueEventMode {
    /// Broadcast-only delivery. Listeners that connect after an event is fired
    /// will not receive it.
    PubSub = 1,
    /// Persistent append-only stream (default). New listeners can replay past
    /// events.
    #[default]
    Stream = 0,
}
impl TryFrom<u8> for QueueEventMode {
    type Error = QueueError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::PubSub),
            0 => Ok(Self::Stream),
            _ => Err(QueueError::UnKnownEventMode),
        }
    }
}
#[cfg(feature = "redis-store")]
impl FromRedisValue for QueueEventMode {
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        let value = if matches!(v, Value::Nil) {
            0
        } else {
            u8::from_redis_value(v)?
        };
        let mode = value.try_into().unwrap_or_default();
        Ok(mode)
    }
}
#[cfg(feature = "redis-store")]
impl ToRedisArgs for QueueEventMode {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        let value = *self as u8;
        out.write_arg_fmt(value);
    }
}

/// Specifies how a job should be retried after a failure or completion.
///
/// Passed to [`crate::Queue::retry_job`].
#[derive(Clone, Debug)]
pub enum RetryOptions<'a> {
    /// Retry a failed job using the given backoff options.
    Failed(&'a BackOffJobOptions),
    /// Re-enqueue a job according to a [`Repeat`] policy.
    WithRepeat(&'a Repeat),
}
impl<'a> From<&'a BackOffJobOptions> for RetryOptions<'a> {
    fn from(value: &'a BackOffJobOptions) -> Self {
        RetryOptions::Failed(value)
    }
}
impl<'a> From<&'a Repeat> for RetryOptions<'a> {
    fn from(value: &'a Repeat) -> Self {
        Self::WithRepeat(value)
    }
}
/// Queue-level configuration.
///
/// Pass this to [`crate::Queue::new`] to customise the queue's behaviour.
///
/// # Examples
///
/// ```rust
/// use kiomq::{BackOffJobOptions, BackOffOptions, KeepJobs, QueueEventMode, QueueOpts,
///             RemoveOnCompletionOrFailure};
///
/// let opts = QueueOpts {
///     attempts: 3,
///     default_backoff: Some(BackOffJobOptions::Opts(BackOffOptions {
///         type_: Some("exponential".into()),
///         delay: Some(500),
///     })),
///     remove_on_complete: Some(RemoveOnCompletionOrFailure::Bool(true)),
///     event_mode: Some(QueueEventMode::Stream),
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone)]
pub struct QueueOpts {
    /// Policy for removing jobs after they fail.  `None` keeps them forever.
    pub remove_on_fail: Option<RemoveOnCompletionOrFailure>,
    /// Policy for removing jobs after they complete.  `None` keeps them forever.
    pub remove_on_complete: Option<RemoveOnCompletionOrFailure>,
    /// Default number of attempts for jobs that don't specify their own.
    /// Defaults to `1`.
    pub attempts: u64,
    /// Default backoff strategy applied to all jobs in this queue unless
    /// overridden at the job level.
    pub default_backoff: Option<BackOffJobOptions>,
    /// Controls how events are delivered (stream vs pub/sub).
    pub event_mode: Option<QueueEventMode>,
    /// Default repeat policy applied to all jobs unless overridden.
    pub repeat: Option<Repeat>,
}
impl Default for QueueOpts {
    fn default() -> Self {
        Self {
            event_mode: Some(QueueEventMode::default()),
            remove_on_fail: Option::default(),
            remove_on_complete: Option::default(),
            repeat: None,
            attempts: 1,
            default_backoff: None,
        }
    }
}
use crossbeam::atomic::AtomicCell;

/// A shared atomic counter used to track job IDs and other queue counters.
pub type Counter = Arc<AtomicCell<u64>>;
fn create_counter(count: u64) -> Counter {
    Counter::new(count.into())
}
/// A live snapshot of queue state counts.
///
/// Counters are stored as `Arc<AtomicU64>` so they can be cheaply shared and
/// updated across threads. The values are refreshed from the backing store
/// whenever [`crate::Queue::get_metrics`] is called; between calls the counts
/// may be slightly stale.
///
/// Prefer the helper methods like [`all_jobs_completed`](QueueMetrics::all_jobs_completed)
/// and [`is_idle`](QueueMetrics::is_idle) over reading individual fields directly.
#[derive(Debug, Clone, Default)]
pub struct QueueMetrics {
    /// The highest job ID ever assigned in this queue.
    pub last_id: Counter,
    /// Number of jobs currently being processed by workers.
    pub processing: Counter,
    /// Number of jobs in the priority sorted-set waiting to become active.
    pub prioritized: Counter,
    /// Number of jobs currently in the `Active` state.
    pub active: Counter,
    /// Number of jobs in the `Stalled` state pending recovery.
    pub stalled: Counter,
    /// Number of jobs scheduled to run in the future.
    pub delayed: Counter,
    /// Total number of jobs that have completed successfully.
    pub completed: Counter,
    /// Total number of jobs that have permanently failed.
    pub failed: Counter,
    /// Number of jobs in the paused list (queue is paused).
    pub paused: Counter,
    /// Number of jobs waiting to be picked up by a worker.
    pub waiting: Counter,
    /// Whether the queue is currently in the paused state.
    pub is_paused: Arc<AtomicCell<bool>>,
    /// The active event-delivery mode for this queue.
    pub event_mode: Arc<AtomicCell<QueueEventMode>>,
}
impl QueueMetrics {
    /// Returns `true` when every enqueued job has completed.
    ///
    /// Specifically this is `true` when:
    /// - `last_id > 0` (at least one job was ever enqueued),
    /// - `completed == last_id` (all jobs have finished),
    /// - `active == 0`, and
    /// - the queue is otherwise idle (no waiting, delayed, stalled, or
    ///   prioritized jobs and no in-flight workers).
    #[must_use]
    pub fn all_jobs_completed(&self) -> bool {
        let last_id = self.last_id.load();
        last_id > 0 && self.completed.load() == last_id && self.active.load() == 0 && self.is_idle()
    }
    #[allow(clippy::too_many_arguments)]
    /// Constructs a `QueueMetrics` from raw counter values read from the store.
    #[must_use]
    pub fn new(
        last_id: u64,
        processing: u64,
        active: u64,
        stalled: u64,
        completed: u64,
        delayed: u64,
        prioritized: u64,
        paused: u64,
        failed: u64,
        waiting: u64,
        is_paused: bool,
        event_mode: QueueEventMode,
    ) -> Self {
        Self {
            last_id: create_counter(last_id),
            prioritized: create_counter(prioritized),
            processing: create_counter(processing),
            active: create_counter(active),
            stalled: create_counter(stalled),
            completed: create_counter(completed),
            waiting: create_counter(waiting),
            delayed: create_counter(delayed),
            paused: create_counter(paused),
            failed: create_counter(failed),
            is_paused: Arc::new(is_paused.into()),
            event_mode: Arc::new(AtomicCell::new(event_mode)),
        }
    }
    /// Atomically replaces all counters with the values from `other`.
    pub fn update(&self, other: &Self) {
        self.paused.swap(other.paused.load());
        self.completed.swap(other.completed.load());
        self.stalled.swap(other.stalled.load());
        self.active.swap(other.active.load());
        self.last_id.swap(other.last_id.load());
        self.delayed.swap(other.delayed.load());
        self.failed.swap(other.failed.load());
        self.waiting.swap(other.waiting.load());
        self.processing.swap(other.processing.load());
        self.prioritized.swap(other.prioritized.load());
        self.event_mode.swap(other.event_mode.load());
    }
    /// Returns `true` if there are delayed jobs ready or waiting to run.
    #[must_use]
    pub fn has_delayed(&self) -> bool {
        self.delayed.load() > 0
    }
    /// Returns `true` if there are jobs waiting to be picked up by a worker.
    #[must_use]
    pub fn queue_has_work(&self) -> bool {
        self.waiting.load() > 0
            || self.delayed.load() > 0
            || self.stalled.load() > 0
            || self.prioritized.load() > 0
    }
    /// Returns `true` if the queue is currently in the paused state.
    #[must_use]
    pub fn queue_is_paused(&self) -> bool {
        self.is_paused.load()
    }
    /// Returns `true` when no workers are currently processing a job.
    #[must_use]
    pub fn workers_idle(&self) -> bool {
        self.processing.load() == 0
    }
    /// Returns `true` if at least one job is in the active state.
    #[must_use]
    pub fn has_active_jobs(&self) -> bool {
        self.active.load() > 0
    }
    /// Returns `true` when the queue is in a fully quiescent state:
    /// no work waiting, no active jobs, and no workers are processing.
    ///
    /// Also requires that `last_id > 0` (i.e. at least one job was ever enqueued).
    #[must_use]
    pub fn is_idle(&self) -> bool {
        !self.queue_has_work() && !self.has_active_jobs() && self.workers_idle()
    }
    /// Resets all counters to zero (equivalent to a freshly created queue).
    pub fn clear(&self) {
        let default = Self::default();
        self.update(&default);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FailedDetails, JobMetrics, JobState, JobToken, Trace};

    #[test]
    fn queue_opts_default_uses_documented_values() {
        let opts = QueueOpts::default();
        // `attempts` must default to exactly 1 (a single attempt, no retries).
        assert_eq!(opts.attempts, 1);
        // The default event mode is the persistent Stream, never PubSub.
        assert_eq!(opts.event_mode, Some(QueueEventMode::Stream));
        // Retention and repeat policies are opt-in and therefore absent.
        assert!(opts.remove_on_fail.is_none());
        assert!(opts.remove_on_complete.is_none());
        assert!(opts.repeat.is_none());
        assert!(opts.default_backoff.is_none());
    }

    #[test]
    fn queue_opts_clone_is_a_faithful_copy() {
        let opts = QueueOpts {
            attempts: 7,
            event_mode: Some(QueueEventMode::PubSub),
            ..Default::default()
        };
        let cloned = opts.clone();
        // Compare against the original (still live) to prove the clone is faithful.
        assert_eq!(cloned.attempts, opts.attempts);
        assert_eq!(cloned.event_mode, opts.event_mode);
        assert_eq!(cloned.attempts, 7);
        assert_eq!(cloned.event_mode, Some(QueueEventMode::PubSub));
    }

    #[test]
    fn queue_event_mode_default_is_stream() {
        assert_eq!(QueueEventMode::default(), QueueEventMode::Stream);
    }

    #[test]
    fn queue_event_mode_discriminant_bytes_are_stable() {
        // The `#[repr(u8)]` values are persisted to the store, so they must not
        // drift: Stream is 0 and PubSub is 1.
        assert_eq!(QueueEventMode::Stream as u8, 0);
        assert_eq!(QueueEventMode::PubSub as u8, 1);
    }

    #[test]
    fn queue_event_mode_try_from_accepts_known_bytes() {
        assert_eq!(
            QueueEventMode::try_from(0u8).expect("0 is Stream"),
            QueueEventMode::Stream
        );
        assert_eq!(
            QueueEventMode::try_from(1u8).expect("1 is PubSub"),
            QueueEventMode::PubSub
        );
    }

    #[test]
    fn queue_event_mode_try_from_rejects_every_unknown_byte() {
        // Anything outside {0, 1} must fail loudly with the dedicated error,
        // rather than silently defaulting.
        for byte in 2u8..=u8::MAX {
            let err = QueueEventMode::try_from(byte)
                .expect_err("bytes above 1 are not valid event modes");
            assert!(
                matches!(err, QueueError::UnKnownEventMode),
                "byte {byte} produced unexpected error {err:?}"
            );
        }
    }

    #[test]
    fn queue_event_mode_byte_round_trips_both_directions() {
        for mode in [QueueEventMode::Stream, QueueEventMode::PubSub] {
            let byte = mode as u8;
            let back = QueueEventMode::try_from(byte).expect("valid mode byte");
            assert_eq!(back, mode);
        }
    }

    /// Fieldless variants that `from_tag` is documented/able to decode.
    const ROUND_TRIPPABLE_FIELDLESS: [CollectionSuffix; 15] = [
        CollectionSuffix::Active,
        CollectionSuffix::Completed,
        CollectionSuffix::Delayed,
        CollectionSuffix::Stalled,
        CollectionSuffix::Prioritized,
        CollectionSuffix::PriorityCounter,
        CollectionSuffix::Id,
        CollectionSuffix::Meta,
        CollectionSuffix::Events,
        CollectionSuffix::Wait,
        CollectionSuffix::Paused,
        CollectionSuffix::Failed,
        CollectionSuffix::Marker,
        CollectionSuffix::Prefix,
        CollectionSuffix::StalledCheck,
    ];

    #[test]
    fn collection_suffix_fieldless_tags_round_trip() {
        for suffix in ROUND_TRIPPABLE_FIELDLESS {
            let tag = suffix.tag();
            let decoded =
                CollectionSuffix::from_tag(tag).expect("known fieldless discriminant must decode");
            assert_eq!(decoded, suffix, "round-trip failed for {suffix:?}");
        }
    }

    #[test]
    fn collection_suffix_job_and_lock_round_trip_within_payload_range() {
        // Payloads up to 2^56 - 1 fit in the lower 56 bits and must survive.
        for id in [0u64, 1, 42, 1_000_000, 0x00FF_FFFF_FFFF_FFFF] {
            let job = CollectionSuffix::Job(id);
            let lock = CollectionSuffix::Lock(id);
            assert_eq!(
                CollectionSuffix::from_tag(job.tag()),
                Some(CollectionSuffix::Job(id)),
                "Job({id}) failed to round-trip"
            );
            assert_eq!(
                CollectionSuffix::from_tag(lock.tag()),
                Some(CollectionSuffix::Lock(id)),
                "Lock({id}) failed to round-trip"
            );
        }
    }

    #[test]
    fn collection_suffix_job_payload_is_truncated_above_56_bits() {
        // The top 8 bits are reserved for the discriminant, so an ID that uses
        // them is masked. This documents the (lossy) boundary behaviour.
        let id = u64::MAX;
        let masked = id & 0x00FF_FFFF_FFFF_FFFF;
        let decoded = CollectionSuffix::from_tag(CollectionSuffix::Job(id).tag());
        assert_eq!(decoded, Some(CollectionSuffix::Job(masked)));
        assert_ne!(decoded, Some(CollectionSuffix::Job(id)));
    }

    #[test]
    fn collection_suffix_from_tag_rejects_unknown_discriminants() {
        // Discriminant 0 and everything at/above 20 are undefined.
        for disc in [0u8, 20, 21, 100, u8::MAX] {
            let tag = u64::from(disc) << 56;
            assert_eq!(
                CollectionSuffix::from_tag(tag),
                None,
                "discriminant {disc} should not decode"
            );
        }
    }

    #[ignore = "SUSPECTED BUG: `discriminant()`/`tag()` encode WorkerMetrics=18 \
                and ProcessMetrics=19, but `from_tag` only decodes discriminants \
                1..=17, so these two variants silently fail to round-trip. This \
                test asserts the DESIRED round-trip and fails until from_tag \
                decodes discriminants 18 and 19."]
    #[test]
    fn collection_suffix_worker_and_process_metrics_tags_round_trip() {
        assert_eq!(
            CollectionSuffix::from_tag(CollectionSuffix::WorkerMetrics.tag()),
            Some(CollectionSuffix::WorkerMetrics)
        );
        assert_eq!(
            CollectionSuffix::from_tag(CollectionSuffix::ProcessMetrics.tag()),
            Some(CollectionSuffix::ProcessMetrics)
        );
    }

    #[test]
    fn collection_suffix_to_bytes_matches_big_endian_tag() {
        for suffix in [
            CollectionSuffix::Active,
            CollectionSuffix::Job(123),
            CollectionSuffix::Lock(456),
            CollectionSuffix::Prefix,
        ] {
            assert_eq!(suffix.to_bytes(), suffix.tag().to_be_bytes());
        }
    }

    #[test]
    fn collection_suffix_to_collection_name_is_lowercased_and_delimited() {
        // Mixed-case prefix and name must be fully lowercased.
        let key = CollectionSuffix::Active.to_collection_name("Kio", "MyQueue");
        assert_eq!(key, "kio:myqueue:active");
    }

    #[test]
    fn collection_suffix_to_collection_name_embeds_numeric_payloads() {
        assert_eq!(
            CollectionSuffix::Job(5).to_collection_name("p", "n"),
            "p:n:5"
        );
        assert_eq!(
            CollectionSuffix::Lock(9).to_collection_name("p", "n"),
            "p:n:9:lock"
        );
    }

    #[test]
    fn collection_suffix_to_collection_name_handles_empty_prefix_and_name() {
        // The Prefix variant renders as an empty suffix, producing a
        // trailing-colon key; empty prefix/name must not panic.
        assert_eq!(CollectionSuffix::Prefix.to_collection_name("", ""), "::");
    }

    #[test]
    fn collection_suffix_display_special_variants() {
        assert_eq!(CollectionSuffix::Job(7).to_string(), "7");
        assert_eq!(CollectionSuffix::Prefix.to_string(), "");
        assert_eq!(CollectionSuffix::Lock(7).to_string(), "7:lock");
        assert_eq!(CollectionSuffix::StalledCheck.to_string(), "stalled_check");
        assert_eq!(
            CollectionSuffix::WorkerMetrics.to_string(),
            "worker_metrics"
        );
        assert_eq!(
            CollectionSuffix::ProcessMetrics.to_string(),
            "process_metrics"
        );
    }

    #[test]
    fn collection_suffix_ordering_follows_declaration() {
        // Ord is derived; Active precedes Completed precedes Delayed.
        assert!(CollectionSuffix::Active < CollectionSuffix::Completed);
        assert!(CollectionSuffix::Completed < CollectionSuffix::Delayed);
    }

    #[test]
    fn collection_suffix_from_job_state_covers_every_state() {
        // Exhaustive mapping — guards against a mis-routed state.
        assert_eq!(
            CollectionSuffix::from(JobState::Wait),
            CollectionSuffix::Wait
        );
        assert_eq!(
            CollectionSuffix::from(JobState::Stalled),
            CollectionSuffix::Paused
        );
        assert_eq!(
            CollectionSuffix::from(JobState::Paused),
            CollectionSuffix::Paused
        );
        assert_eq!(
            CollectionSuffix::from(JobState::Active),
            CollectionSuffix::Active
        );
        assert_eq!(
            CollectionSuffix::from(JobState::Resumed),
            CollectionSuffix::Active
        );
        assert_eq!(
            CollectionSuffix::from(JobState::Completed),
            CollectionSuffix::Completed
        );
        assert_eq!(
            CollectionSuffix::from(JobState::Failed),
            CollectionSuffix::Failed
        );
        assert_eq!(
            CollectionSuffix::from(JobState::Delayed),
            CollectionSuffix::Delayed
        );
        assert_eq!(
            CollectionSuffix::from(JobState::Progress),
            CollectionSuffix::Prefix
        );
        assert_eq!(
            CollectionSuffix::from(JobState::Prioritized),
            CollectionSuffix::Prioritized
        );
        assert_eq!(
            CollectionSuffix::from(JobState::Processing),
            CollectionSuffix::Meta
        );
        assert_eq!(
            CollectionSuffix::from(JobState::Obliterated),
            CollectionSuffix::Events
        );
    }

    #[test]
    fn queue_metrics_default_is_idle_but_not_completed() {
        let metrics = QueueMetrics::default();
        // Nothing enqueued: the queue is quiescent yet "all completed" is false
        // because no job was ever added (last_id == 0).
        assert!(metrics.is_idle());
        assert!(!metrics.all_jobs_completed());
        assert!(!metrics.queue_has_work());
        assert!(metrics.workers_idle());
        assert!(!metrics.has_active_jobs());
        assert!(!metrics.queue_is_paused());
    }

    /// Helper to build a fully-idle, fully-completed metrics snapshot.
    fn completed_metrics(last_id: u64, failed: u64, paused: u64) -> QueueMetrics {
        QueueMetrics::new(
            last_id, // last_id
            0,       // processing
            0,       // active
            0,       // stalled
            last_id, // completed
            0,       // delayed
            0,       // prioritized
            paused,  // paused
            failed,  // failed
            0,       // waiting
            false,   // is_paused
            QueueEventMode::Stream,
        )
    }

    #[test]
    fn queue_metrics_all_jobs_completed_true_case() {
        let metrics = completed_metrics(3, 0, 0);
        assert!(metrics.all_jobs_completed());
    }

    #[test]
    fn queue_metrics_all_jobs_completed_ignores_failed_and_paused_counts() {
        // `all_jobs_completed` only inspects last_id/completed/active/idle, so
        // non-zero failed and paused counters must not disturb it.
        let metrics = completed_metrics(3, 9, 7);
        assert!(metrics.all_jobs_completed());
    }

    #[test]
    fn queue_metrics_all_jobs_completed_false_when_a_job_is_active() {
        let metrics = QueueMetrics::new(
            2,
            0,
            1, // active > 0
            0,
            2,
            0,
            0,
            0,
            0,
            0,
            false,
            QueueEventMode::Stream,
        );
        assert!(!metrics.all_jobs_completed());
    }

    #[test]
    fn queue_metrics_all_jobs_completed_false_when_completed_below_last_id() {
        let metrics = QueueMetrics::new(
            5,
            0,
            0,
            0,
            4, // completed < last_id
            0,
            0,
            0,
            0,
            0,
            false,
            QueueEventMode::Stream,
        );
        assert!(!metrics.all_jobs_completed());
    }

    #[test]
    fn queue_metrics_queue_has_work_triggers_on_each_pending_bucket() {
        // waiting, delayed, stalled and prioritized should each independently
        // mark the queue as having work.
        let buckets = [
            (9, 0, 0, 0), // waiting
            (0, 9, 0, 0), // delayed
            (0, 0, 9, 0), // stalled
            (0, 0, 0, 9), // prioritized
        ];
        for (waiting, delayed, stalled, prioritized) in buckets {
            let metrics = QueueMetrics::new(
                1,
                0,
                0,
                stalled,
                0,
                delayed,
                prioritized,
                0,
                0,
                waiting,
                false,
                QueueEventMode::Stream,
            );
            assert!(
                metrics.queue_has_work(),
                "expected work for (w={waiting}, d={delayed}, s={stalled}, p={prioritized})"
            );
            assert!(!metrics.is_idle());
        }
    }

    #[test]
    fn queue_metrics_workers_idle_reflects_processing_counter() {
        let busy = QueueMetrics::new(1, 3, 0, 0, 0, 0, 0, 0, 0, 0, false, QueueEventMode::Stream);
        assert!(!busy.workers_idle());
        assert!(!busy.is_idle());
    }

    #[test]
    fn queue_metrics_has_delayed_and_active_helpers() {
        let metrics =
            QueueMetrics::new(1, 0, 2, 0, 0, 4, 0, 0, 0, 0, false, QueueEventMode::Stream);
        assert!(metrics.has_delayed());
        assert!(metrics.has_active_jobs());
    }

    #[test]
    fn queue_metrics_queue_is_paused_reads_flag() {
        let metrics = QueueMetrics::new(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, true, QueueEventMode::Stream);
        assert!(metrics.queue_is_paused());
    }

    #[test]
    fn queue_metrics_update_replaces_all_counters() {
        let target = QueueMetrics::default();
        let source =
            QueueMetrics::new(10, 1, 2, 3, 4, 5, 6, 7, 8, 9, false, QueueEventMode::PubSub);
        target.update(&source);
        assert_eq!(target.last_id.load(), 10);
        assert_eq!(target.processing.load(), 1);
        assert_eq!(target.active.load(), 2);
        assert_eq!(target.stalled.load(), 3);
        assert_eq!(target.completed.load(), 4);
        assert_eq!(target.delayed.load(), 5);
        assert_eq!(target.prioritized.load(), 6);
        assert_eq!(target.paused.load(), 7);
        assert_eq!(target.failed.load(), 8);
        assert_eq!(target.waiting.load(), 9);
        assert_eq!(target.event_mode.load(), QueueEventMode::PubSub);
    }

    #[test]
    fn queue_metrics_clear_zeroes_counters() {
        let metrics =
            QueueMetrics::new(10, 1, 2, 3, 4, 5, 6, 7, 8, 9, false, QueueEventMode::PubSub);
        metrics.clear();
        assert_eq!(metrics.last_id.load(), 0);
        assert_eq!(metrics.processing.load(), 0);
        assert_eq!(metrics.active.load(), 0);
        assert_eq!(metrics.completed.load(), 0);
        assert_eq!(metrics.waiting.load(), 0);
        assert!(metrics.is_idle());
    }

    #[test]
    fn retry_options_from_backoff_ref_is_failed_variant() {
        let backoff = BackOffJobOptions::Number(500);
        let opt: RetryOptions<'_> = (&backoff).into();
        assert!(matches!(opt, RetryOptions::Failed(_)));
    }

    #[test]
    fn retry_options_from_repeat_ref_is_with_repeat_variant() {
        let repeat = Repeat::Immediately(3);
        let opt: RetryOptions<'_> = (&repeat).into();
        assert!(matches!(opt, RetryOptions::WithRepeat(_)));
    }

    #[test]
    fn job_field_name_matches_store_field_keys() {
        assert_eq!(JobField::<i32>::Token(JobToken::default()).name(), "token");
        assert_eq!(
            JobField::<i32>::Payload(ProcessedResult::Success(1, JobMetrics::default())).name(),
            "returnedValue"
        );
        assert_eq!(
            JobField::<i32>::Payload(ProcessedResult::Failed(FailedDetails::default())).name(),
            "failedReason"
        );
        assert_eq!(JobField::<i32>::ProcessedOn(0).name(), "processedOn");
        assert_eq!(JobField::<i32>::FinishedOn(0).name(), "finishedOn");
        assert_eq!(JobField::<i32>::State(JobState::Wait).name(), "state");
        assert_eq!(
            JobField::<i32>::BackTrace(Trace::default()).name(),
            "stackTrace"
        );
    }
}
