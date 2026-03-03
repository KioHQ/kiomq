use std::{
    default,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use crate::{
    error::QueueError, BackOffJobOptions, FailedDetails, JobMetrics, JobState, JobToken,
    RemoveOnCompletionOrFailure, Repeat, Trace,
};
use atomig::{Atom, Atomic};
#[cfg(feature = "redis-store")]
use redis::{FromRedisValue, RedisResult, ToRedisArgs, Value};
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
/// An envelope representing the result of running the worker's callback
pub enum ProcessedResult<R> {
    Failed(FailedDetails),
    Success(R, JobMetrics),
}
/// Most frequent set fields on a job
#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum JobField<R> {
    Token(JobToken),
    Payload(ProcessedResult<R>),
    ProcessedOn(u64),
    FinishedOn(u64),
    State(JobState),
    BackTrace(Trace),
}
impl<R> JobField<R> {
    pub fn name(&self) -> &'static str {
        match self {
            JobField::Token(job_token) => "token",
            JobField::Payload(processed_result) => {
                if let ProcessedResult::Success(_, _) = processed_result {
                    "returnedValue"
                } else {
                    "failedReason"
                }
            }
            JobField::ProcessedOn(_) => "processedOn",
            JobField::FinishedOn(_) => "finishedOn",
            JobField::State(job_state) => "state",
            JobField::BackTrace(_) => "stackTrace",
        }
    }
}

use derive_more::{Debug, Display};
use uuid::Uuid;
#[derive(Display, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
pub enum CollectionSuffix {
    Active,    // (list)
    Completed, //Sorted Set
    Delayed,   // ZSET
    Stalled,   // Set
    Prioritized,
    PriorityCounter, // (hash(number))
    Id,              // hash(number)
    Meta,            // key
    Events,          // stream or pub_sub depending on event_mode
    Wait,            // LIST
    Paused,          // LIST
    Failed,          // ZSET
    Marker,
    #[display("{_0}")]
    Job(u64),
    #[display("")]
    Prefix,
    #[display("{_0}:lock")]
    /// Lock(job_id)
    Lock(u64),
    #[display("stalled_check")]
    StalledCheck, // key
    #[display("worker_metrics:{_0}")]
    WorkerMetrics(Uuid),
}

impl CollectionSuffix {
    pub fn to_collection_name(&self, prefix: &str, name: &str) -> String {
        format!("{}:{}:{}", prefix, name, &self).to_lowercase()
    }
    /// create an identifier for this enum
    fn discriminant(&self) -> u8 {
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
            Self::WorkerMetrics(_) => 18,
        }
    }
    pub fn tag(&self) -> u64 {
        let top = (self.discriminant() as u64) << 56; // high 8 bits for variant id
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
            | Self::StalledCheck => top,
            Self::WorkerMetrics(uuid) => top | (uuid.as_u64_pair().1 & 0x00FF_FFFF_FFFF_FFFF),

            // Tagged variants → combine variant id + payload in lower 56 bits
            Self::Job(id) | Self::Lock(id) => top | (id & 0x00FF_FFFF_FFFF_FFFF),
        }
    }
    pub fn to_bytes(&self) -> [u8; 8] {
        self.tag().to_be_bytes()
    }
    /// Decodes a tag back into its enum variant.
    pub fn from_tag(tag: u64) -> Option<Self> {
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
            JobState::Wait => CollectionSuffix::Wait,
            JobState::Stalled => CollectionSuffix::Paused,
            JobState::Active => CollectionSuffix::Active,
            JobState::Paused => CollectionSuffix::Paused,
            JobState::Completed => CollectionSuffix::Completed,
            JobState::Resumed => CollectionSuffix::Active,
            JobState::Failed => CollectionSuffix::Failed,
            JobState::Delayed => CollectionSuffix::Delayed,
            JobState::Progress => CollectionSuffix::Prefix,
            JobState::Prioritized => CollectionSuffix::Prioritized,
            JobState::Processing => CollectionSuffix::Meta,
            JobState::Obliterated => CollectionSuffix::Events,
        }
    }
}

#[cfg(feature = "redis-store")]
use deadpool_redis::redis::RedisWrite;
#[cfg(feature = "redis-store")]
impl ToRedisArgs for CollectionSuffix {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg_fmt(self.to_string().to_lowercase());
    }
}
/// How events are delivered to listeners.
///
/// KioMQ supports two delivery mechanisms:
///
/// | Variant | Description |
/// |---------|-------------|
/// | [`Stream`](QueueEventMode::Stream) *(default)* | Events are written to a persistent append-only stream.  New listeners can replay past events. |
/// | [`PubSub`](QueueEventMode::PubSub) | Events are broadcast in real-time.  Listeners that connect after an event is fired miss it. |
///
/// Set this via [`QueueOpts::event_mode`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, Atom, Eq, PartialEq)]
#[repr(u8)]
pub enum QueueEventMode {
    PubSub = 1,
    #[default]
    Stream = 0,
}
impl TryFrom<u8> for QueueEventMode {
    type Error = QueueError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(QueueEventMode::PubSub),
            0 => Ok(QueueEventMode::Stream),
            _ => Err(QueueError::UnKnownEventMode),
        }
    }
}
#[cfg(feature = "redis-store")]
impl FromRedisValue for QueueEventMode {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let value = if let Value::Nil = v {
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

pub enum RetryOptions<'a> {
    Failed(&'a BackOffJobOptions),
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
///         type_: Some("exponential".to_owned()),
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
            remove_on_fail: Default::default(),
            remove_on_complete: Default::default(),
            repeat: None,
            attempts: 1,
            default_backoff: None,
        }
    }
}

pub(crate) type Counter = Arc<AtomicU64>;
fn create_counter(count: u64) -> Counter {
    Counter::new(count.into())
}
/// A live snapshot of queue state counts.
///
/// Counters are stored as `Arc<AtomicU64>` so they can be cheaply shared and
/// updated across threads.  The values are refreshed from the backing store
/// whenever [`crate::Queue::get_metrics`] is called; between calls the counts may be
/// slightly stale.
///
/// Use helper methods such as [`all_jobs_completed`](QueueMetrics::all_jobs_completed)
/// and [`is_idle`](QueueMetrics::is_idle) rather than inspecting individual
/// fields for common checks.
#[derive(Debug, Clone, Default)]
pub struct QueueMetrics {
    pub last_id: Counter,
    pub processing: Counter,
    pub prioritized: Counter,
    pub active: Counter,
    pub stalled: Counter,
    pub delayed: Counter,
    pub completed: Counter,
    pub failed: Counter,
    pub paused: Counter,
    pub waiting: Counter,
    pub is_paused: Arc<AtomicBool>,
    pub event_mode: Arc<Atomic<QueueEventMode>>,
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
    pub fn all_jobs_completed(&self) -> bool {
        let last_id = self.last_id.load(Ordering::Acquire);
        last_id > 0
            && self.completed.load(Ordering::Acquire) == last_id
            && self.active.load(Ordering::Acquire) == 0
            && self.is_idle()
    }
    #[allow(clippy::too_many_arguments)]
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
            event_mode: Arc::new(Atomic::new(event_mode)),
        }
    }
    pub fn update(&self, other: &Self) {
        self.paused
            .swap(other.paused.load(Ordering::Acquire), Ordering::AcqRel);
        self.completed
            .swap(other.completed.load(Ordering::Acquire), Ordering::AcqRel);
        self.stalled
            .swap(other.stalled.load(Ordering::Acquire), Ordering::AcqRel);
        self.active
            .swap(other.active.load(Ordering::Acquire), Ordering::AcqRel);
        self.last_id
            .swap(other.last_id.load(Ordering::Acquire), Ordering::AcqRel);
        self.delayed
            .swap(other.delayed.load(Ordering::Acquire), Ordering::AcqRel);
        self.failed
            .swap(other.failed.load(Ordering::Acquire), Ordering::AcqRel);
        self.waiting
            .swap(other.waiting.load(Ordering::Acquire), Ordering::AcqRel);
        self.processing
            .swap(other.processing.load(Ordering::Acquire), Ordering::AcqRel);
        self.prioritized
            .swap(other.prioritized.load(Ordering::Acquire), Ordering::AcqRel);
        self.event_mode
            .swap(other.event_mode.load(Ordering::Acquire), Ordering::AcqRel);
    }
    /// Returns `true` if there are delayed jobs ready or waiting to run.
    pub fn has_delayed(&self) -> bool {
        self.delayed.load(Ordering::Acquire) > 0
    }
    /// Returns `true` if there are jobs waiting to be picked up by a worker.
    pub fn queue_has_work(&self) -> bool {
        (self.waiting.load(Ordering::Acquire) > 0
            || self.delayed.load(Ordering::Acquire) > 0
            || self.stalled.load(Ordering::Acquire) > 0
            || self.prioritized.load(Ordering::Acquire) > 0)
    }
    /// Returns `true` if the queue is currently in the paused state.
    pub fn queue_is_paused(&self) -> bool {
        self.is_paused.load(Ordering::Acquire)
    }
    /// Returns `true` when no workers are currently processing a job.
    pub fn workers_idle(&self) -> bool {
        self.processing.load(Ordering::Acquire) == 0
    }
    /// Returns `true` if at least one job is in the active state.
    pub fn has_active_jobs(&self) -> bool {
        self.active.load(Ordering::Acquire) > 0
    }
    /// Returns `true` when the queue is in a fully quiescent state:
    /// no work waiting, no active jobs, and no workers are processing.
    ///
    /// Also requires that `last_id > 0` (i.e. at least one job was ever enqueued).
    pub fn is_idle(&self) -> bool {
        !self.queue_has_work()
            && !self.has_active_jobs()
            && self.workers_idle()
            && self.last_id.load(Ordering::Acquire) > 0
    }
    pub fn clear(&self) {
        let default = Self::default();
        self.update(&default);
    }
}
