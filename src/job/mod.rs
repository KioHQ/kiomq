use crate::stores::Store;
use chrono::{DateTime, Utc};
#[cfg(feature = "redis-store")]
use deadpool_redis::redis::ToRedisArgs;
use derive_more::{Display, FromStr};
#[cfg(feature = "redis-store")]
use redis::{FromRedisValue, RedisResult, Value};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
mod backoff;
mod delay;
mod repeat;
use crate::{job::delay::JobDelay, KioError};
pub use backoff::{BackOff, BackOffJobOptions, BackOffOptions, StoredFn};
pub use repeat::Repeat;
use std::time::Duration;
// Job Metrics
/// Timing and attempt statistics for a completed job.
///
/// Obtain this by calling [`Job::get_metrics`].
#[derive(Debug, Clone, Copy, Hash, Serialize, Deserialize, Display, Default)]
#[display("job {id}-#{attempt} , ran for {ran_for:?}, delayed for {delayed_for:?}")]
pub struct JobMetrics {
    /// How long the processor function ran.
    pub ran_for: Duration,
    /// How long the job waited in the delayed state before becoming active.
    pub delayed_for: Duration,
    /// The attempt number on which the job finished.
    pub attempt: u64,
    /// The configured delay in milliseconds (0 for non-delayed jobs).
    pub delay: u64,
    /// The numeric job ID.
    pub id: u64,
}

/// alias for `DateTime`<Utc>
pub type Dt = DateTime<Utc>;
/// The lifecycle state of a job within the queue.
///
/// Jobs typically flow through `Wait` → `Active` → `Completed` or `Failed`.
/// Other transitions exist for priority queuing, delays, pausing, and stall
/// detection.
#[derive(
    Debug,
    Serialize,
    Deserialize,
    FromStr,
    Default,
    Hash,
    Ord,
    PartialOrd,
    Display,
    Clone,
    Copy,
    PartialEq,
    Eq,
)]
#[serde(rename_all = "camelCase")]
pub enum JobState {
    /// Ready to be picked up by a worker. This is the default state.
    #[default]
    Wait,
    /// In the priority sorted-set, waiting to be moved to active.
    Prioritized,
    /// The worker that held the lock disappeared; the job is pending recovery.
    Stalled,
    /// Currently being processed by a worker.
    Active,
    /// The queue is paused; the job is waiting in the paused list.
    Paused,
    /// The queue has resumed; the job is transitioning back to `Wait`.
    Resumed,
    /// The processor function returned successfully.
    Completed,
    /// The processor returned an error, or the job stalled too many times.
    Failed,
    /// Scheduled to run at a future timestamp.
    Delayed,
    /// A progress-update event (not a persistent job state).
    Progress,
    /// The queue was obliterated and the job has been deleted.
    Obliterated,
    /// The worker has started executing the processor function.
    Processing,
}
#[cfg(feature = "redis-store")]
impl ToRedisArgs for JobState {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + deadpool_redis::redis::RedisWrite,
    {
        out.write_arg_fmt(self.to_string().to_lowercase());
    }
}
/// Per-job configuration options.
///
/// Supply this to [`Queue::add_job`] or [`Queue::bulk_add`] to customise
/// individual job behaviour.  Fields left at `Default` inherit the queue's
/// [`crate::QueueOpts`] values.
///
/// # Examples
///
/// ```rust
/// use kiomq::JobOptions;
///
/// let opts = JobOptions {
///     attempts: 5,
///     priority: 10,
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Serialize, Deserialize, Default, Hash, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct JobOptions {
    /// Scheduling priority (lower values run first). `0` means no priority.
    pub priority: u64,
    /// When to run the job: immediately (`TimeMilis(0)`), after N milliseconds,
    /// or according to a cron expression.
    pub delay: JobDelay,
    /// Optional explicit job ID.  When `None` the store assigns one.
    pub id: Option<u64>,
    /// Maximum number of attempts before the job is permanently marked as
    /// failed.
    pub attempts: u64,
    /// Retention policy applied when the job *completes* successfully.
    /// Defaults to `None` (inherit from [`crate::QueueOpts`]).
    pub remove_on_complete: Option<RemoveOnCompletionOrFailure>,
    /// Retention policy applied when the job *fails* permanently.
    /// Defaults to `None` (inherit from [`crate::QueueOpts`]).
    pub remove_on_fail: Option<RemoveOnCompletionOrFailure>,
    /// Per-job backoff strategy, overriding the queue default.
    pub backoff: Option<BackOffJobOptions>,
    /// Repeat policy; when set the job is re-enqueued after each run.
    pub repeat: Option<Repeat>,
}

/// Controls whether—and how many—completed or failed job records are kept.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, Hash, PartialEq, Eq)]
#[serde(untagged)]
pub enum RemoveOnCompletionOrFailure {
    /// `true` removes the record immediately; `false` retains it forever.
    Bool(bool),
    /// Retain at most this many records; older ones are pruned.
    Int(i64),
    /// Fine-grained policy controlling both age and count.
    Opts(KeepJobs),
}
impl Default for RemoveOnCompletionOrFailure {
    fn default() -> Self {
        Self::Bool(false)
    }
}
/// Fine-grained retention policy for completed/failed jobs.
///
/// Both fields are optional; omit one to use only the other constraint.
#[derive(Debug, Default, Deserialize, Serialize, Clone, Copy, Hash, PartialEq, Eq)]
pub struct KeepJobs {
    /// Maximum age in **seconds** for a job record to be kept.
    pub age: Option<i64>,
    /// Maximum number of job records to keep.
    pub count: Option<i64>,
}
/// A single stack-trace entry captured when a job fails.
#[derive(Debug, Default, Deserialize, Serialize, Clone, Hash, PartialEq, Eq)]
pub struct Trace {
    /// The run (attempt) number on which this trace was captured.
    pub run: u64,
    /// Human-readable failure reason.
    pub reason: String,
    /// Stack frames collected at the point of failure.
    pub frames: Vec<String>,
}
/// Details recorded when a job permanently fails.
#[derive(Debug, Default, Deserialize, Serialize, Clone, Hash, PartialEq, Eq)]
pub struct FailedDetails {
    /// The run (attempt) number on which the job finally failed.
    pub run: u64,
    /// Human-readable failure reason.
    pub reason: String,
}
use chrono::serde::{ts_microseconds, ts_microseconds_option};
use derive_more::Debug;
/// A unit of work managed by a [`Queue`].
///
/// Jobs are created by [`Queue::add_job`] / [`Queue::bulk_add`] and passed to
/// your processor function by the [`crate::Worker`].
///
/// The type parameters `D`, `R`, and `P` represent the job's input data,
/// return value, and progress type respectively.
///
/// `data`, `returned_value`, and `progress` are omitted from `Debug` output
/// to avoid accidentally logging sensitive payloads.
#[derive(Debug, Serialize, Deserialize, Default, Hash, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Job<D, R, P> {
    /// Unique numeric ID assigned by the store on insertion.
    pub id: Option<u64>,
    /// Timestamp of when the job was created (microsecond precision).
    #[serde(rename = "timestamp", alias = "timestamp")]
    #[serde(with = "ts_microseconds")]
    pub ts: Dt,
    /// Human-readable label for the job (does not need to be unique).
    pub name: String,
    /// Current lifecycle state of the job.
    pub state: JobState,
    /// Most recent progress value reported by the processor.
    #[debug(skip)]
    pub progress: Option<P>,
    /// Number of times the processor has been invoked for this job so far.
    pub attempts_made: u64,
    /// Options that were applied when the job was enqueued.
    pub opts: JobOptions,
    /// Configured delay in milliseconds (0 for non-delayed jobs).
    pub delay: u64,
    /// Input payload passed to the processor function.
    #[debug(skip)]
    pub data: Option<D>,
    /// Value returned by the processor on successful completion.
    #[debug(skip)]
    pub returned_value: Option<R>,
    /// Stack traces captured on each failed attempt.
    pub stack_trace: Vec<Trace>,
    /// Failure details from the last (terminal) failed attempt.
    pub failed_reason: Option<FailedDetails>,
    /// Timestamp of when a worker began processing this job.
    #[serde(with = "ts_microseconds_option")]
    pub processed_on: Option<Dt>,
    /// Timestamp of when the job reached a terminal state.
    #[serde(with = "ts_microseconds_option")]
    pub finished_on: Option<Dt>,
    /// Name of the queue this job belongs to.
    pub queue_name: Option<String>,
    /// Worker lock token; set while a worker holds the lock on this job.
    pub token: Option<JobToken>,
    /// Number of times this job has been moved to the stalled state.
    pub stalled_counter: u64,
    /// Arbitrary log lines appended during processing.
    pub logs: Vec<String>,
    /// Scheduling priority (lower value = higher priority).
    pub priority: u64,
}
#[cfg(feature = "redis-store")]
impl FromRedisValue for JobState {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let mut bytes: Vec<u8> = Vec::from_redis_value(v)?;
        let state = Self::from_str(&String::from_utf8(bytes.clone())?)
            .or_else(|_| simd_json::from_slice(&mut bytes))
            .map_err(std::io::Error::other)?;

        Ok(state)
    }
}

use uuid::Uuid;

/// An opaque lock token that identifies a worker's ownership of a job.
///
/// Tokens are issued when a worker picks up a job and must be presented when
/// completing, failing, or extending the lock on that job.  A token mismatch
/// means the lock has been taken over by another worker or has expired.
#[derive(
    Debug,
    derive_more::Display,
    Clone,
    Copy,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
#[display("{_0}-{_1}-{_2}")]
pub struct JobToken(pub Uuid, pub Uuid, pub u64);
#[cfg(feature = "redis-store")]
impl FromRedisValue for JobToken {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let mut bytes: Vec<u8> = Vec::from_redis_value(v)?;
        if bytes == b"null" {
            return Err(std::io::Error::other("null passed").into());
        }
        let token = simd_json::from_slice(&mut bytes)
            .map_err(|_| std::io::Error::other("failed to parse"))?;
        Ok(token)
    }
}

impl Default for JobToken {
    fn default() -> Self {
        Self(Uuid::new_v4(), Uuid::new_v4(), Default::default())
    }
}
#[cfg(feature = "redis-store")]
impl ToRedisArgs for JobToken {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        out.write_arg_fmt(simd_json::to_string(self).unwrap_or_default());
    }
}
// skip comparing the data,progress and return_value field;
impl<D, R, P> Job<D, R, P> {
    /// Boxes this job on the heap, returning `Box<Job<D, R, P>>`.
    pub fn boxed(self) -> Box<Self> {
        Box::new(self)
    }
    /// Constructs a new `Job` with default options and no assigned ID.
    ///
    /// Prefer using [`Queue::add_job`](crate::Queue::add_job) to create jobs
    /// in normal usage; this constructor is primarily for testing and internal use.
    pub fn new(name: &str, data: Option<D>, id: Option<u64>, queue_name: Option<&str>) -> Self {
        let ts = Utc::now();

        Self {
            opts: JobOptions::default(),
            queue_name: queue_name.map(std::borrow::ToOwned::to_owned),
            name: name.to_owned(),
            id,
            ts,
            data,
            returned_value: None,
            progress: None,
            processed_on: None,
            finished_on: None,
            state: JobState::default(),
            delay: 0,
            attempts_made: 0,
            stack_trace: vec![],
            failed_reason: None,
            token: None,
            stalled_counter: 0,
            logs: Vec::new(),
            priority: 0,
        }
    }
    /// Returns timing and attempt statistics for this job, if it has been
    /// processed.
    ///
    /// The returned [`JobMetrics`] captures when the job ran, for how long,
    /// and the number of attempts made.  Returns `None` if the job has not
    /// been processed yet (i.e. `processed_on` and `finished_on` are not set).
    pub fn get_metrics(&self) -> Option<JobMetrics> {
        let delay = self.opts.delay.as_diff_ms(self.ts).cast_unsigned();
        let processed_on = self.processed_on.unwrap_or_default();
        let id = self.id.unwrap_or_default();
        let finished_on = self.finished_on.unwrap_or_default();
        let attempt = self.attempts_made;
        let ran_for = (finished_on - processed_on).to_std().unwrap_or_default();
        let delayed_for = (processed_on - self.ts).to_std().unwrap_or_default();
        Some(JobMetrics {
            ran_for,
            delayed_for,
            attempt,
            delay,
            id,
        })
    }

    /// Applies the given [`JobOptions`] to this job, updating priority, delay,
    /// and other scheduling fields.
    pub fn add_opts(&mut self, opts: JobOptions) {
        self.priority = opts.priority;
        self.delay = opts.delay.as_diff_ms(self.ts).cast_unsigned();
        self.opts = opts;
    }
    /// Updates the job's progress value and persists it to the store.
    ///
    /// Call this from inside your processor function to report incremental
    /// progress.  Listeners subscribed to [`JobState::Progress`] events will
    /// receive the update.
    ///
    /// # Errors
    ///
    /// Returns [`KioError`](crate::KioError) if the store update fails.
    pub fn update_progress<C>(&mut self, value: P, store: &C) -> Result<(), KioError>
    where
        P: Serialize + Clone,
        C: Store<D, R, P>,
    {
        store.update_job_progress(self, value)
    }
}
#[cfg(feature = "redis-store")]
impl<D, R, P> FromRedisValue for Job<D, R, P>
where
    D: for<'de> Deserialize<'de>, // D, R, P must be deserializable
    R: for<'de> Deserialize<'de>, // and have a Default if they are Optional in Rust
    P: for<'de> Deserialize<'de>,
{
    fn from_redis_value(v: &Value) -> redis::RedisResult<Self> {
        use std::io::Error;
        let other = Error::other;
        let mut job: Self = Self::new("", None, None, None);
        let map = v
            .as_map_iter()
            .ok_or_else(|| std::io::Error::other("failed to extract map"))?;
        for (key, value) in map {
            if let (Value::BulkString(key), Value::BulkString(bytes)) = (key, value) {
                let mut bytes = bytes.clone();
                match key.as_slice() {
                    b"id" => job.id = simd_json::from_slice(&mut bytes).map_err(other)?,
                    b"timestamp" => {
                        job.ts = simd_json::from_slice::<Option<u64>>(&mut bytes)
                            .map_err(other)?
                            .and_then(|t| Dt::from_timestamp_micros(t.cast_signed()))
                            .unwrap_or_default();
                    }
                    b"opts" => job.opts = simd_json::from_slice(&mut bytes).map_err(other)?,
                    b"name" => job.name = simd_json::from_slice(&mut bytes).map_err(other)?,
                    b"queuename" | b"queueName" => {
                        job.queue_name = simd_json::from_slice(&mut bytes).map_err(other)?;
                    }
                    b"state" => job.state = JobState::from_redis_value(value)?,
                    b"token" => {
                        job.token = simd_json::from_slice(&mut bytes).unwrap_or_default();
                    }
                    b"progress" => {
                        job.progress = simd_json::from_slice(&mut bytes).map_err(other)?;
                    }
                    b"attemptsmade" | b"attemptsMade" => {
                        job.attempts_made = simd_json::from_slice(&mut bytes).map_err(other)?;
                    }
                    b"delay" => job.delay = simd_json::from_slice(&mut bytes).map_err(other)?,
                    b"priority" => {
                        job.priority = simd_json::from_slice(&mut bytes).map_err(other)?;
                    }
                    b"data" => job.data = simd_json::from_slice(&mut bytes).map_err(other)?,
                    b"returnedvalue" | b"returnedValue" => {
                        job.returned_value = simd_json::from_slice(&mut bytes).map_err(other)?;
                    }
                    b"stacktrace" | b"stackTrace" => {
                        job.stack_trace = simd_json::from_slice(&mut bytes).map_err(other)?;
                    }
                    b"logs" => job.logs = simd_json::from_slice(&mut bytes).map_err(other)?,
                    b"failedreason" | b"failedReason" => {
                        job.failed_reason = simd_json::from_slice(&mut bytes).map_err(other)?;
                    }
                    b"processedon" | b"processedOn" => {
                        job.processed_on = simd_json::from_slice::<Option<u64>>(&mut bytes)
                            .map_err(other)?
                            .and_then(|t| Dt::from_timestamp_micros(t.cast_signed()));
                    } // Assuming Dt is handled by simd_json
                    b"finishedon" | b"finishedOn" => {
                        job.finished_on = simd_json::from_slice::<Option<u64>>(&mut bytes)
                            .map_err(other)?
                            .and_then(|t| Dt::from_timestamp_micros(t.cast_signed()));
                    }
                    b"stalledcounter" | b"stalledCounter" => {
                        job.stalled_counter = simd_json::from_slice(&mut bytes).map_err(other)?;
                    }
                    _ => { /* Ignore unknown fields if your hash might contain others */ }
                }
            }
        }
        Ok(job)
    }
}
