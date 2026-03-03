use std::{collections::HashMap, error::Error, fmt::format, str::FromStr};

use chrono::{DateTime, Utc};
#[cfg(feature = "redis-store")]
use deadpool_redis::{redis::ToRedisArgs, Connection};
use derive_more::{Display, FromStr};
#[cfg(feature = "redis-store")]
use redis::{
    from_redis_value, AsyncCommands, ConnectionLike, FromRedisValue, Pipeline, RedisError,
    RedisResult, Value,
};
use serde::{
    de::{self, DeserializeOwned},
    Deserialize, Serialize,
};

use crate::stores::Store;
mod backoff;
mod delay;
mod repeat;
use crate::{
    events::QueueStreamEvent, job::delay::JobDelay, queue::Queue, CollectionSuffix, KioError,
    KioResult,
};
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
    pub ran_for: Duration,
    pub delayed_for: Duration,
    pub attempt: u64,
    pub delay: u64,
    pub id: u64,
}

/// alias for DateTime<Utc>
pub(crate) type Dt = DateTime<Utc>;
/// The lifecycle state of a job within the queue.
///
/// Jobs typically flow through: `Wait` → `Active` → `Completed` or `Failed`.
/// Other transitions exist for priority queuing, delays, pausing, and stall
/// detection.
///
/// | Variant | Description |
/// |---------|-------------|
/// | `Wait` | Ready to be picked up by a worker (default). |
/// | `Prioritized` | In the priority sorted-set, waiting for a slot. |
/// | `Stalled` | Worker that held the lock disappeared; pending recovery. |
/// | `Active` | Currently being processed by a worker. |
/// | `Paused` | Queue is paused; job is waiting in the paused list. |
/// | `Resumed` | Queue has resumed; job transitions back to wait. |
/// | `Completed` | Processor returned successfully. |
/// | `Failed` | Processor returned an error (or stalled too many times). |
/// | `Delayed` | Scheduled to run at a future timestamp. |
/// | `Progress` | A progress update event (not a persistent state). |
/// | `Obliterated` | Queue was obliterated; job was deleted. |
/// | `Processing` | Worker has started executing the processor function. |
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
    #[default]
    Wait,
    Prioritized,
    Stalled,
    Active,
    Paused,
    Resumed,
    Completed,
    Failed,
    Delayed,
    Progress,
    Obliterated,
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
    /// total number of attempts to try the job until it completes.
    pub remove_on_complete: Option<RemoveOnCompletionOrFailure>,
    pub remove_on_fail: Option<RemoveOnCompletionOrFailure>,
    pub backoff: Option<BackOffJobOptions>,
    pub repeat: Option<Repeat>,
}

/// Determines whether—and how many—jobs are retained after they finish.
///
/// | Variant | Behaviour |
/// |---------|-----------|
/// | `Bool(true)` | Remove the job immediately. |
/// | `Bool(false)` | Keep the job forever (default). |
/// | `Int(n)` | Keep at most `n` jobs; older ones are removed when the count is exceeded. |
/// | `Opts(KeepJobs { age, count })` | Apply age **and/or** count retention. |
#[derive(Debug, Deserialize, Serialize, Clone, Copy, Hash, PartialEq)]
#[serde(untagged)]
pub enum RemoveOnCompletionOrFailure {
    Bool(bool), // if true, remove the job when it completes
    Int(i64),   //  number is passed, its specifies the maximum amount of jobs to keeps
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
#[derive(Debug, Default, Deserialize, Serialize, Clone, Copy, Hash, PartialEq)]
pub struct KeepJobs {
    /// Maximum age in **seconds** for a job record to be kept.
    pub age: Option<i64>,
    /// Maximum number of job records to keep.
    pub count: Option<i64>,
}
#[derive(Debug, Default, Deserialize, Serialize, Clone, Hash, PartialEq)]
pub struct Trace {
    pub run: u64,
    pub reason: String,
    pub frames: Vec<String>,
}
#[derive(Debug, Default, Deserialize, Serialize, Clone, Hash, PartialEq)]
pub struct FailedDetails {
    pub run: u64,
    pub reason: String,
}
use chrono::serde::{ts_microseconds, ts_microseconds_option};
use derive_more::Debug;
/// A unit of work managed by a [`Queue`].
///
/// Jobs are created by [`Queue::add_job`] / [`Queue::bulk_add`] and passed to
/// your processor function by the [`crate::Worker`].
///
/// # Type parameters
///
/// | Parameter | Description |
/// |-----------|-------------|
/// | `D` | Input data type |
/// | `R` | Return / result type |
/// | `P` | Progress update type |
///
/// # Note on data access
///
/// `data`, `returned_value`, and `progress` are skipped in `Debug` output
/// to avoid accidentally logging sensitive payloads.
#[derive(Debug, Serialize, Deserialize, Default, Hash, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Job<D, R, P> {
    pub id: Option<u64>,
    #[serde(rename = "timestamp", alias = "timestamp")]
    #[serde(with = "ts_microseconds")]
    pub ts: Dt,
    pub name: String,
    pub state: JobState,
    #[debug(skip)]
    pub progress: Option<P>,
    pub attempts_made: u64,
    pub opts: JobOptions,
    pub delay: u64,
    #[debug(skip)]
    pub data: Option<D>,
    #[debug(skip)]
    pub returned_value: Option<R>,
    pub stack_trace: Vec<Trace>,
    pub failed_reason: Option<FailedDetails>,
    #[serde(with = "ts_microseconds_option")]
    pub processed_on: Option<Dt>,
    #[serde(with = "ts_microseconds_option")]
    pub finished_on: Option<Dt>,
    pub queue_name: Option<String>,
    pub token: Option<JobToken>, // job_lock token
    pub stalled_counter: u64,
    pub logs: Vec<String>,
    pub priority: u64,
}
#[cfg(feature = "redis-store")]
impl FromRedisValue for JobState {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let mut bytes: Vec<u8> = Vec::from_redis_value(v)?;
        let state = JobState::from_str(&String::from_utf8(bytes.clone())?)
            .or_else(|_| simd_json::from_slice(&mut bytes))
            .map_err(std::io::Error::other)?;

        Ok(state)
    }
}

use uuid::Uuid;

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
            queue_name: queue_name.map(|s| s.to_owned()),
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
        let delay = self.opts.delay.as_diff_ms(self.ts) as u64;
        let processed_on = self.processed_on.unwrap_or_default();
        let id = self.id.unwrap_or_default();
        let finished_on = self.finished_on.unwrap_or_default();
        let attempt = self.attempts_made;
        let ran_for = (finished_on - processed_on).to_std().unwrap_or_default();
        let delayed_for = (processed_on - self.ts).to_std().unwrap_or_default();
        Some(JobMetrics {
            delay,
            id,
            ran_for,
            delayed_for,
            attempt,
        })
    }

    /// Applies the given [`JobOptions`] to this job, updating priority, delay,
    /// and other scheduling fields.
    pub fn add_opts(&mut self, opts: JobOptions) {
        self.priority = opts.priority;
        self.delay = opts.delay.as_diff_ms(self.ts) as u64;
        self.opts = opts.clone();
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
        let mut job: Job<D, R, P> = Job::new("", None, None, None);
        let mut map = v
            .as_map_iter()
            .ok_or(std::io::Error::other("failed to extract map"))?;
        for (key, value) in map {
            if let (Value::BulkString(key), Value::BulkString(bytes)) = (key, value) {
                let mut bytes = bytes.to_vec();
                match key.as_slice() {
                    b"id" => job.id = simd_json::from_slice(&mut bytes).map_err(other)?,
                    b"timestamp" => {
                        job.ts = simd_json::from_slice::<Option<u64>>(&mut bytes)
                            .map_err(other)?
                            .and_then(|t| Dt::from_timestamp_micros(t as i64))
                            .unwrap_or_default();
                    }
                    b"opts" => job.opts = simd_json::from_slice(&mut bytes).map_err(other)?,
                    b"name" => job.name = simd_json::from_slice(&mut bytes).map_err(other)?,
                    b"queuename" | b"queueName" => {
                        job.queue_name = simd_json::from_slice(&mut bytes).map_err(other)?
                    }
                    b"state" => job.state = JobState::from_redis_value(value)?,
                    b"token" => {
                        job.token = simd_json::from_slice(&mut bytes).unwrap_or_default();
                    }
                    b"progress" => {
                        job.progress = simd_json::from_slice(&mut bytes).map_err(other)?
                    }
                    b"attemptsmade" | b"attemptsMade" => {
                        job.attempts_made = simd_json::from_slice(&mut bytes).map_err(other)?
                    }
                    b"delay" => job.delay = simd_json::from_slice(&mut bytes).map_err(other)?,
                    b"priority" => {
                        job.priority = simd_json::from_slice(&mut bytes).map_err(other)?
                    }
                    b"data" => job.data = simd_json::from_slice(&mut bytes).map_err(other)?,
                    b"returnedvalue" | b"returnedValue" => {
                        job.returned_value = simd_json::from_slice(&mut bytes).map_err(other)?
                    }
                    b"stacktrace" | b"stackTrace" => {
                        job.stack_trace = simd_json::from_slice(&mut bytes).map_err(other)?
                    }
                    b"logs" => job.logs = simd_json::from_slice(&mut bytes).map_err(other)?,
                    b"failedreason" | b"failedReason" => {
                        job.failed_reason = simd_json::from_slice(&mut bytes).map_err(other)?;
                    }
                    b"processedon" | b"processedOn" => {
                        job.processed_on = simd_json::from_slice::<Option<u64>>(&mut bytes)
                            .map_err(other)?
                            .and_then(|t| Dt::from_timestamp_micros(t as i64))
                    } // Assuming Dt is handled by simd_json
                    b"finishedon" | b"finishedOn" => {
                        job.finished_on = simd_json::from_slice::<Option<u64>>(&mut bytes)
                            .map_err(other)?
                            .and_then(|t| Dt::from_timestamp_micros(t as i64))
                    }
                    b"stalledcounter" | b"stalledCounter" => {
                        job.stalled_counter = simd_json::from_slice(&mut bytes).map_err(other)?
                    }
                    _ => { /* Ignore unknown fields if your hash might contain others */ }
                }
            }
        }
        Ok(job)
    }
}
