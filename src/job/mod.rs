use std::{collections::HashMap, error::Error, fmt::format, str::FromStr};

use chrono::{DateTime, Utc};
use deadpool_redis::redis::ToRedisArgs;
use deadpool_redis::Connection;
use derive_more::{Display, FromStr};
use redis::{
    from_redis_value, AsyncCommands, ConnectionLike, FromRedisValue, Pipeline, RedisError,
    RedisResult, Value,
};
use serde::{
    de::{self, DeserializeOwned},
    Deserialize, Serialize,
};

mod backoff;
use crate::{events::QueueStreamEvent, queue::Queue, CollectionSuffix, KioError, KioResult};
pub use backoff::{BackOff, BackOffJobOptions, BackOffOptions, StoredFn};
/// alias for DateTime<Utc>
pub(crate) type Dt = DateTime<Utc>;
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
    Priorized,
    Stalled,
    Active,
    Paused,
    Resumed,
    Completed,
    Failed,
    Delayed,
    Progress,
    Processing,
}
impl ToRedisArgs for JobState {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + deadpool_redis::redis::RedisWrite,
    {
        out.write_arg_fmt(self.to_string().to_lowercase());
    }
}
#[derive(Debug, Serialize, Deserialize, Default, Hash, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct JobOptions {
    pub priority: u64,
    pub delay: u64,
    pub id: Option<u64>,
    pub attempts: u64,
    /// total number of attempts to try the job until it completes.
    pub remove_on_complete: Option<RemoveOnCompletionOrFailure>,
    pub remove_on_fail: Option<RemoveOnCompletionOrFailure>,
    pub backoff: Option<BackOffJobOptions>,
}

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
#[derive(Debug, Default, Deserialize, Serialize, Clone, Copy, Hash, PartialEq)]
pub struct KeepJobs {
    pub age: Option<i64>,   // Maximum age in seconds for jobs to kept;
    pub count: Option<i64>, // Maximum Number of jobs to keep
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
    pub fn boxed(self) -> Box<Self> {
        Box::new(self)
    }
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

    pub fn add_opts(&mut self, opts: JobOptions) {
        self.priority = opts.priority;
        self.delay = opts.delay;
        self.opts = opts;
    }
    pub async fn update_progress(&mut self, value: P, conn: &mut Connection) -> Result<(), KioError>
    where
        P: Serialize + Clone,
    {
        use crate::QueueEventMode;
        if let (Some(queue_name), Some(id)) = (&self.queue_name, &self.id) {
            let job_key = format!("{queue_name}:{id}");
            let mut pipeline = redis::pipe();
            pipeline.atomic();
            let progress_str = simd_json::to_string_pretty(&value)?;
            let events_stream_key = format!("{queue_name}:events");
            pipeline.hset(job_key, "progress", &progress_str);
            // check for the queue_event_mode
            let queue_meta_key = format!("{queue_name}:meta");
            let event_mode: Option<QueueEventMode> =
                conn.hget(&queue_meta_key, "event_mode").await?;
            let mode = event_mode.unwrap_or_default();
            match mode {
                QueueEventMode::PubSub => {
                    let event = QueueStreamEvent::<(), P> {
                        job_id: *id,
                        event: JobState::Progress,
                        name: Some(self.name.to_owned()),
                        progress_data: Some(value.clone()),
                        ..Default::default()
                    };
                    pipeline.publish(&events_stream_key, event);
                }
                QueueEventMode::Stream => {
                    let items = [
                        ("event", JobState::Progress.to_string().to_lowercase()),
                        ("job_id", id.to_string()),
                        ("data", progress_str),
                        ("name", self.name.to_string()),
                    ];
                    pipeline.xadd(&events_stream_key, "*", &items);
                }
            }
            let _: redis::Value = pipeline.query_async(conn).await?;
            self.progress = Some(value);
        }
        Ok(())
    }
    /// Append log to existing_logs if there is any
    pub async fn add_log(&mut self, log: &str, conn: &mut Connection) -> KioResult<()> {
        if let (Some(queue_name), Some(id)) = (&self.queue_name, &self.id) {
            let job_key = format!("{queue_name}:{id}");
            let mut existing_logs: Vec<String> = conn.hget(&job_key, "logs").await?;
            // append the next log;
            let date_time = Utc::now();
            let log_with_dt = format!("{date_time}: {log}");
            existing_logs.push(log_with_dt.clone());
            self.logs.push(log_with_dt);
            let v: redis::Value = conn.hset(job_key, "logs", existing_logs).await?;
        }

        Ok(())
    }
}
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
                    b"state" => {
                        job.state = JobState::from_str(&String::from_utf8(bytes.to_vec())?)
                            .or(simd_json::from_slice(&mut bytes))
                            .map_err(std::io::Error::other)?
                    }
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
