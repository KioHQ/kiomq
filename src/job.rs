use std::{collections::HashMap, error::Error, fmt::format, str::FromStr};

use chrono::{DateTime, Utc};
use deadpool_redis::redis::ToRedisArgs;
use derive_more::{Display, FromStr};
use redis::{
    from_redis_value, AsyncCommands, ConnectionLike, FromRedisValue, Pipeline, RedisResult, Value,
};
use serde::{
    de::{self, DeserializeOwned},
    Deserialize, Serialize,
};

use crate::{queue::Queue, CollectionSuffix, KioError};
/// alias for DateTime<Utc>
pub(crate) type Dt = DateTime<Utc>;
#[derive(
    Debug, Serialize, Deserialize, FromStr, Default, Hash, Display, Clone, Copy, PartialEq,
)]
#[serde(rename_all = "camelCase")]
pub enum JobState {
    #[default]
    Wait,
    Stalled,
    Active,
    Paused,
    Resumed,
    Completed,
    Failed,
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
pub struct Job<D, R, P> {
    pub id: Option<String>,
    #[serde(rename = "timestamp", alias = "timestamp")]
    pub ts: i64,
    pub name: String,
    pub state: JobState,
    pub progress: Option<P>,
    pub attempts_made: u64,
    pub delay: u64,
    pub data: Option<D>,
    pub returned_value: Option<R>,
    pub stack_trace: Vec<String>,
    pub failed_reason: Option<String>,
    pub processed_on: Option<u64>,
    pub finished_on: Option<u64>,
    pub queue_name: Option<String>,
    pub token: Option<String>, // job_lock token
    pub stalled_counter: u64,
}

// skip comparing the data,progress and return_value field;
impl<D, R, P> Job<D, R, P> {
    pub fn new(name: &str, data: Option<D>, id: Option<u64>, queue_name: Option<&str>) -> Self {
        let ts = Utc::now().timestamp_millis();
        let id = id.map(|v| v.to_string());

        Self {
            queue_name: queue_name.map(|s| s.to_owned()),
            name: name.to_lowercase(),
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
        }
    }
    pub async fn update_progress<C: redis::aio::ConnectionLike>(
        &mut self,
        value: P,
        mut con: C,
    ) -> Result<(), KioError>
    where
        P: Serialize,
    {
        if let (Some(queue_name), Some(id)) = (&self.queue_name, &self.id) {
            let job_key = format!("{queue_name}:{id}");
            let mut pipeline = redis::pipe();
            let job_str = serde_json::to_string_pretty(&value)?;
            pipeline.hset(job_key, "progress", job_str);

            let result = pipeline.query_async::<redis::Value>(&mut con).await?;
            self.progress = Some(value);
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
        let mut job: Job<D, R, P> = Job::new("", None, None, None);
        let map = HashMap::<String, String>::from_redis_value(v)?;
        for (key, value) in map.iter() {
            match key.to_lowercase().as_str() {
                "id" => job.id = serde_json::from_str(value)?,
                "timestamp" => job.ts = serde_json::from_str(value)?,
                "name" => job.name = serde_json::from_str(value)?,
                "queuename" => job.queue_name = serde_json::from_str(value)?,
                "state" => {
                    job.state = JobState::from_str(value)
                        .or(serde_json::from_str(value))
                        .map_err(std::io::Error::other)?
                }
                "token" => {
                    job.token =
                        serde_json::from_str(value).unwrap_or_else(|_| Some(value.to_owned()));
                }
                "progress" => job.progress = serde_json::from_str(value)?,
                "attemptsmade" => job.attempts_made = serde_json::from_str(value)?,
                "delay" => job.delay = serde_json::from_str(value)?,
                "data" => job.data = serde_json::from_str(value)?,
                "returnedvalue" => job.returned_value = serde_json::from_str(value)?,
                "stacktrace" => job.stack_trace = serde_json::from_str(value)?,
                "failedreason" => {
                    job.failed_reason =
                        serde_json::from_str(value).unwrap_or_else(|_| Some(value.to_owned()));
                }
                "processedon" => job.processed_on = serde_json::from_str(value)?, // Assuming Dt is handled by serde_json
                "finishedon" => job.finished_on = serde_json::from_str(value)?,
                "stalledcounter" => job.stalled_counter = serde_json::from_str(value)?,
                _ => { /* Ignore unknown fields if your hash might contain others */ }
            }
        }

        Ok(job)
    }
}
