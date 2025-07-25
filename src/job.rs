use std::{collections::HashMap, error::Error, fmt::format, str::FromStr};

use chrono::{DateTime, Utc};
use deadpool_redis::redis::ToRedisArgs;
use derive_more::{Display, FromStr};
use redis::{
    from_redis_value, AsyncCommands, ConnectionLike, FromRedisValue, Pipeline, RedisResult, Value,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_redis::RedisDeserialize;

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
    pub id: Option<u64>,
    #[serde(rename = "timestamp", alias = "timestamp")]
    pub ts: i64,
    pub name: String,
    pub state: JobState,
    pub progress: Option<P>,
    pub attempts_made: u64,
    pub delay: u64,
    pub data: Option<D>,
    pub return_value: Option<R>,
    pub stack_trace: Vec<String>,
    pub failed_reason: Option<String>,
    pub processed_on: Option<Dt>,
    pub finished_on: Option<Dt>,
    pub queue_name: Option<String>,
}

// skip comparing the data,progress and return_value field;
impl<D, R, P> Job<D, R, P> {
    pub fn new(name: &str, data: Option<D>, id: Option<u64>, queue_name: Option<&str>) -> Self {
        let ts = Utc::now().timestamp();

        Self {
            queue_name: queue_name.map(|s| s.to_owned()),
            name: name.to_lowercase(),
            id,
            ts,
            data,
            return_value: None,
            progress: None,
            processed_on: None,
            finished_on: None,
            state: JobState::default(),
            delay: 0,
            attempts_made: 0,
            stack_trace: vec![],
            failed_reason: None,
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
        if let (Some(queue_name), Some(id)) = (&self.queue_name, self.id) {
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
            //dbg!(&key, &value);
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
                "progress" => job.progress = serde_json::from_str(value)?,
                "attemptsmade" => job.attempts_made = serde_json::from_str(value)?,
                "delay" => job.delay = serde_json::from_str(value)?,
                "data" => job.data = serde_json::from_str(value)?,
                "returnvalue" => job.return_value = serde_json::from_str(value)?,
                "stacktrace" => job.stack_trace = serde_json::from_str(value)?,
                "failedreason" => job.failed_reason = serde_json::from_str(value)?,
                "processedon" => job.processed_on = serde_json::from_str(value)?, // Assuming Dt is handled by serde_json
                "finishedon" => job.finished_on = serde_json::from_str(value)?,
                _ => { /* Ignore unknown fields if your hash might contain others */ }
            }
        }

        Ok(job)
    }
}
