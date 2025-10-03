use crate::Dt;
use crate::{FailedDetails, JobState, KioError, KioResult};
use chrono::Utc;
use derive_more::Debug;
use serde::{
    de::{value, DeserializeOwned},
    Deserialize, Serialize,
};
use std::str::FromStr;
use uuid::Uuid;
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash, Debug)]
pub struct StreamEventId(pub Dt, pub u64);
impl StreamEventId {
    pub fn from_timestamp_millis(ts: i64) -> Self {
        let dt = Dt::from_timestamp_millis(ts).unwrap_or_default();
        Self(dt, 0)
    }
}
impl FromStr for StreamEventId {
    type Err = KioError;
    fn from_str(id: &str) -> KioResult<Self> {
        let (ts, num) = id
            .split_once('-')
            .ok_or(KioError::IoError(std::io::Error::other(
                "invalid string format, expect timestamp-number",
            )))?;
        let dt = Dt::from_timestamp_millis(ts.parse()?).ok_or(KioError::IoError(
            std::io::Error::other("invalid timestamp format"),
        ))?;
        let id = num.parse()?;

        Ok(Self(dt, id))
    }
}
impl Default for StreamEventId {
    fn default() -> Self {
        Self(Utc::now(), Default::default())
    }
}

#[derive(Debug, Hash, Clone, Serialize, Deserialize)]
pub struct QueueStreamEvent<R, P> {
    pub id: StreamEventId,
    pub priority: Option<u64>,
    pub event: JobState,
    pub delay: Option<u64>,
    pub prev: Option<JobState>,
    pub job_id: u64,
    #[debug(skip)]
    pub retuned_value: Option<R>,
    pub failed_reason: Option<FailedDetails>,
    #[debug(skip)]
    pub progress_data: Option<P>,
    pub name: Option<String>,
    pub worker_id: Option<Uuid>,
}
impl<R: Serialize, P: Serialize> ToRedisArgs for QueueStreamEvent<R, P> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        out.write_arg_fmt(serde_json::to_string_pretty(self).unwrap_or_default());
    }
}

impl<R: DeserializeOwned, P: DeserializeOwned> FromRedisValue for QueueStreamEvent<R, P> {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let msg = String::from_redis_value(v)?;
        let value = serde_json::from_str(&msg).map_err(std::io::Error::other)?;
        Ok(value)
    }
}
impl<R, P> Default for QueueStreamEvent<R, P> {
    fn default() -> Self {
        Self {
            failed_reason: None,
            priority: None,
            id: Default::default(),
            delay: None,
            event: Default::default(),
            prev: Default::default(),
            job_id: Default::default(),
            retuned_value: None,
            name: Default::default(),
            progress_data: None,
            worker_id: None,
        }
    }
}
use redis::{
    streams::{StreamId, StreamReadReply},
    FromRedisValue, ToRedisArgs,
};
impl<R: DeserializeOwned, P: DeserializeOwned> QueueStreamEvent<R, P> {
    pub fn from_stream_read_reply(events_key: &str, reply: StreamReadReply) -> Vec<Self> {
        if let Some(keyed_events) = reply.keys.iter().find(|event| event.key == events_key) {
            let events = keyed_events.ids.iter().flat_map(Self::try_from).collect();
            return events;
        }
        vec![]
    }
}
impl<R: DeserializeOwned, P: DeserializeOwned> TryFrom<&StreamId> for QueueStreamEvent<R, P> {
    type Error = KioError;

    fn try_from(value: &StreamId) -> Result<Self, Self::Error> {
        let mut event = Self {
            id: value.id.parse()?,
            ..Default::default()
        };
        for (key, val) in &value.map {
            let val_str = String::from_redis_value(val)?;
            match key.to_lowercase().as_str() {
                "job_id" => event.job_id = val_str.parse()?,
                "name" => event.name = Some(val_str),
                "delay" => event.delay = serde_json::from_str(&val_str)?,
                "worker_id" => event.worker_id = serde_json::from_str(&val_str)?,
                "priority" => event.priority = serde_json::from_str(&val_str)?,
                "data" => event.progress_data = serde_json::from_str(&val_str)?,

                "returnedvalue" => event.retuned_value = serde_json::from_str(&val_str)?,
                "failedreason" => event.failed_reason = serde_json::from_str(&val_str)?,

                "event" => {
                    let parsed = JobState::from_str(&val_str).map_err(std::io::Error::other)?;
                    event.event = parsed;
                }
                "prev" => event.prev = JobState::from_str(&val_str).ok(),
                _ => { /* Ignore unknown fields if your hash might contain others */ }
            }
        }
        Ok(event)
    }
}
