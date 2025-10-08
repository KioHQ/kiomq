use crate::Dt;
use crate::{FailedDetails, JobState, KioError, KioResult};
use chrono::Utc;
use derive_more::Debug;
use serde::{
    de::{value, DeserializeOwned},
    Deserialize, Serialize,
};
use std::str::FromStr;
use std::time::Instant;
use uuid::Uuid;
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash, Debug)]
pub struct StreamEventId(pub Dt, pub u64);
impl StreamEventId {
    pub fn from_timestamp_millis(ts: i64) -> Self {
        let dt = Dt::from_timestamp_millis(ts).unwrap_or_else(Utc::now);
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
        out.write_arg_fmt(simd_json::to_string_pretty(self).unwrap_or_default());
    }
}

impl<R: DeserializeOwned, P: DeserializeOwned> FromRedisValue for QueueStreamEvent<R, P> {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let mut msg: Vec<u8> = Vec::from_redis_value(v)?;
        let value = simd_json::from_slice(&mut msg).map_err(std::io::Error::other)?;
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
    pub fn from_stream_read_reply(events_key: &str, mut reply: StreamReadReply) -> Vec<Self> {
        if let Some(keyed_events) = reply.keys.iter_mut().find(|event| event.key == events_key) {
            let events = keyed_events
                .ids
                .iter_mut()
                .flat_map(Self::try_from)
                .collect();
            return events;
        }
        vec![]
    }
}
impl<R: DeserializeOwned, P: DeserializeOwned> TryFrom<&mut StreamId> for QueueStreamEvent<R, P> {
    type Error = KioError;

    fn try_from(value: &mut StreamId) -> Result<Self, Self::Error> {
        use std::io::Error;
        let mut event = Self {
            id: value.id.parse()?,
            ..Default::default()
        };
        for (key, val) in value.map.iter_mut() {
            if let redis::Value::BulkString(bytes) = val {
                match key.to_lowercase().as_str() {
                    "job_id" => event.job_id = u64::from_redis_value(val)?,
                    "name" => event.name = Option::from_redis_value(val)?,
                    "delay" => event.delay = Option::from_redis_value(val)?,
                    "worker_id" => {
                        event.worker_id = simd_json::from_slice(bytes).map_err(Error::other)?;
                    }
                    "priority" => event.priority = Option::from_redis_value(val)?,
                    "data" => event.progress_data = simd_json::from_slice(bytes)?,

                    "returnedvalue" => event.retuned_value = simd_json::from_slice(bytes)?,
                    "failedreason" => event.failed_reason = simd_json::from_slice(bytes)?,

                    "event" => {
                        let parsed = JobState::from_redis_value(val)?;
                        event.event = parsed;
                    }
                    "prev" => event.prev = Option::from_redis_value(val)?,

                    _ => { /* Ignore unknown fields if your hash might contain others */ }
                }
            }
        }

        Ok(event)
    }
}
