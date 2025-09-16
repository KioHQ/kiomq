use serde::de::{value, DeserializeOwned};

use crate::{JobState, KioError, KioResult};
use derive_more::Debug;
use std::str::FromStr;
#[derive(Debug, Hash, Clone)]
pub struct QueueStreamEvent<R, P> {
    pub id: String,
    pub priority: Option<u64>,
    pub event: JobState,
    pub delay: Option<u64>,
    pub prev: Option<JobState>,
    pub job_id: String,
    #[debug(skip)]
    pub retuned_value: Option<R>,
    pub failed_reason: Option<String>,
    #[debug(skip)]
    pub progress_data: Option<P>,
    pub name: Option<String>,
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
        }
    }
}
use redis::{
    streams::{StreamId, StreamReadReply},
    FromRedisValue,
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
            id: value.id.to_string(),
            ..Default::default()
        };
        for (key, val) in &value.map {
            let val_str = String::from_redis_value(val)?;
            match key.to_lowercase().as_str() {
                "job_id" => event.job_id = val_str,
                "name" => event.name = Some(val_str),
                "delay" => event.delay = serde_json::from_str(&val_str)?,
                "priority" => event.priority = serde_json::from_str(&val_str)?,
                "data" => event.progress_data = serde_json::from_str(&val_str)?,

                "returnedvalue" => event.retuned_value = serde_json::from_str(&val_str)?,
                "failedreason" => event.failed_reason = Some(val_str),

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
