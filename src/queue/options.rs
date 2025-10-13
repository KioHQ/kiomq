use std::{
    default,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use crate::{
    error::QueueError, BackOffJobOptions, FailedDetails, JobState, JobToken,
    RemoveOnCompletionOrFailure, Repeat,
};
use atomig::{Atom, Atomic};
use redis::{FromRedisValue, RedisResult, ToRedisArgs, Value};
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
/// An envelope representing the result of running the worker's callback
pub enum ProcessedResult<R> {
    Failed(FailedDetails),
    Success(R),
}

use derive_more::{Debug, Display};
#[derive(Display, Serialize)]
pub(crate) enum CollectionSuffix {
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
}

impl CollectionSuffix {
    pub fn to_collection_name(&self, prefix: &str, name: &str) -> String {
        format!("{}:{}:{}", prefix, name, &self).to_lowercase()
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
            JobState::Priorized => CollectionSuffix::Prioritized,
            JobState::Processing => CollectionSuffix::Meta,
            JobState::Obliterated => CollectionSuffix::Events,
        }
    }
}

impl ToRedisArgs for CollectionSuffix {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + deadpool_redis::redis::RedisWrite,
    {
        out.write_arg_fmt(self.to_string().to_lowercase());
    }
}
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
#[derive(Debug, Clone)]
pub struct QueueOpts {
    pub remove_on_fail: Option<RemoveOnCompletionOrFailure>,
    pub remove_on_complete: Option<RemoveOnCompletionOrFailure>,
    pub attempts: u64,
    pub default_backoff: Option<BackOffJobOptions>,
    pub event_mode: Option<QueueEventMode>,
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
#[derive(Debug, Clone, Default)]
pub struct JobMetrics {
    pub last_id: Counter,
    pub processing: Counter,
    pub active: Counter,
    pub stalled: Counter,
    pub delayed: Counter,
    pub completed: Counter,
    pub waiting: Counter,
    pub paused: Arc<AtomicBool>,
    pub event_mode: Arc<Atomic<QueueEventMode>>,
}
impl JobMetrics {
    pub fn all_jobs_completed(&self) -> bool {
        let last_id = self.last_id.load(Ordering::Relaxed);
        last_id > 0
            && self.completed.load(Ordering::Relaxed) == last_id
            && self.active.load(Ordering::Relaxed) == 0
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
        waiting: u64,
        paused: bool,
        event_mode: QueueEventMode,
    ) -> Self {
        Self {
            last_id: create_counter(last_id),
            processing: create_counter(processing),
            active: create_counter(active),
            stalled: create_counter(stalled),
            completed: create_counter(completed),
            waiting: create_counter(waiting),
            delayed: create_counter(delayed),
            paused: Arc::new(paused.into()),
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
        self.waiting
            .swap(other.waiting.load(Ordering::Acquire), Ordering::AcqRel);
        self.processing
            .swap(other.processing.load(Ordering::Acquire), Ordering::AcqRel);
        self.event_mode
            .swap(other.event_mode.load(Ordering::Acquire), Ordering::AcqRel);
    }
    pub fn has_delayed(&self) -> bool {
        self.delayed.load(Ordering::Acquire) > 0
    }
    pub fn queue_has_work(&self) -> bool {
        (self.waiting.load(Ordering::Acquire) > 0
            || self.delayed.load(Ordering::Acquire) > 0
            || self.stalled.load(Ordering::Acquire) > 0)
    }
    pub fn queue_is_paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }
    pub fn workers_idle(&self) -> bool {
        self.processing.load(Ordering::Acquire) == 0
    }
    pub fn has_active_jobs(&self) -> bool {
        self.active.load(Ordering::Acquire) > 0
    }
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
