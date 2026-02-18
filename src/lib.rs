#![allow(dead_code, unused)]
mod error;
mod events;
mod job;
mod queue;
mod stores;
mod timers;
mod utils;
mod worker;
pub use async_backtrace::{frame, framed};
#[cfg(feature = "redis-store")]
pub use deadpool_redis::Config;
pub use error::*;
pub(crate) use events::EventEmitter;
pub use events::EventParameters;
pub use job::*;
pub use queue::*;
pub use stores::*;
pub use timers::Timer;
#[cfg(feature = "redis-store")]
pub use utils::{fetch_redis_pass, get_queue_metrics};
pub use worker::{Worker, WorkerMetrics, WorkerOpts};

pub type KioResult<T> = Result<T, KioError>;
