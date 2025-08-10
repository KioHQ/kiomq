#![allow(dead_code, unused)]
mod error;
mod job;
mod queue;
mod timer;
mod utils;
mod worker;
pub use async_backtrace::{frame, framed};
pub use error::KioError;
pub use job::*;
pub use queue::*;
pub use utils::{fetch_redis_pass, get_job_metrics};
pub use worker::{EventParameters, Worker, WorkerOpts};

pub type KioResult<T> = Result<T, KioError>;
