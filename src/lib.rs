#![allow(dead_code, unused)]
mod error;
mod job;
mod queue;
mod utils;
mod worker;
pub use error::KioError;
pub use job::*;
pub use queue::*;
pub use utils::fetch_redis_pass;
pub use worker::Worker;

pub type KioResult<T> = Result<T, KioError>;
