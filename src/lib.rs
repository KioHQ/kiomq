#![doc = include_str!("../README.md")]
#![warn(
    missing_debug_implementations,
    missing_docs,
    clippy::pedantic,
    clippy::nursery
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
mod error;
mod events;
mod job;
/// Re-exports of test helpers for verifying custom [`Store`] implementations.
pub mod macros;
mod queue;
mod stores;
mod timers;
mod utils;
mod worker;
pub use async_backtrace::frame;
/// Attribute macro that wraps an `async fn` so it appears in
/// [`async_backtrace`](https://docs.rs/async-backtrace) stack traces.
///
/// Apply `#[framed]` to your processor function to get richer async backtraces
/// when a panic or error occurs inside a job. The worker already catches panics
/// and converts them to failures, but having the full async call stack in the
/// trace makes debugging much easier.
///
/// # Example
///
/// ```rust
/// use std::sync::Arc;
/// use kiomq::{framed, InMemoryStore, Job, KioError, Queue, Store, Worker, WorkerOpts};
///
/// #[framed]
/// async fn my_processor<S: Store<u64, u64, ()>>(
///     _store: Arc<S>,
///     job: Job<u64, u64, ()>,
/// ) -> Result<u64, KioError> {
///     let data = job.data.unwrap_or_default();
///     if data == 0 {
///         // Returning Err marks the job as failed and triggers a retry
///         // (up to `attempts` times, as configured in QueueOpts / JobOptions).
///         return Err(std::io::Error::new(std::io::ErrorKind::Other, "zero input").into());
///     }
///     Ok(data * 2)
/// }
///
/// # #[tokio::main]
/// # async fn main() -> kiomq::KioResult<()> {
/// # let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "framed-demo");
/// # let queue = Queue::new(store, None).await?;
/// # let worker = Worker::new_async(&queue, |s, j| my_processor(s, j), Some(WorkerOpts::default()))?;
/// # worker.run()?;
/// # worker.close();
/// # Ok(())
/// # }
/// ```
pub use async_backtrace::framed;
#[cfg(feature = "redis-store")]
pub use deadpool_redis::Config;
pub use error::*;
pub(crate) use events::EventEmitter;
pub use events::EventParameters;
pub use job::*;
pub use queue::*;
pub use stores::*;
pub use timers::{TimedMap, Timer};
#[cfg(feature = "redis-store")]
pub use utils::{fetch_redis_pass, get_queue_metrics};
pub use worker::{TaskInfo, Worker, WorkerMetrics, WorkerOpts};

/// Convenience alias for `Result<T, KioError>`.
pub type KioResult<T> = Result<T, KioError>;
