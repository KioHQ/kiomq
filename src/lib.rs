//! <div align="center">
//!
//! <img src="https://raw.githubusercontent.com/KioHQ/kiomq/HEAD/assets/logo-dark.png" alt="KioMQ logo" width="320" />
//!
//! ## KioMQ
//!
//! **A task-queue and orchestration library for Rust**
//!
//! Built for [Tokio](https://tokio.rs) · Scale up (many workers per machine) · Scale out (Redis + workers across machines)
//!
//! </div>
//!
//! ---
//!
//! **KioMQ** provides the core building blocks to run background work inside your Tokio services:
//!
//! - A [`Queue`] to enqueue tasks/jobs.
//! - One or more [`Worker`]s to process jobs concurrently.
//! - Pluggable [`Store`] backends:
//!   - [`InMemoryStore`] – ephemeral (tests, dev, short-lived tasks).
//!   - **Redis** (`redis-store` feature, default) – durable, distributed workloads.
//!   - **RocksDB** (`rocksdb-store` feature, *under construction*) – embedded persistence.
//! - **Scheduling** – delays, cron expressions, repeat policies.
//! - **Reliability** – retries, backoff strategies, stalled-job detection.
//! - **Observability** – events, progress updates, per-worker metrics.
//!
//! Inspired by [BullMQ](https://docs.bullmq.io/)'s ergonomics, implemented as an
//! embeddable Rust library.
//!
//! ---
//!
//! ### Key features
//!
//! * **Async & sync processors** – async for I/O-bound work, sync (via
//!   `spawn_blocking`) for CPU-bound work.
//! * **Configurable concurrency** – defaults to the number of logical CPUs.
//! * **Event-driven idle workers** – near-zero CPU usage when the queue has no work,
//!   using lock-free atomics and [`tokio::sync::Notify`].
//! * **Bulk enqueue** – [`Queue::bulk_add`] / [`Queue::bulk_add_only`].
//! * **Priority jobs** – enqueue with a priority score.
//! * **Delayed jobs** – fire a job after *N* milliseconds or on a cron schedule.
//! * **Repeat policies** – cron, backoff-driven, fixed interval, immediate.
//!
//! ---
//!
//! ### Installation
//!
//! ```toml
//! [dependencies]
//! kiomq = "0.1"
//! ```
//!
//! #### Cargo features
//!
//! - `redis-store` *(default)* – Redis backend
//! - `rocksdb-store` – RocksDB backend *(under construction)*
//! - `tracing` – [`tracing`](https://docs.rs/tracing) instrumentation
//!
//! ---
//!
//! ### Quick-start
//!
//! #### Async worker
//!
//! ```rust
//! # #[tokio::main]
//! # async fn main() -> kiomq::KioResult<()> {
//! use std::sync::Arc;
//! use kiomq::{InMemoryStore, Job, KioError, Queue, Worker, WorkerOpts};
//!
//! let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "demo");
//! let queue = Queue::new(store, None).await?;
//!
//! let processor = |_store: Arc<_>, job: Job<u64, u64, ()>| async move {
//!     Ok::<u64, KioError>(job.data.unwrap_or_default() * 2)
//! };
//!
//! let worker = Worker::new_async(&queue, processor, Some(WorkerOpts::default()))?;
//! worker.run()?;
//!
//! queue.bulk_add_only((0..10u64).map(|i| (format!("job-{i}"), None, i))).await?;
//!
//! worker.close();
//! # Ok(())
//! # }
//! ```
//!
//! #### Sync worker
//!
//! Sync processors run on a dedicated blocking thread via
//! [`tokio::task::spawn_blocking`](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html),
//! so they will not block Tokio's async executor threads.  Suitable for heavy
//! computation, hashing, image processing, blocking FFI, etc.
//!
//! ```rust
//! # #[tokio::main]
//! # async fn main() -> kiomq::KioResult<()> {
//! use std::sync::Arc;
//! use kiomq::{InMemoryStore, Job, KioError, Queue, Worker, WorkerOpts};
//!
//! let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "demo-sync");
//! let queue = Queue::new(store, None).await?;
//!
//! let processor = |_store: Arc<_>, job: Job<u64, u64, ()>| {
//!     Ok::<u64, KioError>(job.data.unwrap_or_default() * 2)
//! };
//!
//! let worker = Worker::new_sync(&queue, processor, Some(WorkerOpts::default()))?;
//! worker.run()?;
//!
//! queue.add_job("compute", 42u64, None).await?;
//!
//! worker.close();
//! # Ok(())
//! # }
//! ```
//!
//! ---
//!
//! ### Configuration
//!
//! #### Queue options ([`QueueOpts`])
//!
//! ```rust
//! use kiomq::{BackOffJobOptions, BackOffOptions, KeepJobs, QueueEventMode, QueueOpts,
//!             RemoveOnCompletionOrFailure};
//!
//! let queue_opts = QueueOpts {
//!     attempts: 2,
//!     default_backoff: Some(BackOffJobOptions::Opts(BackOffOptions {
//!         type_: Some("exponential".to_owned()),
//!         delay: Some(200),
//!     })),
//!     remove_on_fail: Some(RemoveOnCompletionOrFailure::Opts(KeepJobs {
//!         age: Some(3600), // keep for 1 hour
//!         count: None,
//!     })),
//!     event_mode: Some(QueueEventMode::PubSub),
//!     ..Default::default()
//! };
//! ```
//!
//! #### Per-job options ([`JobOptions`])
//!
//! ```rust
//! use kiomq::JobOptions;
//!
//! let opts = JobOptions { attempts: 5, ..Default::default() };
//! ```
//!
//! #### Worker options ([`WorkerOpts`])
//!
//! ```rust
//! use kiomq::WorkerOpts;
//!
//! let opts = WorkerOpts { concurrency: 8, ..Default::default() };
//! ```
//!
//! ---
//!
//! ### Events & observability
//!
//! Subscribe to job-state events on the queue or directly on a worker:
//!
//! ```rust
//! # #[tokio::main]
//! # async fn main() -> kiomq::KioResult<()> {
//! use kiomq::{EventParameters, InMemoryStore, JobState, Queue};
//!
//! let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "events-demo");
//! let queue = Queue::new(store, None).await?;
//!
//! // Subscribe to a specific state.
//! let _listener_id = queue.on(JobState::Completed, |evt| async move {
//!     // handle completed event
//!     let _ = evt;
//! });
//!
//! // Subscribe to all events.
//! let _listener_id2 = queue.on_all_events(|evt: EventParameters<u64, ()>| async move {
//!     let _ = evt;
//! });
//!
//! // Remove a listener when no longer needed.
//! queue.remove_event_listener(_listener_id);
//! # Ok(())
//! # }
//! ```
//!
//! ---
//!
//! ### Backends
//!
//! #### In-memory
//!
//! [`InMemoryStore`] is ideal for ephemeral workloads: tests, development, and
//! short-lived tasks.  No external dependencies required.
//!
//! #### Redis *(default feature)*
//!
//! The Redis store enables durable, distributed workloads spanning multiple
//! machines.  Requires a running Redis instance:
//!
//! ```bash
//! docker run --rm -p 6379:6379 redis:latest
//! ```
//!
//! #### RocksDB *(under construction)*
//!
//! The RocksDB store is a work-in-progress embedded-persistence backend.
//!
//! ---
//!
//! ### Tokio runtime
//!
//! KioMQ is built on Tokio.  The **multi-thread** runtime is recommended for best
//! throughput:
//!
//! ```toml
//! tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
//! ```
#![allow(dead_code, unused)]
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
pub use async_backtrace::{frame, framed};
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
pub use worker::{Worker, WorkerMetrics, WorkerOpts};

/// Convenience alias for `Result<T, `[`KioError`]`>`.
pub type KioResult<T> = Result<T, KioError>;
