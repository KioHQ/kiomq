#![allow(dead_code)]
#[cfg(feature = "redis-store")]
use deadpool_redis::redis;
use derive_more::Display;
use std::io;
use thiserror::Error;
use tokio::task::JoinError;
use uuid::Uuid;
mod backtrace_utils;
pub(crate) use backtrace_utils::{BacktraceCatcher, CaughtError, CaughtPanicInfo};
use croner::errors::CronError;
/// The top-level error type returned by most KioMQ operations.
///
/// Wraps errors from the underlying store backend (Redis, RocksDB), serialization
/// failures, and domain-specific errors from the queue, job, and worker layers.
#[derive(Debug, Error)]
pub enum KioError {
    #[cfg(feature = "redis-store")]
    #[error(transparent)]
    /// A Redis client error.
    RedisError(#[from] redis::RedisError),
    #[cfg(feature = "redis-store")]
    #[error(transparent)]
    /// A connection-pool error from deadpool-redis.
    DealPoolError(#[from] deadpool_redis::PoolError),
    #[cfg(feature = "redis-store")]
    #[error(transparent)]
    /// Failed to create a deadpool-redis connection pool.
    DealPoolConfig(#[from] deadpool_redis::CreatePoolError),
    #[error(transparent)]
    /// JSON serialisation or deserialisation failure.
    JsonError(#[from] simd_json::Error),
    #[error(transparent)]
    /// Serde value deserialisation failure.
    SerdeDeserializeError(#[from] serde::de::value::Error),
    #[error(transparent)]
    /// Standard I/O error.
    IoError(#[from] io::Error),
    #[error(transparent)]
    /// String formatting error.
    FmtError(#[from] std::fmt::Error),
    #[error(transparent)]
    /// Integer parse failure.
    ParseIntError(#[from] std::num::ParseIntError),
    #[error(transparent)]
    /// Any other boxed error type.
    GeneralError(#[from] Box<dyn std::error::Error + Send>),
    #[error(transparent)]
    /// System clock error.
    SystemTimeError(#[from] std::time::SystemTimeError),
    #[error(transparent)]
    /// A queue-level error.
    QueueError(#[from] QueueError),
    #[error("Failed to Convert: from {from} to {to}")]
    /// Type conversion failed.
    ConversionError {
        /// Source type name.
        from: &'static str,
        /// Target type name.
        to: &'static str,
    },
    #[error("Emitter: {0}")]
    /// Event emitter error with a descriptive message.
    EmitterError(String),
    #[error(transparent)]
    /// A Tokio task join failure.
    JoinError(#[from] JoinError),
    #[error(transparent)]
    /// A worker lifecycle error.
    WorkerError(#[from] WorkerError),
    #[error(transparent)]
    /// A job-level error.
    JobError(#[from] JobError),
    #[error(transparent)]
    /// A cron expression parse error.
    CronerError(#[from] CronError),
    #[cfg(feature = "rocksdb-store")]
    #[error(transparent)]
    /// A RocksDB storage error.
    InMemoryError(#[from] rocksdb::Error),
}

/// Errors specific to [`Worker`](crate::Worker) lifecycle operations.
#[derive(Debug, Display, Error)]
pub enum WorkerError {
    /// Returned by [`Worker::run`](crate::Worker::run) when the worker is already running.
    WorkerAlreadyRunningWithId(Uuid),
    /// Returned by [`Worker::run`](crate::Worker::run) when the worker has already been closed.
    WorkerAlreadyClosed(Uuid),
    /// Internal error emitted when the stalled-job checker encounters a failure.
    FailedToCheckStalledJobs,
}
/// Errors arising from queue-level operations.
#[derive(Debug, Display, Error)]
pub enum QueueError {
    /// The stored event-mode byte does not correspond to any known [`QueueEventMode`](crate::QueueEventMode).
    UnKnownEventMode,
    /// The queue could not be obliterated (e.g. internal store failure).
    FailedToObliterate,
    /// Attempted to obliterate the queue while jobs are still active.
    CantObliterateWhileJobsActive,
    /// Attempted an operation that is not permitted while the queue is paused.
    CantOperateWhenPaused,
    /// The requested delay is below the minimum allowed limit.
    #[display("DelayBelowAllowedLimit {{limit:{limit_ms}, current: {current_ms}}}")]
    DelayBelowAllowedLimit {
        /// The minimum permitted delay in milliseconds.
        limit_ms: u64,
        /// The delay that was actually requested.
        current_ms: u64,
    },
}
/// Errors arising from individual job operations.
#[repr(i8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy, Error)]
pub enum JobError {
    /// The requested job does not exist in the store.
    #[error("The job does not exist")]
    JobNotFound = -1,
    /// The expected lock entry for the job is absent.
    #[error("The job lock does not exist")]
    JobLockNotExist = -2,
    /// The job is not in the state required for the attempted operation.
    #[error("The job is not in the expected state")]
    JobNotInState = -3,
    /// The job cannot progress because it still has unresolved dependencies.
    #[error("The job has pending dependencies")]
    JobPendingDependencies = -4,
    /// The parent job referenced by this child no longer exists.
    #[error("The parent job does not exist")]
    ParentJobNotExist = -5,
    /// The supplied lock token does not match the one currently held.
    #[error("The job lock does not match")]
    JobLockMismatch = -6,
    /// The job's scheduled time has already passed.
    #[error("Job has missed delay deadline")]
    MissedDelayDeadline = -7,
}
