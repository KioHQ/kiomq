use compact_str::CompactString;
use derive_more::Display;
use std::io;
use thiserror::Error;
use tokio::task::JoinError;
use uuid::Uuid;
mod backtrace_utils;
pub use backtrace_utils::{BacktraceCatcher, CaughtError, CaughtPanicInfo};
use croner::errors::CronError;
/// The top-level error type returned by most `KioMQ` operations.
///
/// Wraps errors from the underlying store backend (Redis, `RocksDB`), serialization
/// failures, and domain-specific errors from the queue, job, and worker layers.
#[derive(Debug, Error)]
pub enum KioError {
    #[cfg(feature = "redis-store")]
    #[error(transparent)]
    /// A Redis client error.
    RedisError(#[from] redis::RedisError),
    #[cfg(feature = "redis-store")]
    #[error(transparent)]
    /// A Redis client error.
    RedisParsingError(#[from] redis::ParsingError),
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
    /// `CompactString` formatting error.
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
    EmitterError(CompactString),
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

#[cfg(test)]
mod error_tests {
    use super::{JobError, KioError, QueueError, WorkerError};
    use compact_str::ToCompactString;
    use std::error::Error as StdError;
    use std::io;
    use uuid::Uuid;

    #[test]
    fn conversion_error_message_names_both_types() {
        let err = KioError::ConversionError {
            from: "SourceType",
            to: "TargetType",
        };
        let display = err.to_string();
        assert!(!display.is_empty());
        assert_eq!(display, "Failed to Convert: from SourceType to TargetType");
        // A structural error carries no underlying source.
        assert!(err.source().is_none());
    }

    #[test]
    fn emitter_error_message_embeds_the_reason() {
        let err = KioError::EmitterError("channel closed".to_compact_string());
        let display = err.to_string();
        assert!(!display.is_empty());
        assert_eq!(display, "Emitter: channel closed");
        assert!(err.source().is_none());
    }

    #[test]
    fn io_error_is_transparent_and_delegates_display() {
        let inner = io::Error::other("disk on fire");
        let inner_display = inner.to_string();
        let err = KioError::from(inner);
        assert!(matches!(err, KioError::IoError(_)));
        // `#[error(transparent)]` forwards Display verbatim to the inner error.
        assert_eq!(err.to_string(), inner_display);
        assert!(err.to_string().contains("disk on fire"));
    }

    #[test]
    fn parse_int_error_converts_via_question_mark() {
        fn parse() -> crate::KioResult<u64> {
            let value: u64 = "not-a-number".parse()?;
            Ok(value)
        }
        let err = parse().expect_err("parsing garbage must fail");
        assert!(matches!(err, KioError::ParseIntError(_)));
        // Transparent Display forwards the std error's message; assert on the
        // variant only (the exact std string is not a stability guarantee).
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn fmt_error_converts_and_renders() {
        let err = KioError::from(std::fmt::Error);
        assert!(matches!(err, KioError::FmtError(_)));
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn system_time_error_converts_and_renders() {
        // `duration_since` on an earlier-vs-later pair yields a SystemTimeError.
        let now = std::time::SystemTime::now();
        let later = now + std::time::Duration::from_secs(60);
        let source = now
            .duration_since(later)
            .expect_err("earlier.duration_since(later) must error");
        let err = KioError::from(source);
        assert!(matches!(err, KioError::SystemTimeError(_)));
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn serde_deserialize_error_converts_and_renders() {
        use serde::de::Error as DeError;
        let source = serde::de::value::Error::custom("bad value");
        let err = KioError::from(source);
        assert!(matches!(err, KioError::SerdeDeserializeError(_)));
        assert!(err.to_string().contains("bad value"));
    }

    #[test]
    fn json_error_converts_and_renders() {
        // Feeding invalid JSON to simd-json yields a `simd_json::Error`.
        let mut bytes = b"{ this is not json".to_vec();
        let source =
            simd_json::to_owned_value(&mut bytes).expect_err("invalid JSON must fail to parse");
        let err = KioError::from(source);
        assert!(matches!(err, KioError::JsonError(_)));
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn general_error_wraps_any_boxed_error() {
        let boxed: Box<dyn StdError + Send> = Box::new(io::Error::other("wrapped"));
        let err = KioError::from(boxed);
        assert!(matches!(err, KioError::GeneralError(_)));
        // Transparent Display delegates to the boxed error.
        assert!(err.to_string().contains("wrapped"));
    }

    #[tokio::test]
    async fn join_error_converts_from_panicking_task() {
        let source = tokio::spawn(async { panic!("task blew up") })
            .await
            .expect_err("a panicking task must produce a JoinError");
        let err = KioError::from(source);
        assert!(matches!(err, KioError::JoinError(_)));
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn croner_error_converts_from_invalid_expression() {
        use std::str::FromStr;
        let source = croner::Cron::from_str("this is not a cron expression")
            .expect_err("an invalid cron expression must fail to parse");
        let err = KioError::from(source);
        assert!(matches!(err, KioError::CronerError(_)));
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn queue_error_converts_into_kio_error_and_delegates_display() {
        let err = KioError::from(QueueError::CantOperateWhenPaused);
        assert!(matches!(err, KioError::QueueError(_)));
        // Transparent wrapper renders exactly like the wrapped QueueError.
        assert_eq!(
            err.to_string(),
            QueueError::CantOperateWhenPaused.to_string()
        );
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn worker_error_converts_into_kio_error() {
        let id = Uuid::nil();
        let err = KioError::from(WorkerError::WorkerAlreadyClosed(id));
        assert!(matches!(err, KioError::WorkerError(_)));
        assert_eq!(
            err.to_string(),
            WorkerError::WorkerAlreadyClosed(id).to_string()
        );
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn job_error_converts_into_kio_error() {
        let err = KioError::from(JobError::JobNotFound);
        assert!(matches!(err, KioError::JobError(_)));
        // Transparent Display carries the JobError's descriptive message through.
        assert_eq!(err.to_string(), "The job does not exist");
    }

    #[test]
    fn every_queue_error_variant_renders_non_empty() {
        let variants = [
            QueueError::UnKnownEventMode,
            QueueError::FailedToObliterate,
            QueueError::CantObliterateWhileJobsActive,
            QueueError::CantOperateWhenPaused,
            QueueError::DelayBelowAllowedLimit {
                limit_ms: 100,
                current_ms: 5,
            },
        ];
        for variant in &variants {
            assert!(
                !variant.to_string().is_empty(),
                "Display must not be empty for {variant:?}"
            );
            assert!(
                !format!("{variant:?}").is_empty(),
                "Debug must not be empty for {variant:?}"
            );
        }
    }

    #[test]
    fn delay_below_limit_display_reports_both_values() {
        let err = QueueError::DelayBelowAllowedLimit {
            limit_ms: 250,
            current_ms: 10,
        };
        let display = err.to_string();
        assert!(display.contains("250"), "must report the limit: {display}");
        assert!(
            display.contains("10"),
            "must report the current delay: {display}"
        );
    }

    #[test]
    fn every_worker_error_variant_renders_non_empty() {
        let id = Uuid::from_u128(0xdead_beef);
        let variants = [
            WorkerError::WorkerAlreadyRunningWithId(id),
            WorkerError::WorkerAlreadyClosed(id),
            WorkerError::FailedToCheckStalledJobs,
        ];
        for variant in &variants {
            assert!(
                !variant.to_string().is_empty(),
                "Display must not be empty for {variant:?}"
            );
        }
    }

    #[test]
    fn every_job_error_variant_has_a_descriptive_message() {
        let variants = [
            JobError::JobNotFound,
            JobError::JobLockNotExist,
            JobError::JobNotInState,
            JobError::JobPendingDependencies,
            JobError::ParentJobNotExist,
            JobError::JobLockMismatch,
            JobError::MissedDelayDeadline,
        ];
        for variant in &variants {
            let message = variant.to_string();
            assert!(
                !message.is_empty(),
                "JobError::{variant:?} must have a message"
            );
            // Crate-controlled copy is written in prose, not a bare variant name.
            assert!(
                message.contains(' '),
                "message should read as a sentence: {message}"
            );
        }
    }

    #[test]
    fn job_error_discriminants_are_stable_negative_codes() {
        // These map to store-level status codes and must not drift.
        assert_eq!(JobError::JobNotFound as i8, -1);
        assert_eq!(JobError::JobLockNotExist as i8, -2);
        assert_eq!(JobError::JobNotInState as i8, -3);
        assert_eq!(JobError::JobPendingDependencies as i8, -4);
        assert_eq!(JobError::ParentJobNotExist as i8, -5);
        assert_eq!(JobError::JobLockMismatch as i8, -6);
        assert_eq!(JobError::MissedDelayDeadline as i8, -7);
    }

    #[test]
    fn job_error_supports_equality_and_copy() {
        let a = JobError::JobLockMismatch;
        let b = a; // Copy
        assert_eq!(a, b);
        assert_ne!(JobError::JobNotFound, JobError::JobLockNotExist);
    }

    #[test]
    fn kio_error_debug_is_never_empty() {
        let errors = [
            KioError::ConversionError { from: "a", to: "b" },
            KioError::EmitterError("x".to_compact_string()),
            KioError::from(io::Error::other("io")),
            KioError::from(JobError::JobNotFound),
        ];
        for err in &errors {
            assert!(!format!("{err:?}").is_empty());
        }
    }
}
