#![allow(dead_code)]
use deadpool_redis::redis;
use derive_more::Display;
use std::io;
use thiserror::Error;
use tokio::task::JoinError;
use uuid::Uuid;
mod backtrace_utils;
pub(crate) use backtrace_utils::{BacktraceCatcher, CaughtError, CaughtPanicInfo};
#[derive(Debug, Error)]
pub enum KioError {
    #[error(transparent)]
    RedisError(#[from] redis::RedisError),
    #[error(transparent)]
    DealPoolError(#[from] deadpool_redis::PoolError),
    #[error(transparent)]
    DealPoolConfig(#[from] deadpool_redis::CreatePoolError),
    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error), // Handle serde_json errors
    #[error(transparent)]
    SerdeDeserializeError(#[from] serde::de::value::Error),
    // Standard library errors
    #[error(transparent)]
    IoError(#[from] io::Error),
    #[error(transparent)]
    FmtError(#[from] std::fmt::Error),
    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),
    // Dynamic Error type for handling any other errors
    #[error(transparent)]
    GeneralError(#[from] Box<dyn std::error::Error + Send>),
    #[error(transparent)]
    SystemTimeError(#[from] std::time::SystemTimeError),
    #[error(transparent)]
    QueueError(#[from] QueueError),
    #[error("Failed to Convert: from {from} to {to}")]
    ConversionError {
        from: &'static str,
        to: &'static str,
    },
    #[error("Emitter: {0}")]
    EmitterError(String),
    #[error(transparent)]
    JoinError(#[from] JoinError),
    #[error(transparent)]
    WorkerError(#[from] WorkerError),
    #[error(transparent)]
    JobError(#[from] JobError),
}

#[derive(Debug, Display, Error)]
pub enum WorkerError {
    WorkerAlreadyRunningWithId(Uuid),
    FailedToCheckStalledJobs,
}
#[derive(Debug, Display, Error)]
pub enum QueueError {
    UnKnownEventMode,
    FailedToObliterate,
    CantObliterateWhileJobsActive,
    CantOperateWhenPaused,
    #[display("DelayBelowAllowedLimit {{limit:{limit_ms}, current: {current_ms}}}")]
    DelayBelowAllowedLimit {
        limit_ms: u64,
        current_ms: u64,
    },
}
#[repr(i8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy, Error)]
pub enum JobError {
    #[error("The job does not exist")]
    JobNotFound = -1,
    #[error("The job lock does not exist")]
    JobLockNotExist = -2,
    #[error("The job is not in the expected state")]
    JobNotInState = -3,
    #[error("The job has pending dependencies")]
    JobPendingDependencies = -4,
    #[error("The parent job does not exist")]
    ParentJobNotExist = -5,
    #[error("The job lock does not match")]
    JobLockMismatch = -6,
    #[error("Job has missed delay deadline")]
    MissedDelayDeadline = -7,
}
