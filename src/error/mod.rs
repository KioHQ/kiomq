#![allow(dead_code)]
use deadpool_redis::redis;
use derive_more::Display;
use std::io;
use thiserror::Error;
use uuid::Uuid;

mod backtrace_utils;
pub(crate) use backtrace_utils::{BacktraceCatcher, CaughtError, CaughtPanicInfo};
#[derive(Debug, Error)]
pub enum KioError {
    #[error("RedisError: {0}")]
    RedisError(#[from] redis::RedisError),
    #[error("DeadPoolError: {0}")]
    DealPoolError(#[from] deadpool_redis::PoolError),
    #[error("DeadPoolError: {0}")]
    DealPoolConfig(#[from] deadpool_redis::CreatePoolError),
    #[error("SerdeJsonError: {0}")]
    SerdeJsonError(#[from] serde_json::Error), // Handle serde_json errors
    #[error("SerdeDeserializeError: {0}")]
    SerdeDeserializeError(#[from] serde::de::value::Error),
    #[error("SerdeRedisDecodeError: {0}")]
    SerdeRedisDecodeError(#[from] serde_redis::decode::Error),
    // Standard library errors
    #[error("IOError: {0}")]
    IoError(#[from] io::Error),
    #[error("FmtError: {0}")]
    FmtError(#[from] std::fmt::Error),
    #[error("ParseIntError: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),
    // Dynamic Error type for handling any other errors
    #[error("GeneralError: {0}")]
    GeneralError(#[from] Box<dyn std::error::Error + Send>),
    #[error("SystemTimeError: {0}")]
    SystemTimeError(#[from] std::time::SystemTimeError),
    #[error("QueueError: {0}")]
    QueueError(#[from] QueueError),
    #[error("Failed to Convert: from {from} to {to}")]
    ConversionError {
        from: &'static str,
        to: &'static str,
    },
    #[error("Emitter: {0}")]
    EmitterError(String),

    #[error("WorkerError: {0}")]
    WorkerError(#[from] WorkerError),
    #[error("JobError: {0}")]
    JobError(#[from] JobError),
}

#[derive(Debug, Display, Error)]
pub enum WorkerError {
    WorkerAlreadyRunningWithId(Uuid),
    FailedToCheckStalledJobs,
}
#[derive(Debug, Display, Error)]
pub enum QueueError {
    FailedToObliterate,
    CantObliterateWhileJobsActive,
    CantOperateWhenPaused,
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
}
