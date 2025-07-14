#![allow(dead_code)]
use deadpool_redis::redis;
use derive_more::Display;
use std::io;
use thiserror::Error;
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
    GeneralError(#[from] Box<dyn std::error::Error + Send + Sync>),
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
    WorkerAlreadyRunningWithId(String),
    FailedToCheckStalledJobs,
}
#[derive(Debug, Display, Error)]
pub enum QueueError {
    FailedToObliterate,
    CantObliterateWhileJobsActive,
}
#[derive(Debug, Display, Error)]
pub enum JobError {
    NotFound,
}
