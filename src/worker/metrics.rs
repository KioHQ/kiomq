use crate::Dt;
use chrono::Utc;
#[cfg(feature = "redis-store")]
use redis::{self, FromRedisValue, RedisResult};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_metrics::TaskMetrics;
use uuid::Uuid;

use std::{collections::VecDeque, time::Instant};
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct WorkerMetrics {
    pub worker_id: Uuid,
    pub active_len: usize,
    pub tasks: Vec<TaskInfo>,
}
impl WorkerMetrics {
    pub fn new(worker_id: Uuid, active_len: usize, tasks: Vec<TaskInfo>) -> Self {
        Self {
            worker_id,
            active_len,
            tasks,
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TaskInfo {
    pub task_id: u64,
    pub job_id: u64,
    pub metrics: TaskStats,
    pub last_updated: Dt,
}

impl TaskInfo {
    pub fn new(task_id: u64, job_id: u64, metrics: TaskMetrics) -> Self {
        Self {
            task_id,
            job_id,
            metrics: TaskStats::from_metrics(metrics),
            last_updated: Utc::now(),
        }
    }

    fn update(&mut self, metrics: TaskMetrics) {
        self.metrics = TaskStats::from_metrics(metrics);
        self.last_updated = Utc::now();
    }
}
#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TaskStats {
    pub total_poll_count: u64,
    pub total_slow_poll_count: u64,
    pub total_poll_duration: Duration,
    pub total_idle_duration: Duration,
    pub total_scheduled_duration: Duration,
}
impl TaskStats {
    fn from_metrics(metrics: TaskMetrics) -> Self {
        Self {
            total_poll_count: metrics.total_poll_count,
            total_slow_poll_count: metrics.total_slow_poll_count,
            total_poll_duration: metrics.total_poll_duration,
            total_idle_duration: metrics.total_idle_duration,
            total_scheduled_duration: metrics.total_scheduled_duration,
        }
    }
}

#[cfg(feature = "redis-store")]
impl FromRedisValue for WorkerMetrics {
    fn from_redis_value(v: &redis::Value) -> RedisResult<Self> {
        use std::sync::Arc;
        let mut bytes: Arc<[u8]> = redis::from_redis_value(v)?;
        let mut bytes = Arc::make_mut(&mut bytes);
        let metrics = simd_json::from_slice(bytes).map_err(std::io::Error::other)?;
        Ok(metrics)
    }
}
