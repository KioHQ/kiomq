use crate::Dt;
use chrono::Utc;
#[cfg(feature = "redis-store")]
use redis::{self, FromRedisValue, RedisResult};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_metrics::TaskMetrics;
use uuid::Uuid;

/// Aggregated metrics for a single worker instance.
///
/// Persisted to the store periodically (see
/// [`WorkerOpts::metrics_update_interval`](crate::WorkerOpts::metrics_update_interval))
/// so that operators can monitor per-worker health.
///
/// Retrieve via [`Queue::fetch_worker_metrics`](crate::Queue::fetch_worker_metrics).
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct WorkerMetrics {
    /// Unique identifier of the worker instance.
    pub worker_id: Uuid,
    /// Number of jobs the worker is currently processing.
    pub active_len: usize,
    /// Per-task timing snapshots for each in-flight job.
    pub tasks: Vec<TaskInfo>,
}
impl WorkerMetrics {
    /// Creates a new `WorkerMetrics` snapshot.
    #[must_use]
    pub const fn new(worker_id: Uuid, active_len: usize, tasks: Vec<TaskInfo>) -> Self {
        Self {
            worker_id,
            active_len,
            tasks,
        }
    }
}

/// Timing snapshot for a single in-flight task managed by a worker.
#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TaskInfo {
    /// Internal task identifier (not a job ID).
    pub task_id: u64,
    /// The job being processed by this task.
    pub job_id: u64,
    /// Tokio task-level timing statistics.
    pub metrics: TaskStats,
    /// When these metrics were last refreshed.
    pub last_updated: Dt,
}

impl TaskInfo {
    /// Creates a `TaskInfo` from a [`tokio_metrics::TaskMetrics`] snapshot.
    pub fn new(task_id: u64, job_id: u64, metrics: TaskMetrics) -> Self {
        Self {
            task_id,
            job_id,
            metrics: TaskStats::from_metrics(metrics),
            last_updated: Utc::now(),
        }
    }
    #[allow(dead_code)]
    // No: method to be used later
    fn update(&mut self, metrics: TaskMetrics) {
        self.metrics = TaskStats::from_metrics(metrics);
        self.last_updated = Utc::now();
    }
}
/// Tokio runtime statistics captured for a single in-flight task.
///
/// Values mirror the fields exposed by [`tokio_metrics::TaskMetrics`].
#[allow(clippy::struct_field_names)]
#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TaskStats {
    /// Total number of times the task was polled.
    pub total_poll_count: u64,
    /// Number of polls that exceeded Tokio's slow-poll threshold.
    pub total_slow_poll_count: u64,
    /// Cumulative time spent inside `Future::poll`.
    pub total_poll_duration: Duration,
    /// Cumulative time the task spent waiting to be polled again.
    pub total_idle_duration: Duration,
    /// Cumulative time the task spent in the scheduler queue before being polled.
    pub total_scheduled_duration: Duration,
}
impl TaskStats {
    const fn from_metrics(metrics: TaskMetrics) -> Self {
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
        let bytes = Arc::make_mut(&mut bytes);
        let metrics = simd_json::from_slice(bytes).map_err(std::io::Error::other)?;
        Ok(metrics)
    }
}
