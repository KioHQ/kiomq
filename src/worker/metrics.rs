use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_metrics::TaskMetrics;
use uuid::Uuid;

use std::{collections::VecDeque, time::Instant};
#[derive(Deserialize, Serialize, Debug)]
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

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub struct TaskInfo {
    task_id: u64,
    job_id: u64,
    metrics: TaskStats,
    //last_updated: ,
}

impl TaskInfo {
    pub fn new(task_id: u64, job_id: u64, metrics: TaskMetrics) -> Self {
        Self {
            task_id,
            job_id,
            metrics: TaskStats::from_metrics(metrics),
            //last_updated: Instant::now(),
        }
    }

    fn update(&mut self, metrics: TaskMetrics) {
        self.metrics = TaskStats::from_metrics(metrics);
        //self.last_updated = Instant::now();
    }
}
#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
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
