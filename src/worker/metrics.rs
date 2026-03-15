use crate::Dt;
use chrono::Utc;
#[cfg(feature = "redis-store")]
use redis::{self, FromRedisValue, RedisResult};
use serde::{
    de::{self, Visitor},
    Deserialize, Serialize,
};
use std::fmt;
use std::time::Duration;
use tokio_metrics::TaskMetrics;
use uuid::Uuid;

use hdrhistogram::serialization::{Deserializer, Serializer, V2Serializer};
use hdrhistogram::Histogram;
/// Maximum poll duration we track: 100 seconds in nanoseconds.
pub const HISTOGRAM_MAX_NS: u64 = 100_000_000_000;
/// Significant figures for HDR histogram precision.
pub const HISTOGRAM_SIGFIG: u8 = 2;
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
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TaskInfo {
    /// Internal task identifier (not a job ID).
    pub task_id: u64,
    /// The job being processed by this task.
    pub job_id: u64,
    /// Tokio task-level timing statistics.
    pub metrics: TaskStats,
    /// When these metrics were last refreshed.
    pub last_updated: Dt,
    /// HDR histogram of poll durations (nanoseconds).
    poll_histogram: HistogramWrapper,
}

impl TaskInfo {
    /// Creates a [`TaskInfo`] from a [`tokio_metrics::TaskMetrics`] snapshot.
    pub fn new(task_id: u64, job_id: u64, metrics: TaskMetrics, histogram: Histogram<u64>) -> Self {
        let poll_histogram = HistogramWrapper(histogram);
        Self {
            task_id,
            job_id,
            metrics: TaskStats::from_metrics(metrics),
            last_updated: Utc::now(),
            poll_histogram,
        }
    }
    #[allow(dead_code)]
    /// Update existing `TaskInfo` fields
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
/// A Serializable and Deserializable wrapper for [`Histogram`]
#[derive(Clone, Debug)]
pub struct HistogramWrapper(pub Histogram<u64>);
impl Serialize for HistogramWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut vec = Vec::new();
        V2Serializer::new()
            .serialize(&self.0, &mut vec)
            .map_err(serde::ser::Error::custom)?;
        serializer.serialize_bytes(&vec)
    }
}

impl<'a> Deserialize<'a> for HistogramWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        struct HdrVisitor;

        impl<'de> Visitor<'de> for HdrVisitor {
            type Value = HistogramWrapper;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("HDR V2 serialized bytes")
            }

            fn visit_bytes<E: de::Error>(self, mut v: &[u8]) -> Result<Self::Value, E> {
                let h: Histogram<u64> = Deserializer::new()
                    .deserialize(&mut v)
                    .map_err(de::Error::custom)?;
                Ok(HistogramWrapper(h))
            }

            // serde_json represents bytes as a sequence of u8 — handle that too.
            fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut buf = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(byte) = seq.next_element::<u8>()? {
                    buf.push(byte);
                }
                self.visit_bytes(&buf)
            }
        }
        deserializer.deserialize_bytes(HdrVisitor)
    }
}
impl PartialEq for HistogramWrapper {
    fn eq(&self, other: &Self) -> bool {
        // Histograms are equal when they produce identical serialized bytes.
        let encode = |h: &Histogram<u64>| {
            let mut buf = Vec::new();
            V2Serializer::new().serialize(h, &mut buf).ok()?;
            Some(buf)
        };
        encode(&self.0) == encode(&other.0)
    }
}

impl Eq for HistogramWrapper {}
impl PartialOrd for HistogramWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HistogramWrapper {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .len()
            .cmp(&other.0.len())
            .then_with(|| self.0.max().cmp(&other.0.max()))
    }
}
