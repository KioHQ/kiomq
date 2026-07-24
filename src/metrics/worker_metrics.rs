use crate::Dt;
#[cfg(feature = "redis-store")]
use crate::utils::to_redis_parsing_error;
use chrono::Utc;
#[cfg(feature = "redis-store")]
use redis::{self, FromRedisValue, ParsingError};
use serde::{
    Deserialize, Serialize,
    de::{self, Visitor},
};
use std::fmt;
use std::time::Duration;
use tokio_metrics::TaskMetrics;
use uuid::Uuid;

use hdrhistogram::Histogram;
use hdrhistogram::serialization::{Deserializer, Serializer, V2Serializer};
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
    /// When the metrics were last updated.
    pub last_updated: Dt,
    /// time to live for metrics
    pub ttl_ms: u64,
}
impl WorkerMetrics {
    /// Creates a new `WorkerMetrics` snapshot.
    #[must_use]
    pub fn new(worker_id: Uuid, active_len: usize, tasks: Vec<TaskInfo>, ttl: u64) -> Self {
        use chrono::Utc;
        let last_updated = Utc::now();
        Self {
            ttl_ms: ttl,
            last_updated,
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
    pub poll_histogram: HistogramWrapper,
}

impl TaskInfo {
    /// Creates a [`TaskInfo`] from a [`tokio_metrics::TaskMetrics`] snapshot.
    #[must_use]
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
    fn from_redis_value(v: redis::Value) -> Result<Self, ParsingError> {
        use std::sync::Arc;
        let mut bytes: Arc<[u8]> = redis::from_redis_value(v)?;
        let bytes = Arc::make_mut(&mut bytes);
        let metrics = simd_json::from_slice(bytes).map_err(to_redis_parsing_error)?;
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
            .serialize(self, &mut vec)
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
impl std::ops::DerefMut for HistogramWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl std::ops::Deref for HistogramWrapper {
    type Target = Histogram<u64>;

    fn deref(&self) -> &Self::Target {
        &self.0
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
        self.len()
            .cmp(&other.len())
            .then_with(|| self.0.max().cmp(&other.0.max()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hdrhistogram::Histogram;
    use hdrhistogram::serialization::{Deserializer, Serializer, V2Serializer};
    use std::time::Duration;
    use tokio_metrics::TaskMetrics;
    use uuid::Uuid;

    /// Build a histogram configured exactly like the production poll histogram.
    fn fresh_histogram() -> Histogram<u64> {
        Histogram::new_with_max(HISTOGRAM_MAX_NS, HISTOGRAM_SIGFIG)
            .expect("production histogram bounds must always be valid")
    }

    #[test]
    fn empty_histogram_snapshot_reports_no_samples_without_panicking() {
        let wrapper = HistogramWrapper(fresh_histogram());
        assert_eq!(wrapper.len(), 0, "a fresh histogram must hold zero samples");
        assert!(wrapper.is_empty(), "a fresh histogram must be empty");
        assert_eq!(
            wrapper.0.max(),
            0,
            "empty histogram reports a maximum of zero"
        );
        // Percentile maths on an empty histogram must not divide by zero or panic.
        assert_eq!(
            wrapper.value_at_quantile(0.5),
            0,
            "median of an empty histogram must be zero, not a panic"
        );
        assert_eq!(
            wrapper.value_at_quantile(0.99),
            0,
            "p99 of an empty histogram must be zero, not a panic"
        );
    }

    #[test]
    fn single_sample_histogram_percentiles_return_that_sample() {
        let mut hist = fresh_histogram();
        let sample = 42_000_u64;
        hist.record(sample)
            .expect("value is within histogram bounds");
        let wrapper = HistogramWrapper(hist);
        assert_eq!(wrapper.len(), 1, "exactly one sample was recorded");
        // Every quantile of a single-sample distribution collapses onto that sample
        // (allowing for the histogram's precision-driven bucket rounding).
        for quantile in [0.0_f64, 0.5, 0.99, 1.0] {
            let value = wrapper.value_at_quantile(quantile);
            assert!(
                wrapper.equivalent(value, sample),
                "quantile {quantile} should be equivalent to the only sample"
            );
        }
    }

    #[test]
    fn recording_zero_is_accepted_and_counted() {
        let mut hist = fresh_histogram();
        hist.record(0).expect("zero must be a recordable value");
        let wrapper = HistogramWrapper(hist);
        assert_eq!(wrapper.len(), 1, "recording zero must increment the count");
    }

    #[test]
    fn recording_the_maximum_tracked_value_is_within_bounds() {
        let mut hist = fresh_histogram();
        hist.record(HISTOGRAM_MAX_NS)
            .expect("the declared maximum must be recordable");
        let wrapper = HistogramWrapper(hist);
        assert_eq!(wrapper.len(), 1);
        assert!(
            wrapper.0.max() >= HISTOGRAM_MAX_NS.saturating_sub(HISTOGRAM_MAX_NS / 100),
            "the recorded maximum should sit close to the tracked ceiling"
        );
    }

    #[test]
    fn recording_wildly_out_of_range_returns_an_error_rather_than_corrupting_state() {
        // The histogram has auto-resize disabled, so a value far beyond the
        // ceiling (past the top bucket's precision tolerance) must be rejected.
        let mut hist = fresh_histogram();
        let outcome = hist.record(u64::MAX);
        assert!(
            outcome.is_err(),
            "an unrepresentable value must surface an error, not silently corrupt state"
        );
        assert_eq!(
            hist.len(),
            0,
            "a rejected record must not have mutated the sample count"
        );
    }

    #[test]
    fn production_clamping_idiom_always_records_successfully() {
        // Mirrors the production pattern `record(value.min(HISTOGRAM_MAX_NS))`:
        // clamping to the ceiling must guarantee the record never fails.
        let mut hist = fresh_histogram();
        for raw in [0_u64, HISTOGRAM_MAX_NS, u64::MAX, u64::MAX / 2] {
            hist.record(raw.min(HISTOGRAM_MAX_NS))
                .expect("clamped values must always be recordable");
        }
        assert_eq!(hist.len(), 4, "every clamped value must be counted");
    }

    #[test]
    fn saturating_record_clamps_out_of_range_values_instead_of_dropping_them() {
        let mut hist = fresh_histogram();
        hist.saturating_record(u64::MAX);
        let wrapper = HistogramWrapper(hist);
        assert_eq!(
            wrapper.len(),
            1,
            "saturating_record must always retain the sample"
        );
        // The clamped value lands in the top bucket, whose highest-equivalent
        // value legitimately sits just above the nominal high bound.
        assert!(
            wrapper.0.max() <= wrapper.highest_equivalent(wrapper.high()),
            "the clamped sample must not exceed the histogram's representable ceiling"
        );
    }

    #[test]
    fn hdr_v2_byte_round_trip_preserves_every_sample() {
        let mut hist = fresh_histogram();
        for value in [0_u64, 1, 1_000, 5_000_000, HISTOGRAM_MAX_NS] {
            hist.record(value).expect("all values are within bounds");
        }
        let original = HistogramWrapper(hist);

        let mut buf = Vec::new();
        V2Serializer::new()
            .serialize(&original.0, &mut buf)
            .expect("V2 serialisation must succeed");
        let mut cursor = &buf[..];
        let decoded: Histogram<u64> = Deserializer::new()
            .deserialize(&mut cursor)
            .expect("V2 deserialisation must succeed");
        let decoded = HistogramWrapper(decoded);

        assert_eq!(decoded.len(), original.len(), "sample count must survive");
        assert_eq!(decoded.0.max(), original.0.max(), "maximum must survive");
        assert_eq!(
            decoded, original,
            "byte round-trip must produce an equal histogram"
        );
    }

    #[cfg(feature = "redis-store")]
    #[test]
    fn serde_round_trip_via_json_preserves_histogram() {
        let mut hist = fresh_histogram();
        for value in [12_u64, 34, 5_678, 90_000_000] {
            hist.record(value).expect("all values are within bounds");
        }
        let original = HistogramWrapper(hist);

        let mut json = simd_json::to_string(&original)
            .expect("histogram must serialise to JSON")
            .into_bytes();
        let decoded: HistogramWrapper =
            simd_json::from_slice(&mut json).expect("histogram must deserialise from JSON");

        assert_eq!(
            decoded, original,
            "serde JSON round-trip must preserve the histogram"
        );
    }

    #[test]
    fn equal_samples_produce_equal_wrappers() {
        let build = || {
            let mut hist = fresh_histogram();
            hist.record(100).unwrap();
            hist.record(200).unwrap();
            HistogramWrapper(hist)
        };
        assert_eq!(build(), build(), "identical samples must compare equal");
    }

    #[test]
    fn differing_samples_produce_unequal_wrappers() {
        let mut a = fresh_histogram();
        a.record(100).unwrap();
        let mut b = fresh_histogram();
        b.record(999).unwrap();
        assert_ne!(
            HistogramWrapper(a),
            HistogramWrapper(b),
            "different samples must not compare equal"
        );
    }

    #[test]
    fn ordering_is_by_sample_count_then_by_maximum() {
        let mut few = fresh_histogram();
        few.record(10).unwrap();
        let mut many = fresh_histogram();
        many.record(10).unwrap();
        many.record(20).unwrap();
        assert!(
            HistogramWrapper(few) < HistogramWrapper(many),
            "a histogram with fewer samples must order first"
        );

        // Equal sample counts fall back to comparing the maximum.
        let mut low_max = fresh_histogram();
        low_max.record(10).unwrap();
        let mut high_max = fresh_histogram();
        high_max.record(10_000).unwrap();
        assert!(
            HistogramWrapper(low_max) < HistogramWrapper(high_max),
            "with equal counts the smaller maximum must order first"
        );
    }

    #[test]
    fn deref_exposes_the_underlying_histogram_directly() {
        let mut wrapper = HistogramWrapper(fresh_histogram());
        wrapper.record(500).expect("record through DerefMut");
        assert_eq!(wrapper.len(), 1);
    }

    #[test]
    fn worker_metrics_new_preserves_all_constructor_fields() {
        let id = Uuid::new_v4();
        let ttl = 5_000_u64;
        let metrics = WorkerMetrics::new(id, 3, Vec::new(), ttl);
        assert_eq!(metrics.worker_id, id);
        assert_eq!(metrics.active_len, 3);
        assert_eq!(metrics.ttl_ms, ttl);
        assert!(metrics.tasks.is_empty());
    }

    #[test]
    fn task_stats_from_default_metrics_are_all_zero() {
        let info = TaskInfo::new(1, 2, TaskMetrics::default(), fresh_histogram());
        assert_eq!(info.task_id, 1);
        assert_eq!(info.job_id, 2);
        assert_eq!(info.metrics.total_poll_count, 0);
        assert_eq!(info.metrics.total_slow_poll_count, 0);
        assert_eq!(info.metrics.total_poll_duration, Duration::ZERO);
        assert_eq!(info.metrics.total_idle_duration, Duration::ZERO);
        assert_eq!(info.metrics.total_scheduled_duration, Duration::ZERO);
    }

    #[cfg(feature = "redis-store")]
    #[test]
    fn worker_metrics_serde_round_trip_preserves_tasks_and_histogram() {
        let mut hist = fresh_histogram();
        hist.record(1_234).unwrap();
        hist.record(HISTOGRAM_MAX_NS).unwrap();
        let task = TaskInfo::new(7, 11, TaskMetrics::default(), hist);
        let original = WorkerMetrics::new(Uuid::new_v4(), 1, vec![task], 1_000);

        let mut json = simd_json::to_string(&original)
            .expect("WorkerMetrics must serialise")
            .into_bytes();
        let decoded: WorkerMetrics =
            simd_json::from_slice(&mut json).expect("WorkerMetrics must deserialise");

        assert_eq!(
            decoded, original,
            "a full WorkerMetrics snapshot must survive a serde round-trip"
        );
    }
}
