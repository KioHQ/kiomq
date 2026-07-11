use derive_more::Debug;
use serde::{Deserialize, Serialize};
/// Minimum acceptable delay in milliseconds
pub const MIN_DELAY_MS_LIMIT: u64 = 50;
/// Configuration options for a [`Worker`](crate::Worker).
///
/// All durations are in **milliseconds** unless otherwise noted.
///
/// # Examples
///
/// ```rust
/// use kiomq::WorkerOpts;
///
/// let opts = WorkerOpts {
///     concurrency: 8,
///     lock_duration: 60_000,
///     lock_renew_time: 30_000,
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct WorkerOpts {
    /// Interval between stalled-job checks in milliseconds. Default is `30000`.
    pub stalled_interval: u64,
    /// How long (ms) a job lock is held before the job is considered stalled.
    /// A stalled job is moved back to the wait state so another worker can pick
    /// it up.
    ///
    /// @default 30000
    pub lock_duration: u64,
    /// How long before expiry (ms) the lock is automatically renewed.
    ///
    /// It is not recommended to modify this value, which is by default set to half the lockDuration value,
    /// which is optimal for most use cases.
    pub lock_renew_time: u64,
    /// Maximum number of times a job may be recovered from a stalled state
    /// before it is permanently moved to `failed`.
    pub max_stalled_count: u64,
    /// Maximum number of jobs the worker processes concurrently.
    ///
    /// Defaults to the number of logical CPUs on the host machine.
    pub concurrency: usize,
    /// If `true`, [`Worker::run`](crate::Worker::run) is called automatically
    /// inside the constructor.
    pub autorun: bool,
    /// How often (ms) per-worker metrics are published to the store.
    /// Default is `100`.
    pub metrics_update_interval: u64,
}
impl Default for WorkerOpts {
    fn default() -> Self {
        Self {
            concurrency: num_cpus::get(),

            stalled_interval: 30000,
            lock_duration: 30000,
            lock_renew_time: 15000,
            max_stalled_count: 1,
            metrics_update_interval: 100,
            autorun: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The documented defaults must remain stable — downstream code and the
    /// module docs rely on these exact values.
    #[test]
    fn default_values_match_documented_defaults() {
        let opts = WorkerOpts::default();
        assert_eq!(opts.stalled_interval, 30_000);
        assert_eq!(opts.lock_duration, 30_000);
        assert_eq!(opts.lock_renew_time, 15_000);
        assert_eq!(opts.max_stalled_count, 1);
        assert_eq!(opts.metrics_update_interval, 100);
        assert!(
            !opts.autorun,
            "autorun must default to false so construction never implicitly starts a worker"
        );
    }

    /// The default `lock_renew_time` is documented as half of `lock_duration`;
    /// guard that relationship so the recommendation stays truthful.
    #[test]
    fn default_lock_renew_time_is_half_the_lock_duration() {
        let opts = WorkerOpts::default();
        assert_eq!(opts.lock_renew_time, opts.lock_duration / 2);
    }

    /// Concurrency should default to the number of logical CPUs on the host.
    #[test]
    fn default_concurrency_equals_logical_cpu_count() {
        let opts = WorkerOpts::default();
        assert_eq!(opts.concurrency, num_cpus::get());
    }

    /// `WorkerOpts` is `Copy`; a copy must be independent of its source.
    #[test]
    fn copy_produces_an_independent_value() {
        let original = WorkerOpts::default();
        let mut copy = original;
        copy.concurrency = original.concurrency + 7;
        copy.autorun = !original.autorun;
        assert_ne!(copy.concurrency, original.concurrency);
        assert_ne!(copy.autorun, original.autorun);
        // The original must be untouched by mutations to the copy.
        assert_eq!(original.concurrency, num_cpus::get());
        assert!(!original.autorun);
    }

    /// Functional-update syntax must preserve every field it does not override.
    #[test]
    fn struct_update_preserves_untouched_fields() {
        let opts = WorkerOpts {
            concurrency: 8,
            lock_duration: 60_000,
            ..Default::default()
        };
        assert_eq!(opts.concurrency, 8);
        assert_eq!(opts.lock_duration, 60_000);
        // Untouched fields fall through to the defaults.
        assert_eq!(opts.stalled_interval, 30_000);
        assert_eq!(opts.lock_renew_time, 15_000);
        assert_eq!(opts.max_stalled_count, 1);
        assert_eq!(opts.metrics_update_interval, 100);
    }

    /// There is no validation on `concurrency`, so boundary values (0, 1 and the
    /// maximum) must round-trip through the struct unchanged. This documents the
    /// current no-validation contract.
    #[test]
    fn extreme_concurrency_values_are_stored_verbatim() {
        for value in [0usize, 1, usize::MAX] {
            let opts = WorkerOpts {
                concurrency: value,
                ..Default::default()
            };
            assert_eq!(opts.concurrency, value);
        }
    }

    /// Likewise the millisecond timing fields accept their full `u64` range with
    /// no clamping or rejection.
    #[test]
    fn extreme_timing_values_are_stored_verbatim() {
        let opts = WorkerOpts {
            stalled_interval: 0,
            lock_duration: u64::MAX,
            lock_renew_time: 0,
            max_stalled_count: u64::MAX,
            metrics_update_interval: 0,
            ..Default::default()
        };
        assert_eq!(opts.stalled_interval, 0);
        assert_eq!(opts.lock_duration, u64::MAX);
        assert_eq!(opts.lock_renew_time, 0);
        assert_eq!(opts.max_stalled_count, u64::MAX);
        assert_eq!(opts.metrics_update_interval, 0);
    }

    /// The minimum delay limit is a public constant relied upon elsewhere; pin
    /// its value so accidental changes are caught.
    #[test]
    fn min_delay_ms_limit_is_fifty() {
        assert_eq!(MIN_DELAY_MS_LIMIT, 50);
    }

    /// `WorkerOpts` must survive a serialise/deserialise cycle with every field
    /// intact, including the boundary values.
    #[cfg(feature = "redis-store")]
    #[test]
    fn serde_round_trip_preserves_all_fields() {
        let opts = WorkerOpts {
            concurrency: usize::MAX,
            stalled_interval: 1,
            lock_duration: 2,
            lock_renew_time: 3,
            max_stalled_count: 4,
            metrics_update_interval: 5,
            autorun: true,
        };
        let mut bytes = simd_json::to_vec(&opts).expect("WorkerOpts must serialise");
        let restored: WorkerOpts =
            simd_json::from_slice(&mut bytes).expect("WorkerOpts must deserialise");
        assert_eq!(restored.concurrency, opts.concurrency);
        assert_eq!(restored.stalled_interval, opts.stalled_interval);
        assert_eq!(restored.lock_duration, opts.lock_duration);
        assert_eq!(restored.lock_renew_time, opts.lock_renew_time);
        assert_eq!(restored.max_stalled_count, opts.max_stalled_count);
        assert_eq!(
            restored.metrics_update_interval,
            opts.metrics_update_interval
        );
        assert_eq!(restored.autorun, opts.autorun);
    }
}
