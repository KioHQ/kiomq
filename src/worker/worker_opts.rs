use derive_more::Debug;
pub(crate) const MIN_DELAY_MS_LIMIT: u64 = 50;
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
#[derive(Debug, Clone, Copy)]
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
