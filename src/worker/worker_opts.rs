use derive_more::Debug;

use crate::Dt;
pub(crate) const MIN_DELAY_MS_LIMIT: u64 = 100;
#[derive(Debug, Clone)]
pub struct WorkerOpts {
    /// Number of milliseconds between stallness checks. Default is 30000
    pub stalled_interval: u64,
    /// Duration of the lock for the job in milliseconds. The lock represents that a worker is processing the job.
    /// If the lock is lost, the job will be eventually be picked up by the stalled checker and
    /// move back to wait so that another worker can process it again.
    /// @default 30000
    pub lock_duration: u64,
    /// The time in milliseconds before the lock is automatically renewed.
    /// It is not recommended to modify this value, which is by default set to halv the lockDuration value,
    /// which is optimal for most use cases.
    pub lock_renew_time: u64,
    /// Amount of times a job can be recovered from a stalled stalled_interval to the `wait` state.
    /// If this is exceeded, the job is moved to `failed`
    pub max_stalled_count: u64,
    /// The numbers of tasks running concurrency with the current executor thread pool.
    pub concurrency: usize,
    /// The interval to check for delayed jobs or next jobs to schedule
    pub autorun: bool,
}
impl Default for WorkerOpts {
    fn default() -> Self {
        Self {
            concurrency: num_cpus::get(),

            stalled_interval: 30000,
            lock_duration: 30000,
            lock_renew_time: 15000,
            max_stalled_count: 1,
            autorun: Default::default(),
        }
    }
}
