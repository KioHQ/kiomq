use derive_more::Debug;

use crate::Dt;
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
    pub lock_renew_time: Dt,
    /// Amount of times a job can be recovered from a stalled stalled_interval to the `wait` state.
    /// If this is exceeded, the job is moved to `failed`
    pub max_stalled_count: i32,
    pub autorun: bool,
}
impl Default for WorkerOpts {
    fn default() -> Self {
        Self {
            stalled_interval: 30000,
            lock_duration: 30000,
            lock_renew_time: Default::default(),
            max_stalled_count: 1,
            autorun: Default::default(),
        }
    }
}
