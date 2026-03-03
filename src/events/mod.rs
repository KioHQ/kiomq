use crate::{FailedDetails, Job, JobError, JobMetrics, JobState};
use derive_more::Debug;
#[cfg(feature = "redis-store")]
use redis::AsyncCommands;
use uuid::Uuid;
/// The payload delivered to event listeners registered on a [`Queue`](crate::Queue).
///
/// Each variant corresponds to a [`JobState`](crate::JobState) transition or
/// observability event.  Subscribe via [`Queue::on`](crate::Queue::on) or
/// [`Queue::on_all_events`](crate::Queue::on_all_events).
///
/// # Examples
///
/// ```rust
/// # #[tokio::main]
/// # async fn main() -> kiomq::KioResult<()> {
/// use kiomq::{EventParameters, InMemoryStore, JobState, Queue};
///
/// let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "evt-demo");
/// let queue = Queue::new(store, None).await?;
///
/// queue.on_all_events(|evt: EventParameters<u64, ()>| async move {
///     match evt {
///         EventParameters::Completed { job_id, .. } => {
///             println!("job {job_id} completed");
///         }
///         EventParameters::Failed { job_id, reason, .. } => {
///             println!("job {job_id} failed: {}", reason.reason);
///         }
///         _ => {}
///     }
/// });
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub enum EventParameters<R, P> {
    /// A job was moved into the priority sorted-set.
    Prioritized {
        /// Numeric job ID.
        job_id: u64,
        /// Job name.
        name: Option<String>,
        /// Assigned priority score.
        priority: u64,
    },
    /// A new job was added to the queue.
    Added {
        /// Numeric job ID.
        job_id: u64,
        /// Job name.
        name: Option<String>,
    },
    /// A delayed or stalled job is now waiting to be processed.
    WaitingToRun {
        /// Numeric job ID.
        job_id: u64,
        /// The state the job was in before transitioning to wait.
        prev_state: Option<JobState>,
    },
    /// A job has been moved to the delayed state.
    Delayed {
        /// Numeric job ID.
        job_id: u64,
        /// How long the job will wait before becoming eligible.
        delay: Duration,
    },
    /// A job has been picked up by a worker and is now active.
    Active {
        /// Numeric job ID.
        job_id: u64,
        /// The state the job was in before becoming active.
        prev_state: Option<JobState>,
    },
    /// A job finished successfully.
    Completed {
        /// Numeric job ID.
        job_id: u64,
        /// Timing and attempt statistics.
        job_metrics: JobMetrics,
        /// How far in advance this run was scheduled (0 for non-delayed jobs).
        expected_delay: Duration,
        /// The state the job was in before completing.
        prev_state: Option<JobState>,
        /// The value returned by the processor.
        #[debug(skip)]
        result: R,
    },
    /// A placeholder event with no meaningful payload (e.g. queue drained).
    Void,
    /// A progress update reported by the processor.
    Progress {
        /// Numeric job ID.
        job_id: u64,
        /// The progress value emitted by the processor.
        #[debug(skip)]
        data: P,
    },
    /// A job was detected as stalled and moved for recovery.
    Stalled {
        /// Numeric job ID.
        job_id: u64,
        /// The state the job was in before stalling.
        prev_state: JobState,
    },
    /// A job permanently failed.
    Failed {
        /// Failure details (reason and run count).
        reason: FailedDetails,
        /// Numeric job ID.
        job_id: u64,
        /// The state the job was in before failing.
        prev_state: JobState,
    },
    /// A worker started or finished processing a job.
    Processing {
        /// The worker that picked up the job.
        worker_id: Uuid,
        /// Numeric job ID.
        job_id: u64,
        /// The job's new state.
        status: JobState,
    },
}
use std::{sync::Arc, time::Duration};
pub type Events = JobState;
use crate::stores::Store;
use serde::de::DeserializeOwned;
use typed_emitter::TypedEmitter;
pub(crate) type Emitter<R, P> = TypedEmitter<JobState, EventParameters<R, P>>;
pub(crate) type EventEmitter<R, P> = Arc<Emitter<R, P>>;
mod redis_events;
pub use redis_events::{QueueStreamEvent, StreamEventId};

use crate::KioResult;
impl<R: DeserializeOwned, P: DeserializeOwned> EventParameters<R, P> {
    /// Converts a raw store event into the corresponding typed [`EventParameters`] variant.
    pub async fn from_queue_event(event: QueueStreamEvent<R, P>) -> KioResult<Self> {
        let job_state = event.event;
        let job_id = event.job_id;
        let parameter = match job_state {
            JobState::Prioritized => Self::Prioritized {
                job_id: event.job_id,
                name: event.name,
                priority: event.priority.unwrap_or_default(),
            },
            JobState::Wait if event.prev.is_none() => Self::Added {
                job_id: event.job_id,
                name: event.name,
            },
            JobState::Wait => Self::WaitingToRun {
                job_id: event.job_id,
                prev_state: event.prev,
            },
            JobState::Stalled => Self::Stalled {
                job_id: event.job_id,
                prev_state: event.prev.unwrap_or_default(),
            },
            JobState::Active => Self::Active {
                job_id,
                prev_state: event.prev,
            },
            JobState::Paused => Self::Void,
            JobState::Resumed => Self::Void,
            JobState::Completed => {
                let job_metrics = event.metrics.unwrap_or_default();
                Self::Completed {
                    job_metrics,
                    job_id,
                    prev_state: event.prev,
                    expected_delay: Duration::from_millis(job_metrics.delay),
                    result: event.returned_value.expect("there is no result"),
                }
            }
            JobState::Failed => Self::Failed {
                reason: event.failed_reason.unwrap_or_default(),
                job_id: event.job_id,
                prev_state: event.prev.unwrap_or_default(),
            },
            JobState::Delayed => Self::Delayed {
                job_id: event.job_id,
                delay: Duration::from_millis(event.delay.unwrap_or_default()),
            },
            JobState::Progress => Self::Progress {
                job_id: event.job_id,
                data: event.progress_data.expect("expecting a value"),
            },
            JobState::Processing => Self::Processing {
                worker_id: event.worker_id.unwrap_or_default(),
                job_id: event.job_id,
                status: event.prev.unwrap_or_default(),
            },
            JobState::Obliterated => Self::Void,
        };

        Ok(parameter)
    }
}
