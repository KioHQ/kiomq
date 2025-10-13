use crate::{FailedDetails, Job, JobState};
use derive_more::Debug;
use redis::AsyncCommands;
use uuid::Uuid;
#[derive(Clone, Debug)]
pub enum EventParameters<D, R, P> {
    Prioritized {
        job_id: u64,
        name: Option<String>,
        priority: u64,
    },
    Added {
        job_id: u64,
        name: Option<String>,
    },
    WaitingToRun {
        job_id: u64,
        prev_state: Option<JobState>,
    },
    Delayed {
        job_id: u64,
        delay: Duration,
    },
    Active {
        job: Job<D, R, P>,
        prev_state: Option<JobState>,
    },
    Completed {
        job: Job<D, R, P>,
        prev_state: Option<JobState>,
        #[debug(skip)]
        result: R,
    },
    Void, // drained, closed,
    Progress {
        job_id: u64,
        #[debug(skip)]
        data: P,
    },
    Stalled {
        job_id: u64,
        prev_state: JobState,
    },
    Failed {
        reason: FailedDetails,
        job_id: u64,
        prev_state: JobState,
    },
    Processing {
        worker_id: Uuid,
        job_id: u64,
        status: JobState,
    },
}
use std::{sync::Arc, time::Duration};
pub type Events = JobState;
use serde::de::DeserializeOwned;
use typed_emitter::TypedEmitter;
pub(crate) type EventEmitter<D, R, P> = Arc<TypedEmitter<JobState, EventParameters<D, R, P>>>;
mod redis_events;
pub use redis_events::{QueueStreamEvent, StreamEventId};

use crate::KioResult;
impl<D: DeserializeOwned, R: DeserializeOwned, P: DeserializeOwned> EventParameters<D, R, P> {
    pub async fn from_queue_event(
        queue_name: &str,
        event: QueueStreamEvent<R, P>,
        mut conn: &mut deadpool_redis::Connection,
    ) -> KioResult<Self> {
        let job_state = event.event;
        let job_id = &event.job_id;
        let job_id_key = format!("{queue_name}{job_id}");
        let fetch_job = conn.hgetall::<_, Job<D, R, P>>(&job_id_key);
        let parameter = match job_state {
            JobState::Priorized => Self::Prioritized {
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
            JobState::Active => {
                let job = fetch_job.await?;
                Self::Active {
                    job,
                    prev_state: event.prev,
                }
            }
            JobState::Paused => Self::Void,
            JobState::Resumed => Self::Void,
            JobState::Completed => {
                let job = fetch_job.await?;
                Self::Completed {
                    job,
                    prev_state: event.prev,
                    result: event.retuned_value.expect("there is no result"),
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
