use crate::{Job, JobState};
#[derive(Clone, Debug)]
pub enum EventParameters<D, R, P> {
    Active {
        job: Job<D, R, P>,
        prev_state: Option<JobState>,
    },
    Completed {
        job: Job<D, R, P>,
        prev_state: Option<JobState>,
        result: R,
    },
    Error(String),
    JobIdStatePair {
        job_id: String,
        prev: JobState,
    },
    Void, // drained, closed,
    Progress {
        job: Job<D, R, P>,
        progress: P,
    },
    Stalled {
        job_id: String,
        prev_state: JobState,
    },
    Failed {
        job: Job<D, R, P>,
        error: String,
        prev_state: JobState,
    },
}
use std::sync::Arc;
pub type Events = JobState;
use typed_emitter::TypedEmitter;
pub(crate) type EventEmitter<D, R, P> = Arc<TypedEmitter<JobState, EventParameters<D, R, P>>>;
