use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
/// alias for DateTime<Utc>
type Dt = DateTime<Utc>;
use std::sync::Arc;
#[derive(Debug, Serialize, Deserialize, Default)]
pub enum JobState {
    #[default]
    Waiting,
    Stalled,
    Active,
    Completed,
}
#[derive(Serialize, Deserialize, Default)]

pub struct Job<'a, D, R, P> {
    pub id: u64,
    pub name: &'a str,
    pub state: JobState,
    pub queue: Option<Queue>,
    pub progress: Option<P>,
    pub attempts_made: u64,
    pub delay: u64,
    pub data: D,
    pub return_value: Option<R>,
    pub stack_trace: Vec<&'a str>,
    pub failed_reason: Option<&'a str>,
    pub processed_on: Option<Dt>,
    pub finished_on: Option<Dt>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Queue;
