use std::str::FromStr;

use chrono::{TimeDelta, Utc};
use croner::{errors::CronError, Cron};
use serde::{Deserialize, Serialize};

use crate::Dt;
#[derive(Debug, Clone, PartialEq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
#[derive(derive_more::Display)]
pub enum JobDelay {
    TimeMins(i64),
    FromCron(Box<Cron>),
}
impl Default for JobDelay {
    fn default() -> Self {
        Self::TimeMins(0)
    }
}
impl JobDelay {
    pub fn next_occurrance_timestamp_ms(&self) -> Option<i64> {
        let ts = Utc::now();
        match self {
            JobDelay::TimeMins(ms) => {
                if *ms <= 0 {
                    return None;
                }
                let next = ts + TimeDelta::milliseconds(*ms);
                Some(next.timestamp_millis())
            }
            JobDelay::FromCron(cron) => cron
                .find_next_occurrence(&ts, false)
                .ok()
                .map(|dt| dt.timestamp_millis()),
        }
    }
    pub fn as_diff_ms(&self, dt: Dt) -> i64 {
        match self {
            JobDelay::TimeMins(ms) => *ms,
            JobDelay::FromCron(cron) => {
                let next_dt = cron.find_next_occurrence(&dt, false).expect("failed");
                (next_dt - dt).num_milliseconds()
            }
        }
    }
}

impl From<Cron> for JobDelay {
    fn from(value: Cron) -> Self {
        Self::FromCron(Box::new(value))
    }
}
impl From<i64> for JobDelay {
    fn from(value: i64) -> Self {
        Self::TimeMins(value)
    }
}
impl TryFrom<&str> for JobDelay {
    type Error = CronError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let parsed = Cron::from_str(value)?;
        Ok(Self::FromCron(Box::new(parsed)))
    }
}
