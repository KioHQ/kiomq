use std::str::FromStr;

use chrono::{TimeDelta, Utc};
use croner::{errors::CronError, Cron};
use serde::{Deserialize, Serialize};

use crate::Dt;
/// Controls when a job becomes eligible to run.
///
/// | Variant | Behaviour |
/// |---------|-----------|
/// | `TimeMilis(0)` *(default)* | Run immediately. |
/// | `TimeMilis(n)` | Delay by `n` milliseconds. |
/// | `FromCron(expr)` | Delay until the next occurrence of the cron schedule. |
///
/// # Examples
///
/// ```rust
/// use kiomq::JobOptions;
///
/// // Delay by 5 seconds
/// let opts = JobOptions { delay: 5_000i64.into(), ..Default::default() };
/// ```
#[derive(Debug, Clone, PartialEq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
#[derive(derive_more::Display)]
pub enum JobDelay {
    TimeMilis(i64),
    FromCron(Box<Cron>),
}
impl Default for JobDelay {
    fn default() -> Self {
        Self::TimeMilis(0)
    }
}
impl JobDelay {
    /// Returns the timestamp (in milliseconds since the Unix epoch) at which
    /// the job should next become eligible to run, or `None` if the delay is
    /// zero (run immediately).
    pub fn next_occurrance_timestamp_ms(&self) -> Option<i64> {
        let ts = Utc::now();
        match self {
            JobDelay::TimeMilis(ms) => {
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
    /// Returns the delay in milliseconds relative to `dt`.
    ///
    /// For `TimeMilis`, this is the stored value directly.  For `FromCron`,
    /// this is the number of milliseconds until the next cron occurrence after
    /// `dt`.
    pub fn as_diff_ms(&self, dt: Dt) -> i64 {
        match self {
            JobDelay::TimeMilis(ms) => *ms,
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
        Self::TimeMilis(value)
    }
}
impl TryFrom<&str> for JobDelay {
    type Error = CronError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let parsed = Cron::from_str(value)?;
        Ok(Self::FromCron(Box::new(parsed)))
    }
}
