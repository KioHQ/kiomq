use crate::Dt;
use chrono::{TimeDelta, Utc};
use serde::{Deserialize, Serialize};

use crate::KioResult;

use super::{BackOff, BackOffJobOptions};
use std::{str::FromStr, sync::Arc};

use croner::{errors::CronError, Cron};
#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq)]
/// Repeats options for job: either Run immediately, using backoff options or a cron schedule
pub enum Repeat {
    /// use a cron pattern to use;
    WithCron(Box<Cron>),
    /// Use back opts
    WithBackOff(BackOffJobOptions),
    /// Every (ms, max_attempts)
    Every {
        delay_ms: i64,
        max_attempts: Option<u64>,
    },
    /// Repeat Immediately if attempts are less than specified max_attempts
    Immediately(u64),
}
impl Repeat {
    pub fn from_cron_str(pattern: &str) -> Result<Self, CronError> {
        let cron = Cron::from_str(pattern)?;
        Ok(Self::WithCron(Box::new(cron)))
    }
    pub fn from_back_off(opts: BackOffJobOptions) -> Self {
        Self::WithBackOff(opts)
    }
    pub fn repeat_every_for_times(every_ms: i64, max_attempts: Option<u64>) -> Self {
        Self::Every {
            delay_ms: every_ms,
            max_attempts,
        }
    }
    /// returns the next delayed in time from now in timestamp_mills
    pub fn next_occurrence(&self, backoff: &BackOff, attempts: u64) -> Option<i64> {
        let now = Utc::now();
        match self {
            Repeat::WithCron(cron) => cron
                .find_next_occurrence(&now, false)
                .ok()
                .map(|dt| dt.timestamp_millis()),
            Repeat::WithBackOff(opts) => {
                let opts = BackOff::normalize(Some(opts))?;
                let delay_fn = backoff.lookup_strategy(opts, None)?;
                let next_delay_ms = delay_fn(attempts as i64);
                let next_dt = now + TimeDelta::milliseconds(next_delay_ms);
                Some(next_dt.timestamp_millis())
            }
            Repeat::Every {
                delay_ms,
                max_attempts,
            } => {
                if let Some(max_ts) = max_attempts {
                    if attempts >= *max_ts {
                        return None;
                    }
                }
                let next_dt = now + TimeDelta::milliseconds(*delay_ms);
                Some(next_dt.timestamp_millis())
            }
            Repeat::Immediately(max_attempts) => {
                if attempts >= *max_attempts {
                    return None;
                }
                // add to waiting job list immediately or worker queue
                // use Sentinel value of 0 here
                Some(0)
            }
        }
    }
}
impl From<BackOffJobOptions> for Repeat {
    fn from(value: BackOffJobOptions) -> Self {
        Self::from_back_off(value)
    }
}
impl From<(i64, Option<u64>)> for Repeat {
    fn from(value: (i64, Option<u64>)) -> Self {
        Self::Every {
            delay_ms: value.0,
            max_attempts: value.1,
        }
    }
}
impl TryFrom<&str> for Repeat {
    type Error = CronError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::from_cron_str(value)
    }
}
