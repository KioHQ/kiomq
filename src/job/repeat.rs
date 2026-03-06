use super::{BackOff, BackOffJobOptions};
use chrono::{TimeDelta, Utc};
use croner::{errors::CronError, Cron};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
/// Repeat / scheduling policy for a job.
///
/// When a [`Repeat`] is set on a job via [`crate::JobOptions`]'s `repeat` field,
/// the queue automatically re-enqueues the job after each successful run according to
/// the policy.
///
/// | Variant | Behaviour |
/// |---------|-----------|
/// | `WithCron` | Re-run at the next cron-schedule occurrence. |
/// | `WithBackOff` | Re-run after a backoff-derived delay. |
/// | `Every { delay_ms, max_attempts }` | Re-run every `delay_ms` ms, up to `max_attempts` times (unlimited if `None`). |
/// | `Immediately(max_attempts)` | Re-run as quickly as possible until `max_attempts` is reached. |
///
/// # Examples
///
/// ```rust
/// use kiomq::Repeat;
///
/// // Repeat every 10 seconds, at most 5 times.
/// let policy = Repeat::Every { delay_ms: 10_000, max_attempts: Some(5) };
/// ```
#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq)]
/// Repeats options for job: either Run immediately, using backoff options or a cron schedule
pub enum Repeat {
    /// Re-run at the next occurrence of a cron schedule.
    WithCron(Box<Cron>),
    /// Re-run after a delay calculated by a [`BackOffJobOptions`] strategy.
    WithBackOff(BackOffJobOptions),
    /// Re-run every `delay_ms` milliseconds, at most `max_attempts` times
    /// (unlimited when `None`).
    Every {
        /// Delay between runs in milliseconds.
        delay_ms: i64,
        /// Maximum number of repetitions; `None` means unlimited.
        max_attempts: Option<u64>,
    },
    /// Re-run as fast as possible until `max_attempts` is reached.
    Immediately(u64),
}
impl Repeat {
    /// Constructs a [`Repeat::WithCron`] from a cron expression string.
    ///
    /// # Errors
    ///
    /// Returns a [`CronError`] if `pattern` is not a valid cron expression.
    pub fn from_cron_str(pattern: &str) -> Result<Self, CronError> {
        let cron = Cron::from_str(pattern)?;
        Ok(Self::WithCron(Box::new(cron)))
    }
    /// Constructs a [`Repeat::WithBackOff`] from the given options.
    #[must_use]
    pub const fn from_back_off(opts: BackOffJobOptions) -> Self {
        Self::WithBackOff(opts)
    }
    /// Constructs a [`Repeat::Every`] that fires every `every_ms` milliseconds,
    /// stopping after `max_attempts` runs (unlimited when `None`).
    #[must_use]
    pub const fn repeat_every_for_times(every_ms: i64, max_attempts: Option<u64>) -> Self {
        Self::Every {
            delay_ms: every_ms,
            max_attempts,
        }
    }
    /// Returns the Unix timestamp in milliseconds at which the job should next
    /// run, or `None` when the policy has been exhausted.
    ///
    /// A return value of `0` is a sentinel meaning "move to the wait list
    /// immediately" (used by [`Repeat::Immediately`]).
    #[must_use]
    pub fn next_occurrence(&self, backoff: &BackOff, attempts: u64) -> Option<i64> {
        let now = Utc::now();
        match self {
            Self::WithCron(cron) => cron
                .find_next_occurrence(&now, false)
                .ok()
                .map(|dt| dt.timestamp_millis()),
            Self::WithBackOff(opts) => {
                let opts = BackOff::normalize(Some(opts))?;
                let delay_fn = backoff.lookup_strategy(opts, None)?;
                let next_delay_ms = delay_fn(attempts as i64);
                let next_dt = now + TimeDelta::milliseconds(next_delay_ms);
                Some(next_dt.timestamp_millis())
            }
            Self::Every {
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
            Self::Immediately(max_attempts) => {
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
