use super::{BackOff, BackOffJobOptions};
use chrono::{TimeDelta, Utc};
use croner::{Cron, errors::CronError};
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

#[cfg(test)]
mod tests {
    // Overflow panics originate inside chrono; pinning their exact text is brittle.
    #![allow(clippy::should_panic_without_expect, clippy::doc_markdown)]
    use super::*;
    use crate::BackOffOptions;

    #[test]
    fn test_every_unlimited_always_reschedules() {
        let backoff = BackOff::new();
        let repeat = Repeat::Every {
            delay_ms: 1_000,
            max_attempts: None,
        };
        // With no cap, even an enormous attempt count keeps rescheduling.
        assert!(repeat.next_occurrence(&backoff, u64::MAX).is_some());
    }

    #[test]
    fn test_every_stops_exactly_at_max_attempts() {
        let backoff = BackOff::new();
        let repeat = Repeat::Every {
            delay_ms: 1_000,
            max_attempts: Some(3),
        };
        assert!(
            repeat.next_occurrence(&backoff, 2).is_some(),
            "one below the cap still runs"
        );
        assert!(
            repeat.next_occurrence(&backoff, 3).is_none(),
            "reaching the cap stops the repeat"
        );
        assert!(
            repeat.next_occurrence(&backoff, 4).is_none(),
            "beyond the cap stays stopped"
        );
    }

    #[test]
    fn test_every_zero_max_attempts_never_runs() {
        let backoff = BackOff::new();
        let repeat = Repeat::Every {
            delay_ms: 1_000,
            max_attempts: Some(0),
        };
        assert!(repeat.next_occurrence(&backoff, 0).is_none());
    }

    #[test]
    fn test_every_returns_future_timestamp() {
        let backoff = BackOff::new();
        let now = Utc::now().timestamp_millis();
        let ts = Repeat::Every {
            delay_ms: 30_000,
            max_attempts: None,
        }
        .next_occurrence(&backoff, 0)
        .expect("should schedule a future run");
        assert!(ts >= now + 29_000);
    }

    #[test]
    fn test_immediately_stops_at_max_attempts() {
        let backoff = BackOff::new();
        let repeat = Repeat::Immediately(3);
        assert_eq!(repeat.next_occurrence(&backoff, 0), Some(0));
        assert_eq!(repeat.next_occurrence(&backoff, 2), Some(0));
        assert!(repeat.next_occurrence(&backoff, 3).is_none());
        assert!(repeat.next_occurrence(&backoff, 100).is_none());
    }

    #[test]
    fn test_immediately_zero_never_runs() {
        let backoff = BackOff::new();
        assert!(
            Repeat::Immediately(0)
                .next_occurrence(&backoff, 0)
                .is_none()
        );
    }

    #[test]
    fn test_immediately_uses_zero_sentinel() {
        let backoff = BackOff::new();
        // `0` is a sentinel meaning "enqueue immediately", not a real timestamp.
        assert_eq!(
            Repeat::Immediately(u64::MAX).next_occurrence(&backoff, 0),
            Some(0)
        );
    }

    #[test]
    fn test_with_backoff_number_zero_returns_none() {
        // A zero fixed delay normalises to `None`, so no reschedule occurs.
        let backoff = BackOff::new();
        let repeat = Repeat::WithBackOff(BackOffJobOptions::Number(0));
        assert!(repeat.next_occurrence(&backoff, 1).is_none());
    }

    #[test]
    fn test_with_backoff_fixed_returns_future_timestamp() {
        let backoff = BackOff::new();
        let now = Utc::now().timestamp_millis();
        let repeat = Repeat::WithBackOff(BackOffJobOptions::Number(5_000));
        let ts = repeat
            .next_occurrence(&backoff, 1)
            .expect("fixed backoff should reschedule");
        assert!(ts >= now + 4_000);
    }

    #[test]
    fn test_with_cron_next_occurrence_is_future() {
        let backoff = BackOff::new();
        let now = Utc::now().timestamp_millis();
        let repeat = Repeat::from_cron_str("0 * * * * *").expect("valid cron");
        let ts = repeat
            .next_occurrence(&backoff, 0)
            .expect("cron should reschedule");
        assert!(ts > now);
    }

    #[test]
    fn test_from_cron_str_rejects_invalid_pattern() {
        assert!(Repeat::from_cron_str("clearly not valid").is_err());
    }

    #[test]
    fn test_repeat_every_for_times_constructor() {
        let repeat = Repeat::repeat_every_for_times(2_000, Some(4));
        assert_eq!(
            repeat,
            Repeat::Every {
                delay_ms: 2_000,
                max_attempts: Some(4),
            }
        );
    }

    #[test]
    fn test_from_back_off_constructor() {
        let opts = BackOffJobOptions::Number(1_000);
        assert_eq!(
            Repeat::from_back_off(opts.clone()),
            Repeat::WithBackOff(opts)
        );
    }

    #[test]
    fn test_from_tuple_builds_every() {
        let repeat: Repeat = (1_500_i64, Some(7_u64)).into();
        assert_eq!(
            repeat,
            Repeat::Every {
                delay_ms: 1_500,
                max_attempts: Some(7),
            }
        );
    }

    #[test]
    fn test_from_backoff_job_options_builds_with_backoff() {
        let repeat: Repeat = BackOffJobOptions::Number(250).into();
        assert_eq!(repeat, Repeat::WithBackOff(BackOffJobOptions::Number(250)));
    }

    #[test]
    fn test_try_from_str_valid_cron() {
        let repeat = Repeat::try_from("0 * * * * *").expect("valid cron");
        assert!(matches!(repeat, Repeat::WithCron(_)));
    }

    #[cfg(feature = "redis-store")]
    #[test]
    fn test_every_and_immediately_serde_roundtrip() {
        let cases = [
            Repeat::Every {
                delay_ms: 10_000,
                max_attempts: Some(5),
            },
            Repeat::Every {
                delay_ms: 1,
                max_attempts: None,
            },
            Repeat::Immediately(9),
            Repeat::WithBackOff(BackOffJobOptions::Number(500)),
        ];
        for repeat in cases {
            let mut bytes = simd_json::to_string(&repeat)
                .expect("serialise")
                .into_bytes();
            let back: Repeat = simd_json::from_slice(&mut bytes).expect("deserialise");
            assert_eq!(back, repeat);
        }
    }

    // KNOWN ROBUSTNESS GAP: the exponential strategy correctly *saturates* the
    // delay to `i64::MAX`, but `next_occurrence` then computes
    // `now + TimeDelta::milliseconds(delay)`, which overflows chrono's DateTime
    // and panics. A repeat-with-exponential-backoff job that runs enough times
    // therefore panics rather than rescheduling. Pinned with `#[should_panic]`;
    // see the summary.
    #[test]
    #[should_panic]
    fn test_with_backoff_exponential_high_attempts_overflows_and_panics() {
        let backoff = BackOff::new();
        let repeat = Repeat::WithBackOff(BackOffJobOptions::Opts(BackOffOptions {
            type_: Some("exponential".into()),
            delay: Some(100),
        }));
        let _unreached = repeat.next_occurrence(&backoff, 64);
    }

    // KNOWN ROBUSTNESS GAP: an `i64::MAX` per-run delay overflows the same
    // `now + TimeDelta` addition and panics.
    #[test]
    #[should_panic]
    fn test_every_i64_max_delay_overflows_and_panics() {
        let backoff = BackOff::new();
        let _unreached = Repeat::Every {
            delay_ms: i64::MAX,
            max_attempts: None,
        }
        .next_occurrence(&backoff, 0);
    }
}
