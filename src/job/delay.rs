use std::str::FromStr;

use chrono::{TimeDelta, Utc};
use croner::{Cron, errors::CronError};
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
            Self::TimeMilis(ms) => {
                if *ms <= 0 {
                    return None;
                }
                let next = ts + TimeDelta::milliseconds(*ms);
                Some(next.timestamp_millis())
            }
            Self::FromCron(cron) => cron
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
            Self::TimeMilis(ms) => *ms,
            Self::FromCron(cron) => {
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

#[cfg(test)]
mod tests {
    // Overflow panics originate inside chrono; pinning their exact text is brittle.
    #![allow(clippy::should_panic_without_expect)]
    use super::*;

    #[test]
    fn test_default_is_time_milis_zero() {
        assert_eq!(JobDelay::default(), JobDelay::TimeMilis(0));
    }

    #[test]
    fn test_next_occurrence_zero_returns_none() {
        // Zero means "run immediately", so there is no future timestamp.
        assert!(
            JobDelay::TimeMilis(0)
                .next_occurrance_timestamp_ms()
                .is_none()
        );
    }

    #[test]
    fn test_next_occurrence_negative_returns_none() {
        // Any non-positive delay collapses to "run immediately".
        assert!(
            JobDelay::TimeMilis(-5_000)
                .next_occurrance_timestamp_ms()
                .is_none()
        );
        assert!(
            JobDelay::TimeMilis(i64::MIN)
                .next_occurrance_timestamp_ms()
                .is_none()
        );
    }

    #[test]
    fn test_next_occurrence_positive_is_in_the_future() {
        let before = Utc::now().timestamp_millis();
        let next = JobDelay::TimeMilis(60_000)
            .next_occurrance_timestamp_ms()
            .expect("a positive delay must yield a timestamp");
        assert!(
            next >= before + 59_000,
            "expected roughly a minute into the future"
        );
    }

    #[test]
    fn test_as_diff_ms_returns_stored_value_directly() {
        let now = Utc::now();
        assert_eq!(JobDelay::TimeMilis(0).as_diff_ms(now), 0);
        assert_eq!(JobDelay::TimeMilis(1_234).as_diff_ms(now), 1_234);
        assert_eq!(JobDelay::TimeMilis(-1).as_diff_ms(now), -1);
        assert_eq!(JobDelay::TimeMilis(i64::MAX).as_diff_ms(now), i64::MAX);
        assert_eq!(JobDelay::TimeMilis(i64::MIN).as_diff_ms(now), i64::MIN);
    }

    #[test]
    fn test_from_i64_builds_time_milis() {
        assert_eq!(JobDelay::from(9_000_i64), JobDelay::TimeMilis(9_000));
        let via_into: JobDelay = 42_i64.into();
        assert_eq!(via_into, JobDelay::TimeMilis(42));
    }

    #[test]
    fn test_try_from_valid_cron_parses_to_cron_variant() {
        let delay = JobDelay::try_from("0 * * * * *").expect("valid cron should parse");
        assert!(matches!(delay, JobDelay::FromCron(_)));
    }

    #[test]
    fn test_try_from_invalid_cron_errors() {
        assert!(JobDelay::try_from("definitely not a cron").is_err());
    }

    #[test]
    fn test_cron_delay_next_occurrence_is_future_and_positive() {
        // Fires at second 0 of every minute; the next occurrence is strictly in
        // the future and at most ~a minute away.
        let delay = JobDelay::try_from("0 * * * * *").expect("valid cron");
        let now = Utc::now();
        let diff = delay.as_diff_ms(now);
        assert!(
            diff > 0,
            "the next cron occurrence must be strictly after `dt`"
        );
        assert!(
            diff <= 61_000,
            "a per-minute schedule fires within ~a minute"
        );
        let ts = delay
            .next_occurrance_timestamp_ms()
            .expect("a cron delay must yield a timestamp");
        assert!(ts > now.timestamp_millis());
    }

    #[cfg(feature = "redis-store")]
    #[test]
    fn test_time_milis_serde_untagged_roundtrip() {
        let delay = JobDelay::TimeMilis(7_500);
        let mut bytes = simd_json::to_string(&delay)
            .expect("serialise")
            .into_bytes();
        let back: JobDelay = simd_json::from_slice(&mut bytes).expect("deserialise");
        assert_eq!(back, delay);

        // A bare JSON number must deserialise into the `TimeMilis` arm.
        let mut raw = b"250".to_vec();
        let parsed: JobDelay = simd_json::from_slice(&mut raw).expect("parse number");
        assert_eq!(parsed, JobDelay::TimeMilis(250));
    }

    // KNOWN ROBUSTNESS GAP: a very large positive delay overflows chrono's
    // `DateTime + TimeDelta` and panics instead of saturating. Pinned here with
    // `#[should_panic]`; see the summary for details.
    #[test]
    #[should_panic]
    fn test_next_occurrence_i64_max_overflows_and_panics() {
        JobDelay::TimeMilis(i64::MAX).next_occurrance_timestamp_ms();
    }
}
