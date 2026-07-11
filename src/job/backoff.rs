use compact_str::{CompactString, ToCompactString};
use crossbeam_skiplist::SkipMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
type BackoffFn = dyn Fn(i64) -> StoredFn + Send + Sync;
/// A per-attempt delay function: receives the attempt count and returns the
/// delay in milliseconds.
pub type StoredFn = Arc<dyn Fn(i64) -> i64 + Send + Sync>;

/// Detailed backoff configuration.
///
/// Pair with [`BackOffJobOptions::Opts`] or [`crate::QueueOpts`]'s `default_backoff` field.
///
/// # Built-in strategies
///
/// | `type_` | Formula |
/// |---------|---------|
/// | `"exponential"` | `2^attempt * delay_ms` |
/// | `"fixed"` | `delay_ms` (constant) |
///
/// Custom strategies can be registered on a queue via
/// [`crate::Queue::register_backoff_strategy`].
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct BackOffOptions {
    /// Name of the backoff strategy.  Built-ins: `"exponential"`, `"fixed"`.
    #[serde(rename = "type")]
    pub type_: Option<CompactString>,
    /// Base delay in milliseconds used by the strategy formula.
    pub delay: Option<i64>,
}
/// Specifies the backoff policy for job retries.
///
/// | Variant | Meaning |
/// |---------|---------|
/// | `Number(n)` | Use the `"fixed"` strategy with a delay of `n` ms. |
/// | `Opts(opts)` | Use a fully configured [`BackOffOptions`]. |
///
/// # Examples
///
/// ```rust
/// use kiomq::{BackOffJobOptions, BackOffOptions};
///
/// // Simple fixed delay of 1 second
/// let simple = BackOffJobOptions::Number(1_000);
///
/// // Exponential backoff starting at 200 ms
/// let exp = BackOffJobOptions::Opts(BackOffOptions {
///     type_: Some("exponential".to_owned()),
///     delay: Some(200),
/// });
/// ```
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
#[serde(untagged)]
pub enum BackOffJobOptions {
    /// Use the `"fixed"` strategy with this constant delay in milliseconds.
    Number(i64),
    /// Fully configured backoff options including strategy name and base delay.
    Opts(BackOffOptions),
}

/// Registry of backoff strategies used to schedule job retries.
///
/// Two built-in strategies are registered by default:
///
/// - `"exponential"` — delay grows as `2^attempt * base_delay_ms`.
/// - `"fixed"` — constant delay regardless of attempt count.
///
/// Additional strategies can be added with [`BackOff::register`].
#[derive(Clone, Default)]
pub struct BackOff {
    /// Map of strategy name → factory function.
    pub builtin_strategies: Arc<SkipMap<CompactString, Arc<BackoffFn>>>,
}

impl BackOff {
    /// Creates a new `BackOff` registry pre-loaded with the `"exponential"`
    /// and `"fixed"` built-in strategies.
    #[must_use]
    pub fn new() -> Self {
        let backoff = Self::default();
        backoff.register("exponential", |delay: i64| {
            Arc::new(move |atempts: i64| -> i64 {
                // Saturate rather than panic/wrap: uncapped repeat jobs push the
                // attempt count arbitrarily high, and `2^attempt * delay`
                // overflows i64 long before that.
                2_i64
                    .saturating_pow(u32::try_from(atempts).unwrap_or(u32::MAX))
                    .saturating_mul(delay)
            })
        });

        backoff.register("fixed", |delay: i64| Arc::new(move |_attempts| delay));
        backoff
    }

    /// Registers a custom backoff strategy under `name`.
    ///
    /// The `strategy` factory receives the base delay and returns a
    /// [`StoredFn`] that maps attempt → `delay_ms`. If a strategy with the
    /// same name is already registered it will be overwritten.
    pub fn register(
        &self,
        name: &str,
        strategy: impl Fn(i64) -> Arc<dyn Fn(i64) -> i64 + Send + Sync> + 'static + Send + Sync,
    ) {
        self.builtin_strategies
            .insert(name.to_compact_string(), Arc::new(strategy));
    }
    /// Normalises a [`BackOffJobOptions`] into a [`BackOffOptions`], returning
    /// `None` when no backoff is configured or the numeric delay is zero.
    #[must_use]
    pub fn normalize(backoff: Option<&BackOffJobOptions>) -> Option<BackOffOptions> {
        let backoff = backoff?;
        match backoff {
            BackOffJobOptions::Number(num) => {
                if *num == 0 {
                    return None;
                }
                let opts = BackOffOptions {
                    delay: Some(*num),
                    type_: Some("fixed".to_compact_string()),
                };
                Some(opts)
            }
            BackOffJobOptions::Opts(opts) => Some(opts.clone()),
        }
    }
    /// Calculates the delay in milliseconds for the given `attempts` count.
    ///
    /// Returns `None` if `backoff_opts` is `None` or no matching strategy is
    /// found.
    #[must_use]
    pub fn calculate(
        &self,
        backoff_opts: Option<BackOffOptions>,
        attempts: i64,
        custom_strategy: Option<StoredFn>,
    ) -> Option<i64> {
        if let Some(opts) = backoff_opts
            && let Some(strategy) = self.lookup_strategy(opts, custom_strategy)
        {
            let calculated_delay = strategy(attempts);
            return Some(calculated_delay);
        }

        None
    }

    /// Returns `true` if a strategy with the given `key` has been registered.
    #[must_use]
    pub fn has_strategy(&self, key: &str) -> bool {
        self.builtin_strategies.contains_key(key)
    }

    /// Looks up a [`StoredFn`] for the strategy described by `backoff`.
    ///
    /// Falls back to `custom_strategy` when no built-in match is found.
    /// Returns `None` if neither source provides a strategy.
    #[must_use]
    pub fn lookup_strategy(
        &self,
        backoff: BackOffOptions,
        custom_strategy: Option<StoredFn>,
    ) -> Option<StoredFn>
where {
        if let Some(t) = backoff.type_
            && let (Some(entry), Some(delay)) =
                (self.builtin_strategies.get(t.as_str()), backoff.delay)
        {
            let strategy = entry.value();
            return Some(strategy(delay));
        }

        if let Some(strategy) = custom_strategy {
            return Some(strategy);
        }

        None
    }
}

impl std::fmt::Debug for BackOff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let keys: Vec<_> = self
            .builtin_strategies
            .iter()
            .map(|v| v.key().clone())
            .collect();

        f.debug_struct("BackOff")
            .field("builtin_strategies", &keys)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponential_backoff() {
        let backoff = BackOff::new();
        let strategy = backoff
            .lookup_strategy(
                BackOffOptions {
                    delay: Some(100),
                    type_: Some("exponential".to_compact_string()),
                },
                None,
            )
            .expect("exponential strategy should exist");
        assert_eq!(strategy(1), 200); // 2^1 * 100
        assert_eq!(strategy(2), 400);
        assert_eq!(strategy(3), 800);
        assert_eq!(strategy(4), 1600);
        assert_eq!(strategy(5), 3200);
    }
    #[test]
    fn test_exponential_backoff_high_attempt_does_not_overflow() {
        // A repeat-with-exponential-backoff job increments `attempts` on every
        // run and is never capped, so the attempt count eventually reaches the
        // point where `2^attempt * delay` overflows i64. The delay function
        // must saturate rather than panic (debug) / wrap to a negative delay
        // (release).
        let backoff = BackOff::new();
        let strategy = backoff
            .lookup_strategy(
                BackOffOptions {
                    delay: Some(100),
                    type_: Some("exponential".to_compact_string()),
                },
                None,
            )
            .expect("exponential strategy should exist");

        let delay = strategy(64);
        assert_eq!(
            delay,
            i64::MAX,
            "overflowing backoff must saturate, not wrap"
        );
    }
    #[test]
    fn test_fixed_back() {
        let backoff = BackOff::new();
        let strategy = backoff
            .lookup_strategy(
                BackOffOptions {
                    delay: Some(100),
                    type_: Some("fixed".to_compact_string()),
                },
                None,
            )
            .expect("fixed strategy should exist");
        assert_eq!(strategy(2), 100);
        assert_eq!(strategy(3), 100);
        assert_eq!(strategy(i64::MAX), 100);
    }

    #[test]
    fn test_normalize_number_zero_returns_none() {
        // A zero fixed delay is treated as "no backoff configured".
        assert!(BackOff::normalize(Some(&BackOffJobOptions::Number(0))).is_none());
    }

    #[test]
    fn test_normalize_none_returns_none() {
        assert!(BackOff::normalize(None).is_none());
    }

    #[test]
    fn test_normalize_negative_number_uses_fixed_strategy() {
        // Only exactly-zero is dropped; a negative delay still normalises to
        // the "fixed" strategy carrying that (negative) delay.
        let opts = BackOff::normalize(Some(&BackOffJobOptions::Number(-100)))
            .expect("negative delay should normalise");
        assert_eq!(opts.delay, Some(-100));
        assert_eq!(opts.type_.as_deref(), Some("fixed"));
    }

    #[test]
    fn test_normalize_opts_pass_through_unchanged() {
        let source = BackOffOptions {
            type_: Some("exponential".to_compact_string()),
            delay: Some(42),
        };
        let normalised = BackOff::normalize(Some(&BackOffJobOptions::Opts(source.clone())))
            .expect("opts should pass through");
        assert_eq!(normalised, source);
    }

    #[test]
    fn test_exponential_attempt_zero_returns_base_delay() {
        let backoff = BackOff::new();
        let strategy = backoff
            .lookup_strategy(
                BackOffOptions {
                    delay: Some(250),
                    type_: Some("exponential".to_compact_string()),
                },
                None,
            )
            .expect("exponential strategy should exist");
        // 2^0 * 250 == 250
        assert_eq!(strategy(0), 250);
    }

    #[test]
    fn test_exponential_negative_attempts_saturate_to_max() {
        // `u32::try_from` fails for a negative attempt count and falls back to
        // `u32::MAX`, which saturates the power (and product) to `i64::MAX`.
        let backoff = BackOff::new();
        let strategy = backoff
            .lookup_strategy(
                BackOffOptions {
                    delay: Some(100),
                    type_: Some("exponential".to_compact_string()),
                },
                None,
            )
            .expect("exponential strategy should exist");
        assert_eq!(strategy(-1), i64::MAX);
    }

    #[test]
    fn test_exponential_negative_delay_saturates_to_min() {
        // A saturated power multiplied by a negative base delay must saturate to
        // `i64::MIN` rather than wrapping to a positive value.
        let backoff = BackOff::new();
        let strategy = backoff
            .lookup_strategy(
                BackOffOptions {
                    delay: Some(-100),
                    type_: Some("exponential".to_compact_string()),
                },
                None,
            )
            .expect("exponential strategy should exist");
        assert_eq!(strategy(64), i64::MIN);
    }

    #[test]
    fn test_fixed_ignores_attempt_count_at_boundaries() {
        let backoff = BackOff::new();
        let strategy = backoff
            .lookup_strategy(
                BackOffOptions {
                    delay: Some(777),
                    type_: Some("fixed".to_compact_string()),
                },
                None,
            )
            .expect("fixed strategy should exist");
        assert_eq!(strategy(0), 777);
        assert_eq!(strategy(i64::MAX), 777);
        assert_eq!(strategy(i64::MIN), 777);
    }

    #[test]
    fn test_has_strategy_reports_builtins_and_unknowns() {
        let backoff = BackOff::new();
        assert!(backoff.has_strategy("exponential"));
        assert!(backoff.has_strategy("fixed"));
        assert!(!backoff.has_strategy("does-not-exist"));
    }

    #[test]
    fn test_register_overwrites_existing_strategy() {
        let backoff = BackOff::new();
        backoff.register("fixed", |_delay| Arc::new(|_attempts| 9));
        let strategy = backoff
            .lookup_strategy(
                BackOffOptions {
                    delay: Some(100),
                    type_: Some("fixed".to_compact_string()),
                },
                None,
            )
            .expect("fixed strategy should exist");
        assert_eq!(strategy(1), 9, "re-registering must overwrite the built-in");
    }

    #[test]
    fn test_lookup_falls_back_to_custom_when_type_unknown() {
        let backoff = BackOff::new();
        let custom: StoredFn = Arc::new(|attempts| attempts * 3);
        let strategy = backoff
            .lookup_strategy(
                BackOffOptions {
                    delay: Some(100),
                    type_: Some("no-such-strategy".to_compact_string()),
                },
                Some(custom),
            )
            .expect("custom fallback should be used");
        assert_eq!(strategy(4), 12);
    }

    #[test]
    fn test_lookup_falls_back_to_custom_when_delay_missing() {
        let backoff = BackOff::new();
        let custom: StoredFn = Arc::new(|_attempts| 5);
        let strategy = backoff
            .lookup_strategy(
                BackOffOptions {
                    delay: None,
                    type_: Some("exponential".to_compact_string()),
                },
                Some(custom),
            )
            .expect("missing delay should fall through to the custom strategy");
        assert_eq!(strategy(1), 5);
    }

    #[test]
    fn test_lookup_returns_none_without_builtin_or_custom() {
        let backoff = BackOff::new();
        // No `type_` at all.
        assert!(
            backoff
                .lookup_strategy(
                    BackOffOptions {
                        delay: Some(100),
                        type_: None,
                    },
                    None,
                )
                .is_none()
        );
        // Known `type_` but missing delay and no custom fallback.
        assert!(
            backoff
                .lookup_strategy(
                    BackOffOptions {
                        delay: None,
                        type_: Some("exponential".to_compact_string()),
                    },
                    None,
                )
                .is_none()
        );
        // Unknown `type_` and no custom fallback.
        assert!(
            backoff
                .lookup_strategy(
                    BackOffOptions {
                        delay: Some(1),
                        type_: Some("unknown".to_compact_string()),
                    },
                    None,
                )
                .is_none()
        );
    }

    #[test]
    fn test_lookup_prefers_builtin_over_custom() {
        let backoff = BackOff::new();
        let custom: StoredFn = Arc::new(|_attempts| -1);
        let strategy = backoff
            .lookup_strategy(
                BackOffOptions {
                    delay: Some(100),
                    type_: Some("fixed".to_compact_string()),
                },
                Some(custom),
            )
            .expect("a matching built-in should win over the custom fallback");
        assert_eq!(strategy(1), 100);
    }

    #[test]
    fn test_calculate_none_opts_returns_none() {
        let backoff = BackOff::new();
        assert!(backoff.calculate(None, 5, None).is_none());
    }

    #[test]
    fn test_calculate_uses_custom_strategy_when_no_builtin_matches() {
        let backoff = BackOff::new();
        let custom: StoredFn = Arc::new(|attempts| attempts + 1);
        let delay = backoff
            .calculate(
                Some(BackOffOptions {
                    delay: None,
                    type_: None,
                }),
                6,
                Some(custom),
            )
            .expect("custom strategy should produce a delay");
        assert_eq!(delay, 7);
    }

    #[test]
    fn test_calculate_fixed_strategy() {
        let backoff = BackOff::new();
        let delay = backoff
            .calculate(
                Some(BackOffOptions {
                    delay: Some(321),
                    type_: Some("fixed".to_compact_string()),
                }),
                99,
                None,
            )
            .expect("fixed strategy should produce a delay");
        assert_eq!(delay, 321);
    }

    #[test]
    fn test_clone_shares_strategy_registry() {
        // The registry lives behind an `Arc`, so strategies registered on a clone
        // are observable through the original and vice versa.
        let original = BackOff::new();
        let clone = original.clone();
        clone.register("cloned-only", |_delay| Arc::new(|_attempts| 1));
        assert!(
            original.has_strategy("cloned-only"),
            "clones must share the underlying registry"
        );
    }

    #[cfg(feature = "redis-store")]
    #[test]
    fn test_back_off_job_options_serde_untagged_roundtrip() {
        let number = BackOffJobOptions::Number(500);
        let mut bytes = simd_json::to_string(&number)
            .expect("serialise")
            .into_bytes();
        let back: BackOffJobOptions = simd_json::from_slice(&mut bytes).expect("deserialise");
        assert_eq!(back, number);

        // A bare JSON number must deserialise into the `Number` variant.
        let mut raw = b"1000".to_vec();
        let parsed: BackOffJobOptions = simd_json::from_slice(&mut raw).expect("parse number");
        assert_eq!(parsed, BackOffJobOptions::Number(1000));

        let opts = BackOffJobOptions::Opts(BackOffOptions {
            type_: Some("exponential".to_compact_string()),
            delay: Some(200),
        });
        let mut ob = simd_json::to_string(&opts).expect("serialise").into_bytes();
        let ob_back: BackOffJobOptions = simd_json::from_slice(&mut ob).expect("deserialise");
        assert_eq!(ob_back, opts);
    }
}
