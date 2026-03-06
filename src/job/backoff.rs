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
    pub type_: Option<String>,
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
    pub builtin_strategies: Arc<SkipMap<String, Arc<BackoffFn>>>,
}

impl BackOff {
    /// Creates a new `BackOff` registry pre-loaded with the `"exponential"`
    /// and `"fixed"` built-in strategies.
    #[must_use]
    pub fn new() -> Self {
        let backoff = Self::default();
        backoff.register("exponential", |delay: i64| {
            Arc::new(move |atempts: i64| -> i64 { 2_i64.pow(atempts as u32) * delay })
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
            .insert(name.to_owned(), Arc::new(strategy));
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
                    type_: Some("fixed".to_string()),
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
        if let Some(opts) = backoff_opts {
            if let Some(strategy) = self.lookup_strategy(opts, custom_strategy) {
                let calculated_delay = strategy(attempts);
                return Some(calculated_delay);
            }
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
        if let Some(t) = backoff.type_ {
            if let (Some(entry), Some(delay)) =
                (self.builtin_strategies.get(t.as_str()), backoff.delay)
            {
                let strategy = entry.value();
                return Some(strategy(delay));
            }
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
    fn test_expotential_backoff() {
        let backoff = BackOff::new();
        let strategy_found = backoff.lookup_strategy(
            BackOffOptions {
                delay: Some(100),
                type_: Some("exponential".to_string()),
            },
            None,
        );
        if let Some(strategy) = strategy_found {
            assert_eq!(strategy(1), 200); // 2*1 * 100
            assert_eq!(strategy(2), 400);
            assert_eq!(strategy(3), 800);
            assert_eq!(strategy(4), 1600);
            assert_eq!(strategy(5), 3200);
        }
    }
    #[test]
    fn test_fixed_back() {
        let backoff = BackOff::new();
        let strategy_found = backoff.lookup_strategy(
            BackOffOptions {
                delay: Some(100),
                type_: Some("fixed".to_string()),
            },
            None,
        );
        if let Some(strategy) = strategy_found {
            assert_eq!(strategy(2), 100);
            assert_eq!(strategy(3), 100);
            assert_eq!(strategy(3), 100);
        }
    }
}
