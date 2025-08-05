use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
type BackoffFn = dyn Fn(i64) -> StoredFn + Send + Sync;
pub type StoredFn = Arc<dyn Fn(i64) -> i64 + Send + Sync>;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BackOffOptions {
    #[serde(rename = "type")]
    pub type_: Option<String>,
    pub delay: Option<i64>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum BackOffJobOptions {
    Number(i64),
    Opts(BackOffOptions),
}

//import lazy_static
#[derive(Clone, Default)]
pub struct BackOff {
    pub builtin_strategies: HashMap<&'static str, Arc<BackoffFn>>,
}

impl BackOff {
    pub fn new() -> Self {
        let mut backoff = BackOff::default();
        backoff.register("exponential", |delay: i64| {
            Arc::new(move |atempts: i64| -> i64 { 2_i64.pow(atempts as u32) * delay })
        });

        backoff.register("fixed", |delay: i64| Arc::new(move |_attempts| delay));
        backoff
    }

    pub fn register(
        &mut self,
        name: &'static str,
        strategy: impl Fn(i64) -> Arc<dyn Fn(i64) -> i64 + Send + Sync> + 'static + Send + Sync,
    ) {
        self.builtin_strategies.insert(name, Arc::new(strategy));
    }
    pub fn normalize(backoff: Option<BackOffJobOptions>) -> Option<BackOffOptions> {
        backoff.as_ref()?;
        let backoff = backoff.unwrap();
        match backoff {
            BackOffJobOptions::Number(num) => {
                if num == 0 {
                    return None;
                }
                let opts = BackOffOptions {
                    delay: Some(num),
                    type_: Some("exponential".to_string()),
                };
                Some(opts)
            }
            BackOffJobOptions::Opts(opts) => Some(opts),
            _ => None,
        }
    }
    pub fn calculate(
        &self,
        backoff_opts: Option<BackOffOptions>,
        atempts: i64,
        custom_strategy: Option<StoredFn>,
    ) -> Option<i64> {
        if let Some(opts) = backoff_opts {
            if let Some(strategy) = self.lookup_strategy(opts, custom_strategy) {
                let calculated_delay = strategy(atempts);
                return Some(calculated_delay);
            }
        }

        None
    }

    pub fn has_strategy(&self, key: &str) -> bool {
        self.builtin_strategies.contains_key(key)
    }

    pub fn lookup_strategy(
        &self,
        backoff: BackOffOptions,
        custom_strategy: Option<StoredFn>,
    ) -> Option<StoredFn>
where {
        if let (Some(t)) = backoff.type_ {
            if let (Some(strategy), Some(delay)) =
                (self.builtin_strategies.get(t.as_str()), backoff.delay)
            {
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
        f.debug_struct("BackOff")
            .field("builtin_strategies", &self.builtin_strategies.keys())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_expotential_backoff() {
        let mut backoff = BackOff::new();
        let strategy_found = backoff.lookup_strategy(
            BackOffOptions {
                delay: Some(100),
                type_: Some("exponential".to_string()),
            },
            None,
        );
        if let Some(strategy) = strategy_found {
            assert_eq!(strategy(1), 200);
        }
    }
    #[test]
    fn test_fixed_back() {
        let mut backoff = BackOff::new();
        let strategy_found = backoff.lookup_strategy(
            BackOffOptions {
                delay: Some(100),
                type_: Some("fixed".to_string()),
            },
            None,
        );
        if let Some(strategy) = strategy_found {
            assert_eq!(strategy(200), 100);
        }
    }
}
