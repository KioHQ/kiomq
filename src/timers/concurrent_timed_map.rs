use crossbeam::atomic::AtomicCell;
use crossbeam_skiplist::{map::Entry, SkipMap};
use derive_more::IsVariant;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::Duration;
/// A container for Tracking expiry of entries
#[derive(Debug, IsVariant, Clone)]
pub enum ExpiryValue<V> {
    Constant(Arc<Mutex<V>>),
    Expirable {
        value: Arc<Mutex<V>>,
        expires_at: Arc<AtomicCell<Instant>>,
    },
}
impl<V> ExpiryValue<V> {
    /// checks if the current value is expired and returns a `bool`.
    ///
    /// Always returns false if the current variant is [`ExpiryValue::Constant`]
    pub fn is_expired(&self) -> bool {
        match self {
            Self::Constant(_) => false,
            Self::Expirable {
                value: _,
                expires_at,
            } => Instant::now() >= expires_at.load(),
        }
    }
    ///  Returns a [`MutexGuard`] with the value .
    pub fn get(&self) -> Arc<Mutex<V>> {
        match self {
            Self::Constant(inner) => inner.clone(),
            Self::Expirable {
                value,
                expires_at: _,
            } => value.clone(),
        }
    }
}
#[derive(Debug)]
/// A concurrent map that can
/// evict entries after a configurable TTL.
///
/// Entries are inserted either with no expiry ([`insert_constant`](TimedMap::insert_constant))
/// or with a TTL ([`insert_expirable`](TimedMap::insert_expirable)).
/// Eviction is lazy: expired keys are removed in batch when either [`purge_expired`](TimedMap::purge_expired)
///  or [`iter`](TimedMap::iter) is called.
pub struct TimedMap<K: Ord + 'static, V> {
    inner: SkipMap<K, ExpiryValue<V>>,
    disable_expiration: AtomicCell<bool>,
}
impl<K: Ord + 'static + Send, V> Default for TimedMap<K, V> {
    fn default() -> Self {
        Self {
            inner: SkipMap::default(),
            disable_expiration: AtomicCell::default(),
        }
    }
}
impl<K: Ord, V> TimedMap<K, V> {
    /// Toggles whether entries are evicted on expiry.
    ///
    /// When expiration is disabled (toggled off) entries inserted with a TTL
    /// will be kept indefinitely until toggled back on.
    pub fn toggle_expiration(&self) {
        let previous_state = self.disable_expiration.load();
        let _ = self
            .disable_expiration
            .compare_exchange(previous_state, !previous_state);
    }
}
impl<K: Ord + Clone + Send + 'static + Sync, V: Send + 'static + Sync> TimedMap<K, V> {
    /// Creates an empty `TimedMap` with expiration enabled.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    /// Inserts `key → value` with no TTL (the entry never expires).
    pub fn insert_constant(&self, key: K, val: V) {
        let entry = ExpiryValue::Constant(Arc::new(val.into()));
        self.inner.insert(key, entry);
    }
    /// Inserts `key → value` that expires after `timeout`.
    ///
    /// If expiration is currently disabled (see [`toggle_expiration`](TimedMap::toggle_expiration))
    /// the entry is stored without a TTL.
    pub fn insert_expirable(&self, key: K, val: V, timeout: Duration) {
        if self.disable_expiration.load() {
            return self.insert_constant(key, val);
        }
        let expires_at = Instant::now() + timeout;
        let entry = ExpiryValue::Expirable {
            value: Arc::new(Mutex::new(val)),
            expires_at: Arc::new(AtomicCell::new(expires_at)),
        };
        self.inner.insert(key, entry);
    }
    /// Returns `Some(Arc<Mutex<V>>)` if the key exists and has not expired,
    /// or `None` if it is absent or expired (in which case the entry is evicted).
    pub fn get(&self, key: &K) -> Option<Arc<Mutex<V>>> {
        let found = self.inner.get(key)?;
        if found.value().is_expired() {
            self.inner.remove(key);
            return None;
        }
        Some(found.value().get())
    }
    /// Returns an [`Iterator`] of non-expired values and also evicts expired ones.
    pub fn iter(&self) -> impl Iterator<Item = Entry<'_, K, ExpiryValue<V>>> {
        self.inner.iter().filter_map(|entry| {
            if entry.value().is_expired() {
                entry.remove();
                return None;
            }
            Some(entry)
        })
    }
    ///  Return true is the map is empty.
    ///
    ///  **Note**: Expired entries present will show up
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    /// Returns whether a key  exists if not expired.
    ///
    /// Also evicts expired values
    pub fn contains_key(&self, key: &K) -> bool {
        let exists = self.inner.contains_key(key);
        if exists {
            return self.get(key).is_some();
        }
        false
    }

    /// Returns the number of entries currently tracked in the expiry queue.
    pub fn len_expired(&self) -> usize {
        self.inner
            .iter()
            .filter(|entry| entry.value().is_expirable())
            .count()
    }
    /// Removes the entry for `key`, cancelling its expiry if one was set.
    pub fn remove(&self, key: &K) {
        let _ = self.inner.remove(key);
    }
    /// Updates or sets the expiry deadline for the entry at `key`.
    ///
    /// If the entry already has an expiry key it is reset to `duration` from
    /// now; otherwise a new expiry is registered.  Returns the next `Instant`
    /// on success or `None` if the entry does not exist.
    pub fn update_expiration_status(&self, key: &K, duration: Duration) -> Option<Instant> {
        let found = self.inner.get(key)?;
        let existing = found.value();
        let next_instant = Instant::now() + duration;
        match existing {
            ExpiryValue::Constant(_) => None,
            ExpiryValue::Expirable {
                value: _,
                expires_at,
            } => {
                expires_at.swap(next_instant);
                Some(next_instant)
            }
        }
    }
    /// Returns `true` if automatic expiration is currently active.
    pub fn expires_entries(&self) -> bool {
        !self.disable_expiration.load()
    }

    /// Removes all entries and clears the expiry queue.
    pub fn clear(&self) {
        self.inner.clear();
    }
    /// Removes all entries whose TTL has elapsed.
    ///
    /// This is a no-op when expiration is disabled. The method removes all expired entries
    pub fn purge_expired(&self) {
        if !self.expires_entries() {
            return;
        }
        self.inner
            .iter()
            .filter(|entry| entry.value().is_expired())
            .for_each(|entry| {
                entry.remove();
            });
    }
}
#[cfg(test)]
mod tests {
    use super::TimedMap;
    use std::sync::Arc;
    use tokio::time::{sleep, Duration};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_purge_removes_expired_async() {
        let map: Arc<TimedMap<u64, u64>> = Arc::new(TimedMap::new());
        map.insert_expirable(1, 100, Duration::from_millis(50));

        sleep(Duration::from_millis(80)).await;

        map.purge_expired();

        assert!(!map.inner.contains_key(&1));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_inserts_and_purge_async() {
        let map = Arc::new(TimedMap::new());
        let mut handles = Vec::new();

        for i in 0..50u64 {
            let m = Arc::clone(&map);
            handles.push(tokio::spawn(async move {
                for j in 0..10u64 {
                    let k = i * 100 + j;
                    m.insert_expirable(k, k, Duration::from_millis(30));
                }
            }));
        }

        for h in handles {
            let _ = h.await;
        }

        sleep(Duration::from_millis(60)).await;

        map.purge_expired();

        assert_eq!(map.len_expired(), 0);
    }
}
