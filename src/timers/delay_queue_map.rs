use crossbeam::atomic::AtomicCell;
use crossbeam_skiplist::SkipMap;
use derive_more::Debug;
use futures::StreamExt;
use parking_lot::Mutex;
use std::hash::Hash;
use tokio::time::Duration;
use tokio_util::time::{delay_queue::Key, DelayQueue};
/// A value together with its optional expiry key in the delay queue.
#[derive(Debug)]
pub struct ValueKeyPair<V> {
    #[debug(skip)]
    /// The stored value, protected by a mutex for interior mutability.
    pub value: Mutex<V>,
    /// The delay-queue key associated with this entry's expiry, if any.
    pub key: AtomicCell<Option<Key>>,
}
impl<V> ValueKeyPair<V> {
    /// Wraps `value` with no expiry key assigned yet.
    pub fn new(value: V) -> Self {
        Self {
            value: value.into(),
            key: AtomicCell::default(),
        }
    }
}
#[derive(Debug)]
/// A concurrent map that can automatically evict entries after a configurable TTL.
///
/// Entries are inserted either with no expiry ([`insert_constant`](TimedMap::insert_constant))
/// or with a TTL ([`insert_expirable`](TimedMap::insert_expirable)). Eviction is
/// lazy: expired keys are removed in batch when [`purge_expired`](TimedMap::purge_expired)
/// is called.
pub struct TimedMap<K: Ord + 'static, V> {
    /// The delay-queue
    delay_queue: tokio::sync::Mutex<DelayQueue<K>>,
    /// The underlying concurrent skip-list storing all key-value pairs.
    pub inner: SkipMap<K, ValueKeyPair<V>>,
    disable_expiration: AtomicCell<bool>,
}
impl<K: Ord + 'static + Send + Hash, V> Default for TimedMap<K, V> {
    fn default() -> Self {
        Self {
            inner: SkipMap::default(),
            disable_expiration: AtomicCell::default(),
            delay_queue: tokio::sync::Mutex::default(),
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
impl<K: Ord + Clone + Send + 'static + Sync + Hash, V: Send + 'static + Sync> TimedMap<K, V> {
    /// Creates an empty `TimedMap` with expiration enabled.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    /// Inserts `key → value` with no TTL (the entry never expires).
    pub fn insert_constant(&self, key: K, value: V) {
        let pair = ValueKeyPair::new(value);
        self.inner.insert(key, pair);
    }
    /// Inserts `key → value` that expires after `timeout`.
    ///
    /// If expiration is currently disabled (see [`toggle_expiration`](TimedMap::toggle_expiration))
    /// the entry is stored without a TTL.
    pub async fn insert_expirable(&self, key: K, value: V, timeout: Duration) {
        if self.disable_expiration.load() {
            return self.insert_constant(key, value);
        }
        let mut delay_queue = self.delay_queue.lock().await;
        // if a value already exists, resets its ttl to the next new instead;
        if let Some(entry) = self.inner.get(&key) {
            if let Some(key) = entry.value().key.load() {
                delay_queue.reset(&key, timeout);
            }
            drop(delay_queue);
            *entry.value().value.lock() = value;
            return;
        }
        let next_key = delay_queue.insert(key.clone(), timeout);
        drop(delay_queue);
        let pair = ValueKeyPair::new(value);
        pair.key.store(Some(next_key));
        self.inner.insert(key, pair);
    }

    /// Returns the number of entries currently tracked in the expiry queue.
    pub fn len_expired(&self) -> usize {
        // short  and faster path;
        if let Ok(queue_lock) = self.delay_queue.try_lock() {
            return queue_lock.len();
        }
        self.inner
            .iter()
            .filter(|entry| entry.value().key.load().is_some())
            .count()
    }
    /// Removes the entry for `key`, cancelling its expiry if one was set.
    pub fn remove(&self, key: &K) {
        // no need to cancel the expiry key here; the entry is being removed
        let _ = self.inner.remove(key);
    }
    /// Updates or sets the expiry deadline for the entry at `key`.
    ///
    /// If the entry already has an expiry key it is reset to `duration` from
    /// now; otherwise a new expiry is registered.  Returns the delay-queue key
    /// on success or `None` if the entry does not exist.
    pub async fn update_expiration_status(&self, key: &K, duration: Duration) -> Option<Key> {
        let found = self.inner.get(key)?;
        let previous_handle = found.value().key.load()?;
        self.delay_queue
            .lock()
            .await
            .reset(&previous_handle, duration);
        Some(previous_handle)
    }
    /// Returns `true` if automatic expiration is currently active.
    pub fn expires_entries(&self) -> bool {
        !self.disable_expiration.load()
    }

    /// Removes all entries and clears the expiry queue.
    pub fn clear(&self) {
        self.inner.clear();
        if let Ok(mut delay_queue) = self.delay_queue.try_lock() {
            delay_queue.clear();
        }
    }
    /// Removes all entries whose TTL has elapsed.
    ///
    /// This is a no-op when expiration is disabled. The method polls the
    /// delay queue for a short timeout so it doesn't block indefinitely.
    pub async fn purge_expired(&self) {
        use tokio_util::time::FutureExt;
        if !self.expires_entries() {
            return;
        }
        let timeout = Duration::from_millis(1);
        // clean any queued for deletion;
        let mut delay_queue = self.delay_queue.lock().await;
        while let Ok(Some(expired)) = delay_queue.next().timeout(timeout).await {
            let key = expired.into_inner();
            self.inner.remove(&key);
        }
        drop(delay_queue);
    }
}
#[cfg(test)]
mod tests {
    use super::TimedMap;
    use std::sync::Arc;
    use tokio::time::{sleep, Duration};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_purge_removes_expired() {
        let map: Arc<TimedMap<u64, u64>> = Arc::new(TimedMap::new());
        map.insert_expirable(1, 100, Duration::from_millis(50))
            .await;

        sleep(Duration::from_millis(80)).await;

        map.purge_expired().await;

        assert!(!map.inner.contains_key(&1));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_inserts_and_purge() {
        let map = Arc::new(TimedMap::new());
        let mut handles = Vec::new();

        for i in 0..50u64 {
            let m = Arc::clone(&map);
            handles.push(tokio::spawn(async move {
                for j in 0..10u64 {
                    let k = i * 100 + j;
                    m.insert_expirable(k, k, Duration::from_millis(30)).await;
                }
            }));
        }

        for h in handles {
            let _ = h.await;
        }

        sleep(Duration::from_millis(60)).await;

        map.purge_expired().await;

        assert_eq!(map.len_expired(), 0);
    }
}
