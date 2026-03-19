use arc_swap::ArcSwapOption;
use crossbeam_skiplist::SkipMap;
use derive_more::Debug;
use futures_delay_queue::{delay_queue, DelayHandle, DelayQueue, Receiver};
use futures_intrusive::buffer::GrowingHeapBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::time::Duration;
use xutex::Mutex;
/// A value together with its optional expiry key in the delay queue.
#[derive(Debug)]
pub struct ValueKeyPair<V> {
    #[debug(skip)]
    /// The stored value, protected by a mutex for interior mutability.
    pub value: Mutex<V>,
    /// The delay-queue key associated with this entry's expiry, if any.
    pub key: ArcSwapOption<DelayHandle>,
}
impl<V> ValueKeyPair<V> {
    /// Wraps `value` with no expiry key assigned yet.
    pub fn new(value: V) -> Self {
        Self {
            value: Mutex::new(value),
            key: ArcSwapOption::default(),
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
    queue: DelayQueue<K, GrowingHeapBuf<K>>,
    reciever: Receiver<K>,
    /// The underlying concurrent skip-list storing all key-value pairs.
    pub inner: SkipMap<K, ValueKeyPair<V>>,
    disable_expiration: AtomicBool,
}
impl<K: Ord + 'static + Send, V> Default for TimedMap<K, V> {
    fn default() -> Self {
        let (queue, reciever) = delay_queue::<K>();
        Self {
            queue,
            reciever,
            inner: SkipMap::default(),
            disable_expiration: AtomicBool::default(),
        }
    }
}
impl<K: Ord, V> TimedMap<K, V> {
    /// Toggles whether entries are evicted on expiry.
    ///
    /// When expiration is disabled (toggled off) entries inserted with a TTL
    /// will be kept indefinitely until toggled back on.
    pub fn toggle_expiration(&self) {
        let previous_state = self.disable_expiration.load(Ordering::Acquire);
        let _ = self.disable_expiration.compare_exchange(
            previous_state,
            !previous_state,
            Ordering::AcqRel,
            Ordering::SeqCst,
        );
    }
}
impl<K: Ord + Clone + Send + 'static, V: Send + 'static> TimedMap<K, V> {
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
    pub fn insert_expirable(&self, key: K, value: V, timeout: Duration) {
        let pair = ValueKeyPair::new(value);
        if !self.disable_expiration.load(Ordering::Acquire) {
            // Remove any stale expiry key for this key so it doesn't
            // purge the freshly inserted value later.
            if let Some(old_entry) = self.inner.get(&key) {
                if let Some(old_key) = old_entry
                    .value()
                    .key
                    .load_full()
                    .and_then(|entry| Arc::into_inner(entry))
                {
                    old_key.cancel();
                }
            }
            let handle = self.queue.insert(key.clone(), timeout);
            pair.key.store(Some(handle.into()));
        }
        self.inner.insert(key, pair);
    }
    /// Returns the number of entries currently tracked in the expiry queue.
    #[allow(clippy::future_not_send)]
    pub async fn len_expired(&self) -> usize {
        self.inner
            .iter()
            .filter(|entry| entry.value().key.load().is_some())
            .count()
    }
    /// Removes the entry for `key`, cancelling its expiry if one was set.
    pub fn remove(&self, key: &K) {
        if let Some(removed) = self.inner.remove(key) {
            if let Some(handle) = removed
                .value()
                .key
                .swap(None)
                .take()
                .and_then(Arc::into_inner)
            {
                handle.cancel();
            }
        }
    }
    /// Updates or sets the expiry deadline for the entry at `key`.
    ///
    /// If the entry already has an expiry key it is reset to `duration` from
    /// now; otherwise a new expiry is registered.  Returns the delay-queue key
    /// on success or `None` if the entry does not exist.
    #[allow(clippy::future_not_send)]
    pub async fn update_expiration_status(&self, key: &K, duration: Duration) {
        if let Some(found) = self.inner.get(key) {
            if let Some(previous_key) = found.value().key.swap(None).and_then(Arc::into_inner) {
                let _ = previous_key.reset(duration);
                return;
            }

            let new_key = self.queue.insert(key.clone(), duration);
            found.value().key.store(Some(new_key.into()));
        }
    }
    /// Returns `true` if automatic expiration is currently active.
    pub fn expires_entries(&self) -> bool {
        !self.disable_expiration.load(Ordering::Acquire)
    }

    /// Removes all entries and clears the expiry queue.
    pub fn clear(&self) {
        tokio::task::block_in_place(|| {
            self.inner.iter().for_each(|entry| {
                if let Some(key) = entry
                    .value()
                    .key
                    .swap(None)
                    .and_then(|entry| Arc::into_inner(entry))
                {
                    key.cancel();
                }
            });
            self.inner.clear();
        });
    }
    /// Removes all entries whose TTL has elapsed.
    ///
    /// This is a no-op when expiration is disabled. The method polls the
    /// delay queue for a short timeout so it doesn't block indefinitely.
    #[allow(clippy::future_not_send)]
    pub async fn purge_expired(&self) {
        use tokio_util::time::FutureExt;
        if !self.expires_entries() {
            return;
        }
        let timeout = Duration::from_micros(500);
        // clean any queued for deletion;
        while let Ok(Some(expired)) = self.reciever.receive().timeout(timeout).await {
            self.inner.remove(&expired);
        }
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
        map.insert_expirable(1, 100, Duration::from_millis(50));

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
                    m.insert_expirable(k, k, Duration::from_millis(30));
                }
            }));
        }

        for h in handles {
            let _ = h.await;
        }

        sleep(Duration::from_millis(60)).await;

        map.purge_expired().await;

        assert_eq!(map.len_expired().await, 0);
    }
}
