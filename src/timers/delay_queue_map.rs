use atomic_refcell::AtomicRefCell;
use crossbeam_queue::SegQueue;
use crossbeam_skiplist::SkipMap;
use crossbeam_utils::atomic::AtomicCell;
use tokio::time::Duration;
use tokio_util::time::{delay_queue::Key, DelayQueue};
use xutex::Mutex;
#[derive(Debug)]
pub struct ValueKeyPair<V> {
    pub value: AtomicRefCell<V>,
    pub key: AtomicCell<Option<Key>>,
}
impl<V> ValueKeyPair<V> {
    pub fn new(value: V) -> Self {
        Self {
            value: value.into(),
            key: AtomicCell::default(),
        }
    }
}

#[derive(derive_more::Debug)]
/// A map with timed value-eviction
pub struct TimedMap<K: Ord, V> {
    #[debug(skip)]
    expiries: Mutex<DelayQueue<K>>,
    pub inner: SkipMap<K, ValueKeyPair<V>>,
}
impl<K: Ord, V> Default for TimedMap<K, V> {
    fn default() -> Self {
        Self {
            expiries: Mutex::new(DelayQueue::default()),
            inner: Default::default(),
        }
    }
}
impl<K: Ord + Clone + Send + 'static, V: Send + 'static> TimedMap<K, V> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert_constant(&self, key: K, value: V) {
        let pair = ValueKeyPair::new(value);
        let pair = self.inner.insert(key, pair);
    }
    pub fn insert_expirable(&self, key: K, value: V, timeout: Duration) {
        let mut expiries = self.expiries.lock();
        let pair = ValueKeyPair::new(value);
        let expiry_key = expiries.insert(key.clone(), timeout);
        pair.key.store(Some(expiry_key));
        let pair = self.inner.insert(key, pair);
    }
    pub fn len_expired(&self) -> usize {
        self.expiries.lock().len()
    }
    pub fn remove(&self, key: &K) {
        if let Some(removed) = self.inner.remove(key) {
            if let Some(expiry_key) = removed.value().key.load() {
                self.expiries.lock().remove(&expiry_key);
            }
        }
    }
    pub fn some_expiring_soon(&self) -> bool {
        self.expiries.lock().peek().is_some()
    }
    pub fn update_expiration_status(&self, key: &K, duration: Duration) -> Option<Key> {
        let mut expiries = self.expiries.lock();
        let found = self.inner.get(key)?;
        if let Some(previous_key) = found.value().key.load() {
            expiries.reset(&previous_key, duration);
            return Some(previous_key);
        }
        let new_key = expiries.insert(key.clone(), duration);
        found.value().key.store(Some(new_key));
        Some(new_key)
    }

    pub fn clear(&self) {
        self.inner.clear();
        self.expiries.lock().clear();
    }
    pub async fn purge_expired(&self) {
        use futures::StreamExt;
        use tokio_util::time::FutureExt;
        let timeout = Duration::from_micros(10);
        // clean any queued for deletion;
        while let Ok(Some(value)) = self
            .expiries
            .as_async()
            .lock()
            .await
            .next()
            .timeout(timeout)
            .await
        {
            let key = value.into_inner();
            self.inner.remove(&key);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::TimedMap;
    use std::sync::Arc;
    use tokio::time::{sleep, Duration};

    // Basic test: insert an expirable entry, wait, purge, ensure remove returns None.
    #[tokio::test]
    async fn test_purge_removes_expired() {
        let map: Arc<TimedMap<u64, u64>> = Arc::new(TimedMap::new());
        map.insert_expirable(1, 100, Duration::from_millis(50));

        // wait for it to expire
        sleep(Duration::from_millis(80)).await;

        map.purge_expired().await;

        // after purge, remove should return None
        assert!(!map.inner.contains_key(&1));
    }

    // Concurrency test: spawn many inserters and updaters, then purge and ensure no lingering expiries.
    #[tokio::test]
    async fn test_concurrent_inserts_and_purge() {
        let map = Arc::new(TimedMap::new());
        let mut handles = Vec::new();

        for i in 0..50u64 {
            let m = Arc::clone(&map);
            handles.push(tokio::spawn(async move {
                // insert many keys with small expiry
                for j in 0..10u64 {
                    let k = i * 100 + j;
                    m.insert_expirable(k, k, Duration::from_millis(30));
                }
            }));
        }

        // wait for all inserts to complete
        for h in handles {
            let _ = h.await;
        }

        // wait for expirations to be ready
        sleep(Duration::from_millis(60)).await;

        // purge them all
        map.purge_expired().await;

        // there should be no scheduled expiries left
        assert_eq!(map.len_expired(), 0);
    }
}
