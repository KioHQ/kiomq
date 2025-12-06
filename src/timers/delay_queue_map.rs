use crossbeam_queue::SegQueue;
use crossbeam_utils::atomic::AtomicCell;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use tokio::time::Duration;
use tokio_util::time::{delay_queue::Key, DelayQueue};

#[derive(Debug)]
pub struct ValueKeyPair<V> {
    pub value: V,
    pub key: AtomicCell<Option<Key>>,
}
impl<V> ValueKeyPair<V> {
    pub fn new(value: V) -> Self {
        Self {
            value,
            key: AtomicCell::default(),
        }
    }
}

#[derive(Debug)]
/// A map with timed value-eviction
pub struct TimedMap<K, V> {
    expiries: tokio::sync::Mutex<DelayQueue<K>>,
    pub inner: Mutex<BTreeMap<K, ValueKeyPair<V>>>,
    removal_queue: SegQueue<Key>,
}
impl<K, V> Default for TimedMap<K, V> {
    fn default() -> Self {
        Self {
            expiries: Default::default(),
            inner: Default::default(),
            removal_queue: SegQueue::new(),
        }
    }
}
impl<K: Ord + Clone, V> TimedMap<K, V> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert_constant(&self, key: K, value: V) {
        let pair = ValueKeyPair::new(value);
        let pair = self.inner.lock().insert(key, pair);
    }
    pub async fn insert_expirable(&self, key: K, value: V, timeout: Duration) {
        let mut expiries = self.expiries.lock().await;
        let mut inner = self.inner.lock();
        let pair = ValueKeyPair::new(value);
        let expiry_key = expiries.insert(key.clone(), timeout);
        pair.key.store(Some(expiry_key));
        let pair = inner.insert(key, pair);
    }
    pub async fn len_expired(&self) -> usize {
        self.expiries.lock().await.len()
    }
    pub fn remove(&self, key: &K) -> Option<V> {
        let removed = self.inner.lock().remove(key)?;
        if let Some(expiry_key) = removed.key.load() {
            self.removal_queue.push(expiry_key);
        }
        Some(removed.value)
    }
    pub async fn some_expiring_soon(&self) -> bool {
        self.expiries.lock().await.peek().is_some()
    }
    pub async fn update_expiration_status(&self, key: &K, duration: Duration) -> Option<Key> {
        let mut expiries = self.expiries.lock().await;
        let mut inner = self.inner.lock();
        let found = inner.get(key)?;
        if let Some(previous_key) = found.key.load() {
            expiries.reset(&previous_key, duration);
            return Some(previous_key);
        }
        let new_key = expiries.insert(key.clone(), duration);
        found.key.store(Some(new_key));
        Some(new_key)
    }
    pub fn clear(&self) {
        self.inner.lock().clear();
    }
    pub async fn purge_expired(&self) {
        use futures::StreamExt;
        use tokio_util::time::FutureExt;
        let timeout = Duration::from_millis(10);
        let mut expiries = self.expiries.lock().await;
        // clean any queued for deletion;
        while let Ok(Some(value)) = expiries.next().timeout(timeout).await {
            let key = value.into_inner();
            self.inner.lock().remove(&key);
        }
        while let Some(key) = self.removal_queue.pop() {
            let _ = expiries.remove(&key);
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
        map.insert_expirable(1, 100, Duration::from_millis(50))
            .await;

        // wait for it to expire
        sleep(Duration::from_millis(80)).await;

        map.purge_expired().await;

        // after purge, remove should return None
        assert!(map.remove(&1).is_none());
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
                    m.insert_expirable(k, k, Duration::from_millis(30)).await;
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
        assert_eq!(map.len_expired().await, 0);
    }
}
