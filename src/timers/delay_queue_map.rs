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
        let pair = ValueKeyPair::new(value);
        let expiry_key = self.expiries.lock().await.insert(key.clone(), timeout);
        pair.key.store(Some(expiry_key));
        let pair = self.inner.lock().insert(key, pair);
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
    pub async fn update_expiration_status(&self, key: &K, duration: Duration) {
        if let Some(found) = self.inner.lock().get(key) {
            if let Some(previous_key) = found.key.load() {
                self.expiries.lock().await.reset(&previous_key, duration);
            } else {
                self.expiries.lock().await.insert(key.clone(), duration);
            }
        }
    }
    pub fn clear(&self) {
        self.inner.lock().clear();
    }
    pub async fn purge_expired(&self) {
        use futures::StreamExt;
        use tokio_util::time::FutureExt;
        let timeout = Duration::from_micros(300);
        // clean any queued for deletion;
        if let Ok(Some(value)) = self.expiries.lock().await.next().timeout(timeout).await {
            let key = value.get_ref();
            self.inner.lock().remove(key);
        }
        while let Some(key) = self.removal_queue.pop() {
            self.expiries.lock().await.remove(&key);
        }
    }
}
