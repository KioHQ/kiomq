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
    expiries: Mutex<DelayQueue<K>>,
    pub inner: Mutex<BTreeMap<K, ValueKeyPair<V>>>,
}
impl<K, V> Default for TimedMap<K, V> {
    fn default() -> Self {
        Self {
            expiries: Default::default(),
            inner: Default::default(),
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
    pub fn insert_expirable(&self, key: K, value: V, timeout: Duration) {
        let pair = ValueKeyPair::new(value);
        let expiry_key = self.expiries.lock().insert(key.clone(), timeout);
        pair.key.store(Some(expiry_key));
        let pair = self.inner.lock().insert(key, pair);
    }
    pub fn len_expired(&self) -> usize {
        self.expiries.lock().len()
    }
    pub fn remove(&self, key: &K) -> Option<V> {
        let removed = self.inner.lock().remove(key)?;
        if let Some(expiry_key) = removed.key.load() {
            self.expiries.lock().remove(&expiry_key);
        }
        Some(removed.value)
    }
    pub fn some_expiring_soon(&self) -> bool {
        self.expiries.lock().peek().is_some()
    }
    pub fn update_expiration_status(&self, key: &K, duration: Duration) {
        if let Some(found) = self.inner.lock().get(&key) {
            if let Some(previous_key) = found.key.load() {
                self.expiries.lock().reset(&previous_key, duration);
            }
        }
    }
    pub fn clear(&self) {
        self.inner.lock().clear();
        self.expiries.lock().clear();
    }
    pub async fn purge_expired(&self) {
        use futures::StreamExt;
        use tokio_util::time::FutureExt;
        let timeout = Duration::from_micros(1);
        if let Ok(Some(value)) = self.expiries.lock().next().timeout(timeout).await {
            let key = value.get_ref();
            self.inner.lock().remove(key);
        }
    }
}
