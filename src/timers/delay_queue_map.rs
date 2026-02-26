use std::sync::atomic::{AtomicBool, Ordering};

use atomic_refcell::AtomicRefCell;
use crossbeam_queue::SegQueue;
use crossbeam_skiplist::SkipMap;
use crossbeam_utils::atomic::AtomicCell;
use parking_lot::Mutex;
use tokio::runtime::Handle;
use tokio::time::Duration;
use tokio_util::time::{delay_queue::Key, DelayQueue};
use xutex::AsyncMutex;

#[derive(Debug)]
pub struct ValueKeyPair<V> {
    pub value: Mutex<V>,
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
    expiries: AsyncMutex<DelayQueue<K>>,
    pub inner: SkipMap<K, ValueKeyPair<V>>,
    disable_expiration: AtomicBool,
}
impl<K: Ord, V> Default for TimedMap<K, V> {
    fn default() -> Self {
        Self {
            expiries: AsyncMutex::new(DelayQueue::default()),
            inner: Default::default(),
            disable_expiration: AtomicBool::default(),
        }
    }
}
impl<K: Ord, V> TimedMap<K, V> {
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
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert_constant(&self, key: K, value: V) {
        let pair = ValueKeyPair::new(value);
        let pair = self.inner.insert(key, pair);
    }
    pub fn insert_expirable(&self, key: K, value: V, timeout: Duration) {
        let pair = ValueKeyPair::new(value);
        if !self.disable_expiration.load(Ordering::Acquire) {
            // To use async-mutex here, use block_in_place (on rt-multithread) - Subject to
            // change in the future
            let expiry_key = tokio::task::block_in_place(|| {
                Handle::current().block_on(async {
                    let mut expiries = self.expiries.lock().await;
                    // Remove any stale expiry key for this key so it doesn't
                    // purge the freshly inserted value later.
                    if let Some(old_entry) = self.inner.get(&key) {
                        if let Some(old_key) = old_entry.value().key.load() {
                            expiries.remove(&old_key);
                        }
                    }
                    expiries.insert(key.clone(), timeout)
                })
            });
            pair.key.store(Some(expiry_key));
        }
        self.inner.insert(key, pair);
    }
    pub async fn len_expired(&self) -> usize {
        self.expiries.lock().await.len()
    }
    pub fn remove(&self, key: &K) {
        if let Some(removed) = self.inner.remove(key) {
            if let Some(expiry_key) = removed.value().key.load() {
                // To use async-mutex here, use block_in_place (on rt-multithread)
                // - Subject to change in the future
                tokio::task::block_in_place(|| {
                    Handle::current().block_on(async {
                        self.expiries.lock().await.remove(&expiry_key);
                    })
                });
            }
        }
    }
    pub async fn update_expiration_status(&self, key: &K, duration: Duration) -> Option<Key> {
        let mut expiries = self.expiries.lock().await;
        let found = self.inner.get(key)?;
        if let Some(previous_key) = found.value().key.load() {
            expiries.reset(&previous_key, duration);
            return Some(previous_key);
        }
        let new_key = expiries.insert(key.clone(), duration);
        found.value().key.store(Some(new_key));
        Some(new_key)
    }
    pub fn expires_entries(&self) -> bool {
        !self.disable_expiration.load(Ordering::Acquire)
    }

    pub fn clear(&self) {
        self.inner.clear();
        // To use async-mutex here, use block_in_place (on rt-multithread) - Subject to
        // change in the future
        tokio::task::block_in_place(|| {
            Handle::current().block_on(async {
                self.expiries.lock().await.clear();
            })
        })
    }
    pub async fn purge_expired(&self) {
        if !self.expires_entries() {
            return;
        }
        use futures::StreamExt;
        use tokio_util::time::FutureExt;
        let timeout = Duration::from_micros(10);
        let mut expiries = self.expiries.lock().await;
        // clean any queued for deletion;
        while let Ok(Some(expired)) = expiries.next().timeout(timeout).await {
            let key = expired.into_inner();
            self.inner.remove(&key);
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
