use crossbeam::atomic::AtomicCell;
use crossbeam_skiplist::{SkipMap, map::Entry};
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
    use tokio::time::{Duration, sleep};

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

        join_all_bounded(handles).await;

        sleep(Duration::from_millis(60)).await;

        map.purge_expired();

        assert_eq!(map.len_expired(), 0);
    }

    // `TimedMap` measures expiry with `std::time::Instant`, which is NOT driven
    // by Tokio's virtual clock, so `tokio::time::pause`/`advance` cannot make
    // these deterministic. Instead use small real sleeps and bound every awaited
    // join with `tokio::time::timeout` so a broken map cannot hang the suite.
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::timeout;

    /// Joins a set of task handles, failing (rather than hanging) if any task
    /// does not complete within the bound.
    async fn join_all_bounded(handles: Vec<tokio::task::JoinHandle<()>>) {
        for handle in handles {
            let joined = timeout(Duration::from_secs(10), handle).await;
            let inner = joined.expect("task must finish within the timeout");
            inner.expect("spawned task must not panic");
        }
    }

    #[tokio::test]
    async fn constant_entries_never_expire() {
        let map: TimedMap<u64, u64> = TimedMap::new();
        map.insert_constant(1, 100);
        sleep(Duration::from_millis(20)).await;
        let value = map.get(&1).expect("constant entry must remain present");
        assert_eq!(*value.lock(), 100);
        map.purge_expired();
        assert!(map.contains_key(&1), "purge must never evict constants");
        assert_eq!(
            map.len_expired(),
            0,
            "constants are not counted as expirable"
        );
    }

    #[tokio::test]
    async fn zero_ttl_entry_is_treated_as_already_expired() {
        let map: TimedMap<u64, u64> = TimedMap::new();
        map.insert_expirable(7, 1, Duration::ZERO);
        // now >= expires_at holds immediately for a zero TTL, so a read evicts it.
        assert!(
            map.get(&7).is_none(),
            "a zero-TTL entry must read back as expired"
        );
        assert!(
            !map.contains_key(&7),
            "the zero-TTL entry must have been evicted on read"
        );
    }

    #[tokio::test]
    async fn entry_expiring_in_the_past_via_short_ttl_is_evicted_on_access() {
        let map: TimedMap<u64, u64> = TimedMap::new();
        map.insert_expirable(1, 42, Duration::from_millis(5));
        sleep(Duration::from_millis(25)).await;
        assert!(map.get(&1).is_none(), "expired entry must not be returned");
        // The read itself must evict the underlying node.
        assert!(!map.inner.contains_key(&1), "expired entry must be evicted");
    }

    #[tokio::test]
    async fn very_large_ttl_entry_remains_live() {
        let map: TimedMap<u64, u64> = TimedMap::new();
        map.insert_expirable(1, 99, Duration::from_hours(8760)); // one year
        sleep(Duration::from_millis(20)).await;
        let value = map.get(&1).expect("long-TTL entry must remain live");
        assert_eq!(*value.lock(), 99);
        assert_eq!(
            map.len_expired(),
            1,
            "the entry is still tracked as expirable"
        );
    }

    #[tokio::test]
    async fn removing_before_expiry_cancels_the_entry() {
        let map: TimedMap<u64, u64> = TimedMap::new();
        map.insert_expirable(1, 5, Duration::from_mins(1));
        assert!(map.contains_key(&1));
        map.remove(&1);
        assert!(
            map.get(&1).is_none(),
            "a removed entry must not be retrievable"
        );
        assert!(
            map.is_empty(),
            "map must be empty after removing its only key"
        );
    }

    #[tokio::test]
    async fn reinserting_the_same_key_replaces_the_previous_value_and_ttl() {
        let map: TimedMap<u64, u64> = TimedMap::new();
        map.insert_expirable(1, 1, Duration::from_millis(5));
        // Overwrite with a fresh, long TTL before the first would expire.
        map.insert_expirable(1, 2, Duration::from_mins(1));
        sleep(Duration::from_millis(25)).await;
        let value = map
            .get(&1)
            .expect("the re-inserted long-lived value must survive");
        assert_eq!(*value.lock(), 2, "the newest value must win");
        assert_eq!(map.len_expired(), 1, "there must be exactly one live entry");
    }

    #[tokio::test]
    async fn update_expiration_status_extends_a_live_entry() {
        let map: TimedMap<u64, u64> = TimedMap::new();
        map.insert_expirable(1, 10, Duration::from_millis(20));
        let next = map.update_expiration_status(&1, Duration::from_mins(1));
        assert!(next.is_some(), "updating a live expirable key must succeed");
        // The original short TTL would have elapsed here; the extension keeps it.
        sleep(Duration::from_millis(40)).await;
        assert!(
            map.get(&1).is_some(),
            "extending the TTL must keep the entry alive past the original deadline"
        );
    }

    #[tokio::test]
    async fn update_expiration_status_rejects_constants_and_missing_keys() {
        let map: TimedMap<u64, u64> = TimedMap::new();
        map.insert_constant(1, 10);
        assert!(
            map.update_expiration_status(&1, Duration::from_secs(1))
                .is_none(),
            "constants have no expiry to update"
        );
        assert!(
            map.update_expiration_status(&999, Duration::from_secs(1))
                .is_none(),
            "a missing key cannot be updated"
        );
    }

    #[tokio::test]
    async fn purge_on_empty_map_is_a_harmless_no_op() {
        let map: TimedMap<u64, u64> = TimedMap::new();
        map.purge_expired();
        assert!(map.is_empty());
        assert_eq!(map.len_expired(), 0);
        // Repeated drains must stay safe.
        map.purge_expired();
        map.clear();
        assert!(map.is_empty());
    }

    #[tokio::test]
    async fn purge_evicts_only_expired_entries_and_preserves_the_rest() {
        let map: TimedMap<u64, u64> = TimedMap::new();
        map.insert_expirable(1, 1, Duration::from_millis(5)); // will expire
        map.insert_expirable(2, 2, Duration::from_mins(1)); // stays live
        map.insert_constant(3, 3); // never expires
        sleep(Duration::from_millis(25)).await;
        map.purge_expired();
        assert!(!map.inner.contains_key(&1), "expired key must be purged");
        assert!(
            map.contains_key(&2),
            "live expirable key must survive purge"
        );
        assert!(map.contains_key(&3), "constant key must survive purge");
    }

    #[tokio::test]
    async fn expiry_ordering_respects_relative_deadlines() {
        let map: TimedMap<u64, u64> = TimedMap::new();
        // Staggered TTLs: only the earliest deadline should have elapsed at the
        // observation point.
        map.insert_expirable(1, 1, Duration::from_millis(5));
        map.insert_expirable(2, 2, Duration::from_mins(1));
        sleep(Duration::from_millis(25)).await;
        assert!(
            map.get(&1).is_none(),
            "the earlier deadline must fire first"
        );
        assert!(
            map.get(&2).is_some(),
            "the later deadline must still be pending"
        );
    }

    #[tokio::test]
    async fn disabling_expiration_stores_expirable_inserts_as_constants() {
        let map: TimedMap<u64, u64> = TimedMap::new();
        assert!(map.expires_entries(), "expiration is enabled by default");
        map.toggle_expiration();
        assert!(!map.expires_entries(), "toggling must disable expiration");
        // While disabled, an "expirable" insert with a tiny TTL is kept forever.
        map.insert_expirable(1, 1, Duration::from_millis(1));
        sleep(Duration::from_millis(20)).await;
        assert!(
            map.get(&1).is_some(),
            "with expiration disabled the entry must not expire"
        );
        assert_eq!(
            map.len_expired(),
            0,
            "the entry was stored as a constant, not an expirable"
        );
    }

    #[tokio::test]
    async fn purge_is_skipped_while_expiration_is_disabled() {
        let map: TimedMap<u64, u64> = TimedMap::new();
        map.insert_expirable(1, 1, Duration::from_millis(1));
        map.toggle_expiration();
        sleep(Duration::from_millis(20)).await;
        // The pre-existing expirable is genuinely past its deadline, but purge is
        // a no-op while disabled, so the node is retained.
        map.purge_expired();
        assert!(
            map.inner.contains_key(&1),
            "purge must not evict anything while expiration is disabled"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_inserts_and_removes_leave_a_consistent_map() {
        // Tasks race to insert then remove disjoint key ranges; every removed key
        // must be gone and every untouched key must survive. Guards against lost
        // or duplicated entries under contention.
        let map: Arc<TimedMap<u64, u64>> = Arc::new(TimedMap::new());
        let tasks = 16u64;
        let per_task = 100u64;
        let mut handles = Vec::new();
        for t in 0..tasks {
            let m = Arc::clone(&map);
            handles.push(tokio::spawn(async move {
                let base = t * per_task;
                for k in base..base + per_task {
                    m.insert_constant(k, k);
                }
                for k in (base..base + per_task).step_by(2) {
                    m.remove(&k);
                }
            }));
        }
        join_all_bounded(handles).await;

        // Exactly the odd-offset keys must remain, across all task ranges.
        let mut remaining = 0usize;
        for t in 0..tasks {
            let base = t * per_task;
            for k in base..base + per_task {
                if k % 2 == 1 {
                    let value = map.get(&k).expect("odd-offset keys must survive");
                    assert_eq!(*value.lock(), k, "value must match its key");
                    remaining += 1;
                } else {
                    assert!(map.get(&k).is_none(), "even-offset keys must be removed");
                }
            }
        }
        let expected =
            usize::try_from(tasks * (per_task / 2)).expect("expected count must fit in usize");
        assert_eq!(remaining, expected, "no entries may be lost or duplicated");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_reinsertion_of_a_shared_key_never_loses_the_key() {
        // Tasks hammer the same key; whatever the interleaving, exactly one entry
        // must remain holding one of the written values.
        let map: Arc<TimedMap<u64, u64>> = Arc::new(TimedMap::new());
        let writes = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for t in 0..20u64 {
            let m = Arc::clone(&map);
            let w = Arc::clone(&writes);
            handles.push(tokio::spawn(async move {
                for _ in 0..50 {
                    m.insert_constant(1, t);
                    w.fetch_add(1, Ordering::AcqRel);
                }
            }));
        }
        join_all_bounded(handles).await;

        assert_eq!(writes.load(Ordering::Acquire), 20 * 50);
        let value = map.get(&1).expect("the shared key must survive the race");
        assert!(
            *value.lock() < 20,
            "value must be one of the written task ids"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_purge_and_insert_never_panics_or_hangs() {
        // A purger races inserters mixing expirable and constant entries;
        // constants must never be lost and the interaction must terminate.
        let map: Arc<TimedMap<u64, u64>> = Arc::new(TimedMap::new());
        let mut handles = Vec::new();
        for t in 0..8u64 {
            let m = Arc::clone(&map);
            handles.push(tokio::spawn(async move {
                for j in 0..100u64 {
                    let k = t * 1000 + j;
                    if j % 2 == 0 {
                        m.insert_expirable(k, k, Duration::from_millis(1));
                    } else {
                        m.insert_constant(k, k);
                    }
                }
            }));
        }
        let m = Arc::clone(&map);
        handles.push(tokio::spawn(async move {
            for _ in 0..50 {
                m.purge_expired();
                tokio::task::yield_now().await;
            }
        }));
        join_all_bounded(handles).await;

        // Every constant (odd j) must still be present.
        for t in 0..8u64 {
            for j in (1..100u64).step_by(2) {
                let k = t * 1000 + j;
                assert!(
                    map.contains_key(&k),
                    "constant key {k} must never be purged"
                );
            }
        }
    }
}
