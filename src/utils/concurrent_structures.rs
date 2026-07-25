use crossbeam::atomic::AtomicCell;
use crossbeam_skiplist::SkipMap;
use std::ops::RangeBounds;

#[derive(Debug)]
pub struct ConcurrentDeque<T> {
    data: SkipMap<i64, T>,
    // Push-only key allocators, so keys are never reused.
    head_idx: AtomicCell<i64>,
    tail_idx: AtomicCell<i64>,
    // Bumped after insert, claimed before removal.
    len: AtomicCell<usize>,
}

impl<T: Send + 'static> Default for ConcurrentDeque<T> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + 'static> ConcurrentDeque<T> {
    #[inline]
    pub fn new() -> Self {
        Self {
            data: SkipMap::new(),
            // Using 0 and 1 to distinguish the initial push directions
            head_idx: AtomicCell::new(0),
            tail_idx: AtomicCell::new(1),
            len: AtomicCell::new(0),
        }
    }

    #[inline]
    pub fn push_front(&self, value: T) {
        let idx = self.head_idx.fetch_sub(1);
        self.data.insert(idx, value);
        self.len.fetch_add(1);
    }

    #[inline]
    pub fn push_back(&self, value: T) {
        let idx = self.tail_idx.fetch_add(1);
        self.data.insert(idx, value);
        self.len.fetch_add(1);
    }

    /// Indices are not reset, so an in-flight push can't reuse a key.
    pub fn clear(&self) {
        for _ in 0..self.len.swap(0) {
            self.data
                .pop_front()
                .expect("a claimed element must be present");
        }
    }

    /// Claims through `len` before removing, so `len`, `is_empty` and pops always agree.
    #[inline]
    pub fn pop_front(&self) -> Option<T>
    where
        T: Clone,
    {
        self.len.fetch_update(|n| n.checked_sub(1)).ok()?;
        let entry = self
            .data
            .pop_front()
            .expect("a claimed element must be present");
        Some(entry.value().clone())
    }

    /// See [`Self::pop_front`].
    #[inline]
    pub fn pop_back(&self) -> Option<T>
    where
        T: Clone,
    {
        self.len.fetch_update(|n| n.checked_sub(1)).ok()?;
        let entry = self
            .data
            .pop_back()
            .expect("a claimed element must be present");
        Some(entry.value().clone())
    }

    /// Exact, O(1).
    #[inline]
    pub fn len(&self) -> usize {
        self.len.load()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn contains_value(&self, value: &T) -> bool
    where
        T: PartialEq,
    {
        self.data.iter().any(|entry| entry.value() == value)
    }

    /// Removes every element equal to `value`, claiming each through `len` first.
    pub fn remove_value(&self, value: &T)
    where
        T: PartialEq,
    {
        for entry in self.data.iter().filter(|entry| entry.value() == value) {
            if self.len.fetch_update(|n| n.checked_sub(1)).is_ok() && !entry.remove() {
                // Lost the entry to a concurrent pop; release the claim.
                self.len.fetch_add(1);
            }
        }
    }

    /// Don't call `remove` on yielded entries; use [`Self::remove_value`].
    #[inline]
    pub fn iter(&self) -> crossbeam_skiplist::map::Iter<'_, i64, T> {
        self.data.iter()
    }

    /// Returns an iterator over a subset of the deque based on index keys.
    #[inline]
    pub fn range<R>(&self, range: R) -> crossbeam_skiplist::map::Range<'_, i64, R, i64, T>
    where
        R: RangeBounds<i64>,
    {
        self.data.range(range)
    }
}

#[cfg(test)]
mod concurrent_deque_tests {
    // Pedantic/nursery lints that are noise in test scaffolding.
    #![allow(clippy::significant_drop_tightening, clippy::single_match_else)]
    use super::ConcurrentDeque;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// Drains the deque front to back.
    fn drain_all<T: Clone + Send + 'static>(deque: &ConcurrentDeque<T>) -> Vec<T> {
        std::iter::from_fn(|| deque.pop_front()).collect()
    }

    #[test]
    fn new_deque_is_empty_and_yields_nothing() {
        let deque: ConcurrentDeque<i64> = ConcurrentDeque::new();
        assert!(deque.is_empty(), "a freshly created deque must be empty");
        assert_eq!(deque.len(), 0, "empty deque length must be zero");
        assert_eq!(deque.pop_front(), None, "pop_front on empty must be None");
        assert_eq!(deque.pop_back(), None, "pop_back on empty must be None");
        assert!(!deque.contains_value(&0), "empty deque contains nothing");
        assert_eq!(deque.iter().count(), 0, "empty deque iterator is empty");
    }

    #[test]
    fn default_matches_new() {
        let deque: ConcurrentDeque<i64> = ConcurrentDeque::default();
        assert!(deque.is_empty(), "Default deque must start empty");
        assert_eq!(deque.len(), 0);
    }

    #[test]
    fn push_back_preserves_fifo_order() {
        let deque = ConcurrentDeque::new();
        for value in 0..5i64 {
            deque.push_back(value);
        }
        assert_eq!(deque.len(), 5);
        let drained = drain_all(&deque);
        assert_eq!(
            drained,
            vec![0, 1, 2, 3, 4],
            "push_back then pop_front must be FIFO"
        );
        assert!(deque.is_empty(), "deque must be empty after full drain");
    }

    #[test]
    fn push_front_prepends_in_reverse() {
        let deque = ConcurrentDeque::new();
        for value in 0..5i64 {
            deque.push_front(value);
        }
        // Each push_front lands ahead of the previous, so front-to-back is the
        // reverse of insertion order.
        let drained = drain_all(&deque);
        assert_eq!(
            drained,
            vec![4, 3, 2, 1, 0],
            "successive push_front must reverse the visible order"
        );
    }

    #[test]
    fn interleaved_pushes_order_by_ends() {
        let deque = ConcurrentDeque::new();
        deque.push_back(10);
        deque.push_front(5);
        deque.push_back(20);
        deque.push_front(1);
        let ordered: Vec<i64> = deque.iter().map(|e| *e.value()).collect();
        assert_eq!(
            ordered,
            vec![1, 5, 10, 20],
            "ends must interleave correctly"
        );
        assert_eq!(
            deque.pop_front(),
            Some(1),
            "smallest key pops from the front"
        );
        assert_eq!(deque.pop_back(), Some(20), "largest key pops from the back");
        assert_eq!(deque.len(), 2, "two items must remain after the two pops");
    }

    #[test]
    fn single_element_pops_from_both_ends() {
        let deque = ConcurrentDeque::new();
        deque.push_back(99);
        assert_eq!(deque.len(), 1);
        assert!(!deque.is_empty());
        assert_eq!(deque.pop_back(), Some(99), "single element pops from back");
        assert!(deque.is_empty());

        let deque = ConcurrentDeque::new();
        deque.push_front(99);
        assert_eq!(
            deque.pop_front(),
            Some(99),
            "single element pops from front"
        );
        assert!(deque.is_empty());
    }

    #[test]
    fn pop_front_and_pop_back_meet_in_the_middle() {
        let deque = ConcurrentDeque::new();
        for value in 0..6i64 {
            deque.push_back(value);
        }
        assert_eq!(deque.pop_front(), Some(0));
        assert_eq!(deque.pop_back(), Some(5));
        assert_eq!(deque.pop_front(), Some(1));
        assert_eq!(deque.pop_back(), Some(4));
        assert_eq!(deque.pop_front(), Some(2));
        assert_eq!(deque.pop_back(), Some(3));
        assert_eq!(deque.pop_front(), None, "deque must be exhausted");
        assert_eq!(deque.pop_back(), None, "deque must be exhausted");
    }

    #[test]
    fn clear_empties_and_preserves_ordering() {
        let deque = ConcurrentDeque::new();
        for value in 0..10i64 {
            deque.push_back(value);
        }
        deque.clear();
        assert!(deque.is_empty(), "clear must empty the deque");
        assert_eq!(deque.len(), 0);
        assert_eq!(deque.pop_front(), None);

        // Indices are not reset, but ordering must match a fresh deque.
        deque.push_back(100);
        deque.push_front(50);
        let ordered: Vec<i64> = deque.iter().map(|e| *e.value()).collect();
        assert_eq!(ordered, vec![50, 100], "ordering must survive a clear");
    }

    #[test]
    fn contains_value_reflects_membership() {
        let deque = ConcurrentDeque::new();
        deque.push_back(7);
        deque.push_front(3);
        assert!(
            deque.contains_value(&7),
            "pushed-back value must be present"
        );
        assert!(
            deque.contains_value(&3),
            "pushed-front value must be present"
        );
        assert!(
            !deque.contains_value(&42),
            "absent value must not be present"
        );
        deque.pop_front();
        assert!(
            !deque.contains_value(&3),
            "popped value must no longer be present"
        );
    }

    #[test]
    fn remove_value_keeps_len_exact() {
        let deque = ConcurrentDeque::new();
        for value in [1i64, 2, 1, 3] {
            deque.push_back(value);
        }
        deque.remove_value(&1);
        assert_eq!(deque.len(), 2);
        assert_eq!(drain_all(&deque), vec![2, 3]);
        deque.clear();
        assert_eq!(deque.len(), 0);
    }

    #[test]
    fn range_selects_key_subset() {
        let deque = ConcurrentDeque::new();
        for value in 0..5i64 {
            deque.push_back(value); // value v -> key (v + 1)
        }
        // Keys are 1..=5 for values 0..=4. Select keys 2..=4 -> values 1,2,3.
        let selected: Vec<i64> = deque.range(2..=4).map(|e| *e.value()).collect();
        assert_eq!(selected, vec![1, 2, 3], "range must select the key window");
    }

    #[test]
    fn large_push_back_keeps_length_and_order() {
        let deque = ConcurrentDeque::new();
        let count = 10_000i64;
        for value in 0..count {
            deque.push_back(value);
        }
        assert_eq!(
            deque.len(),
            count as usize,
            "len must count every inserted item exactly once"
        );
        let drained = drain_all(&deque);
        assert_eq!(
            drained.len(),
            count as usize,
            "drain must recover every item"
        );
        assert!(
            drained.windows(2).all(|w| w[0] < w[1]),
            "drained order must be strictly ascending for push_back"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_push_back_loses_no_updates() {
        let deque = Arc::new(ConcurrentDeque::<i64>::new());
        let producers = 8i64;
        let per_producer = 1_000i64;
        let mut handles = Vec::new();
        for p in 0..producers {
            let d = Arc::clone(&deque);
            handles.push(tokio::spawn(async move {
                for j in 0..per_producer {
                    // Globally unique value so we can assert a perfect set.
                    d.push_back(p * per_producer + j);
                }
            }));
        }
        for handle in handles {
            handle.await.expect("producer task must not panic");
        }

        let expected = (producers * per_producer) as usize;
        assert_eq!(
            deque.len(),
            expected,
            "every concurrent push_back must survive with no lost updates"
        );
        let values: HashSet<i64> = deque.iter().map(|e| *e.value()).collect();
        assert_eq!(values.len(), expected, "all inserted keys must be distinct");
        assert!(
            values.contains(&0) && values.contains(&(producers * per_producer - 1)),
            "boundary values must be present"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_mixed_pushes_preserve_total_count() {
        let deque = Arc::new(ConcurrentDeque::<i64>::new());
        let workers = 8i64;
        let per_worker = 1_000i64;
        let mut handles = Vec::new();
        for w in 0..workers {
            let d = Arc::clone(&deque);
            handles.push(tokio::spawn(async move {
                for j in 0..per_worker {
                    let value = w * per_worker + j;
                    if value % 2 == 0 {
                        d.push_front(value);
                    } else {
                        d.push_back(value);
                    }
                }
            }));
        }
        for handle in handles {
            handle.await.expect("worker task must not panic");
        }

        let expected = (workers * per_worker) as usize;
        assert_eq!(
            deque.len(),
            expected,
            "mixed concurrent push_front/push_back must preserve the total count"
        );
        let values: HashSet<i64> = deque.iter().map(|e| *e.value()).collect();
        assert_eq!(values.len(), expected, "no value may be lost or duplicated");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_drain_pops_each_item_exactly_once() {
        let deque = Arc::new(ConcurrentDeque::<i64>::new());
        let total = 8_000i64;
        for value in 0..total {
            deque.push_back(value);
        }

        let consumers = 8;
        let collected = Arc::new(parking_lot::Mutex::new(Vec::<i64>::new()));
        let mut handles = Vec::new();
        for _ in 0..consumers {
            let d = Arc::clone(&deque);
            let sink = Arc::clone(&collected);
            handles.push(tokio::spawn(async move {
                let local: Vec<i64> = std::iter::from_fn(|| d.pop_front()).collect();
                sink.lock().extend(local);
            }));
        }
        for handle in handles {
            handle.await.expect("consumer task must not panic");
        }

        let drained = collected.lock();
        assert_eq!(
            drained.len(),
            total as usize,
            "every item must be popped exactly once across all consumers"
        );
        let unique: HashSet<i64> = drained.iter().copied().collect();
        assert_eq!(
            unique.len(),
            total as usize,
            "no item may be popped twice under concurrent draining"
        );
        assert!(deque.is_empty(), "deque must be empty once fully drained");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_producers_and_consumers_conserve_every_item() {
        let deque = Arc::new(ConcurrentDeque::<i64>::new());
        let done = Arc::new(AtomicBool::new(false));
        let consumed = Arc::new(AtomicUsize::new(0));
        // High volume so pops race pushes hard.
        let producers = 8i64;
        let per_producer = 8_000i64;
        let consumers = 8;
        let expected = (producers * per_producer) as usize;

        let mut consumer_handles = Vec::new();
        for _ in 0..consumers {
            let d = Arc::clone(&deque);
            let done = Arc::clone(&done);
            let consumed = Arc::clone(&consumed);
            consumer_handles.push(tokio::spawn(async move {
                loop {
                    if let Some(_value) = d.pop_front() {
                        consumed.fetch_add(1, Ordering::Relaxed);
                    } else if done.load(Ordering::Acquire) && d.is_empty() {
                        break;
                    } else {
                        tokio::task::yield_now().await;
                    }
                }
            }));
        }

        let mut producer_handles = Vec::new();
        for p in 0..producers {
            let d = Arc::clone(&deque);
            producer_handles.push(tokio::spawn(async move {
                for j in 0..per_producer {
                    d.push_back(p * per_producer + j);
                }
            }));
        }
        for handle in producer_handles {
            handle.await.expect("producer task must not panic");
        }
        // Signal completion only after every item has been produced.
        done.store(true, Ordering::Release);
        for handle in consumer_handles {
            handle.await.expect("consumer task must not panic");
        }

        assert_eq!(
            consumed.load(Ordering::Relaxed),
            expected,
            "concurrent producers and consumers must conserve every item"
        );
        assert!(deque.is_empty(), "no item may be left behind");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn opposite_end_pops_never_share_an_item() {
        // A lone item is both the front and the back; exactly one popper may win.
        for round in 0..2_000i64 {
            let deque = Arc::new(ConcurrentDeque::<i64>::new());
            deque.push_back(round);
            let front = {
                let d = Arc::clone(&deque);
                tokio::spawn(async move { d.pop_front() })
            };
            let back = {
                let d = Arc::clone(&deque);
                tokio::spawn(async move { d.pop_back() })
            };
            let front = front.await.expect("front popper must not panic");
            let back = back.await.expect("back popper must not panic");
            assert_eq!(
                usize::from(front.is_some()) + usize::from(back.is_some()),
                1,
                "round {round}: the single item must be delivered exactly once"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn mixed_ends_push_and_pop_conserve_every_item() {
        // Pushes and pops interleave at both ends.
        let deque = Arc::new(ConcurrentDeque::<i64>::new());
        let workers = 8i64;
        let per_worker = 4_000i64;
        let popped = Arc::new(parking_lot::Mutex::new(Vec::<i64>::new()));
        let mut handles = Vec::new();
        for w in 0..workers {
            let d = Arc::clone(&deque);
            let sink = Arc::clone(&popped);
            handles.push(tokio::spawn(async move {
                let mut local = Vec::new();
                for j in 0..per_worker {
                    let value = w * per_worker + j;
                    if value % 2 == 0 {
                        d.push_front(value);
                        local.extend(d.pop_back());
                    } else {
                        d.push_back(value);
                        local.extend(d.pop_front());
                    }
                }
                sink.lock().extend(local);
            }));
        }
        for handle in handles {
            handle.await.expect("worker task must not panic");
        }

        let mut seen = popped.lock().clone();
        seen.extend(drain_all(&deque));
        let expected = (workers * per_worker) as usize;
        let unique: HashSet<i64> = seen.iter().copied().collect();
        assert_eq!(seen.len(), expected, "no item may be lost or duplicated");
        assert_eq!(unique.len(), expected, "every item must be distinct");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn len_stays_exact_under_contention() {
        // Each worker pushes then pops, so `len` must stay in [base, base + workers].
        let base = 64usize;
        let workers = 6usize;
        let deque = Arc::new(ConcurrentDeque::<i64>::new());
        for value in 0..base as i64 {
            deque.push_back(value);
        }
        let done = Arc::new(AtomicBool::new(false));

        let observer = {
            let d = Arc::clone(&deque);
            let done = Arc::clone(&done);
            tokio::spawn(async move {
                while !done.load(Ordering::Acquire) {
                    let len = d.len();
                    assert!(
                        (base..=base + workers).contains(&len),
                        "len {len} escaped [{base}, {}]",
                        base + workers
                    );
                    assert!(!d.is_empty(), "is_empty must agree with len");
                    tokio::task::yield_now().await;
                }
            })
        };

        let mut handles = Vec::new();
        for w in 0..workers {
            let d = Arc::clone(&deque);
            handles.push(tokio::spawn(async move {
                for j in 0..20_000i64 {
                    if (w as i64 + j) % 2 == 0 {
                        d.push_back(j);
                        assert!(d.pop_front().is_some(), "pop must never miss");
                    } else {
                        d.push_front(j);
                        assert!(d.pop_back().is_some(), "pop must never miss");
                    }
                }
            }));
        }
        for handle in handles {
            handle.await.expect("worker task must not panic");
        }
        done.store(true, Ordering::Release);
        observer.await.expect("observer task must not panic");

        assert_eq!(deque.len(), base, "len must settle on the base count");
        assert_eq!(deque.iter().count(), base, "len must match the entries");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn clear_during_pushes_never_overwrites_or_hangs() {
        let deque = Arc::new(ConcurrentDeque::<i64>::new());
        let pushed = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for p in 0..4i64 {
            let d = Arc::clone(&deque);
            let pushed = Arc::clone(&pushed);
            handles.push(tokio::spawn(async move {
                for j in 0..5_000i64 {
                    d.push_back(p * 5_000 + j);
                    pushed.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        let clearer = {
            let d = Arc::clone(&deque);
            tokio::spawn(async move {
                for _ in 0..100 {
                    d.clear();
                    let _ = d.pop_front();
                    tokio::task::yield_now().await;
                }
            })
        };
        for handle in handles {
            handle.await.expect("producer task must not panic");
        }
        clearer.await.expect("clearer task must not panic");
        assert_eq!(
            deque.len(),
            deque.iter().count(),
            "len must match the surviving entries"
        );

        // Pushes after the last clear must all be retrievable and distinct.
        deque.clear();
        for value in 0..1_000i64 {
            deque.push_back(value);
        }
        assert_eq!(drain_all(&deque), (0..1_000).collect::<Vec<_>>());
        assert_eq!(pushed.load(Ordering::Relaxed), 20_000);
    }
}
