use crossbeam::atomic::AtomicCell;

use crossbeam_skiplist::SkipMap;
use std::ops::RangeBounds;

#[derive(Debug)]
pub struct ConcurrentDeque<T> {
    data: SkipMap<i64, T>,
    head_idx: AtomicCell<i64>,
    tail_idx: AtomicCell<i64>,
}

impl<T: Send + 'static> Default for ConcurrentDeque<T> {
    fn default() -> Self {
        Self::new()
    }
}
impl<T: Send + 'static> ConcurrentDeque<T> {
    pub fn new() -> Self {
        Self {
            data: SkipMap::new(),
            // Using 0 and 1 to distinguish the initial push directions
            head_idx: AtomicCell::new(0),
            tail_idx: AtomicCell::new(1),
        }
    }

    pub fn push_front(&self, value: T) {
        let idx = self.head_idx.fetch_sub(1);
        self.data.insert(idx, value);
    }

    pub fn push_back(&self, value: T) {
        let idx = self.tail_idx.fetch_add(1);
        self.data.insert(idx, value);
    }
    pub fn clear(&self) {
        self.data.clear();
        self.head_idx.store(0);
        self.tail_idx.store(1);
    }

    pub fn pop_front(&self) -> Option<T>
    where
        T: Clone,
    {
        // front() gets the entry with the smallest key
        let entry = self.data.front()?;
        let key = *entry.key();

        if let Some(entry) = self.data.remove(&key) {
            return Some(entry.value().clone());
        }
        None
    }

    pub fn pop_back(&self) -> Option<T>
    where
        T: Clone,
    {
        // back() gets the entry with the largest key
        let entry = self.data.back()?;
        let key = *entry.key();

        if let Some(entry) = self.data.remove(&key) {
            return Some(entry.value().clone());
        }
        None
    }

    pub fn len(&self) -> usize {
        // remove the actual number of item available instead of an appromixation by self.data.len
        self.iter().count()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    pub fn contains_value(&self, value: &T) -> bool
    where
        T: PartialEq,
    {
        self.data.iter().any(|entry| entry.value() == value)
    }

    pub fn iter(&self) -> crossbeam_skiplist::map::Iter<'_, i64, T> {
        self.data.iter()
    }

    /// Returns an iterator over a subset of the deque based on index keys.
    pub fn range<R>(&self, range: R) -> crossbeam_skiplist::map::Range<'_, i64, R, i64, T>
    where
        R: RangeBounds<i64>,
    {
        self.data.range(range)
    }
}
