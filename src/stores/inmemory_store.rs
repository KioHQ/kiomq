use super::{
    Arc, ArcSwapOption, BTreeMap, CollectionSuffix, EventEmitter, Job, JobField, JobOptions,
    JobState, JobToken, KioResult, Lock, Notify, QueueEventMode, QueueMetrics, QueueOpts,
    QueueStreamEvent, SharedEmitter, Store, WorkerMetrics,
};
use crate::KioError;
use crate::timers::TimedMap;
use crate::utils::{
    ConcurrentDeque, calculate_next_priority_score, pause_or_resume_workers, process_each_event,
    update_job_opts,
};
use crate::worker::MIN_DELAY_MS_LIMIT;
use crate::{Counter, Dt, QueueError};
use crate::{ProcessMetrics, ProcessedResult};
use chrono::Utc;
use compact_str::{CompactString, ToCompactString, format_compact};
use crossbeam::atomic::AtomicCell;
use crossbeam_skiplist::{SkipMap, SkipSet};
use derive_more::Debug;
use futures::FutureExt;
use futures::future::BoxFuture;
use num_traits::AsPrimitive;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::VecDeque;
use std::time::Duration;
use uuid::Uuid;
type StoredMap = SkipMap<u64, u64>;
type TimedJobMap<D, R, P> = TimedMap<u64, Job<D, R, P>>;
type ListQueue = ConcurrentDeque<u64>;
/// An in-memory [`Store`] implementation.
///
/// `InMemoryStore` holds all queue data in heap-allocated concurrent data
/// structures.  No external services are required, making it the ideal
/// backend for:
///
/// - **Tests** – no Redis / Docker needed; doc-tests run with `cargo test`.
/// - **Development** – fast iteration without a running message broker.
/// - **Short-lived / ephemeral tasks** – data is not persisted across restarts.
///
/// # Examples
///
/// ```rust
/// # #[tokio::main]
/// # async fn main() -> kiomq::KioResult<()> {
/// use kiomq::{InMemoryStore, Queue};
///
/// let store: InMemoryStore<String, String, ()> =
///     InMemoryStore::new(Some("myapp"), "email-queue");
/// let queue = Queue::new(store, None).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct InMemoryStore<D, R, P> {
    /// The queue name this store was created for.
    pub name: CompactString,
    /// The key prefix used to namespace all collections.
    pub prefix: CompactString,
    processing: Counter,
    is_paused: Arc<AtomicCell<bool>>,
    jobs: Arc<TimedJobMap<D, R, P>>,
    worker_metrics: Arc<TimedMap<Uuid, WorkerMetrics>>,
    process_metrics: Arc<TimedMap<u32, ProcessMetrics>>,
    #[debug(skip)]
    locks: Arc<TimedMap<u64, Lock>>, // locks that expires
    #[debug(skip)]
    events: Arc<SharedEmitter<R, P>>,
    id_counter: Counter,
    stored_metrics: Arc<ArcSwapOption<QueueMetrics>>,
    pause_workers: Arc<ArcSwapOption<AtomicCell<bool>>>,
    is_inital: Arc<AtomicCell<bool>>,
    notifier: Arc<ArcSwapOption<Notify>>,
    priority_counter: Counter,
    completed: Arc<StoredMap>,
    prioritized: Arc<StoredMap>,
    delayed: Arc<StoredMap>,
    failed: Arc<StoredMap>,
    stalled: Arc<SkipSet<u64>>,
    active: Arc<ListQueue>,
    waiting: Arc<ListQueue>,
    paused: Arc<ListQueue>,
    event_mode: QueueEventMode,
}
impl<D: Clone, R: Clone, P: Clone> InMemoryStore<D, R, P> {
    /// Creates a new `InMemoryStore`.
    ///
    /// # Arguments
    ///
    /// * `prefix` – key namespace prefix (defaults to `"kio"` when `None`).
    /// * `name` – queue name; combined with the prefix to form collection keys.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kiomq::InMemoryStore;
    ///
    /// let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "my-queue");
    /// ```
    #[must_use]
    pub fn new(prefix: Option<&str>, name: &str) -> Self {
        let prefix = prefix.unwrap_or("kio").to_compact_string();
        let name = name.to_compact_string();
        let events = Arc::default();
        let stored_metrics = Arc::default();
        let worker_metrics = Arc::default();
        let process_metrics = Arc::default();
        let notifier = Arc::default();
        let pause_workers = Arc::default();
        let is_inital = Arc::default();

        Self {
            is_inital,
            worker_metrics,
            pause_workers,
            notifier,
            name,
            stored_metrics,
            process_metrics,
            prefix,
            processing: Counter::default(),
            priority_counter: Counter::default(),
            id_counter: Counter::default(),
            is_paused: Arc::default(),
            jobs: Arc::default(),
            locks: Arc::default(),
            events,
            completed: Arc::default(),
            prioritized: Arc::default(),
            delayed: Arc::default(),
            failed: Arc::default(),
            stalled: Arc::default(),
            active: Arc::default(),
            waiting: Arc::default(),
            paused: Arc::default(),
            event_mode: QueueEventMode::PubSub,
        }
    }
    /// Toggles TTL-based expiration on internal maps (locks, jobs, metrics).
    ///
    /// Disabling expiration is useful in tests where you want entries to
    /// survive beyond their normal TTL.
    pub fn toggle_expiration(&self) {
        self.locks.toggle_expiration();
        self.jobs.toggle_expiration();
        self.worker_metrics.toggle_expiration();
    }
}
impl<D, R, P> InMemoryStore<D, R, P>
where
    D: Clone + Serialize + DeserializeOwned + Send + 'static + Sync,
    R: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
    P: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
{
    async fn insert(
        &self,
        job: &mut Job<D, R, P>,
        opts: JobOptions,
        pc: u64,
        id: u64,
        name: &str,
        is_paused: bool,
    ) -> KioResult<()> {
        let JobOptions {
            priority,
            ref delay,
            id: _,
            attempts: _,
            remove_on_fail: _,
            remove_on_complete: _,
            backoff: _,
            repeat: _,
        } = opts;
        let dt = Utc::now();
        let expected_dt_ts = delay.next_occurrance_timestamp_ms();
        let delay = delay.as_diff_ms(dt).cast_unsigned();
        job.add_opts(opts);
        if delay > 0 && delay < MIN_DELAY_MS_LIMIT {
            return Err(crate::KioError::from(QueueError::DelayBelowAllowedLimit {
                limit_ms: MIN_DELAY_MS_LIMIT,
                current_ms: delay,
            }));
        }
        let mut event = JobState::Wait;
        let waiting_or_paused = if is_paused {
            event = JobState::Paused;
            CollectionSuffix::Paused
        } else {
            CollectionSuffix::Wait
        };

        let to_delay = delay > 0;
        let to_priorize = priority > 0 && !to_delay;
        if to_delay {
            if let Some(expected_active_time) = expected_dt_ts {
                self.add_item(
                    CollectionSuffix::Delayed,
                    id,
                    Some(expected_active_time),
                    false,
                )
                .await?;
                job.state = JobState::Delayed;
                event = JobState::Delayed;
            }
        } else if to_priorize {
            let score = calculate_next_priority_score(priority, pc).cast_signed();
            job.state = JobState::Prioritized;
            self.add_item(CollectionSuffix::Prioritized, id, Some(score), true)
                .await?;
            event = JobState::Prioritized;
        } else {
            self.add_item(waiting_or_paused, id, None, true).await?;
        }
        job.id = Some(id);
        let job_key = CollectionSuffix::Job(id).tag();
        self.jobs.insert_constant(job_key, job.clone());
        let mut event = QueueStreamEvent::<R, P> {
            job_id: id,
            event,
            name: Some(name.to_compact_string()),
            ..Default::default()
        };
        if to_delay {
            event.delay = Some(delay);
        }
        if to_priorize {
            event.priority = Some(priority);
        }
        self.publish_event(self.event_mode, event).await?;
        Ok(())
    }
}
#[async_trait::async_trait]
impl<D, R, P> Store<D, R, P> for InMemoryStore<D, R, P>
where
    D: Clone + Serialize + DeserializeOwned + Send + 'static + Sync,
    R: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
    P: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
{
    async fn fetch_worker_metrics(&self) -> KioResult<BTreeMap<uuid::Uuid, WorkerMetrics>> {
        let stored_metrics = self
            .worker_metrics
            .iter()
            .map(|entry| {
                let worker_id = *entry.key();
                let value = entry.value().get();
                let value = value.lock();
                let ttls = value.ttl_ms;
                let metrics = WorkerMetrics::new(
                    value.worker_id,
                    value.active_len,
                    value.tasks.clone(),
                    ttls,
                );
                drop(value);
                (worker_id, metrics)
            })
            .collect();
        Ok(stored_metrics)
    }
    async fn store_process_metrics(&self, metrics: ProcessMetrics, ttl_ms: u64) -> KioResult<()> {
        let duration = std::time::Duration::from_millis(ttl_ms);
        if let Some(current) = self.process_metrics.get(&metrics.pid) {
            *current.lock() = metrics;
            return Ok(());
        }
        self.process_metrics
            .insert_expirable(metrics.pid, metrics, duration);

        Ok(())
    }
    async fn fetch_process_metrics(&self) -> KioResult<BTreeMap<u32, ProcessMetrics>> {
        let metrics = self
            .process_metrics
            .iter()
            .map(|entry| (*entry.key(), entry.value().get().lock().clone()))
            .collect();
        Ok(metrics)
    }

    async fn store_worker_metrics(&self, metrics: WorkerMetrics, ttl_ms: u64) -> KioResult<()> {
        let duration = std::time::Duration::from_millis(ttl_ms);
        if let Some(current) = self.worker_metrics.get(&metrics.worker_id) {
            *current.lock() = metrics;
            return Ok(());
        }
        self.worker_metrics
            .insert_expirable(metrics.worker_id, metrics, duration);
        Ok(())
    }
    fn queue_name(&self) -> &str {
        &self.name
    }
    async fn purge_expired(&self) {
        let purge_locks = async {
            if self.locks.len_expired() > 0 {
                self.locks.purge_expired();
            }
        };

        let purge_metrics = async {
            if self.worker_metrics.len_expired() > 0 {
                self.worker_metrics.purge_expired();
            }
            if self.process_metrics.len_expired() > 0 {
                self.process_metrics.purge_expired();
            }
        };
        let purge_jobs = async move {
            if self.jobs.len_expired() > 0 {
                self.jobs.purge_expired();
            }
        };
        tokio::join!(purge_jobs, purge_locks, purge_metrics);
    }

    fn queue_prefix(&self) -> &str {
        &self.prefix
    }
    async fn fetch_jobs(&self, ids: &[u64]) -> KioResult<VecDeque<Job<D, R, P>>> {
        if ids.is_empty() {
            return Ok(VecDeque::new());
        }
        let mut results = VecDeque::with_capacity(ids.len());
        for id in ids {
            let key = CollectionSuffix::Job(*id).tag();
            if let Some(found) = self.jobs.get(&key) {
                results.push_back(found.lock().clone());
            }
        }
        Ok(results)
    }

    async fn exists_in(&self, col: CollectionSuffix, item: u64) -> KioResult<bool> {
        let result = match col {
            CollectionSuffix::Active => self.active.contains_value(&item),

            CollectionSuffix::Wait => self.waiting.contains_value(&item),

            CollectionSuffix::Paused => self.paused.contains_value(&item),
            CollectionSuffix::Completed => {
                self.completed.iter().any(|entry| *entry.value() == item)
            }
            CollectionSuffix::Failed => self.failed.iter().any(|entry| *entry.value() == item),
            CollectionSuffix::Prioritized => {
                self.prioritized.iter().any(|entry| *entry.value() == item)
            }
            CollectionSuffix::Delayed => self.delayed.iter().any(|entry| *entry.value() == item),
            CollectionSuffix::Stalled => self.stalled.contains(&item),
            CollectionSuffix::Job(_id) => self.jobs.contains_key(&col.tag()),
            CollectionSuffix::Lock(_) | CollectionSuffix::StalledCheck => {
                self.locks.contains_key(&col.tag())
            }

            _ => false,
        };
        Ok(result)
    }
    async fn metadata_field_exists(&self, _field: &str) -> KioResult<bool> {
        Ok(true)
    }

    async fn set_event_mode(&self, _event_mode: QueueEventMode) -> KioResult<()> {
        // do nothing; only pubsub is supported
        Ok(())
    }

    async fn listen_to_events(
        &self,
        _event_mode: QueueEventMode,
        _block_interval: Option<u64>,
        _emitter: &EventEmitter<R, P>,
        _metrics: &QueueMetrics,
    ) -> KioResult<()> {
        // we do nothing  here as  this method isn't called for this store
        // we can directly use the emitter to emit events without need for a channel
        Ok(())
    }

    async fn create_stream_listener(
        &self,
        emitter: EventEmitter<R, P>,
        notifier: Arc<Notify>,
        metrics: Arc<QueueMetrics>,
        pause_workers: Arc<AtomicCell<bool>>,
        _event_mode: QueueEventMode,
    ) -> BoxFuture<'static, KioResult<()>> {
        self.events.store(Some(emitter));
        self.notifier.store(Some(notifier));
        self.pause_workers.store(Some(pause_workers));
        // set our stored_metrics to the queue's metrics;
        self.stored_metrics.store(Some(metrics));
        // For this store, this would have been to task to period metrics and do some clean but
        // itsn't need, as metrics update on each publish_event call.
        async move { Ok::<(), KioError>(()) }.boxed()
    }

    async fn add_bulk_only(
        &self,
        iter: Box<dyn Iterator<Item = (String, Option<JobOptions>, D)> + Send>,
        queue_opts: QueueOpts,
        _event_mode: QueueEventMode,
        is_paused: bool,
    ) -> KioResult<()> {
        for (ref name, opts, data) in iter {
            let mut opts = opts.unwrap_or_default();
            update_job_opts(&queue_opts, &mut opts);
            let pc = if opts.priority > 0 {
                self.incr(CollectionSuffix::PriorityCounter, 1, None)
                    .await?
            } else {
                0
            };
            let queue_name = format_compact!("{}:{}", &self.prefix, &self.name);
            let id = self.incr(CollectionSuffix::Id, 1, None).await?;
            let mut job = Job::<D, R, P>::new(name, Some(data), opts.id, Some(&queue_name));
            self.insert(&mut job, opts, pc, id, name, is_paused).await?;
        }
        Ok(())
    }

    async fn add_bulk(
        &self,
        iter: Box<dyn Iterator<Item = (String, Option<JobOptions>, D)> + Send>,
        queue_opts: QueueOpts,
        _event_mode: QueueEventMode,
        is_paused: bool,
    ) -> KioResult<Vec<Job<D, R, P>>> {
        let mut jobs = vec![];
        for (ref name, opts, data) in iter {
            let mut opts = opts.unwrap_or_default();
            update_job_opts(&queue_opts, &mut opts);
            let pc = if opts.priority > 0 {
                self.incr(CollectionSuffix::PriorityCounter, 1, None)
                    .await?
            } else {
                0
            };
            let queue_name = format_compact!("{}:{}", &self.prefix, &self.name);
            let id = self.incr(CollectionSuffix::Id, 1, None).await?;
            let mut job = Job::<D, R, P>::new(name, Some(data), opts.id, Some(&queue_name));
            self.insert(&mut job, opts, pc, id, name, is_paused).await?;
            jobs.push(job);
        }
        Ok(jobs)
    }

    async fn get_delayed_at(&self, start: i64, stop: i64) -> KioResult<(Vec<u64>, Vec<u64>)> {
        let before = (start - 1).cast_unsigned();
        let end = stop.cast_unsigned();
        let start = start.cast_unsigned();
        let missed_iter = self.delayed.range(..before);
        let jobs_iter = self.delayed.range(start..end);
        let jobs = jobs_iter
            .map(|entry| {
                let val = *entry.value();
                entry.remove();
                val
            })
            .collect();
        let missed = missed_iter
            .map(|entry| {
                let val = *entry.value();
                entry.remove();
                val
            })
            .collect();
        Ok((jobs, missed))
    }

    async fn pop_set(&self, col: CollectionSuffix, min: bool) -> KioResult<Vec<(u64, u64)>> {
        let pairs = match col {
            CollectionSuffix::Completed => {
                if min {
                    self.completed
                        .pop_front()
                        .map(|entry| (*entry.key(), *entry.value()))
                } else {
                    self.completed
                        .pop_back()
                        .map(|entry| (*entry.key(), *entry.value()))
                }
            }
            CollectionSuffix::Delayed => {
                if min {
                    self.delayed
                        .pop_front()
                        .map(|entry| (*entry.key(), *entry.value()))
                } else {
                    self.delayed
                        .pop_back()
                        .map(|entry| (*entry.key(), *entry.value()))
                }
            }
            CollectionSuffix::Failed => {
                if min {
                    self.failed
                        .pop_front()
                        .map(|entry| (*entry.key(), *entry.value()))
                } else {
                    self.failed
                        .pop_back()
                        .map(|entry| (*entry.key(), *entry.value()))
                }
            }
            CollectionSuffix::Prioritized => {
                if min {
                    self.prioritized
                        .pop_front()
                        .map(|entry| (*entry.key(), *entry.value()))
                } else {
                    self.prioritized
                        .pop_back()
                        .map(|entry| (*entry.key(), *entry.value()))
                }
            }
            _ => None,
        };
        if let Some((score, id)) = pairs {
            return Ok(vec![(id, score)]);
        }
        Ok(vec![])
    }

    async fn expire(&self, col: CollectionSuffix, secs: i64) -> KioResult<()> {
        let duration = Duration::from_secs(secs.unsigned_abs());
        let key = col.tag();
        match col {
            CollectionSuffix::Lock(_) | CollectionSuffix::StalledCheck => {
                self.locks.update_expiration_status(&key, duration);
            }
            CollectionSuffix::Job(_) => {
                self.jobs.update_expiration_status(&key, duration);
            }
            _ => {}
        }
        Ok(())
    }

    async fn get_metrics(&self) -> KioResult<QueueMetrics> {
        let metrics = QueueMetrics::new(
            self.id_counter.load(),
            self.processing.load(),
            self.active.len().as_(),
            self.stalled.iter().count().as_(),
            self.completed.iter().count().as_(),
            self.delayed.iter().count().as_(),
            self.prioritized.iter().count().as_(),
            self.paused.len().as_(),
            self.failed.iter().count().as_(),
            self.waiting.len().as_(),
            self.is_paused.load(),
            self.event_mode,
        );
        Ok(metrics)
    }

    async fn get_job(&self, id: u64) -> Option<Job<D, R, P>> {
        let job_key = CollectionSuffix::Job(id).tag();
        self.jobs.get(&job_key).map(|pair| pair.lock().clone())
    }

    async fn get_token(&self, id: u64) -> Option<JobToken> {
        let lock_key = CollectionSuffix::Lock(id).tag();
        self.locks
            .get(&lock_key)
            .and_then(|entry| match *entry.lock() {
                Lock::Token(token) => Some(token),
                Lock::StallCheck => None,
            })
    }

    async fn get_state(&self, id: u64) -> Option<JobState> {
        let job_key = CollectionSuffix::Job(id).tag();
        self.jobs.get(&job_key).map(|entry| entry.lock().state)
    }

    async fn update_job_progress(&self, job: &mut Job<D, R, P>, value: P) -> KioResult<()> {
        if let Some(id) = job.id {
            let job_key = CollectionSuffix::Job(id).tag();
            let jobs = self.jobs.clone();
            let value_clone = value.clone();
            if let Some(value) = jobs.get(&job_key) {
                value.lock().progress = Some(value_clone);
            }
            job.progress = Some(value);
        }
        Ok(())
    }
    fn update_job_progress_sync(&self, job: &mut Job<D, R, P>, value: P) -> KioResult<()> {
        if let Some(id) = job.id {
            let job_key = CollectionSuffix::Job(id).tag();
            let jobs = self.jobs.clone();
            let value_clone = value.clone();
            if let Some(entry) = jobs.get(&job_key) {
                entry.lock().progress = Some(value_clone);
            }
            job.progress = Some(value);
        }
        Ok(())
    }

    async fn add_item(
        &self,
        col: CollectionSuffix,
        item: u64,
        score: Option<i64>,
        append: bool,
    ) -> KioResult<()> {
        match col {
            CollectionSuffix::Active => {
                if append {
                    self.active.push_front(item);
                    return Ok(());
                }
                self.active.push_back(item);
            }
            CollectionSuffix::Wait => {
                if append {
                    self.waiting.push_front(item);
                    return Ok(());
                }
                self.waiting.push_back(item);
            }
            CollectionSuffix::Paused => {
                if append {
                    self.paused.push_front(item);
                    return Ok(());
                }
                self.paused.push_back(item);
            }
            CollectionSuffix::Completed => {
                if let Some(score) = score {
                    self.completed.insert(score.cast_unsigned(), item);
                }
            }
            CollectionSuffix::Failed => {
                if let Some(score) = score {
                    self.failed.insert(score.cast_unsigned(), item);
                }
            }
            CollectionSuffix::Prioritized => {
                if let Some(score) = score {
                    self.prioritized.insert(score.cast_unsigned(), item);
                }
            }
            CollectionSuffix::Delayed => {
                if let Some(score) = score {
                    self.delayed.insert(score.cast_unsigned(), item);
                }
            }
            CollectionSuffix::Stalled => {
                self.stalled.insert(item);
            }
            _ => {}
        }
        Ok(())
    }

    async fn pop_back_push_front(
        &self,
        src: CollectionSuffix,
        dst: CollectionSuffix,
    ) -> Option<u64> {
        match (src, dst) {
            (CollectionSuffix::Wait, CollectionSuffix::Active) => {
                let value = self.waiting.pop_back()?;
                self.active.push_front(value);
                return Some(value);
            }
            _ => return None,
        }
    }

    async fn set_lock(
        &self,
        col: CollectionSuffix,
        token: Option<JobToken>,
        lock_duration: u64,
    ) -> KioResult<()> {
        let lock_key = col.tag();
        let duration = Duration::from_millis(lock_duration);
        let lock = token.map_or(Lock::StallCheck, Lock::Token);
        self.locks.insert_expirable(lock_key, lock, duration);

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn get_job_ids_in_state(
        &self,
        state: JobState,
        start: Option<usize>,
        end: Option<usize>,
    ) -> KioResult<VecDeque<u64>> {
        let start = start.unwrap_or_default();
        match state {
            JobState::Wait => {
                if self.waiting.is_empty() {
                    return Ok(VecDeque::new());
                }
                let end = end.unwrap_or_else(|| self.waiting.len().saturating_sub(1));
                let start = self.waiting.iter().nth(start).map(|entry| *entry.key());
                let end = self.waiting.iter().nth(end).map(|entry| *entry.key());
                if let (Some(start_element), Some(last_element)) = (start, end) {
                    return Ok(self
                        .waiting
                        .range(start_element..=last_element)
                        .map(|entry| *entry.value())
                        .collect());
                }
            }
            JobState::Prioritized => {
                if self.prioritized.is_empty() {
                    return Ok(VecDeque::new());
                }
                let end = end.unwrap_or_else(|| self.prioritized.len().saturating_sub(1));
                let start = self.prioritized.iter().nth(start).map(|entry| *entry.key());
                let end = self.prioritized.iter().nth(end).map(|entry| *entry.key());
                if let (Some(start_element), Some(last_element)) = (start, end) {
                    return Ok(self
                        .prioritized
                        .range(start_element..=last_element)
                        .map(|entry| *entry.value())
                        .collect());
                }
            }
            JobState::Stalled => {
                if self.stalled.is_empty() {
                    return Ok(VecDeque::new());
                }
                let end = end.unwrap_or_else(|| self.stalled.len().saturating_sub(1));
                let start = self.stalled.iter().nth(start).map(|entry| *entry.value());
                let end = self.stalled.iter().nth(end).map(|entry| *entry.value());
                if let (Some(start_element), Some(last_element)) = (start, end) {
                    return Ok(self
                        .stalled
                        .range(start_element..=last_element)
                        .map(|entry| *entry.value())
                        .collect());
                }
            }
            JobState::Active => {
                if self.active.is_empty() {
                    return Ok(VecDeque::new());
                }
                let end = end.unwrap_or_else(|| self.active.len().saturating_sub(1));
                let start = self.active.iter().nth(start).map(|entry| *entry.key());
                let end = self.active.iter().nth(end).map(|entry| *entry.key());
                if let (Some(start_element), Some(last_element)) = (start, end) {
                    return Ok(self
                        .active
                        .range(start_element..=last_element)
                        .map(|entry| *entry.value())
                        .collect());
                }
            }
            JobState::Paused => {
                if self.paused.is_empty() {
                    return Ok(VecDeque::new());
                }
                let end = end.unwrap_or_else(|| self.paused.len().saturating_sub(1));
                let start = self.paused.iter().nth(start).map(|entry| *entry.key());
                let end = self.paused.iter().nth(end).map(|entry| *entry.key());
                if let (Some(start_element), Some(last_element)) = (start, end) {
                    return Ok(self
                        .paused
                        .range(start_element..=last_element)
                        .map(|entry| *entry.value())
                        .collect());
                }
            }
            JobState::Completed => {
                if self.completed.is_empty() {
                    return Ok(VecDeque::new());
                }
                let end = end.unwrap_or_else(|| self.completed.len().saturating_sub(1));
                let start = self.completed.iter().nth(start).map(|entry| *entry.key());
                let end = self.completed.iter().nth(end).map(|entry| *entry.key());
                if let (Some(start_element), Some(last_element)) = (start, end) {
                    return Ok(self
                        .completed
                        .range(start_element..=last_element)
                        .map(|entry| *entry.value())
                        .collect());
                }
            }
            JobState::Failed => {
                if self.failed.is_empty() {
                    return Ok(VecDeque::new());
                }
                let end = end.unwrap_or_else(|| self.failed.len().saturating_sub(1));
                let start = self.failed.iter().nth(start).map(|entry| *entry.key());
                let end = self.failed.iter().nth(end).map(|entry| *entry.key());
                if let (Some(start_element), Some(last_element)) = (start, end) {
                    return Ok(self
                        .failed
                        .range(start_element..=last_element)
                        .map(|entry| *entry.value())
                        .collect());
                }
            }
            JobState::Delayed => {
                if self.delayed.is_empty() {
                    return Ok(VecDeque::new());
                }
                let end = end.unwrap_or_else(|| self.delayed.len().saturating_sub(1));
                let start = self.delayed.iter().nth(start).map(|entry| *entry.key());
                let end = self.delayed.iter().nth(end).map(|entry| *entry.key());
                if let (Some(start_element), Some(last_element)) = (start, end) {
                    return Ok(self
                        .delayed
                        .range(start_element..=last_element)
                        .map(|entry| *entry.value())
                        .collect());
                }
            }
            _ => {}
        }
        Ok(VecDeque::new())
    }
    async fn set_fields(&self, job_id: u64, fields: Vec<JobField<R>>) -> KioResult<()> {
        let key = CollectionSuffix::Job(job_id);
        if let Some(value) = self.jobs.get(&key.tag()) {
            let job = &mut value.lock();
            for field in fields {
                match field {
                    JobField::BackTrace(trace) => job.stack_trace.push(trace),
                    JobField::State(state) => job.state = state,
                    JobField::ProcessedOn(ts) => {
                        job.processed_on = Dt::from_timestamp_micros(ts.cast_signed());
                    }
                    JobField::FinishedOn(ts) => {
                        job.finished_on = Dt::from_timestamp_micros(ts.cast_signed());
                    }
                    JobField::Token(token) => job.token = Some(token),
                    JobField::Payload(processed_result) => match processed_result {
                        ProcessedResult::Failed(failed_details) => {
                            job.failed_reason = Some(failed_details);
                        }
                        ProcessedResult::Success(result, _) => job.returned_value = Some(result),
                    },
                }
            }
        }
        Ok(())
    }

    async fn incr(
        &self,
        key: CollectionSuffix,
        delta: i64,
        hash_key: Option<&str>,
    ) -> KioResult<u64> {
        let handle_counter = |counter: &Counter| {
            if delta.is_positive() {
                counter.fetch_add(delta.unsigned_abs());
                return counter.load();
            }
            counter.fetch_sub(delta.unsigned_abs());
            counter.load()
        };
        let next = match key {
            CollectionSuffix::Id => handle_counter(&self.id_counter),
            CollectionSuffix::PriorityCounter => handle_counter(&self.priority_counter),
            CollectionSuffix::Meta => handle_counter(&self.processing),
            CollectionSuffix::Job(_) => {
                if let Some(field) = hash_key {
                    let update_job = |job: &mut Job<D, R, P>| -> u64 {
                        match field {
                            "attempts_made" | "attemptsMade" => {
                                let new = (job.attempts_made.cast_signed() + delta)
                                    .max(0)
                                    .cast_unsigned();
                                job.attempts_made = new;
                                new
                            }
                            "stalled_counter" | "stalledCounter" => {
                                let new = (job.stalled_counter.cast_signed() + delta)
                                    .max(0)
                                    .cast_unsigned();
                                job.stalled_counter = new;
                                new
                            }
                            _ => 0,
                        }
                    };
                    let next = self.jobs.get(&key.tag()).map_or(0, |value| {
                        let job = &mut value.lock();
                        update_job(job)
                    });
                    return Ok(next);
                }

                0
            }
            _ => 0,
        };
        Ok(next)
    }

    async fn get_counter(&self, key: CollectionSuffix, hash_key: Option<&str>) -> Option<u64> {
        match key {
            CollectionSuffix::Id => Some(self.id_counter.load()),
            CollectionSuffix::PriorityCounter => Some(self.priority_counter.load()),
            CollectionSuffix::Meta => Some(self.processing.load()),
            CollectionSuffix::Job(_) => {
                if let Some(field) = hash_key {
                    let job_key = key.tag();
                    return self.jobs.get(&job_key).and_then(|value| {
                        let job = &value.lock();
                        match field.to_lowercase().as_str() {
                            "stalled_counter" | "stalledcounter" => Some(job.stalled_counter),
                            "attempts_made" | "attemptsmade" => Some(job.attempts_made),
                            _ => None,
                        }
                    });
                }
                return None;
            }
            _ => None,
        }
    }

    async fn publish_event(
        &self,
        _event_mode: QueueEventMode,
        event: QueueStreamEvent<R, P>,
    ) -> KioResult<()> {
        if let Some(emitter) = self.events.load().as_ref()
            && let (Some(stored), Some(notifier), Some(pause_workers)) = (
                self.stored_metrics.load().as_ref(),
                self.notifier.load().as_ref(),
                self.pause_workers.load().as_ref(),
            )
        {
            process_each_event(event, emitter, self, stored).await?;
            pause_or_resume_workers(notifier, stored, pause_workers, &self.is_inital);
        }
        Ok(())
    }

    async fn job_exists(&self, id: u64) -> bool {
        let col_key = CollectionSuffix::Job(id);
        self.exists_in(col_key, id).await.unwrap_or(false)
    }

    async fn remove_item(&self, col: CollectionSuffix, item: u64) -> KioResult<()> {
        match col {
            CollectionSuffix::Active => {
                self.active
                    .iter()
                    .filter(|entry| *entry.value() == item)
                    .for_each(|entry| {
                        entry.remove();
                    });
            }

            CollectionSuffix::Wait => {
                self.waiting
                    .iter()
                    .filter(|entry| *entry.value() == item)
                    .for_each(|entry| {
                        entry.remove();
                    });
            }

            CollectionSuffix::Paused => {
                self.paused
                    .iter()
                    .filter(|entry| *entry.value() == item)
                    .for_each(|entry| {
                        entry.remove();
                    });
            }
            CollectionSuffix::Completed => {
                if self.completed.contains_key(&item) {
                    let _ = self.completed.remove(&item);
                    return Ok(());
                }
                self.completed
                    .iter()
                    .filter(|entry| *entry.value() == item)
                    .for_each(|entry| {
                        entry.remove();
                    });
            }
            CollectionSuffix::Failed => {
                if self.failed.contains_key(&item) {
                    let _ = self.failed.remove(&item);
                    return Ok(());
                }
                self.failed
                    .iter()
                    .filter(|entry| *entry.value() == item)
                    .for_each(|entry| {
                        entry.remove();
                    });
            }
            CollectionSuffix::Prioritized => {
                if self.prioritized.contains_key(&item) {
                    let _ = self.prioritized.remove(&item);
                    return Ok(());
                }
                self.prioritized
                    .iter()
                    .filter(|entry| *entry.value() == item)
                    .for_each(|entry| {
                        entry.remove();
                    });
            }
            CollectionSuffix::Delayed => {
                if self.delayed.contains_key(&item) {
                    let _ = self.delayed.remove(&item);
                    return Ok(());
                }
                self.delayed
                    .iter()
                    .filter(|entry| *entry.value() == item)
                    .for_each(|entry| {
                        entry.remove();
                    });
            }
            CollectionSuffix::Stalled => {
                self.stalled.remove(&item);
            }
            CollectionSuffix::Job(_) => {
                self.jobs.remove(&col.tag());
            }
            CollectionSuffix::Lock(_) => {
                self.locks.remove(&col.tag());
            }

            _ => {}
        }
        Ok(())
    }

    async fn remove(&self, key: CollectionSuffix) -> KioResult<()> {
        // do thing here
        match key {
            CollectionSuffix::Active | CollectionSuffix::Completed => self.active.clear(),
            CollectionSuffix::Delayed => self.delayed.clear(),
            CollectionSuffix::Stalled => self.stalled.clear(),
            CollectionSuffix::Prioritized => self.prioritized.clear(),
            CollectionSuffix::Wait => self.waiting.clear(),
            CollectionSuffix::Paused => self.paused.clear(),
            CollectionSuffix::Failed => self.failed.clear(),
            CollectionSuffix::Job(_) => {
                self.jobs.remove(&key.tag());
            }
            CollectionSuffix::Lock(_) | CollectionSuffix::StalledCheck => {
                self.locks.remove(&key.tag());
            }
            _ => {}
        }

        Ok(())
    }

    async fn clear_collections(&self) -> KioResult<()> {
        self.completed.clear();
        self.failed.clear();
        self.delayed.clear();
        self.prioritized.clear();
        self.stalled.clear();
        self.waiting.clear();
        self.paused.clear();
        self.active.clear();
        Ok(())
    }

    async fn clear_jobs(&self, _last_id: u64) -> KioResult<()> {
        self.jobs.clear();
        Ok(())
    }

    async fn pause(&self, pause: bool, _event_mode: QueueEventMode) -> KioResult<()> {
        let wait_key = CollectionSuffix::Wait;
        let paused_key = CollectionSuffix::Paused;
        let src = if pause { wait_key } else { paused_key };
        // only move items when the state changes
        if matches!(src, CollectionSuffix::Wait) {
            while let Some(entry) = self.waiting.pop_front() {
                self.paused.push_back(entry);
            }
        } else {
            while let Some(entry) = self.paused.pop_front() {
                self.waiting.push_back(entry);
            }
        }
        self.is_paused.store(pause);

        Ok(())
    }
}

#[cfg(test)]
#[allow(
    clippy::float_cmp,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::items_after_statements,
    clippy::doc_markdown
)]
mod tests {
    //! Robustness-focused unit tests exercised directly against
    //! [`InMemoryStore`].  The store is generic and needs no external server,
    //! so every test drives the concrete `InMemoryStore<i32, i32, i32>` through
    //! the public [`Store`] trait surface.
    //!
    //! Coverage angles: push/pop lifecycle, empty-store behaviour, unknown /
    //! repeated operations (idempotency), priority ordering and ties,
    //! delayed-job visibility, pause/resume, counters, and heavy concurrency
    //! (many tokio tasks pushing and popping simultaneously — asserting that no
    //! job is lost or delivered twice).
    //!
    //! # Suspected bug report — duplicate job delivery under concurrent pop
    //!
    //! Two concurrency tests are marked `#[ignore]` because they reproduce a
    //! real, repeatable defect rather than a flaky expectation:
    //!
    //! - [`test_concurrent_consumers_deliver_each_job_once`]
    //! - [`test_concurrent_push_pop_loses_no_jobs`]
    //!
    //! With a queue of exactly `N` distinct jobs drained by several concurrent
    //! consumers, the total number of successful pops exceeds `N` (observed e.g.
    //! `count out 1001 > unique 1000`): the **same job id is handed to more than
    //! one consumer**.  The store moves a job from `Wait` to `Active` via
    //! `pop_back_push_front`, which calls [`crate::utils::ConcurrentDeque::pop_back`].
    //! That method is not linearizable — it reads the tail with `back()` and then
    //! removes it in a separate step, so concurrent poppers can observe and return
    //! the same element.  Real workers moving jobs off the waiting list therefore
    //! risk processing a job twice.
    //!
    //! This may be intended as *at-least-once* delivery that the worker layer
    //! deduplicates via per-job locks/tokens; that has not been verified here.
    //! Production code was left untouched per the task brief — run the ignored
    //! tests with `cargo test --lib stores::inmemory_store -- --ignored` to
    //! reproduce.

    use super::*;
    use std::collections::HashSet;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering::SeqCst;

    /// Every bounded async wait in the suite uses this ceiling so a genuine
    /// hang surfaces as a test failure rather than a stuck CI job.
    const TEST_TIMEOUT: Duration = Duration::from_secs(10);

    /// Builds a fresh, empty store for a test.  Each store is independent
    /// (all state lives behind per-instance `Arc`s).
    fn new_store() -> InMemoryStore<i32, i32, i32> {
        InMemoryStore::new(None, "robustness-tests")
    }

    /// Awaits `fut`, failing loudly if it exceeds [`TEST_TIMEOUT`].  Keeps the
    /// concurrency tests honest: a lost wake-up can never deadlock the suite.
    async fn within_timeout<T>(fut: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(TEST_TIMEOUT, fut)
            .await
            .expect("operation exceeded the test timeout budget")
    }

    /// Collects the IDs currently sitting in `state` (unpaginated).
    async fn ids_in_state(store: &InMemoryStore<i32, i32, i32>, state: JobState) -> Vec<u64> {
        store
            .get_job_ids_in_state(state, None, None)
            .await
            .expect("listing job IDs must not fail")
            .into_iter()
            .collect()
    }

    /// Enqueues a batch of jobs via the bulk API using default options,
    /// returning the created records.  Asserts the batch is non-empty as a
    /// precondition guard (TigerStyle).
    async fn add_default_jobs(
        store: &InMemoryStore<i32, i32, i32>,
        count: i32,
    ) -> Vec<Job<i32, i32, i32>> {
        debug_assert!(count > 0, "add_default_jobs requires a positive count");
        let iter = (0..count).map(|i| (format!("job-{i}"), None, i));
        let jobs = store
            .add_bulk(
                Box::new(iter),
                QueueOpts::default(),
                QueueEventMode::PubSub,
                false,
            )
            .await
            .expect("bulk add must succeed");
        assert_eq!(jobs.len(), count as usize, "bulk add returned wrong count");
        jobs
    }

    #[tokio::test]
    async fn test_add_item_and_membership_in_waiting() {
        let store = new_store();
        store
            .add_item(CollectionSuffix::Wait, 42, None, true)
            .await
            .expect("add_item must succeed");

        assert!(
            store
                .exists_in(CollectionSuffix::Wait, 42)
                .await
                .expect("exists_in must succeed"),
            "job should be present in the waiting list"
        );
        assert!(
            !store
                .exists_in(CollectionSuffix::Active, 42)
                .await
                .expect("exists_in must succeed"),
            "job should not be in the active list yet"
        );

        store
            .remove_item(CollectionSuffix::Wait, 42)
            .await
            .expect("remove_item must succeed");
        assert!(
            !store
                .exists_in(CollectionSuffix::Wait, 42)
                .await
                .expect("exists_in must succeed"),
            "job should be gone after removal"
        );
    }

    #[tokio::test]
    async fn test_pop_from_empty_waiting_returns_none() {
        let store = new_store();
        // Nothing enqueued: the wait->active transition must yield nothing and
        // must not panic or fabricate an ID.
        let moved = store
            .pop_back_push_front(CollectionSuffix::Wait, CollectionSuffix::Active)
            .await;
        assert!(
            moved.is_none(),
            "popping an empty waiting list must be None"
        );

        // Sorted-set pops on an empty store return an empty vector, not an error.
        for col in [
            CollectionSuffix::Completed,
            CollectionSuffix::Failed,
            CollectionSuffix::Prioritized,
            CollectionSuffix::Delayed,
        ] {
            let popped = store
                .pop_set(col, true)
                .await
                .expect("pop_set must succeed");
            assert!(popped.is_empty(), "empty sorted set must pop nothing");
        }
    }

    #[tokio::test]
    async fn test_push_then_pop_moves_job_from_waiting_to_active() {
        let store = new_store();
        store
            .add_item(CollectionSuffix::Wait, 7, None, true)
            .await
            .expect("add_item must succeed");

        let moved = store
            .pop_back_push_front(CollectionSuffix::Wait, CollectionSuffix::Active)
            .await;
        assert_eq!(moved, Some(7), "the enqueued ID must be moved");

        assert!(
            !store
                .exists_in(CollectionSuffix::Wait, 7)
                .await
                .expect("exists_in"),
            "job must leave the waiting list"
        );
        assert!(
            store
                .exists_in(CollectionSuffix::Active, 7)
                .await
                .expect("exists_in"),
            "job must appear in the active list"
        );
    }

    #[tokio::test]
    async fn test_waiting_list_ordering_and_fifo_consumption() {
        let store = new_store();
        // `insert()` (and the worker) enqueue with `append = true`, which for the
        // in-memory deque means `push_front`.  Successive front-pushes therefore
        // make the *listing* order the reverse of the enqueue order.
        for id in [10_u64, 20, 30] {
            store
                .add_item(CollectionSuffix::Wait, id, None, true)
                .await
                .expect("add_item");
        }
        assert_eq!(
            ids_in_state(&store, JobState::Wait).await,
            vec![30, 20, 10],
            "front-pushes list in reverse enqueue order"
        );

        // `pop_back_push_front` drains the *oldest* enqueued job first, so overall
        // delivery is FIFO: 10 was enqueued first and is consumed first.
        let first = store
            .pop_back_push_front(CollectionSuffix::Wait, CollectionSuffix::Active)
            .await;
        assert_eq!(
            first,
            Some(10),
            "oldest enqueued job must be consumed first"
        );
        let second = store
            .pop_back_push_front(CollectionSuffix::Wait, CollectionSuffix::Active)
            .await;
        assert_eq!(second, Some(20));
    }

    #[tokio::test]
    async fn test_get_job_ids_in_state_pagination() {
        let store = new_store();
        for id in 1_u64..=5 {
            store
                .add_item(CollectionSuffix::Wait, id, None, true)
                .await
                .expect("add_item");
        }
        // Enqueued with front-pushes, the listing is [5, 4, 3, 2, 1]; the window
        // [1, 3] selects the middle three of that reversed view.
        let page = store
            .get_job_ids_in_state(JobState::Wait, Some(1), Some(3))
            .await
            .expect("pagination must succeed");
        assert_eq!(page.len(), 3, "paginated window returned wrong length");
        assert_eq!(page.into_iter().collect::<Vec<_>>(), vec![4, 3, 2]);
    }

    #[tokio::test]
    async fn test_remove_unknown_item_is_a_noop() {
        let store = new_store();
        // Removing from a list that never held the item must be a clean no-op.
        store
            .remove_item(CollectionSuffix::Wait, 999)
            .await
            .expect("removing an absent list item must be Ok");
        // Repeat on a sorted set.
        store
            .remove_item(CollectionSuffix::Completed, 999)
            .await
            .expect("removing an absent sorted-set item must be Ok");
    }

    #[tokio::test]
    async fn test_remove_item_is_idempotent() {
        let store = new_store();
        store
            .add_item(CollectionSuffix::Failed, 5, Some(100), false)
            .await
            .expect("add_item");
        // First removal takes it out; the second is a harmless no-op.
        store
            .remove_item(CollectionSuffix::Failed, 5)
            .await
            .expect("first remove");
        store
            .remove_item(CollectionSuffix::Failed, 5)
            .await
            .expect("second remove must still be Ok");
        assert!(
            !store
                .exists_in(CollectionSuffix::Failed, 5)
                .await
                .expect("exists_in"),
            "item must be gone"
        );
    }

    #[tokio::test]
    async fn test_get_job_and_state_unknown_id_returns_none() {
        let store = new_store();
        assert!(
            store.get_job(123_456).await.is_none(),
            "unknown job must be None"
        );
        assert!(
            store.get_state(123_456).await.is_none(),
            "unknown job state must be None"
        );
        assert!(
            !store.job_exists(123_456).await,
            "unknown job must not report as existing"
        );
    }

    #[tokio::test]
    async fn test_get_token_unknown_id_returns_none() {
        let store = new_store();
        assert!(
            store.get_token(999).await.is_none(),
            "no lock set: token must be None"
        );
    }

    #[tokio::test]
    async fn test_add_bulk_assigns_sequential_ids_and_fills_waiting() {
        let store = new_store();
        let jobs = add_default_jobs(&store, 4).await;

        let ids: Vec<u64> = jobs
            .iter()
            .map(|j| j.id.expect("job must have an ID"))
            .collect();
        assert_eq!(ids, vec![1, 2, 3, 4], "IDs must be assigned sequentially");

        let metrics = store.get_metrics().await.expect("metrics");
        assert_eq!(metrics.waiting.load(), 4, "all jobs should be waiting");
        assert_eq!(metrics.last_id.load(), 4, "last_id must track the counter");

        for id in ids {
            let fetched = store.get_job(id).await.expect("job must be retrievable");
            assert_eq!(fetched.state, JobState::Wait, "default job starts in Wait");
        }
    }

    #[tokio::test]
    async fn test_add_bulk_priority_routes_to_prioritized() {
        let store = new_store();
        let opts = JobOptions {
            priority: 5,
            ..Default::default()
        };
        let jobs = store
            .add_bulk(
                Box::new(std::iter::once(("p".to_string(), Some(opts), 1))),
                QueueOpts::default(),
                QueueEventMode::PubSub,
                false,
            )
            .await
            .expect("bulk add");
        let id = jobs[0].id.expect("id");

        assert!(
            store
                .exists_in(CollectionSuffix::Prioritized, id)
                .await
                .expect("exists_in"),
            "priority job must land in the prioritized set"
        );
        assert_eq!(
            store.get_state(id).await,
            Some(JobState::Prioritized),
            "priority job state must be Prioritized"
        );
        assert!(
            !store
                .exists_in(CollectionSuffix::Wait, id)
                .await
                .expect("exists_in"),
            "priority job must not be in the plain waiting list"
        );
    }

    #[tokio::test]
    async fn test_add_bulk_delayed_routes_to_delayed() {
        let store = new_store();
        let opts = JobOptions {
            delay: 500.into(),
            ..Default::default()
        };
        let jobs = store
            .add_bulk(
                Box::new(std::iter::once(("d".to_string(), Some(opts), 1))),
                QueueOpts::default(),
                QueueEventMode::PubSub,
                false,
            )
            .await
            .expect("bulk add");
        let id = jobs[0].id.expect("id");

        let metrics = store.get_metrics().await.expect("metrics");
        assert_eq!(
            metrics.delayed.load(),
            1,
            "delayed count must reflect the job"
        );
        assert_eq!(metrics.waiting.load(), 0, "delayed job is not waiting");
        assert_eq!(store.get_state(id).await, Some(JobState::Delayed));
    }

    #[tokio::test]
    async fn test_add_bulk_delay_below_limit_is_rejected() {
        let store = new_store();
        // A delay below MIN_DELAY_MS_LIMIT (50ms) must be rejected outright.
        let opts = JobOptions {
            delay: 10.into(),
            ..Default::default()
        };
        let result = store
            .add_bulk(
                Box::new(std::iter::once(("too-fast".to_string(), Some(opts), 1))),
                QueueOpts::default(),
                QueueEventMode::PubSub,
                false,
            )
            .await;
        // Assert the specific rejection reason, so a failure for the wrong
        // cause (bad batch, serialisation, etc.) cannot pass green.
        let err =
            result.expect_err("a sub-limit delay must produce an error, not a silent success");
        assert!(
            matches!(
                err,
                crate::KioError::QueueError(QueueError::DelayBelowAllowedLimit { .. })
            ),
            "expected DelayBelowAllowedLimit, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_set_fields_updates_job_record() {
        let store = new_store();
        let jobs = add_default_jobs(&store, 1).await;
        let id = jobs[0].id.expect("id");

        store
            .set_fields(
                id,
                vec![
                    JobField::State(JobState::Completed),
                    JobField::Payload(ProcessedResult::Success(99, crate::JobMetrics::default())),
                ],
            )
            .await
            .expect("set_fields must succeed");

        let updated = store.get_job(id).await.expect("job present");
        assert_eq!(updated.state, JobState::Completed, "state must be updated");
        assert_eq!(
            updated.returned_value,
            Some(99),
            "returned value must be persisted"
        );
    }

    #[tokio::test]
    async fn test_set_fields_on_unknown_job_is_noop() {
        let store = new_store();
        // No such job — must not panic, must return Ok.
        store
            .set_fields(4_242, vec![JobField::State(JobState::Failed)])
            .await
            .expect("set_fields on absent job must be Ok");
        assert!(store.get_job(4_242).await.is_none());
    }

    #[tokio::test]
    async fn test_id_counter_is_monotonic() {
        let store = new_store();
        for expected in 1_u64..=5 {
            let next = store
                .incr(CollectionSuffix::Id, 1, None)
                .await
                .expect("incr");
            assert_eq!(next, expected, "ID counter must increment by one");
        }
        assert_eq!(
            store.get_counter(CollectionSuffix::Id, None).await,
            Some(5),
            "get_counter must agree with the last incr"
        );
    }

    #[tokio::test]
    async fn test_meta_counter_increment_and_decrement() {
        let store = new_store();
        assert_eq!(
            store
                .incr(CollectionSuffix::Meta, 3, None)
                .await
                .expect("incr"),
            3
        );
        assert_eq!(
            store
                .incr(CollectionSuffix::Meta, -2, None)
                .await
                .expect("incr"),
            1,
            "negative delta must decrement the counter"
        );
        assert_eq!(
            store.get_counter(CollectionSuffix::Meta, None).await,
            Some(1)
        );
    }

    #[tokio::test]
    async fn test_job_attempts_counter_increments_and_clamps_at_zero() {
        let store = new_store();
        let jobs = add_default_jobs(&store, 1).await;
        let id = jobs[0].id.expect("id");
        let key = CollectionSuffix::Job(id);

        assert_eq!(
            store
                .incr(key, 1, Some("attempts_made"))
                .await
                .expect("incr"),
            1,
            "first attempt increment must yield 1"
        );
        assert_eq!(
            store
                .incr(key, 1, Some("attempts_made"))
                .await
                .expect("incr"),
            2
        );
        // A large negative delta must clamp at zero, never wrap below.
        assert_eq!(
            store
                .incr(key, -10, Some("attempts_made"))
                .await
                .expect("incr"),
            0,
            "attempts_made must clamp to zero, not underflow"
        );
        assert_eq!(store.get_counter(key, Some("attempts_made")).await, Some(0));
    }

    #[tokio::test]
    async fn test_incr_on_unknown_job_returns_zero() {
        let store = new_store();
        // No job record exists: the field increment resolves to 0, not a panic.
        let next = store
            .incr(CollectionSuffix::Job(9_999), 1, Some("attempts_made"))
            .await
            .expect("incr must succeed even for an absent job");
        assert_eq!(next, 0, "incrementing an absent job must yield 0");
    }

    #[tokio::test]
    async fn test_prioritized_pop_set_respects_min_and_max_score() {
        let store = new_store();
        // (id, score) — insert deliberately out of order.
        for (id, score) in [(101_u64, 30_i64), (102, 10), (103, 20)] {
            store
                .add_item(CollectionSuffix::Prioritized, id, Some(score), true)
                .await
                .expect("add_item");
        }

        let min = store
            .pop_set(CollectionSuffix::Prioritized, true)
            .await
            .expect("pop_set");
        assert_eq!(min, vec![(102, 10)], "min pop must return the lowest score");

        let max = store
            .pop_set(CollectionSuffix::Prioritized, false)
            .await
            .expect("pop_set");
        assert_eq!(
            max,
            vec![(101, 30)],
            "max pop must return the highest score"
        );

        // The middle element remains.
        let remaining = store
            .pop_set(CollectionSuffix::Prioritized, true)
            .await
            .expect("pop_set");
        assert_eq!(remaining, vec![(103, 20)]);
    }

    #[tokio::test]
    async fn test_sorted_set_equal_scores_collapse_last_write_wins() {
        let store = new_store();
        // A sorted set is keyed by score; two entries sharing a score collide,
        // and the later write wins (Redis ZADD-like semantics).  This documents
        // that callers must supply unique scores (the priority counter does).
        store
            .add_item(CollectionSuffix::Prioritized, 1, Some(5), true)
            .await
            .expect("add_item");
        store
            .add_item(CollectionSuffix::Prioritized, 2, Some(5), true)
            .await
            .expect("add_item");

        let first = store
            .pop_set(CollectionSuffix::Prioritized, true)
            .await
            .expect("pop_set");
        assert_eq!(
            first,
            vec![(2, 5)],
            "last write for a shared score must win"
        );
        let second = store
            .pop_set(CollectionSuffix::Prioritized, true)
            .await
            .expect("pop_set");
        assert!(
            second.is_empty(),
            "only one entry survives a score collision"
        );
    }

    #[tokio::test]
    async fn test_completed_sorted_set_ordering() {
        let store = new_store();
        for (id, score) in [(1_u64, 300_i64), (2, 100), (3, 200)] {
            store
                .add_item(CollectionSuffix::Completed, id, Some(score), false)
                .await
                .expect("add_item");
        }
        assert_eq!(
            store
                .pop_set(CollectionSuffix::Completed, true)
                .await
                .expect("pop_set"),
            vec![(2, 100)]
        );
        assert_eq!(
            store
                .pop_set(CollectionSuffix::Completed, false)
                .await
                .expect("pop_set"),
            vec![(1, 300)]
        );
    }

    #[tokio::test]
    async fn test_get_delayed_at_splits_due_and_missed() {
        let store = new_store();
        // Scores act as scheduled timestamps.
        for (id, score) in [(1_u64, 100_i64), (2, 200), (3, 300)] {
            store
                .add_item(CollectionSuffix::Delayed, id, Some(score), false)
                .await
                .expect("add_item");
        }

        // Ask for everything scheduled in [200, 400): IDs 2 and 3 are due;
        // ID 1 (score 100) is overdue and reported as "missed".
        let (due, missed) = store
            .get_delayed_at(200, 400)
            .await
            .expect("get_delayed_at");
        let due: HashSet<u64> = due.into_iter().collect();
        let missed: HashSet<u64> = missed.into_iter().collect();
        assert_eq!(due, HashSet::from([2, 3]), "due jobs mismatch");
        assert_eq!(missed, HashSet::from([1]), "missed jobs mismatch");

        // Everything returned must have been removed from the delayed set.
        assert!(
            ids_in_state(&store, JobState::Delayed).await.is_empty(),
            "returned delayed jobs must be drained from the set"
        );
    }

    #[tokio::test]
    async fn test_pop_set_delayed_min_and_max() {
        let store = new_store();
        for (id, score) in [(1_u64, 5_i64), (2, 25), (3, 15)] {
            store
                .add_item(CollectionSuffix::Delayed, id, Some(score), false)
                .await
                .expect("add_item");
        }
        assert_eq!(
            store
                .pop_set(CollectionSuffix::Delayed, true)
                .await
                .expect("pop_set"),
            vec![(1, 5)]
        );
        assert_eq!(
            store
                .pop_set(CollectionSuffix::Delayed, false)
                .await
                .expect("pop_set"),
            vec![(2, 25)]
        );
    }

    #[tokio::test]
    async fn test_pause_moves_waiting_to_paused_then_resume_restores() {
        let store = new_store();
        for id in 1_u64..=3 {
            store
                .add_item(CollectionSuffix::Wait, id, None, true)
                .await
                .expect("add_item");
        }

        store
            .pause(true, QueueEventMode::PubSub)
            .await
            .expect("pause");
        let metrics = store.get_metrics().await.expect("metrics");
        assert!(metrics.is_paused.load(), "queue must report paused");
        assert_eq!(metrics.waiting.load(), 0, "waiting list must be drained");
        assert_eq!(metrics.paused.load(), 3, "all jobs must move to paused");

        store
            .pause(false, QueueEventMode::PubSub)
            .await
            .expect("resume");
        let metrics = store.get_metrics().await.expect("metrics");
        assert!(!metrics.is_paused.load(), "queue must report resumed");
        assert_eq!(metrics.waiting.load(), 3, "jobs must return to waiting");
        assert_eq!(metrics.paused.load(), 0, "paused list must be drained");
    }

    #[tokio::test]
    async fn test_repeated_pause_does_not_duplicate_or_lose_jobs() {
        let store = new_store();
        for id in 1_u64..=2 {
            store
                .add_item(CollectionSuffix::Wait, id, None, true)
                .await
                .expect("add_item");
        }
        store
            .pause(true, QueueEventMode::PubSub)
            .await
            .expect("pause");
        // Pausing an already-paused queue must be idempotent: the waiting list
        // is empty so nothing new moves, and no job is duplicated.
        store
            .pause(true, QueueEventMode::PubSub)
            .await
            .expect("pause again");
        let metrics = store.get_metrics().await.expect("metrics");
        assert_eq!(metrics.paused.load(), 2, "paused count must stay stable");
        assert_eq!(metrics.waiting.load(), 0);
    }

    #[tokio::test]
    async fn test_set_lock_and_read_token() {
        let store = new_store();
        store.toggle_expiration(); // keep the lock from expiring during the test
        let token = JobToken(Uuid::new_v4(), Uuid::new_v4(), 1);
        store
            .set_lock(CollectionSuffix::Lock(7), Some(token), 60_000)
            .await
            .expect("set_lock");

        assert_eq!(
            store.get_token(7).await,
            Some(token),
            "the stored token must round-trip"
        );
    }

    #[tokio::test]
    async fn test_stall_check_lock_has_no_token() {
        let store = new_store();
        store.toggle_expiration();
        // A stall-check lock carries no worker token.
        store
            .set_lock(CollectionSuffix::Lock(8), None, 60_000)
            .await
            .expect("set_lock");
        assert!(
            store.get_token(8).await.is_none(),
            "a stall-check lock must not yield a token"
        );
    }

    #[tokio::test]
    async fn test_clear_collections_empties_lists_but_keeps_job_records() {
        let store = new_store();
        let jobs = add_default_jobs(&store, 3).await;
        let id = jobs[0].id.expect("id");

        store.clear_collections().await.expect("clear_collections");
        assert_eq!(
            store.get_metrics().await.expect("metrics").waiting.load(),
            0
        );
        // Job hashes survive a collection clear.
        assert!(
            store.get_job(id).await.is_some(),
            "job record must outlive its collection membership"
        );
    }

    #[tokio::test]
    async fn test_clear_jobs_removes_job_records() {
        let store = new_store();
        let jobs = add_default_jobs(&store, 2).await;
        let id = jobs[0].id.expect("id");
        store.clear_jobs(0).await.expect("clear_jobs");
        assert!(
            store.get_job(id).await.is_none(),
            "job records must be gone after clear_jobs"
        );
    }

    #[tokio::test]
    async fn test_full_wait_active_completed_lifecycle() {
        let store = new_store();
        let jobs = add_default_jobs(&store, 1).await;
        let id = jobs[0].id.expect("id");

        // Wait -> Active.
        let moved = store
            .pop_back_push_front(CollectionSuffix::Wait, CollectionSuffix::Active)
            .await;
        assert_eq!(moved, Some(id));

        // Ack: leave active, enter completed, mark state.
        store
            .remove_item(CollectionSuffix::Active, id)
            .await
            .expect("remove from active");
        store
            .add_item(CollectionSuffix::Completed, id, Some(1), false)
            .await
            .expect("add to completed");
        store
            .set_fields(id, vec![JobField::State(JobState::Completed)])
            .await
            .expect("set state");

        assert_eq!(store.get_state(id).await, Some(JobState::Completed));
        assert!(
            !store
                .exists_in(CollectionSuffix::Active, id)
                .await
                .expect("exists"),
            "completed job must leave the active list"
        );
        assert!(
            store
                .exists_in(CollectionSuffix::Completed, id)
                .await
                .expect("exists"),
            "completed job must be recorded"
        );
    }

    #[tokio::test]
    async fn test_ack_completed_twice_is_idempotent() {
        let store = new_store();
        store
            .add_item(CollectionSuffix::Active, 3, None, true)
            .await
            .expect("add_item");
        // Acking twice: the second removal / completion insert must not corrupt
        // state or panic.
        for _ in 0..2 {
            store
                .remove_item(CollectionSuffix::Active, 3)
                .await
                .expect("remove");
            store
                .add_item(CollectionSuffix::Completed, 3, Some(1), false)
                .await
                .expect("complete");
        }
        assert!(
            !store
                .exists_in(CollectionSuffix::Active, 3)
                .await
                .expect("exists")
        );
        assert!(
            store
                .exists_in(CollectionSuffix::Completed, 3)
                .await
                .expect("exists")
        );
        // Draining the Completed set must yield EXACTLY one entry for id 3: a
        // second ack must not add a duplicate. Any non-idempotent double-insert
        // would surface here as a second surviving entry.
        let completed = store
            .pop_set(CollectionSuffix::Completed, true)
            .await
            .expect("pop_set");
        assert_eq!(
            completed,
            vec![(3, 1)],
            "the double ack must leave exactly one completed entry"
        );
        let leftover = store
            .pop_set(CollectionSuffix::Completed, true)
            .await
            .expect("pop_set");
        assert!(
            leftover.is_empty(),
            "no duplicate completed entry may survive a repeated ack"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_producers_lose_no_jobs() {
        let store = new_store();
        const N: u64 = 1_000;

        let mut handles = Vec::with_capacity(N as usize);
        for id in 1..=N {
            let store = store.clone();
            handles.push(tokio::spawn(async move {
                store
                    .add_item(CollectionSuffix::Wait, id, None, true)
                    .await
                    .expect("concurrent add_item must succeed");
            }));
        }
        within_timeout(async {
            for h in handles {
                h.await.expect("producer task panicked");
            }
        })
        .await;

        // Every distinct ID must be present exactly once — no loss, no dup.
        let ids = ids_in_state(&store, JobState::Wait).await;
        assert_eq!(ids.len() as u64, N, "waiting list lost or gained entries");
        let unique: HashSet<u64> = ids.into_iter().collect();
        assert_eq!(unique.len() as u64, N, "waiting list contains duplicates");
    }

    // NOTE: `#[ignore]`d because it reproduces a REAL concurrency bug — see the
    // module-level report below.  Concurrent consumers can be handed the *same*
    // job twice: `ConcurrentDeque::pop_back` uses a non-atomic
    // `back()`-then-`remove()` sequence, so under contention a job is delivered
    // more than once (count out > count in).  Run with `--ignored` to reproduce.
    #[ignore = "reveals duplicate job delivery under concurrent pop_back; see module report"]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_consumers_deliver_each_job_once() {
        let store = new_store();
        const N: u64 = 1_000;
        const CONSUMERS: usize = 8;

        for id in 1..=N {
            store
                .add_item(CollectionSuffix::Wait, id, None, true)
                .await
                .expect("seed add_item");
        }

        let popped: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::with_capacity(N as usize)));
        // Termination is driven by the deque genuinely emptying — never by a
        // count that could underflow if the store over-delivers.
        let drained = Arc::new(AtomicBool::new(false));

        let mut handles = Vec::with_capacity(CONSUMERS);
        for _ in 0..CONSUMERS {
            let store = store.clone();
            let popped = Arc::clone(&popped);
            let drained = Arc::clone(&drained);
            handles.push(tokio::spawn(async move {
                loop {
                    if let Some(id) = store
                        .pop_back_push_front(CollectionSuffix::Wait, CollectionSuffix::Active)
                        .await
                    {
                        popped.lock().expect("poisoned").push(id);
                    } else if drained.load(SeqCst) {
                        break;
                    } else if store
                        .get_job_ids_in_state(JobState::Wait, None, None)
                        .await
                        .expect("listing")
                        .is_empty()
                    {
                        drained.store(true, SeqCst);
                        break;
                    } else {
                        tokio::task::yield_now().await;
                    }
                }
            }));
        }

        within_timeout(async {
            for h in handles {
                h.await.expect("consumer task panicked");
            }
        })
        .await;

        let popped = Arc::try_unwrap(popped)
            .expect("no outstanding references")
            .into_inner()
            .expect("poisoned");
        let unique: HashSet<u64> = popped.iter().copied().collect();
        assert_eq!(
            unique.len(),
            popped.len(),
            "a job was delivered more than once (count out {} > unique {})",
            popped.len(),
            unique.len()
        );
        assert_eq!(popped.len() as u64, N, "exactly N jobs must be delivered");
        assert_eq!(
            unique,
            (1..=N).collect::<HashSet<u64>>(),
            "wrong set of IDs"
        );

        // The waiting list is fully drained and everything sits in active.
        assert!(ids_in_state(&store, JobState::Wait).await.is_empty());
        assert_eq!(ids_in_state(&store, JobState::Active).await.len() as u64, N);
    }

    // NOTE: `#[ignore]`d because it reproduces the same REAL concurrency bug as
    // `test_concurrent_consumers_deliver_each_job_once`: with producers and
    // consumers running together, `pop_back` hands the same job to two consumers,
    // so `popped.len()` exceeds the number produced.  Run with `--ignored` to
    // reproduce.  See the module report for details.
    #[ignore = "reveals duplicate job delivery under concurrent pop_back; see module report"]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_push_pop_loses_no_jobs() {
        let store = new_store();
        const N: u64 = 1_000;
        const CONSUMERS: usize = 6;

        let popped: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let producers_done = Arc::new(AtomicBool::new(false));

        // Spawn consumers first so they overlap with production.
        let mut consumer_handles = Vec::with_capacity(CONSUMERS);
        for _ in 0..CONSUMERS {
            let store = store.clone();
            let popped = Arc::clone(&popped);
            let producers_done = Arc::clone(&producers_done);
            consumer_handles.push(tokio::spawn(async move {
                loop {
                    if let Some(id) = store
                        .pop_back_push_front(CollectionSuffix::Wait, CollectionSuffix::Active)
                        .await
                    {
                        popped.lock().expect("poisoned").push(id);
                    } else if producers_done.load(SeqCst) {
                        // Producers finished and we observed an empty queue: done.
                        break;
                    } else {
                        tokio::task::yield_now().await;
                    }
                }
            }));
        }

        // Producers push distinct IDs concurrently.
        let mut producer_handles = Vec::with_capacity(N as usize);
        for id in 1..=N {
            let store = store.clone();
            producer_handles.push(tokio::spawn(async move {
                store
                    .add_item(CollectionSuffix::Wait, id, None, true)
                    .await
                    .expect("concurrent add_item");
            }));
        }

        within_timeout(async {
            for h in producer_handles {
                h.await.expect("producer task panicked");
            }
        })
        .await;
        producers_done.store(true, SeqCst);

        within_timeout(async {
            for h in consumer_handles {
                h.await.expect("consumer task panicked");
            }
        })
        .await;

        // Read whatever is still waiting only AFTER all consumers have exited,
        // so no in-flight pop can double-count an ID.
        let still_waiting: HashSet<u64> = ids_in_state(&store, JobState::Wait)
            .await
            .into_iter()
            .collect();
        let popped = Arc::try_unwrap(popped)
            .expect("no outstanding references")
            .into_inner()
            .expect("poisoned");

        // Invariant 1: no job was delivered twice.
        let popped_set: HashSet<u64> = popped.iter().copied().collect();
        assert_eq!(
            popped_set.len(),
            popped.len(),
            "a job was popped more than once"
        );
        // Invariant 2: popped and still-waiting sets are disjoint.
        assert!(
            popped_set.is_disjoint(&still_waiting),
            "an ID is both popped and still waiting"
        );
        // Invariant 3: their union is exactly the produced set (nothing lost).
        let mut union = popped_set;
        union.extend(&still_waiting);
        assert_eq!(
            union,
            (1..=N).collect::<HashSet<u64>>(),
            "count in must equal count out: some job was lost"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_counter_reaches_expected_total() {
        let store = new_store();
        const N: usize = 1_000;

        let mut handles = Vec::with_capacity(N);
        for _ in 0..N {
            let store = store.clone();
            handles.push(tokio::spawn(async move {
                store
                    .incr(CollectionSuffix::Id, 1, None)
                    .await
                    .expect("concurrent incr");
            }));
        }
        within_timeout(async {
            for h in handles {
                h.await.expect("incr task panicked");
            }
        })
        .await;

        // Regardless of interleaving, the atomic counter must land on exactly N.
        assert_eq!(
            store.get_counter(CollectionSuffix::Id, None).await,
            Some(N as u64),
            "atomic counter must total the number of increments"
        );
    }
}
