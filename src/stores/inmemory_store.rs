use super::*;
use crate::timers::TimedMap;
use crate::utils::{
    calculate_next_priority_score, create_listener_handle, pause_or_resume_workers,
    process_each_event, update_job_opts,
};
use crate::worker::MIN_DELAY_MS_LIMIT;
use crate::{Counter, Dt, FailedDetails, QueueError};
use chrono::Utc;
use crossbeam_queue::SegQueue;
use crossbeam_skiplist::{SkipMap, SkipSet};
use derive_more::Debug;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::VecDeque;
use std::time::Duration;
use uuid::Uuid;
type StoredMap = SkipMap<u64, u64>;
use crate::JobError;
use std::sync::atomic::Ordering;
type TimedJobMap<D, R, P> = TimedMap<u64, Job<D, R, P>>;
type ListQueue = SkipMap<i64, u64>;
#[derive(Clone, Debug)]
pub struct InMemoryStore<D, R, P> {
    pub name: String,
    pub prefix: String,
    processing: Counter,
    is_paused: Arc<AtomicBool>,
    jobs: Arc<TimedJobMap<D, R, P>>,
    worker_metrics: Arc<TimedMap<Uuid, WorkerMetrics>>,
    #[debug(skip)]
    locks: Arc<TimedMap<u64, Lock>>, // locks that expires
    #[debug(skip)]
    events: Arc<SharedEmitter<R, P>>,
    id_counter: Counter,
    stored_metrics: Arc<ArcSwapOption<QueueMetrics>>,
    pause_workers: Arc<ArcSwapOption<AtomicBool>>,
    is_inital: Arc<AtomicBool>,
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
    pub fn new(prefix: Option<&str>, name: &str) -> Self {
        let prefix = prefix.unwrap_or("kio").to_lowercase();
        let name = name.to_lowercase();
        let events = Arc::default();
        let stored_metrics = Arc::default();
        let worker_metrics = Arc::default();
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
            attempts,
            remove_on_fail,
            remove_on_complete,
            ref backoff,
            repeat: _,
        } = opts;
        let dt = Utc::now();
        let expected_dt_ts = delay.next_occurrance_timestamp_ms();
        let delay_dt = delay.clone();
        let delay = delay.as_diff_ms(dt) as u64;
        job.add_opts(opts);
        if delay > 0 && delay < MIN_DELAY_MS_LIMIT {
            return Err(crate::KioError::from(QueueError::DelayBelowAllowedLimit {
                limit_ms: MIN_DELAY_MS_LIMIT,
                current_ms: delay,
            }));
        };
        let mut event = JobState::Wait;
        let waiting_or_paused = if !is_paused {
            CollectionSuffix::Wait
        } else {
            event = JobState::Paused;
            CollectionSuffix::Paused
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
            let score = calculate_next_priority_score(priority, pc) as i64;
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
            name: Some(name.to_owned()),
            ..Default::default()
        };
        if to_delay {
            event.delay = Some(delay)
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
    fn fetch_worker_metrics(&self) -> KioResult<BTreeMap<uuid::Uuid, WorkerMetrics>> {
        let stored_metrics = self
            .worker_metrics
            .inner
            .iter()
            .map(|entry| {
                let worker_id = *entry.key();
                let value = entry.value().value.lock();
                let metrics =
                    WorkerMetrics::new(value.worker_id, value.active_len, value.tasks.clone());

                (worker_id, metrics)
            })
            .collect();
        Ok(stored_metrics)
    }
    async fn store_worker_metrics(&self, metrics: WorkerMetrics, ttl_ms: u64) -> KioResult<()> {
        let duration = std::time::Duration::from_millis(ttl_ms);
        self.worker_metrics
            .insert_expirable(metrics.worker_id, metrics, duration)
            .await;
        Ok(())
    }
    fn queue_name(&self) -> &str {
        &self.name
    }
    async fn purge_expired(&self) {
        let purge_locks = async {
            if self.locks.len_expired().await > 0 {
                self.locks.purge_expired().await;
            }
        };

        let purge_metrics = async {
            if self.worker_metrics.len_expired().await > 0 {
                self.worker_metrics.purge_expired().await;
            }
        };
        let purge_jobs = async move {
            if self.jobs.len_expired().await > 0 {
                self.jobs.purge_expired().await;
            }
        };
        tokio::join!(purge_jobs, purge_locks, purge_metrics);
    }

    fn queue_prefix(&self) -> &str {
        &self.prefix
    }
    fn fetch_jobs(&self, ids: &[u64]) -> KioResult<VecDeque<Job<D, R, P>>> {
        if ids.is_empty() {
            return Ok(VecDeque::new());
        }
        let mut results = VecDeque::with_capacity(ids.len());
        for id in ids {
            let key = CollectionSuffix::Job(*id).tag();
            if let Some(found) = self.jobs.inner.get(&key) {
                results.push_back(found.value().value.lock().clone());
            }
        }
        Ok(results)
    }

    async fn exists_in(&self, col: CollectionSuffix, item: u64) -> KioResult<bool> {
        let result = match col {
            CollectionSuffix::Active => self.active.iter().any(|entry| *entry.value() == item),

            CollectionSuffix::Wait => self.waiting.iter().any(|entry| *entry.value() == item),

            CollectionSuffix::Paused => self.paused.iter().any(|entry| *entry.value() == item),
            CollectionSuffix::Completed => {
                self.completed.iter().any(|entry| *entry.value() == item)
            }
            CollectionSuffix::Failed => self.failed.iter().any(|entry| *entry.value() == item),
            CollectionSuffix::Prioritized => {
                self.prioritized.iter().any(|entry| *entry.value() == item)
            }
            CollectionSuffix::Delayed => self.delayed.iter().any(|entry| *entry.value() == item),
            CollectionSuffix::Stalled => self.stalled.contains(&item),
            CollectionSuffix::Job(id) => self.jobs.inner.contains_key(&col.tag()),
            CollectionSuffix::Lock(_) | CollectionSuffix::StalledCheck => {
                self.locks.inner.contains_key(&col.tag())
            }

            _ => false,
        };
        Ok(result)
    }
    async fn metadata_field_exists(&self, field: &str) -> KioResult<bool> {
        Ok(true)
    }

    async fn set_event_mode(&self, event_mode: QueueEventMode) -> KioResult<()> {
        // do nothing; only pubsub is supported
        Ok(())
    }

    async fn listen_to_events(
        &self,
        event_mode: QueueEventMode,
        block_interval: Option<u64>,
        emitter: &EventEmitter<R, P>,
        metrics: &QueueMetrics,
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
        pause_workers: Arc<AtomicBool>,
        event_mode: QueueEventMode,
    ) -> KioResult<JoinHandle<KioResult<()>>> {
        self.events.store(Some(emitter.clone()));
        self.notifier.store(Some(notifier.clone()));
        self.pause_workers.store(Some(pause_workers.clone()));
        // set our stored_metrics to the queue's metrics;
        self.stored_metrics.store(Some(metrics));
        let task = tokio::spawn(async move { Ok(()) });
        Ok(task)
    }

    async fn add_bulk_only(
        &self,
        iter: Box<dyn Iterator<Item = (String, Option<JobOptions>, D)> + Send>,
        queue_opts: QueueOpts,
        event_mode: QueueEventMode,
        is_paused: bool,
    ) -> KioResult<()> {
        for (ref name, opts, data) in iter {
            let mut opts = opts.unwrap_or_default();
            update_job_opts(&queue_opts, &mut opts);
            let mut pc = 0;
            if opts.priority > 0 {
                pc = self
                    .incr(CollectionSuffix::PriorityCounter, 1, None)
                    .await?;
            }
            let queue_name = format!("{}:{}", &self.prefix, &self.name);
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
        event_mode: QueueEventMode,
        is_paused: bool,
    ) -> KioResult<Vec<Job<D, R, P>>> {
        let mut jobs = vec![];
        for (ref name, opts, data) in iter {
            let mut opts = opts.unwrap_or_default();
            update_job_opts(&queue_opts, &mut opts);
            let mut pc = 0;
            if opts.priority > 0 {
                pc = self
                    .incr(CollectionSuffix::PriorityCounter, 1, None)
                    .await?;
            }
            let queue_name = format!("{}:{}", &self.prefix, &self.name);
            let id = self.incr(CollectionSuffix::Id, 1, None).await?;
            let mut job = Job::<D, R, P>::new(name, Some(data), opts.id, Some(&queue_name));
            self.insert(&mut job, opts, pc, id, name, is_paused).await?;
            jobs.push(job);
        }
        Ok(jobs)
    }

    async fn get_delayed_at(&self, start: i64, stop: i64) -> KioResult<(Vec<u64>, Vec<u64>)> {
        let before = (start - 1) as u64;
        let end = stop as u64;
        let start = start as u64;
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
                self.locks.update_expiration_status(&key, duration).await;
            }
            CollectionSuffix::Job(id) => {
                self.jobs.update_expiration_status(&key, duration).await;
            }
            _ => {}
        }
        Ok(())
    }

    async fn get_metrics(&self) -> KioResult<QueueMetrics> {
        let mut metrics = QueueMetrics::new(
            self.id_counter.load(Ordering::Acquire),
            self.processing.load(Ordering::Acquire),
            self.active.len() as u64,
            self.stalled.len() as u64,
            self.completed.len() as u64,
            self.delayed.len() as u64,
            self.prioritized.len() as u64,
            self.paused.len() as u64,
            self.failed.len() as u64,
            self.waiting.len() as u64,
            self.is_paused.load(Ordering::Acquire),
            self.event_mode,
        );
        Ok(metrics)
    }

    async fn get_job(&self, id: u64) -> Option<Job<D, R, P>> {
        let job_key = CollectionSuffix::Job(id).tag();
        self.jobs
            .inner
            .get(&job_key)
            .map(|pair| pair.value().value.lock().clone())
    }

    async fn get_token(&self, id: u64) -> Option<JobToken> {
        let lock_key = CollectionSuffix::Lock(id).tag();
        self.locks
            .inner
            .get(&lock_key)
            .and_then(|entry| match *entry.value().value.lock() {
                Lock::Token(token) => Some(token),
                _ => None,
            })
    }

    async fn get_state(&self, id: u64) -> Option<JobState> {
        let job_key = CollectionSuffix::Job(id).tag();
        self.jobs
            .inner
            .get(&job_key)
            .map(|entry| entry.value().value.lock().state)
    }

    fn update_job_progress(&self, job: &mut Job<D, R, P>, value: P) -> KioResult<()> {
        if let Some(id) = job.id {
            let job_key = CollectionSuffix::Job(id).tag();
            let jobs = self.jobs.clone();
            let value_clone = value.clone();
            if let Some(entry) = jobs.inner.get(&job_key) {
                entry.value().value.lock().progress = Some(value_clone);
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
        let mut now = Utc::now().timestamp_millis();
        match col {
            CollectionSuffix::Active => {
                let mut changed = false;
                let before = now;
                if append {
                    if let Some(first_entry) = self.active.front() {
                        now = *first_entry.key() - 1;
                        changed = true;
                    }
                }
                self.active.insert(now, item);
            }
            CollectionSuffix::Wait => {
                if append {
                    if let Some(first_entry) = self.waiting.front() {
                        now = *first_entry.key() - 1;
                    }
                }
                self.waiting.insert(now, item);
            }
            CollectionSuffix::Paused => {
                if append {
                    if let Some(first_entry) = self.paused.front() {
                        now = *first_entry.key() - 1;
                    }
                }
                self.paused.insert(now, item);
            }
            CollectionSuffix::Completed => {
                if let Some(score) = score {
                    self.completed.insert(score as u64, item);
                }
            }
            CollectionSuffix::Failed => {
                if let Some(score) = score {
                    self.failed.insert(score as u64, item);
                }
            }
            CollectionSuffix::Prioritized => {
                if let Some(score) = score {
                    self.prioritized.insert(score as u64, item);
                }
            }
            CollectionSuffix::Delayed => {
                if let Some(score) = score {
                    self.delayed.insert(score as u64, item);
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
                let ts_now = Utc::now().timestamp_millis();
                self.active.insert(ts_now, *value.value());
                return Some(*value.value());
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
        let mut lock = Lock::StallCheck;
        if let Some(token) = token {
            lock = Lock::Token(token);
        }
        self.locks.insert_expirable(lock_key, lock, duration).await;

        Ok(())
    }

    fn get_job_ids_in_state(
        &self,
        state: JobState,
        start: Option<usize>,
        end: Option<usize>,
    ) -> KioResult<VecDeque<u64>> {
        let start = start.unwrap_or_default();
        match state {
            JobState::Wait => {
                let end = end.unwrap_or(self.waiting.len().saturating_sub(1));
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
                let end = end.unwrap_or(self.prioritized.len().saturating_sub(1));
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
                let end = end.unwrap_or(self.stalled.len().saturating_sub(1));
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
                let end = end.unwrap_or(self.active.len().saturating_sub(1));
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
                let end = end.unwrap_or(self.paused.len().saturating_sub(1));
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
                let end = end.unwrap_or(self.completed.len().saturating_sub(1));
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
                let end = end.unwrap_or(self.failed.len().saturating_sub(1));
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
                let end = end.unwrap_or(self.delayed.len().saturating_sub(1));
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
        if let Some(mut pair) = self.jobs.inner.get(&key.tag()) {
            let job = &mut pair.value().value.lock();
            for field in fields {
                match field {
                    JobField::BackTrace(trace) => job.stack_trace.push(trace),
                    JobField::State(state) => job.state = state,
                    JobField::ProcessedOn(ts) => {
                        job.processed_on = Dt::from_timestamp_micros(ts as i64);
                    }
                    JobField::FinishedOn(ts) => {
                        job.finished_on = Dt::from_timestamp_micros(ts as i64);
                    }
                    JobField::Token(token) => job.token = Some(token),
                    JobField::Payload(processed_result) => match processed_result {
                        ProcessedResult::Failed(failed_details) => {
                            job.failed_reason = Some(failed_details)
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
                counter.fetch_add(delta.unsigned_abs(), Ordering::AcqRel);
                return counter.load(Ordering::Acquire);
            }
            counter.fetch_sub(delta.unsigned_abs(), Ordering::AcqRel);
            counter.load(Ordering::Acquire)
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
                                let new = (job.attempts_made as i64 + delta).max(0) as u64;
                                job.attempts_made = new;
                                new
                            }
                            "stalled_counter" | "stalledCounter" => {
                                let new = (job.stalled_counter as i64 + delta).max(0) as u64;
                                job.stalled_counter = new;
                                new
                            }
                            _ => 0,
                        }
                    };
                    let mut next = 0;
                    if let Some(pair) = self.jobs.inner.get(&key.tag()) {
                        let job = &mut pair.value().value.lock();
                        next = update_job(job)
                    }
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
            CollectionSuffix::Id => Some(self.id_counter.load(Ordering::Acquire)),
            CollectionSuffix::PriorityCounter => {
                Some(self.priority_counter.load(Ordering::Acquire))
            }
            CollectionSuffix::Meta => Some(self.processing.load(Ordering::Acquire)),
            CollectionSuffix::Job(_) => {
                if let Some(field) = hash_key {
                    let job_key = key.tag();
                    return self.jobs.inner.get(&job_key).and_then(|pair| {
                        let job = &pair.value().value.lock();
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
        event_mode: QueueEventMode,
        event: QueueStreamEvent<R, P>,
    ) -> KioResult<()> {
        let key = event.id;
        if let Some(emitter) = self.events.load().as_ref() {
            let metrics = self.get_metrics();
            if let (Some(stored), Some(notifier), Some(pause_workers)) = (
                self.stored_metrics.load().as_ref(),
                self.notifier.load().as_ref(),
                self.pause_workers.load().as_ref(),
            ) {
                process_each_event(event, emitter, self, stored).await?;
                pause_or_resume_workers(notifier, stored, pause_workers, &self.is_inital);
            }
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
            CollectionSuffix::Job(id) => {
                self.jobs.remove(&col.tag());
            }
            CollectionSuffix::Lock(id) => {
                self.locks.remove(&col.tag());
            }

            _ => {}
        }
        Ok(())
    }

    fn remove(&self, key: CollectionSuffix) -> KioResult<()> {
        // do thing here
        match key {
            CollectionSuffix::Active => self.active.clear(),
            CollectionSuffix::Completed => self.active.clear(),
            CollectionSuffix::Delayed => self.delayed.clear(),
            CollectionSuffix::Stalled => self.stalled.clear(),
            CollectionSuffix::Prioritized => self.prioritized.clear(),
            CollectionSuffix::Wait => self.waiting.clear(),
            CollectionSuffix::Paused => self.paused.clear(),
            CollectionSuffix::Failed => self.failed.clear(),
            CollectionSuffix::Job(_) => {
                self.jobs.remove(&key.tag());
            }
            CollectionSuffix::Lock(_) => {
                self.locks.remove(&key.tag());
            }
            CollectionSuffix::StalledCheck => {
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

    async fn clear_jobs(&self, last_id: u64) -> KioResult<()> {
        self.jobs.clear();
        Ok(())
    }

    async fn pause(&self, pause: bool, event_mode: QueueEventMode) -> KioResult<()> {
        let wait_key = CollectionSuffix::Wait;
        let paused_key = CollectionSuffix::Paused;
        let src = if pause { wait_key } else { paused_key };
        // only move items when the state changes
        if self
            .is_paused
            .compare_exchange(!pause, pause, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            if matches!(src, CollectionSuffix::Wait) {
                while let Some(entry) = self.waiting.pop_front() {
                    let key = *entry.key();
                    let value = *entry.value();
                    self.paused.insert(key, value);
                }
            } else {
                while let Some(entry) = self.paused.pop_front() {
                    let key = *entry.key();
                    let value = *entry.value();
                    self.waiting.insert(key, value);
                }
            }
        }
        Ok(())
    }
}
