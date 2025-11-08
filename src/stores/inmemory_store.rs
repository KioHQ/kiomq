use super::*;
use crate::utils::{
    calculate_next_priority_score, create_listener_handle, process_each_event, update_job_opts,
};
use crate::worker::MIN_DELAY_MS_LIMIT;
use crate::{Counter, Dt, FailedDetails, QueueError};
use chrono::{TimeDelta, Utc};
use crossbeam_queue::SegQueue;
use crossbeam_skiplist::{SkipMap, SkipSet};
use derive_more::Debug;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::VecDeque;
use std::default;
use std::time::Duration;
use timed_map::TimedMap;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
type StoredMap = SkipMap<u64, u64>;
use crate::JobError;
use std::sync::atomic::Ordering;
enum Lock {
    Token(JobToken),
    StallCheck,
}
type TimedJobMap<D, R, P> = TimedMap<u64, Job<D, R, P>>;
type ListQueue = SkipMap<Dt, u64>;
#[derive(Clone, Debug)]
pub struct InMemoryStore<D, R, P> {
    pub name: String,
    pub prefix: String,
    processing: Counter,
    is_paused: Arc<AtomicBool>,
    jobs: Arc<Mutex<TimedJobMap<D, R, P>>>,
    #[debug(skip)]
    locks: Arc<Mutex<TimedMap<u64, Lock>>>, // locks that expired
    #[debug(skip)]
    events: Arc<SegQueue<QueueStreamEvent<R, P>>>,
    id_counter: Counter,
    priority_counter: Counter,
    completed: Arc<StoredMap>,
    pub priorized: Arc<StoredMap>,
    pub delayed: Arc<StoredMap>,
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
        Self {
            name,
            prefix,
            processing: Counter::default(),
            priority_counter: Counter::default(),
            id_counter: Counter::default(),
            is_paused: Arc::default(),
            jobs: Arc::default(),
            locks: Arc::default(),
            events,
            completed: Arc::default(),
            priorized: Arc::default(),
            delayed: Arc::default(),
            failed: Arc::default(),
            stalled: Arc::default(),
            active: Arc::default(),
            waiting: Arc::default(),
            paused: Arc::default(),
            event_mode: QueueEventMode::PubSub,
        }
    }
    async fn exists_in(&self, item: u64, col: CollectionSuffix) -> bool {
        match col {
            CollectionSuffix::Active => self.active.iter().any(|entry| *entry.value() == item),

            CollectionSuffix::Wait => self.waiting.iter().any(|entry| *entry.value() == item),

            CollectionSuffix::Paused => self.paused.iter().any(|entry| *entry.value() == item),
            CollectionSuffix::Completed => {
                self.completed.iter().any(|entry| *entry.value() == item)
            }
            CollectionSuffix::Failed => self.failed.iter().any(|entry| *entry.value() == item),
            CollectionSuffix::Prioritized => {
                self.priorized.iter().any(|entry| *entry.value() == item)
            }
            CollectionSuffix::Delayed => self.delayed.iter().any(|entry| *entry.value() == item),
            CollectionSuffix::Stalled => self.stalled.contains(&item),
            CollectionSuffix::Job(id) => {
                id == item && self.jobs.lock().await.contains_key(&col.tag())
            }
            CollectionSuffix::Lock(_) | CollectionSuffix::StalledCheck => {
                self.locks.lock().await.contains_key(&col.tag())
            }

            _ => false,
        }
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
            //priorized.insert(score as u64, id);
            job.state = JobState::Priorized;
            self.add_item(CollectionSuffix::Prioritized, id, Some(score), true)
                .await?;
            event = JobState::Priorized;
        } else {
            self.add_item(waiting_or_paused, id, None, true).await?;
        }
        job.id = Some(id);
        let job_key = CollectionSuffix::Job(id).tag();
        self.jobs
            .lock()
            .await
            .insert_constant_unchecked(job_key, job.clone());
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
    fn queue_name(&self) -> &str {
        &self.name
    }

    fn queue_prefix(&self) -> &str {
        &self.prefix
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
        emitter: &EventEmitter<D, R, P>,
        metrics: &JobMetrics,
    ) -> KioResult<()> {
        if let Some(stream_event) = self.events.pop() {
            //dbg!(&stream_event);
            process_each_event(stream_event, emitter, self, metrics).await?
        }
        Ok(())
    }

    async fn create_stream_listener(
        &self,
        emitter: EventEmitter<D, R, P>,
        notifier: Arc<Notify>,
        metrics: Arc<JobMetrics>,
        pause_workers: Arc<AtomicBool>,
        event_mode: QueueEventMode,
    ) -> KioResult<JoinHandle<KioResult<()>>> {
        create_listener_handle(self, emitter, notifier, metrics, pause_workers, event_mode).await
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
                    self.priorized
                        .pop_front()
                        .map(|entry| (*entry.key(), *entry.value()))
                } else {
                    self.priorized
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
        let duration = Duration::from_secs(secs as u64);
        let key = col.tag();
        match col {
            CollectionSuffix::Lock(_) | CollectionSuffix::StalledCheck => {
                self.locks
                    .lock()
                    .await
                    .update_expiration_status(key, duration)
                    .map_err(std::io::Error::other)?;
            }
            CollectionSuffix::Job(id) => {
                self.jobs
                    .lock()
                    .await
                    .update_expiration_status(key, duration)
                    .map_err(std::io::Error::other)?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn get_metrics(&self) -> KioResult<JobMetrics> {
        let mut metrics = JobMetrics::new(
            self.id_counter.load(Ordering::Acquire),
            self.processing.load(Ordering::Acquire),
            self.active.len() as u64,
            self.stalled.len() as u64,
            self.completed.len() as u64,
            self.delayed.len() as u64,
            self.waiting.len() as u64,
            self.is_paused.load(Ordering::Acquire),
            self.event_mode,
        );
        Ok(metrics)
    }

    async fn get_job(&self, id: u64) -> Option<Job<D, R, P>> {
        let job_key = CollectionSuffix::Job(id).tag();
        self.jobs.lock().await.get(&job_key).cloned()
    }

    async fn get_token(&self, id: u64) -> Option<JobToken> {
        let lock_key = CollectionSuffix::Lock(id).tag();
        self.locks
            .lock()
            .await
            .get(&lock_key)
            .and_then(|entry| match entry {
                Lock::Token(token) => Some(*token),
                _ => None,
            })
    }

    async fn get_state(&self, id: u64) -> Option<JobState> {
        let job_key = CollectionSuffix::Job(id).tag();
        self.jobs
            .lock()
            .await
            .get(&job_key)
            .map(|entry| entry.state)
    }

    fn update_job_progress(&self, job: &mut Job<D, R, P>, value: P) -> KioResult<()> {
        use tokio::runtime::Handle;
        use tokio::task;

        if let Some(id) = job.id {
            let job_key = CollectionSuffix::Job(id).tag();
            let jobs = self.jobs.clone();
            let value_clone = value.clone();
            task::block_in_place(move || {
                Handle::current().block_on(async {
                    if let Some(entry) = jobs.lock().await.get_mut(&job_key) {
                        entry.progress = Some(value_clone);
                    }
                });
            });
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
                let mut now = Utc::now();
                let mut changed = false;
                let before = now;
                if append {
                    // first itme
                    if let Some(first_entry) = self.active.front() {
                        //dbg!("here", first_entry.value());
                        now = *first_entry.key() - TimeDelta::milliseconds(10);
                        changed = true;
                    }
                }
                self.active.insert(now, item);
            }
            CollectionSuffix::Wait => {
                let mut now = Utc::now();
                if append {
                    // first itme
                    if let Some(first_entry) = self.waiting.front() {
                        now = *first_entry.key() - TimeDelta::milliseconds(10);
                    }
                }
                self.waiting.insert(now, item);
            }
            CollectionSuffix::Paused => {
                let mut now = Utc::now();
                if append {
                    // first itme
                    if let Some(first_entry) = self.paused.front() {
                        now = *first_entry.key() - TimeDelta::milliseconds(10);
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
                    self.priorized.insert(score as u64, item);
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
                let now = Utc::now();
                self.active.insert(now, *value.value());
                return Some(*value.value());
            }
            _ => return None,
        }
    }

    async fn move_job_to_state(
        &self,
        job_id: u64,
        from: JobState,
        to: JobState,
        value: Option<ProcessedResult<R>>,
        ts: Option<i64>,
        backtrace: Option<Trace>,
        event_mode: QueueEventMode,
        is_paused: bool,
    ) -> KioResult<()> {
        let job_key = CollectionSuffix::Job(job_id);
        let move_to_failed_or_completed = matches!(to, JobState::Failed | JobState::Completed);
        let previous_suffix = from.into();
        let next_state_suffix = to.into();
        if is_paused {
            return Ok(());
        }
        if !self.job_exist(job_id).await {
            return Err(JobError::JobNotFound.into());
        }
        if move_to_failed_or_completed {
            self.incr(job_key, 1, Some("attemptsMade")).await?;
            self.remove_item(previous_suffix, job_id).await?;
            let score = ts.unwrap_or_else(|| Utc::now().timestamp_micros());
            self.add_item(next_state_suffix, job_id, Some(score), false)
                .await?;
        } else {
            let exists_in_list = self.exists_in(job_id, next_state_suffix).await;
            if !exists_in_list {
                self.remove_item(previous_suffix, job_id).await?;
                self.add_item(next_state_suffix, job_id, None, false)
                    .await?;
            }
        }
        // upate the job here;

        if let Some(mut job) = self.jobs.lock().await.get_mut(&job_key.tag()) {
            job.state = to;
            if let Some(backtrace) = backtrace.as_ref() {
                job.stack_trace.push(backtrace.clone());
            }
            if let Some(value) = value.as_ref() {
                match value {
                    ProcessedResult::Failed(failed_details) => {
                        job.failed_reason = Some(failed_details.clone())
                    }
                    ProcessedResult::Success(done) => job.returned_value = Some(done.clone()),
                };
                job.finished_on = ts.and_then(Dt::from_timestamp_micros);
            }
        }
        let mut event: QueueStreamEvent<R, P> = QueueStreamEvent {
            event: to,
            prev: Some(from),
            job_id,
            ..Default::default()
        };
        if let Some(data) = value {
            match data {
                ProcessedResult::Failed(failed_details) => {
                    event.failed_reason = Some(failed_details)
                }
                ProcessedResult::Success(value) => event.returned_value = Some(value),
            }
        }
        self.publish_event(event_mode, event).await?;
        Ok(())
    }

    async fn set_lock(&self, job_id: u64, token: JobToken, lock_duration: u64) -> KioResult<()> {
        let lock_key = CollectionSuffix::Lock(job_id).tag();
        let duration = Duration::from_millis(lock_duration);
        self.locks
            .lock()
            .await
            .insert_expirable(lock_key, Lock::Token(token), duration);

        Ok(())
    }

    async fn set_fields(&self, job_id: u64, fields: &[(&str, String)]) -> KioResult<()> {
        let key = CollectionSuffix::Job(job_id);
        if let Some(mut job) = self.jobs.lock().await.get_mut(&key.tag()) {
            for (key, value) in fields {
                match *key {
                    "processedOn" | "processed_on" => {
                        job.processed_on =
                            simd_json::from_reader::<_, Option<u64>>(value.as_bytes())
                                .ok()
                                .flatten()
                                .and_then(|t| Dt::from_timestamp_micros(t as i64))
                    }
                    "token" => job.token = simd_json::from_reader(value.as_bytes()).ok(),
                    _ => {}
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
                counter.fetch_add(delta as u64, Ordering::AcqRel);
                return counter.load(Ordering::Acquire);
            }
            counter.fetch_sub(delta as u64, Ordering::AcqRel);
            counter.load(Ordering::Acquire)
        };
        let next = match key {
            CollectionSuffix::Id => handle_counter(&self.id_counter),
            CollectionSuffix::PriorityCounter => handle_counter(&self.priority_counter),
            CollectionSuffix::Meta => handle_counter(&self.processing),
            CollectionSuffix::Job(_) => {
                if let Some(field) = hash_key {
                    let update_job = |job: &mut Job<D, R, P>| match field {
                        "attempts_made" | "attemptsMade" => {
                            job.attempts_made += ((job.attempts_made) as i64 + delta) as u64;
                            job.attempts_made
                        }
                        "stalled_counter" | "stalledCounter" => {
                            job.stalled_counter += ((job.attempts_made) as i64 + delta) as u64;
                            job.stalled_counter
                        }
                        _ => 0,
                    };
                    let mut next = 0;
                    if let Some(job) = self.jobs.lock().await.get_mut(&key.tag()) {
                        next = update_job(job)
                    }
                    return Ok(next);
                };
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
                    return self.jobs.lock().await.get(&job_key).and_then(|job| {
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
        self.events.push(event);
        Ok(())
    }

    async fn move_stalled_jobs(
        &self,
        opts: &WorkerOpts,
        (is_paused, target): (bool, JobState),
        event_mode: QueueEventMode,
    ) -> KioResult<(Vec<u64>, Vec<u64>)> {
        let mut failed = vec![];
        let mut stall = vec![];
        let ts = Utc::now().timestamp_micros();
        let stalled_check_key = CollectionSuffix::StalledCheck;
        let check_key_exists = self
            .exists_in(stalled_check_key.tag(), CollectionSuffix::StalledCheck)
            .await;
        if check_key_exists {
            return Ok((vec![], vec![]));
        }

        if !self.stalled.is_empty() {
            for entry in self.stalled.iter() {
                let id = *entry.value();
                let job_key = CollectionSuffix::Job(id);

                let lock_exists = self.exists_in(id, CollectionSuffix::Lock(id)).await;
                if lock_exists {
                    let stalled_count = self.incr(job_key, 1, Some("stalledCounter")).await?;
                    let attempts_made = self
                        .get_counter(job_key, Some("attempts_made"))
                        .await
                        .unwrap_or_default();
                    let from = self.get_state(id).await.unwrap_or_default();

                    if stalled_count > opts.max_stalled_count {
                        // Add job removal option logic here
                        let reason = "job stalled more than allowable limit".to_lowercase();
                        let to = JobState::Failed;
                        let failed_reason = FailedDetails {
                            run: attempts_made + 1,
                            reason,
                        };
                        self.move_job_to_state(
                            id,
                            from,
                            to,
                            Some(ProcessedResult::Failed(failed_reason)),
                            None,
                            None,
                            event_mode,
                            is_paused,
                        )
                        .await?;
                        failed.push(id);
                    } else {
                        self.move_job_to_state(
                            id,
                            JobState::Active,
                            target,
                            None,
                            None,
                            None,
                            event_mode,
                            is_paused,
                        )
                        .await?;
                        // emit  stalled;
                        stall.push(id);
                    }
                }
            }
        } else {
            // move all active jobs to stalled
            for entry in self.active.iter() {
                let dt = entry.key();
                let id = *entry.value();
                let now = Utc::now();
                let diff = (now - dt).num_milliseconds();
                let lock = CollectionSuffix::Lock(id);
                if !self.exists_in(id, lock).await && diff as u64 > opts.stalled_interval {
                    self.add_item(CollectionSuffix::Stalled, id, None, true)
                        .await?;
                    entry.remove();
                }
            }
        }

        Ok((failed, stall))
    }

    async fn job_exist(&self, id: u64) -> bool {
        let col_key = CollectionSuffix::Job(id);
        self.exists_in(id, col_key).await
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
                if self.priorized.contains_key(&item) {
                    let _ = self.priorized.remove(&item);
                    return Ok(());
                }
                self.priorized
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
                self.jobs.lock().await.remove(&col.tag());
            }
            CollectionSuffix::Lock(id) => {
                self.locks.lock().await.remove(&col.tag());
            }

            _ => {}
        }
        Ok(())
    }

    fn remove(&self, key: CollectionSuffix) -> KioResult<()> {
        // do thing here

        Ok(())
    }

    async fn clear_collections(&self) -> KioResult<()> {
        self.completed.clear();
        self.failed.clear();
        self.delayed.clear();
        self.priorized.clear();
        self.stalled.clear();
        self.waiting.clear();
        self.paused.clear();
        self.active.clear();
        Ok(())
    }

    async fn clear_jobs(&self, last_id: u64) -> KioResult<()> {
        self.jobs.lock().await.clear();
        Ok(())
    }

    async fn pause(&self, pause: bool, event_mode: QueueEventMode) -> KioResult<()> {
        let wait_key = CollectionSuffix::Wait;
        let paused_key = CollectionSuffix::Paused;
        let src = if pause { wait_key } else { paused_key };
        // only move items when the state changes
        if !self
            .is_paused
            .compare_exchange(!pause, pause, Ordering::AcqRel, Ordering::Relaxed)
            .unwrap_or(true)
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
