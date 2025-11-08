use crossbeam_queue::SegQueue;
use futures::future::Future;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::{FutureExt, StreamExt};
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::error::{JobError, KioError, QueueError};
use crate::events::{QueueStreamEvent, StreamEventId};
use crate::job::{Job, JobState};
use crate::utils::{
    calculate_next_priority_score, prepare_for_insert, process_queue_events, promote_jobs,
    query_all_batched, resume_helper, serialize_into_pairs, update_job_opts, JobQueue,
    ReadStreamArgs,
};
use crate::worker::{WorkerOpts, MIN_DELAY_MS_LIMIT};
use crate::{
    get_job_metrics, queue, BackOff, BackOffJobOptions, BackOffOptions, Dt, FailedDetails,
    JobOptions, JobToken, KeepJobs, KioResult, RemoveOnCompletionOrFailure, Repeat, StoredFn,
    Trace,
};
use async_backtrace::backtrace;
use chrono::{TimeDelta, Utc};
use deadpool_redis::{Config, Pool, Runtime};
use serde::de::{value, DeserializeOwned, Error};
use serde::{ser, Deserialize, Serialize};
mod options;
use crate::stores::Store;

use crate::{EventEmitter, EventParameters};
use atomig::Atomic;
use derive_more::Debug;
pub use options::{CollectionSuffix, JobMetrics, QueueEventMode, QueueOpts, RetryOptions};
pub(crate) use options::{Counter, ProcessedResult};

use redis::{
    self, pipe, AsyncCommands, FromRedisValue, JsonAsyncCommands, LposOptions, Pipeline,
    RedisResult, ToRedisArgs, Value,
};
#[derive(Debug, Clone)]
pub struct Queue<D, R, P, S> {
    pub paused: Arc<AtomicBool>,
    pub job_count: Arc<AtomicU64>,
    pub current_metrics: Arc<JobMetrics>,
    pub opts: QueueOpts,
    pub(crate) event_mode: Arc<Atomic<QueueEventMode>>,
    emitter: EventEmitter<D, R, P>,
    pub(crate) store: Arc<S>,
    #[debug(skip)]
    pub stream_listener: Arc<JoinHandle<KioResult<()>>>,
    pub(crate) backoff: BackOff,
    pub(crate) worker_notifier: Arc<Notify>,
    pub pause_workers: Arc<AtomicBool>,
}

impl<
        D: Clone + Serialize + DeserializeOwned + Send + 'static,
        R: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
        S: Clone + Store<D, R, P> + Send + 'static,
        P: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
    > Queue<D, R, P, S>
{
    pub async fn new(store: S, queue_opts: Option<QueueOpts>) -> KioResult<Self> {
        use typed_emitter::TypedEmitter;
        let opts = queue_opts.unwrap_or_default();
        let emitter = Arc::new(TypedEmitter::new());
        let mut metrics = JobMetrics::default();
        if let Ok(current_metrics) = store.get_metrics().await {
            metrics = current_metrics;
        }
        let events_mode_exits: bool = store.metadata_field_exists("event_mode").await?;
        let event_mode = metrics.event_mode.clone();
        if let Some(passed_mode) = opts.event_mode {
            if !events_mode_exits && passed_mode != event_mode.load(Ordering::Acquire) {
                store.set_event_mode(passed_mode).await?;
                event_mode.swap(passed_mode, Ordering::AcqRel);
            }
        }
        let worker_notifier: Arc<Notify> = Arc::default();
        let current_metrics = Arc::new(metrics);
        let pause_workers: Arc<AtomicBool> = Arc::default();
        let is_paused = current_metrics.paused.load(Ordering::Relaxed);
        let store = Arc::new(store);
        let task = store
            .create_stream_listener(
                emitter.clone(),
                worker_notifier.clone(),
                current_metrics.clone(),
                pause_workers.clone(),
                event_mode.load(Ordering::Acquire),
            )
            .await?;
        let stream_listener = Arc::new(task);
        Ok(Self {
            store,
            event_mode,
            pause_workers,
            worker_notifier,
            backoff: BackOff::new(),
            opts,
            current_metrics,
            stream_listener,
            job_count: Arc::default(),
            emitter,
            paused: Arc::new(AtomicBool::new(is_paused)),
        })
    }

    pub async fn bulk_add<I: Iterator<Item = (String, Option<JobOptions>, D)> + Send + 'static>(
        &self,
        iter: I,
    ) -> KioResult<Vec<Job<D, R, P>>> {
        let event_mode = self.event_mode.load(Ordering::Acquire);
        let is_paused = self.is_paused();
        self.store
            .add_bulk(Box::new(iter), self.opts.clone(), event_mode, is_paused)
            .await
    }
    pub async fn bulk_add_only<
        I: Iterator<Item = (String, Option<JobOptions>, D)> + Send + 'static,
    >(
        &self,
        iter: I,
    ) -> KioResult<()> {
        let event_mode = self.event_mode.load(Ordering::Acquire);
        let is_paused = self.is_paused();
        self.store
            .add_bulk_only(Box::new(iter), self.opts.clone(), event_mode, is_paused)
            .await
    }

    pub async fn add_job(
        &self,
        name: &str,
        data: D,
        opts: Option<JobOptions>,
    ) -> Result<Job<D, R, P>, KioError> {
        let mut opts = opts.unwrap_or_default();
        let event_mode = self.event_mode.load(Ordering::Acquire);
        let is_paused = self.is_paused();
        let queue_opts = self.opts.clone();
        let iter = std::iter::once((name.to_string(), Some(opts), data));
        let mut jobs = self
            .store
            .add_bulk(Box::new(iter), queue_opts, event_mode, is_paused)
            .await?;
        let job = jobs.pop().expect("failed to insert");
        Ok(job)
    }
    pub fn current_jobs(&self) -> u64 {
        self.job_count.load(std::sync::atomic::Ordering::Acquire)
    }
    pub async fn get_job(&self, id: u64) -> Option<Job<D, R, P>> {
        self.store.get_job(id).await
    }

    pub async fn move_job_to_state(
        &self,
        job_id: u64,
        from: JobState,
        to: JobState,
        value: Option<ProcessedResult<R>>,
        ts: Option<i64>,
        backtrace: Option<Trace>,
    ) -> KioResult<()> {
        let event_mode = self.event_mode.load(Ordering::Acquire);
        let is_paused = self.is_paused();
        self.store
            .move_job_to_state(
                job_id, from, to, value, ts, backtrace, event_mode, is_paused,
            )
            .await
    }
    /// pauses the queue if not resumed and vice-versa
    pub async fn pause_or_resume(&self) -> Result<(), KioError> {
        // if its paused
        let pause = !self.is_paused();
        let event_mode = self.event_mode.load(Ordering::Acquire);
        self.store.pause(pause, event_mode).await?;
        let state = if pause {
            JobState::Paused
        } else {
            JobState::Resumed
        };
        let event = QueueStreamEvent::<R, P> {
            event: state,
            ..Default::default()
        };
        self.store.publish_event(event_mode, event).await?;
        self.paused
            .store(pause, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    pub async fn extend_lock(
        &self,
        job_id: u64,
        lock_duration: u64,
        token: JobToken,
    ) -> KioResult<(bool)> {
        let previous: Option<JobToken> = self.store.get_token(job_id).await;
        if let Some(prev_token) = previous {
            if prev_token == token {
                self.store.set_lock(job_id, token, lock_duration).await?;
                self.store
                    .remove_item(CollectionSuffix::Stalled, job_id)
                    .await?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub async fn make_stalled_jobs_wait(
        &self,
        opts: &WorkerOpts,
    ) -> KioResult<(Vec<u64>, Vec<u64>)> {
        let event_mode = self.event_mode.load(Ordering::Acquire);
        self.store
            .move_stalled_jobs(opts, self.get_target_list(), event_mode)
            .await
    }

    pub fn get_target_list(&self) -> (bool, JobState) {
        let paused = self.is_paused();
        if paused {
            return (paused, JobState::Paused);
        }
        (paused, JobState::Wait)
    }

    pub async fn move_to_active(
        &self,
        token: JobToken,
        opts: &WorkerOpts,
    ) -> KioResult<MoveToActiveResult<D, R, P>> {
        let ts = Utc::now().timestamp_micros();
        let (is_paused, target_state) = self.get_target_list();
        let mut job_id: Option<u64> = self
            .store
            .pop_back_push_front(CollectionSuffix::Wait, CollectionSuffix::Active)
            .await;
        let mut prepare_job = |id: u64| async move {
            let prev_state: Option<JobState> = self.store.get_state(id).await;
            let job = self
                .prepare_job_for_processing(
                    token,
                    id,
                    ts as u64,
                    opts,
                    prev_state.unwrap_or_default(),
                )
                .await?;

            Ok::<_, KioError>((job, prev_state))
        };
        match job_id {
            Some(job_id) => Ok(MoveToActiveResult::from_job_state_pair(
                prepare_job(job_id).await?,
            )),
            None => {
                if let Some(id) = self.move_job_from_priorty_to_active().await? {
                    let (job, state) = prepare_job(id).await?;
                    return Ok(MoveToActiveResult::ProcessJob(job.boxed()));
                }

                let mut next_delay = 1;
                next_delay /= 0x1000;

                Ok(MoveToActiveResult::DelayUntil(next_delay))
            }
        }
        // fetch the next delayed_timestamp;
    }
    pub async fn prepare_job_for_processing(
        &self,
        token: JobToken,
        job_id: u64,
        ts: u64,
        opts: &WorkerOpts,
        prev_state: JobState,
    ) -> KioResult<Job<D, R, P>> {
        self.store
            .set_lock(job_id, token, opts.lock_duration)
            .await?;
        self.move_job_to_state(job_id, prev_state, JobState::Active, None, None, None)
            .await?;
        let items = [
            ("processedOn", simd_json::to_string(&ts)?),
            ("token", simd_json::to_string(&token)?),
        ];
        self.store.set_fields(job_id, &items).await?;
        let job = self
            .store
            .get_job(job_id)
            .await
            .ok_or(JobError::JobNotFound)?;
        Ok(job)
    }

    pub(crate) async fn move_job_to_finished_or_failed(
        &self,
        job_id: u64,
        ts: i64,
        token: JobToken,
        move_to_state: JobState,
        processed: ProcessedResult<R>,
        backtrace: Option<Trace>,
    ) -> KioResult<Job<D, R, P>> {
        let job_exists: bool = self.store.job_exist(job_id).await;
        if !job_exists {
            return Err(JobError::JobNotFound.into());
        }
        let lock_token: Option<JobToken> = self.store.get_token(job_id).await;
        if let Some(local) = lock_token {
            if local != token {
                return Err(JobError::JobLockMismatch.into());
            }
            self.store.remove(CollectionSuffix::Lock(job_id));
            self.store
                .remove_item(CollectionSuffix::Stalled, job_id)
                .await?;
        } else if backtrace.is_some() {
            return Err(JobError::JobLockNotExist.into());
        }
        let prev_state = self.store.get_state(job_id).await.unwrap_or_default();
        // Todo: remove any dependencies too here ;
        self.move_job_to_state(
            job_id,
            prev_state,
            move_to_state,
            Some(processed),
            Some(ts),
            backtrace,
        )
        .await;

        //remove element from stalled set too;

        let job = self
            .store
            .get_job(job_id)
            .await
            .ok_or(JobError::JobNotFound)?;
        Ok(job)
    }
    pub async fn emit(&self, event: JobState, data: EventParameters<D, R, P>) {
        self.emitter.emit(event, data).await
    }
    pub fn on<F, C>(&self, event: JobState, callback: C) -> Uuid
    where
        C: Fn(EventParameters<D, R, P>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        self.emitter.on(event, callback)
    }
    pub fn on_all_events<F, C>(&self, callback: C) -> Uuid
    where
        C: Fn(EventParameters<D, R, P>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        self.emitter.on_all(callback)
    }
    pub fn remove_event_listener(&self, id: Uuid) -> Option<Uuid> {
        self.emitter.remove_listener(id)
    }

    pub async fn obliterate(&self) -> KioResult<()> {
        self.delete_all_jobs().await?;
        // delete all other grouped collections;
        self.store.clear_collections().await?;
        let event_mode = self.event_mode.load(Ordering::Acquire);
        let event = JobState::Obliterated;
        let last_id = self.current_metrics.last_id.load(Ordering::Acquire);
        let item: QueueStreamEvent<R, P> = QueueStreamEvent {
            job_id: last_id,
            event,
            ..Default::default()
        };
        self.store.publish_event(event_mode, item).await?;
        self.current_metrics.clear();
        self.store.clear_collections().await?;
        Ok(())
    }
    async fn delete_all_jobs(&self) -> KioResult<()> {
        let last_id = self.current_metrics.last_id.load(Ordering::Acquire);
        self.store.clear_jobs(last_id).await
    }

    pub async fn promote_delayed_jobs(
        &self,
        date_time: Dt,
        mut interval_ms: i64,
        job_queue: JobQueue,
    ) -> KioResult<()> {
        promote_jobs(self, date_time, interval_ms, job_queue).await
    }

    async fn move_job_from_priorty_to_active(&self) -> KioResult<Option<u64>> {
        let mut min_priority_job: Vec<(u64, u64)> = self
            .store
            .pop_set(CollectionSuffix::Prioritized, true)
            .await?;

        if let Some((job_id, score)) = min_priority_job.pop() {
            let _: () = self
                .store
                .add_item(CollectionSuffix::Active, job_id, None, true)
                .await?;
            return Ok(Some(job_id));
        }

        let _: () = self.store.remove(CollectionSuffix::PriorityCounter)?;

        Ok(None)
    }

    pub async fn clean_up_job(
        &self,
        job_id: u64,
        remove_options: Option<RemoveOnCompletionOrFailure>,
    ) -> KioResult<()> {
        let id = job_id;
        if let Some(remove_options) = remove_options {
            match remove_options {
                RemoveOnCompletionOrFailure::Bool(remove_immediately) => {
                    if remove_immediately {
                        self.store.remove(CollectionSuffix::Job(job_id));
                    }
                }
                RemoveOnCompletionOrFailure::Int(max_to_keep) => {
                    if max_to_keep.is_positive() && (id as i64) > max_to_keep {
                        self.store.remove(CollectionSuffix::Job(job_id));
                    }
                }
                RemoveOnCompletionOrFailure::Opts(KeepJobs { age, count }) => {
                    if let Some(expire_in_secs) = age {
                        self.store
                            .expire(CollectionSuffix::Job(job_id), expire_in_secs)
                            .await?;
                    }
                    if let Some(max_to_keep) = count {
                        if max_to_keep.is_positive() && (id as i64) > max_to_keep {
                            self.store.remove(CollectionSuffix::Job(job_id));
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(derive_more::Debug)]
pub enum MoveToActiveResult<D, R, P> {
    Paused,
    RateLimit(u64),
    DelayUntil(u64),
    #[debug("ProcessJob({0}) from state{1}", _0.id.unwrap_or_default(), _0.state)]
    ProcessJob(Box<Job<D, R, P>>),
}
impl<D, R, P> MoveToActiveResult<D, R, P> {
    fn from_job_state_pair((job, state): (Job<D, R, P>, Option<JobState>)) -> Self {
        Self::ProcessJob(job.boxed())
    }
}
// ----- UTILITY FUNCTIONS -------------------

impl<D, R, P, S: Store<D, R, P>> Queue<D, R, P, S> {
    pub fn register_backoff_strategy(
        &self,
        name: &str,
        strategy: impl Fn(i64) -> Arc<dyn Fn(i64) -> i64 + Send + Sync> + 'static + Send + Sync,
    ) {
        if !self.backoff.has_strategy(name) {
            self.backoff.register(name, strategy);
        }
    }
    pub fn calculate_next_delay_ms(
        &self,
        backoff_job_opts: &BackOffJobOptions,
        attempts: i64,
    ) -> Option<i64> {
        let backoff_opts = BackOff::normalize(Some(backoff_job_opts))?;
        self.backoff.calculate(Some(backoff_opts), attempts, None)
    }
    pub async fn retry_job<'a, T: Into<RetryOptions<'a>>>(
        &self,
        job_id: u64,
        opts: T,
        attempts: u64,
    ) -> KioResult<()> {
        let opts = opts.into();
        match opts {
            RetryOptions::Failed(backoff_job_opts) => {
                self.retry_failed(job_id, backoff_job_opts, attempts).await
            }
            RetryOptions::WithRepeat(repeat) => {
                if let Some(next_delayed_timestamp) =
                    repeat.next_occurrence(&self.backoff, attempts)
                {
                    match next_delayed_timestamp {
                        0 => {
                            self.store
                                .add_item(CollectionSuffix::Wait, job_id, None, true)
                                .await?
                        }
                        _ => {
                            self.store
                                .add_item(
                                    CollectionSuffix::Delayed,
                                    job_id,
                                    Some(next_delayed_timestamp),
                                    true,
                                )
                                .await?
                        }
                    };
                }
                Ok(())
            }
        }
    }
    async fn retry_failed(
        &self,
        job_id: u64,
        backoff_job_opts: &BackOffJobOptions,
        attempts: u64,
    ) -> KioResult<()> {
        let ts = Utc::now();

        if let Some(next_delay) = self.calculate_next_delay_ms(backoff_job_opts, attempts as i64) {
            let expected_active_time = ts + TimeDelta::milliseconds(next_delay);
            self.store
                .add_item(
                    CollectionSuffix::Delayed,
                    job_id,
                    Some(expected_active_time.timestamp_millis()),
                    false,
                )
                .await?;
            self.store
                .remove_item(CollectionSuffix::Failed, job_id)
                .await?;
        }

        Ok(())
    }
    pub fn is_paused(&self) -> bool {
        self.current_metrics.paused.load(Ordering::Acquire)
    }
    pub fn pause_active_workers(&self) {
        self.pause_workers.store(true, Ordering::Release);
    }
    pub fn resume_workers(&self) {
        resume_helper(
            &self.current_metrics,
            &self.pause_workers,
            &self.worker_notifier,
        );
    }
    pub async fn get_metrics(&self) -> KioResult<JobMetrics> {
        let updated = self.store.get_metrics().await?;
        self.current_metrics.update(&updated);
        Ok(updated)
    }
    pub async fn update_processing_count(
        &self,
        increment: bool,
        worker_id: Uuid,
        job_id: u64,
        state: JobState,
    ) -> KioResult<u64> {
        let delta = if increment { 1_i64 } else { -1_i64 };
        self.store
            .incr(CollectionSuffix::Meta, delta, Some("processing"))
            .await?;
        let event_mode = self.event_mode.load(Ordering::Acquire);
        // this event, doesn't have the return and progress fields
        let event = QueueStreamEvent::<R, P> {
            job_id,
            event: JobState::Processing,
            prev: Some(state),
            worker_id: Some(worker_id),
            ..Default::default()
        };
        self.store.publish_event(event_mode, event).await?;
        let current = self
            .store
            .get_counter(CollectionSuffix::Meta, Some("processing"))
            .await
            .unwrap_or_default();
        Ok(current)
    }
}
