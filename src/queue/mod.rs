use crate::error::{JobError, KioError, QueueError};
use crate::events::{QueueStreamEvent, StreamEventId};
use crate::job::{Job, JobState};
use crate::timers::DelayQueueTimer;
use crate::utils::{
    calculate_next_priority_score, process_queue_events, promote_jobs, resume_helper,
    serialize_into_pairs, update_job_opts, JobQueue, ReadStreamArgs,
};
use crossbeam::queue::SegQueue;
use futures::future::Future;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::{FutureExt, StreamExt};
use std::collections::{BTreeMap, VecDeque};
use std::marker::{PhantomData, PhantomPinned};
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
#[cfg(feature = "tracing")]
use tracing::{debug_span, Instrument, Span};
use uuid::Uuid;

use crate::worker::{WorkerMetrics, WorkerOpts, MIN_DELAY_MS_LIMIT};
use crate::{
    queue, BackOff, BackOffJobOptions, BackOffOptions, Dt, FailedDetails, JobOptions, JobToken,
    KeepJobs, KioResult, RemoveOnCompletionOrFailure, Repeat, StoredFn, Trace,
};
use async_backtrace::backtrace;
use chrono::{TimeDelta, Utc};
#[cfg(feature = "redis-store")]
use deadpool_redis::{Config, Pool, Runtime};
use serde::de::{value, DeserializeOwned, Error};
use serde::{ser, Deserialize, Serialize};
mod options;
use crate::stores::Store;

use crate::{EventEmitter, EventParameters};
use atomig::Atomic;
use derive_more::Debug;
pub use options::{CollectionSuffix, QueueEventMode, QueueMetrics, QueueOpts, RetryOptions};
pub(crate) use options::{Counter, JobField, ProcessedResult};

#[cfg(feature = "redis-store")]
use redis::{
    self, pipe, AsyncCommands, FromRedisValue, JsonAsyncCommands, LposOptions, Pipeline,
    RedisResult, ToRedisArgs, Value,
};
/// A task queue that holds and manages jobs.
///
/// `Queue` is the central hub of KioMQ.  It stores jobs, drives state
/// transitions (waiting → active → completed / failed), emits events, and
/// coordinates with [`crate::Worker`]s.
///
/// # Type parameters
///
/// | Parameter | Description |
/// |-----------|-------------|
/// | `D` | Job *input* data type |
/// | `R` | Job *return* (result) type |
/// | `P` | Job *progress* type |
/// | `S` | Backing [`Store`] implementation |
///
/// # Examples
///
/// ```rust
/// # #[tokio::main]
/// # async fn main() -> kiomq::KioResult<()> {
/// use kiomq::{InMemoryStore, Queue};
///
/// let store: InMemoryStore<String, String, ()> = InMemoryStore::new(None, "my-queue");
/// let queue = Queue::new(store, None).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Queue<D, R, P, S> {
    #[cfg(feature = "tracing")]
    resource_span: Span,
    pub paused: Arc<AtomicBool>,
    pub job_count: Arc<AtomicU64>,
    pub current_metrics: Arc<QueueMetrics>,
    pub opts: QueueOpts,
    pub(crate) event_mode: Arc<Atomic<QueueEventMode>>,
    emitter: EventEmitter<R, P>,
    pub(crate) store: Arc<S>,
    #[debug(skip)]
    pub stream_listener: Arc<JoinHandle<KioResult<()>>>,
    pub(crate) backoff: BackOff,
    pub(crate) worker_notifier: Arc<Notify>,
    pub pause_workers: Arc<AtomicBool>,
    _data: PhantomData<D>,
}

impl<
        D: Clone + Serialize + DeserializeOwned + Send + 'static,
        R: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
        S: Clone + Store<D, R, P> + Send + 'static,
        P: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
    > Queue<D, R, P, S>
{
    /// Creates a new `Queue` backed by the given `store`.
    ///
    /// Reads existing metrics from the store so that a queue that is re-opened
    /// after a restart retains the last known state counts.
    ///
    /// # Arguments
    ///
    /// * `store` – a [`Store`] implementation (e.g. [`crate::InMemoryStore`] or `RedisStore`).
    /// * `queue_opts` – optional [`QueueOpts`]; uses sensible defaults when `None`.
    ///
    /// # Errors
    ///
    /// Returns [`KioError`] if the store cannot be initialised (e.g. a Redis
    /// connection failure).
    ///
    /// # Examples
    ///
    /// ```rust
    /// # #[tokio::main]
    /// # async fn main() -> kiomq::KioResult<()> {
    /// use kiomq::{InMemoryStore, Queue, QueueOpts};
    ///
    /// let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "demo");
    /// let queue = Queue::new(store, Some(QueueOpts::default())).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(store: S, queue_opts: Option<QueueOpts>) -> KioResult<Self> {
        use typed_emitter::TypedEmitter;
        let opts = queue_opts.unwrap_or_default();
        let emitter = Arc::new(TypedEmitter::new());
        let mut metrics = QueueMetrics::default();
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
        let _queue_name = store.queue_name();
        #[cfg(feature = "tracing")]
        let resource_span = debug_span!("Queue", _queue_name);
        let worker_notifier: Arc<Notify> = Arc::default();
        let current_metrics = Arc::new(metrics);
        let pause_workers: Arc<AtomicBool> = Arc::default();
        let is_paused = current_metrics.is_paused.load(Ordering::Relaxed);
        let store = Arc::new(store);
        #[cfg(feature = "tracing")]
        let task = store
            .create_stream_listener(
                emitter.clone(),
                worker_notifier.clone(),
                current_metrics.clone(),
                pause_workers.clone(),
                event_mode.load(Ordering::Acquire),
            )
            .instrument(resource_span.clone())
            .await?;
        #[cfg(not(feature = "tracing"))]
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
            #[cfg(feature = "tracing")]
            resource_span,
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
            _data: PhantomData,
        })
    }

    /// Enqueues multiple jobs in a single batch and returns them.
    ///
    /// Each item in `iter` is a tuple of `(name, options, data)`.  When
    /// `options` is `None` the queue's default [`QueueOpts`] are applied.
    ///
    /// Returns the created [`Job`] objects, which contain the assigned IDs.
    ///
    /// See also [`bulk_add_only`](Self::bulk_add_only) if you don't need the
    /// returned jobs.
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
    /// Enqueues multiple jobs in a single batch, discarding the results.
    ///
    /// Identical to [`bulk_add`](Self::bulk_add) but avoids allocating the
    /// returned `Vec` when you only care about side effects (fire-and-forget).
    ///
    /// # Examples
    ///
    /// ```rust
    /// # #[tokio::main]
    /// # async fn main() -> kiomq::KioResult<()> {
    /// use kiomq::{InMemoryStore, Queue};
    ///
    /// let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "bulk-demo");
    /// let queue = Queue::new(store, None).await?;
    ///
    /// queue.bulk_add_only((0..5u64).map(|i| (format!("job-{i}"), None, i))).await?;
    /// # Ok(())
    /// # }
    /// ```
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

    /// Enqueues a single job and returns it.
    ///
    /// # Arguments
    ///
    /// * `name` – a human-readable label for the job (does not need to be unique).
    /// * `data` – the job payload.
    /// * `opts` – optional per-job [`JobOptions`]; queue defaults are used when `None`.
    ///
    /// # Errors
    ///
    /// Returns [`KioError`] if the underlying store fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # #[tokio::main]
    /// # async fn main() -> kiomq::KioResult<()> {
    /// use kiomq::{InMemoryStore, Queue};
    ///
    /// let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "add-job-demo");
    /// let queue = Queue::new(store, None).await?;
    ///
    /// let job = queue.add_job("process", 42u64, None).await?;
    /// assert!(job.id.is_some());
    /// # Ok(())
    /// # }
    /// ```
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
    /// Returns the number of jobs currently being processed by workers.
    ///
    /// This is an in-memory counter maintained by the queue itself (not a
    /// store round-trip).  Use [`get_metrics`](Self::get_metrics) for a full
    /// snapshot of all state counts.
    pub fn current_jobs(&self) -> u64 {
        self.job_count.load(std::sync::atomic::Ordering::Acquire)
    }
    /// Retrieves a job by its numeric ID, or `None` if it no longer exists.
    ///
    /// Jobs may be absent because they were removed according to
    /// [`RemoveOnCompletionOrFailure`] retention settings or via
    /// [`obliterate`](Self::obliterate).
    ///
    /// # Examples
    ///
    /// ```rust
    /// # #[tokio::main]
    /// # async fn main() -> kiomq::KioResult<()> {
    /// use kiomq::{InMemoryStore, Queue};
    ///
    /// let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "get-job-demo");
    /// let queue = Queue::new(store, None).await?;
    ///
    /// let job = queue.add_job("fetch-me", 99u64, None).await?;
    /// let id = job.id.unwrap();
    ///
    /// let fetched = queue.get_job(id).await;
    /// assert!(fetched.is_some());
    /// # Ok(())
    /// # }
    /// ```
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
        let job_key = CollectionSuffix::Job(job_id);
        let move_to_failed_or_completed = matches!(to, JobState::Failed | JobState::Completed);
        let previous_suffix = from.into();
        let next_state_suffix = to.into();
        if is_paused {
            return Ok(());
        }
        if !self.store.job_exists(job_id).await {
            return Err(JobError::JobNotFound.into());
        }
        if move_to_failed_or_completed {
            self.store.incr(job_key, 1, Some("attemptsMade")).await?;
            self.store.remove_item(previous_suffix, job_id).await?;
            let score = ts.unwrap_or_else(|| Utc::now().timestamp_micros());
            self.store
                .add_item(next_state_suffix, job_id, Some(score), false)
                .await?;
        } else {
            let exists_in_list = self.store.exists_in(next_state_suffix, job_id).await?;
            if !exists_in_list {
                self.store.remove_item(previous_suffix, job_id).await?;
                self.store
                    .add_item(next_state_suffix, job_id, None, false)
                    .await?;
            }
        }
        let mut fields: Vec<JobField<R>> = vec![JobField::State(to)];

        if let Some(backtrace) = backtrace.as_ref() {
            //job.stack_trace.push(backtrace.clone());
            fields.push(JobField::BackTrace(backtrace.clone()));
        }
        if let Some(rec) = value.as_ref() {
            fields.push(JobField::Payload(rec.clone()));
            if let Some(ts) = ts {
                fields.push(JobField::FinishedOn(ts.unsigned_abs()));
            }
        }
        self.store.set_fields(job_id, fields).await?;
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
                ProcessedResult::Success(value, metrics) => {
                    event.returned_value = Some(value);
                    event.metrics = Some(metrics);
                }
            }
        }
        self.store.publish_event(event_mode, event).await?;
        Ok(())
    }
    /// Toggles the paused/resumed state of the queue.
    ///
    /// * When **paused**, workers stop picking up new jobs; jobs already being
    ///   processed continue to completion.
    /// * When **resumed**, workers start picking up new jobs again.
    ///
    /// Emits a [`JobState::Paused`] or [`JobState::Resumed`] event respectively.
    ///
    /// # Errors
    ///
    /// Returns [`KioError`] if the underlying store operation fails.
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
        self.paused
            .store(pause, std::sync::atomic::Ordering::Relaxed);
        self.store.publish_event(event_mode, event).await?;
        Ok(())
    }

    /// Attempts to extend the lock on an active job.
    ///
    /// Returns `true` if the lock was successfully extended, or `false` if the
    /// provided `token` does not match the token currently held on the job.
    /// A token mismatch usually means the job's lock has already been acquired
    /// by another worker or has expired.
    ///
    /// # Arguments
    ///
    /// * `job_id` – the numeric ID of the job whose lock you want to extend.
    /// * `lock_duration` – the new lock lifetime in **milliseconds**.
    /// * `token` – the [`JobToken`] originally granted to this worker.
    pub async fn extend_lock(
        &self,
        job_id: u64,
        lock_duration: u64,
        token: JobToken,
    ) -> KioResult<(bool)> {
        let previous: Option<JobToken> = self.store.get_token(job_id).await;
        if let Some(prev_token) = previous {
            if prev_token == token {
                self.store
                    .set_lock(CollectionSuffix::Lock(job_id), Some(token), lock_duration)
                    .await?;
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
        let (is_paused, target) = self.get_target_list();
        let mut failed = vec![];
        let mut stall = vec![];
        let ts = Utc::now().timestamp_micros();
        let stalled_check_key = CollectionSuffix::StalledCheck;
        let check_key_exists = self
            .store
            .exists_in(CollectionSuffix::StalledCheck, stalled_check_key.tag())
            .await?;
        if check_key_exists {
            return Ok((vec![], vec![]));
        }
        self.store
            .set_lock(stalled_check_key, None, opts.stalled_interval)
            .await?;
        let stalled = self
            .store
            .get_job_ids_in_state(JobState::Stalled, None, None)?;
        if stalled.is_empty() {
            for id in stalled {
                let job_key = CollectionSuffix::Job(id);

                let lock_exists = self.store.exists_in(CollectionSuffix::Lock(id), id).await?;
                if lock_exists {
                    let stalled_count = self.store.incr(job_key, 1, Some("stalledCounter")).await?;
                    let attempts_made = self
                        .store
                        .get_counter(job_key, Some("attempts_made"))
                        .await
                        .unwrap_or_default();
                    let from = self.store.get_state(id).await.unwrap_or_default();

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
                        )
                        .await?;
                        failed.push(id);
                    } else {
                        self.move_job_to_state(id, JobState::Active, target, None, None, None)
                            .await?;
                        stall.push(id);
                    }
                }
            }
        } else {
            // move all active jobs to stalled
            let active_elements = self
                .store
                .get_job_ids_in_state(JobState::Active, None, None)?;
            for id in active_elements {
                let lock = CollectionSuffix::Lock(id);
                if !self.store.exists_in(lock, id).await? {
                    self.store
                        .add_item(CollectionSuffix::Stalled, id, None, true)
                        .await?;
                    self.store.remove_item(CollectionSuffix::Active, id);
                }
            }
        }

        Ok((failed, stall))
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
            .set_lock(
                CollectionSuffix::Lock(job_id),
                Some(token),
                opts.lock_duration,
            )
            .await?;

        self.move_job_to_state(job_id, prev_state, JobState::Active, None, None, None)
            .await?;
        let items = vec![JobField::Token(token), JobField::ProcessedOn(ts)];
        self.store.set_fields(job_id, items).await?;

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
        let job_exists: bool = self.store.job_exists(job_id).await;
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
    /// Emits an event with the given state and parameters to all registered listeners.
    pub async fn emit(&self, event: JobState, data: EventParameters<R, P>) {
        self.emitter.emit(event, data).await
    }
    /// Registers a listener for a specific job-state event.
    ///
    /// Returns a [`Uuid`] that can be passed to [`remove_event_listener`](Self::remove_event_listener)
    /// to deregister the callback.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # #[tokio::main]
    /// # async fn main() -> kiomq::KioResult<()> {
    /// use kiomq::{InMemoryStore, JobState, Queue};
    ///
    /// let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "events");
    /// let queue = Queue::new(store, None).await?;
    ///
    /// let id = queue.on(JobState::Completed, |evt| async move { let _ = evt; });
    /// queue.remove_event_listener(id);
    /// # Ok(())
    /// # }
    /// ```
    pub fn on<F, C>(&self, event: JobState, callback: C) -> Uuid
    where
        C: Fn(EventParameters<R, P>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        self.emitter.on(event, callback)
    }
    /// Registers a listener that fires for **every** job-state event.
    ///
    /// Returns a [`Uuid`] handle that can later be passed to
    /// [`remove_event_listener`](Self::remove_event_listener).
    pub fn on_all_events<F, C>(&self, callback: C) -> Uuid
    where
        C: Fn(EventParameters<R, P>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        self.emitter.on_all(callback)
    }
    /// Removes a previously registered event listener.
    ///
    /// Returns the listener's [`Uuid`] if it was found and removed, or `None`
    /// if no listener with that ID exists.
    pub fn remove_event_listener(&self, id: Uuid) -> Option<Uuid> {
        self.emitter.remove_listener(id)
    }

    /// Deletes **all** jobs and collection data for this queue.
    ///
    /// This is a destructive, irreversible operation.  All jobs in every state
    /// are removed from the store.  A [`JobState::Obliterated`] event is
    /// emitted after the cleanup.
    ///
    /// # Errors
    ///
    /// Returns [`KioError`] if the store fails to clear collections.
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

    pub(crate) async fn promote_delayed_jobs(
        &self,
        date_time: Dt,
        mut interval_ms: i64,
        timers: &DelayQueueTimer<D, R, P, S>,
    ) -> KioResult<()> {
        promote_jobs(self, date_time, interval_ms, timers).await
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
    /// Retrieves multiple jobs by their IDs in one batch.
    ///
    /// Jobs that no longer exist (e.g. removed by retention policies) are
    /// silently omitted from the result.
    pub fn fetch_jobs(&self, ids: &[u64]) -> KioResult<VecDeque<Job<D, R, P>>> {
        self.store.fetch_jobs(ids)
    }
    /// Returns the IDs of jobs currently in the given `state`.
    ///
    /// Use `start` and `end` to paginate large result sets; pass `None` for
    /// both to retrieve all IDs.
    pub fn get_job_ids_in_state(
        &self,
        state: JobState,
        start: Option<usize>,
        end: Option<usize>,
    ) -> KioResult<VecDeque<u64>> {
        self.store.get_job_ids_in_state(state, start, end)
    }
    /// Returns the name of this queue (as provided to the store constructor).
    pub fn name(&self) -> &str {
        self.store.queue_name()
    }
    /// Returns the key prefix used for all collections belonging to this queue.
    pub fn prefix(&self) -> &str {
        self.store.queue_prefix()
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
    /// Registers a custom backoff strategy under the given `name`.
    ///
    /// The strategy is a factory function that receives the *attempt number* and
    /// returns a per-attempt delay function `(attempt) -> delay_ms`.
    ///
    /// If a strategy with the same name already exists it is **not** replaced.
    pub fn register_backoff_strategy(
        &self,
        name: &str,
        strategy: impl Fn(i64) -> Arc<dyn Fn(i64) -> i64 + Send + Sync> + 'static + Send + Sync,
    ) {
        if !self.backoff.has_strategy(name) {
            self.backoff.register(name, strategy);
        }
    }
    /// Calculates the delay in milliseconds before the next retry attempt.
    ///
    /// Returns `None` if the backoff options don't produce a valid delay for
    /// the given attempt count (e.g. the max attempts have been exceeded).
    pub fn calculate_next_delay_ms(
        &self,
        backoff_job_opts: &BackOffJobOptions,
        attempts: i64,
    ) -> Option<i64> {
        let backoff_opts = BackOff::normalize(Some(backoff_job_opts))?;
        self.backoff.calculate(Some(backoff_opts), attempts, None)
    }
    /// Schedules a job for retry according to the given options.
    ///
    /// Accepts either a [`BackOffJobOptions`] (for failed-job backoff) or a
    /// [`Repeat`] (for repeat-scheduling).  The job is moved to the delayed or
    /// wait state as appropriate.
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
    /// Returns `true` if the queue is currently paused.
    ///
    /// This reads the in-memory [`QueueMetrics::is_paused`] flag; it does **not**
    /// perform a store round-trip.  Call [`get_metrics`](Self::get_metrics) first
    /// if you need a fresh value from the store.
    pub fn is_paused(&self) -> bool {
        self.current_metrics.queue_is_paused()
    }
    /// Signals all workers attached to this queue to stop picking up new jobs.
    ///
    /// This sets an atomic flag that workers poll; jobs already being processed
    /// continue to completion.
    pub fn pause_active_workers(&self) {
        self.pause_workers.store(true, Ordering::Release);
    }
    /// Allows workers to resume picking up new jobs after a pause.
    ///
    /// Wakes any workers that are sleeping on the notifier.
    pub fn resume_workers(&self) {
        resume_helper(
            &self.current_metrics,
            &self.pause_workers,
            &self.worker_notifier,
        );
    }
    /// Fetches fresh metrics from the store and updates the in-memory snapshot.
    ///
    /// The returned [`QueueMetrics`] reflects the latest counts from the backing
    /// store.  The queue's `current_metrics` field is also updated in place so
    /// that subsequent reads of [`Queue::current_metrics`] are up-to-date.
    ///
    /// # Errors
    ///
    /// Returns [`KioError`] if the store cannot retrieve the metrics.
    ///
    /// # Note
    ///
    /// For a cheap in-memory read (no store round-trip), read `queue.current_metrics`
    /// directly.  Keep in mind it may be slightly stale between `get_metrics` calls.
    pub async fn get_metrics(&self) -> KioResult<QueueMetrics> {
        let updated = self.store.get_metrics().await?;
        self.current_metrics.update(&updated);
        Ok(updated)
    }
    /// Retrieves per-worker metrics stored in the backing store.
    ///
    /// Returns a map from worker [`Uuid`] to [`WorkerMetrics`].
    pub fn fetch_worker_metrics(&self) -> KioResult<BTreeMap<uuid::Uuid, WorkerMetrics>> {
        self.store.fetch_worker_metrics()
    }
    /// Persists the given worker metrics to the backing store with a TTL.
    ///
    /// Workers call this periodically (controlled by
    /// [`WorkerOpts::metrics_update_interval`]) so that operators can monitor
    /// per-worker task health.
    pub async fn store_worker_metrics(&self, metrics: WorkerMetrics, ttl_ms: u64) -> KioResult<()> {
        self.store.store_worker_metrics(metrics, ttl_ms).await
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
