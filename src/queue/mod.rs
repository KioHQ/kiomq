#![allow(rustdoc::private_intra_doc_links)]
use crate::error::{JobError, KioError};
use crate::events::QueueStreamEvent;
use crate::job::{Job, JobState};
use crate::metrics::{P_METRICS_COLLECTOR, TimerCommand, WorkerMetrics};
use crate::timers::{DelayQueueTimer, TimerSender};
use crate::utils::{promote_jobs, resume_helper};
use crate::worker::{JobMap, ProcessingQueue, WorkerOpts, WorkerState};
use crate::{
    BackOff, BackOffJobOptions, Dt, FailedDetails, JobOptions, JobToken, KeepJobs, KioResult,
    ProcessMetrics, RemoveOnCompletionOrFailure, Trace,
};
use chrono::{TimeDelta, Utc};
use compact_str::ToCompactString;
use crossbeam::atomic::AtomicCell;
use crossbeam_skiplist::SkipMap;
use futures::future::Future;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, VecDeque};
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::sync::broadcast::{self, Sender};
use tokio_util::sync::CancellationToken;
#[cfg(feature = "tracing")]
use tracing::{Instrument, Span, debug_span, info, instrument};
use uuid::Uuid;
mod options;
use crate::stores::Store;

use crate::{EventEmitter, EventParameters};
use arc_swap::ArcSwapOption;
use derive_more::Debug;
pub use options::{CollectionSuffix, QueueEventMode, QueueMetrics, QueueOpts, RetryOptions};
pub use options::{Counter, JobField, ProcessedResult};
/// A type alias representing a map of `worker_ids`, options and useful metadata like `worker_state`
/// and ie.
pub type WorkerMetaData = Arc<
    SkipMap<
        Uuid,
        (
            Arc<AtomicCell<WorkerState>>,
            ProcessingQueue,
            WorkerOpts,
            Dt,
        ),
    >,
>;

/// A task queue that holds and manages jobs.
///
/// `Queue` is the central hub of `KioMQ`.  It stores jobs, drives state
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
    /// Unique identifier for the queue
    pub id: Uuid,
    #[cfg(feature = "tracing")]
    resource_span: Span,
    /// `true` when the queue is in the paused state.
    pub paused: Arc<AtomicCell<bool>>,
    /// In-memory snapshot of queue state counts; updated by [`Queue::get_metrics`].
    pub current_metrics: Arc<QueueMetrics>,
    /// Queue-level configuration supplied at construction time.
    pub opts: QueueOpts,
    pub(crate) event_mode: Arc<AtomicCell<QueueEventMode>>,
    emitter: EventEmitter<R, P>,
    pub(crate) store: Arc<S>,
    pub(crate) workers: WorkerMetaData,
    pub(crate) jobs_in_progress: JobMap<D, R, P>,
    pub(crate) cancel_token: CancellationToken,
    pub(crate) timer_sender: Sender<TimerCommand>,
    timers: Arc<ArcSwapOption<DelayQueueTimer<D, R, P, S>>>,
    #[debug(skip)]
    /// Handle to the background task that listens for store events and forwards
    /// them to registered listeners.
    pub(crate) backoff: BackOff,
    pub(crate) worker_notifier: Arc<Notify>,
    /// Atomic flag set to `true` to signal attached workers to pause picking
    /// up new jobs.
    pub pause_workers: Arc<AtomicCell<bool>>,
    _data: PhantomData<D>,
}

impl<
    D: Clone + Serialize + DeserializeOwned + Send + 'static + Sync,
    R: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
    S: Clone + Store<D, R, P> + Send + 'static + Sync,
    P: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
> Queue<D, R, P, S>
{
    /// add a worker and  its usual metadata in the queue
    pub(crate) fn add_worker(
        &self,
        id: Uuid,
        processing_queue: ProcessingQueue,
        state: Arc<AtomicCell<WorkerState>>,
        opts: WorkerOpts,
        created_at: Dt,
    ) {
        if !self.workers.contains_key(&id) {
            self.workers.insert(
                id,
                (state.clone(), processing_queue.clone(), opts, created_at),
            );
        }
        P_METRICS_COLLECTOR.register_worker(id, (state, processing_queue, opts, created_at));
    }

    /// re-registers the worker to global worker Registry.
    pub(crate) fn add_worker_heartbeat(&self, worker_id: &Uuid) {
        if let Some(entry) = self.workers.get(worker_id) {
            let (state, processing_queue, opts, created_at) = entry.value();

            P_METRICS_COLLECTOR.register_worker(
                *worker_id,
                (state.clone(), processing_queue.clone(), *opts, *created_at),
            );
        }
    }

    /// register a worker's timers to the global timer coordinator.
    pub(crate) async fn register_worker_timers(&self, opts: WorkerOpts) {
        if let Some(timers) = self.timers.load_full() {
            timers.register_worker_timers(opts).await;
        }
    }
    /// remove a worker and its usual metadata from the queue
    pub(crate) fn remove_worker(&self, id: Uuid) {
        self.workers.remove(&id);
        P_METRICS_COLLECTOR.unregister_worker(id);
    }

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
        let metrics = store.get_metrics().await.unwrap_or_default();
        let workers = Arc::default();

        let events_mode_exits: bool = store.metadata_field_exists("event_mode").await?;
        let event_mode = metrics.event_mode.clone();
        if let Some(passed_mode) = opts.event_mode
            && !events_mode_exits
            && passed_mode != event_mode.load()
        {
            store.set_event_mode(passed_mode).await?;
            event_mode.swap(passed_mode);
        }
        let _queue_name = store.queue_name();
        #[cfg(feature = "tracing")]
        let resource_span = debug_span!("Queue", _queue_name);
        let worker_notifier: Arc<Notify> = Arc::default();
        let current_metrics = Arc::new(metrics);
        let pause_workers: Arc<AtomicCell<bool>> = Arc::default();
        let is_paused = current_metrics.is_paused.load();
        let store = Arc::new(store);
        let jobs_in_progress = Arc::default();
        let cancel_token = CancellationToken::new();
        #[cfg(feature = "tracing")]
        let task = store
            .create_stream_listener(
                emitter.clone(),
                worker_notifier.clone(),
                current_metrics.clone(),
                pause_workers.clone(),
                event_mode.load(),
            )
            .instrument(resource_span.clone())
            .await;
        #[cfg(not(feature = "tracing"))]
        let task = store
            .create_stream_listener(
                emitter.clone(),
                worker_notifier.clone(),
                current_metrics.clone(),
                pause_workers.clone(),
                event_mode.load(),
            )
            .await;
        let stream_listener = task;
        let timers = Arc::default();
        let id = Uuid::new_v4();
        let (timer_sender, rx) = broadcast::channel(10000);
        P_METRICS_COLLECTOR.register_queue(id, timer_sender.clone(), current_metrics.clone());
        let queue = Self {
            timer_sender,
            id,
            cancel_token,
            timers,
            jobs_in_progress,
            workers,
            #[cfg(feature = "tracing")]
            resource_span,
            store,
            event_mode,
            pause_workers,
            worker_notifier,
            backoff: BackOff::new(),
            opts,
            current_metrics,
            emitter,
            paused: Arc::new(AtomicCell::new(is_paused)),
            _data: PhantomData,
        };
        let timers = DelayQueueTimer::new(
            queue.jobs_in_progress.clone(),
            queue.clone(),
            queue.workers.clone(),
            P_METRICS_COLLECTOR.tx.clone(),
            rx,
            P_METRICS_COLLECTOR.inner.updating_metrics_receiver.clone(),
            queue.cancel_token.clone(),
            stream_listener,
        );
        queue.timers.store(Some(timers.into()));
        Ok(queue)
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
    ///
    /// # Errors
    ///
    /// Returns [`KioError`] if the underlying store fails.
    #[allow(clippy::future_not_send)]
    pub async fn bulk_add<I: Iterator<Item = (String, Option<JobOptions>, D)> + Send + 'static>(
        &self,
        iter: I,
    ) -> KioResult<Vec<Job<D, R, P>>> {
        let event_mode = self.event_mode.load();
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
    /// let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "bulk-demo");
    /// let queue = Queue::new(store, None).await?;
    ///
    /// queue.bulk_add_only((0..5u64).map(|i| (format!("job-{i}"), None, i))).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[allow(clippy::future_not_send)]
    pub async fn bulk_add_only<
        I: Iterator<Item = (String, Option<JobOptions>, D)> + Send + 'static,
    >(
        &self,
        iter: I,
    ) -> KioResult<()> {
        let event_mode = self.event_mode.load();
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
    ///
    /// # Panics
    ///
    /// Panics if the store returns an empty job list (should never happen in practice).
    #[allow(clippy::future_not_send)]
    pub async fn add_job(
        &self,
        name: &str,
        data: D,
        opts: Option<JobOptions>,
    ) -> Result<Job<D, R, P>, KioError> {
        let opts = opts.unwrap_or_default();
        let event_mode = self.event_mode.load();
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
    #[allow(clippy::future_not_send)]
    pub async fn get_job(&self, id: u64) -> Option<Job<D, R, P>> {
        self.store.get_job(id).await
    }

    /// Moves a job from one state to another and persists all associated field
    /// updates atomically.
    ///
    /// This is the central state-machine transition used internally by workers
    /// when completing, failing, or re-queuing a job. It also publishes the
    /// corresponding event so that listeners are notified.
    ///
    /// # Errors
    ///
    /// Returns [`KioError`] if the job no longer exists or the store fails.
    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(self)))]
    #[allow(clippy::future_not_send)]
    pub(crate) async fn move_job_to_state(
        &self,
        job_id: u64,
        from: JobState,
        to: JobState,
        value: Option<ProcessedResult<R>>,
        ts: Option<i64>,
        backtrace: Option<Trace>,
    ) -> KioResult<()> {
        let event_mode = self.event_mode.load();
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
                    event.failed_reason = Some(failed_details);
                }
                ProcessedResult::Success(value, metrics) => {
                    event.returned_value = Some(value);
                    event.metrics = Some(metrics);
                }
            }
        }
        #[cfg(feature = "tracing")]
        {
            info!("moved job {job_id} from {from} to {to}");
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
    #[allow(clippy::future_not_send)]
    pub async fn pause_or_resume(&self) -> Result<(), KioError> {
        // if its paused
        let metrics = self.get_metrics().await?;
        let pause = !metrics.is_paused.load();
        let event_mode = self.event_mode.load();
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
        self.paused.store(pause);
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
    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(self)))]
    #[allow(clippy::future_not_send)]
    pub(crate) async fn extend_lock(
        &self,
        job_id: u64,
        lock_duration: u64,
        token: JobToken,
    ) -> KioResult<bool> {
        let previous: Option<JobToken> = self.store.get_token(job_id).await;
        if let Some(prev_token) = previous
            && prev_token == token
        {
            self.store
                .set_lock(CollectionSuffix::Lock(job_id), Some(token), lock_duration)
                .await?;
            self.store
                .remove_item(CollectionSuffix::Stalled, job_id)
                .await?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Checks for stalled jobs and moves them back to the wait state.
    ///
    /// A job is considered stalled when its worker lock expires without the
    /// lock being renewed. This method inspects all active jobs, releases
    /// expired locks, and either re-queues the job or moves it to `Failed`
    /// when the stall count exceeds [`WorkerOpts::max_stalled_count`].
    ///
    /// Returns `(recovered, failed)` job-ID vectors.
    ///
    /// # Errors
    ///
    /// Returns [`KioError`] if the store cannot be queried.
    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(self)))]
    #[allow(clippy::future_not_send)]
    pub(crate) async fn make_stalled_jobs_wait(
        &self,
        opts: &WorkerOpts,
    ) -> KioResult<(Vec<u64>, Vec<u64>)> {
        let (_is_paused, target) = self.get_target_list();
        let mut failed = vec![];
        let mut stall = vec![];
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
            .get_job_ids_in_state(JobState::Stalled, None, None)
            .await?;
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
                        let reason = "job stalled more than allowable limit".to_compact_string();
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
                .get_job_ids_in_state(JobState::Active, None, None)
                .await?;
            for id in active_elements {
                let lock = CollectionSuffix::Lock(id);
                if !self.store.exists_in(lock, id).await? {
                    self.store
                        .add_item(CollectionSuffix::Stalled, id, None, true)
                        .await?;
                    self.store.remove_item(CollectionSuffix::Active, id).await?;
                }
            }
        }

        Ok((failed, stall))
    }

    /// Returns `(is_paused, target_state)` where `target_state` is the list a
    /// waiting job should be placed on: `Paused` when the queue is paused,
    /// `Wait` otherwise.
    pub(crate) fn get_target_list(&self) -> (bool, JobState) {
        let paused = self.is_paused();
        if paused {
            return (paused, JobState::Paused);
        }
        (paused, JobState::Wait)
    }

    /// Pops the next waiting job and atomically moves it to the `Active` state.
    ///
    /// Called by workers on each polling cycle. Returns a [`MoveToActiveResult`]
    /// describing what happened (job ready, queue paused, rate-limited, or
    /// delayed until a future timestamp).
    ///
    /// # Errors
    ///
    /// Returns [`KioError`] if the store operation fails.
    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(self)))]
    #[allow(clippy::future_not_send)]
    pub(crate) async fn move_to_active(
        &self,
        token: JobToken,
        opts: &WorkerOpts,
    ) -> KioResult<MoveToActiveResult<D, R, P>> {
        let ts = Utc::now().timestamp_micros();
        let (_is_paused, _target_state) = self.get_target_list();
        let job_id: Option<u64> = self
            .store
            .pop_back_push_front(CollectionSuffix::Wait, CollectionSuffix::Active)
            .await;
        let prepare_job = |id: u64| async move {
            let prev_state: Option<JobState> = self.store.get_state(id).await;
            let job = self
                .prepare_job_for_processing(
                    token,
                    id,
                    ts.cast_unsigned(),
                    opts,
                    prev_state.unwrap_or_default(),
                )
                .await?;

            Ok::<_, KioError>((job, prev_state))
        };
        if let Some(job_id) = job_id {
            Ok(MoveToActiveResult::from_job_state_pair(
                prepare_job(job_id).await?,
            ))
        } else {
            if let Some(id) = self.move_job_from_priorty_to_active().await? {
                let (job, _state) = prepare_job(id).await?;
                return Ok(MoveToActiveResult::ProcessJob(job.boxed()));
            }

            let mut next_delay = 1;
            next_delay /= 0x1000;

            Ok(MoveToActiveResult::DelayUntil(next_delay))
        }
        // fetch the next delayed_timestamp;
    }
    /// Acquires the lock for `job_id`, transitions it to `Active`, and returns
    /// the populated [`Job`] ready for the processor function.
    ///
    /// # Errors
    ///
    /// Returns [`KioError`] if the job no longer exists or the lock cannot be
    /// set.
    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(self)))]
    #[allow(clippy::future_not_send)]
    pub(crate) async fn prepare_job_for_processing(
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

    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(self, _token)))]
    #[allow(clippy::future_not_send)]
    pub(crate) async fn move_job_to_finished_or_failed(
        &self,
        job_id: u64,
        ts: i64,
        _token: JobToken,
        move_to_state: JobState,
        processed: ProcessedResult<R>,
        backtrace: Option<Trace>,
    ) -> KioResult<Job<D, R, P>> {
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
        .await?;

        let job = self
            .store
            .get_job(job_id)
            .await
            .ok_or(JobError::JobNotFound)?;
        Ok(job)
    }
    /// Emits an event with the given state and parameters to all registered listeners.
    #[allow(clippy::future_not_send)]
    pub async fn emit(&self, event: JobState, data: EventParameters<R, P>) {
        self.emitter.emit(event, data).await;
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
    #[must_use]
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
    #[allow(clippy::future_not_send)]
    pub async fn obliterate(&self) -> KioResult<()> {
        self.delete_all_jobs().await?;
        // delete all other grouped collections;
        self.store.clear_collections().await?;
        let event_mode = self.event_mode.load();
        let event = JobState::Obliterated;
        let last_id = self.current_metrics.last_id.load();
        let item: QueueStreamEvent<R, P> = QueueStreamEvent {
            job_id: last_id,
            event,
            ..Default::default()
        };
        self.store.publish_event(event_mode, item).await?;
        self.current_metrics.clear();
        self.store.clear_collections().await?;
        if let Some(timers) = self.timers.load_full() {
            timers.close();
        }
        P_METRICS_COLLECTOR.unregister_queue(self.id);
        Ok(())
    }
    #[allow(clippy::future_not_send)]
    async fn delete_all_jobs(&self) -> KioResult<()> {
        let last_id = self.current_metrics.last_id.load();
        self.store.clear_jobs(last_id).await
    }

    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(self,timer_sender)))]
    pub(crate) async fn promote_delayed_jobs(
        &self,
        date_time: Dt,
        interval_ms: i64,
        timer_sender: &TimerSender,
    ) -> KioResult<()> {
        promote_jobs(self, date_time, interval_ms, timer_sender).await
    }

    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(self)))]
    async fn move_job_from_priorty_to_active(&self) -> KioResult<Option<u64>> {
        let mut min_priority_job: Vec<(u64, u64)> = self
            .store
            .pop_set(CollectionSuffix::Prioritized, true)
            .await?;

        if let Some((job_id, _score)) = min_priority_job.pop() {
            let _: () = self
                .store
                .add_item(CollectionSuffix::Active, job_id, None, true)
                .await?;
            return Ok(Some(job_id));
        }

        let _: () = self.store.remove(CollectionSuffix::PriorityCounter).await?;

        Ok(None)
    }

    /// Applies the retention policy in `remove_options` to `job_id` after it
    /// reaches a terminal state.
    ///
    /// Depending on the policy the job record may be deleted immediately, kept
    /// for a limited age, or pruned once a count threshold is exceeded.
    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(self)))]
    #[allow(clippy::future_not_send)]
    pub(crate) async fn clean_up_job(
        &self,
        job_id: u64,
        remove_options: Option<RemoveOnCompletionOrFailure>,
    ) -> KioResult<()> {
        let id = job_id;
        if let Some(remove_options) = remove_options {
            match remove_options {
                RemoveOnCompletionOrFailure::Bool(remove_immediately) => {
                    if remove_immediately {
                        self.store.remove(CollectionSuffix::Job(job_id)).await?;
                    }
                }
                RemoveOnCompletionOrFailure::Int(max_to_keep) => {
                    if max_to_keep.is_positive()
                        && i64::try_from(id).unwrap_or(i64::MAX) > max_to_keep
                    {
                        self.store.remove(CollectionSuffix::Job(job_id)).await?;
                    }
                }
                RemoveOnCompletionOrFailure::Opts(KeepJobs { age, count }) => {
                    if let Some(expire_in_secs) = age {
                        self.store
                            .expire(CollectionSuffix::Job(job_id), expire_in_secs)
                            .await?;
                    }
                    if let Some(max_to_keep) = count
                        && max_to_keep.is_positive()
                        && i64::try_from(id).unwrap_or(i64::MAX) > max_to_keep
                    {
                        self.store.remove(CollectionSuffix::Job(job_id)).await?;
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
    ///
    /// # Errors
    ///
    /// Returns [`KioError`] if the store lookup fails.
    pub async fn fetch_jobs(&self, ids: &[u64]) -> KioResult<VecDeque<Job<D, R, P>>> {
        self.store.fetch_jobs(ids).await
    }
    /// Returns the IDs of jobs currently in the given `state`.
    ///
    /// Use `start` and `end` to paginate large result sets; pass `None` for
    /// both to retrieve all IDs.
    ///
    /// # Errors
    ///
    /// Returns [`KioError`] if the store lookup fails.
    pub async fn get_job_ids_in_state(
        &self,
        state: JobState,
        start: Option<usize>,
        end: Option<usize>,
    ) -> KioResult<VecDeque<u64>> {
        self.store.get_job_ids_in_state(state, start, end).await
    }
    /// Returns the name of this queue (as provided to the store constructor).
    #[must_use]
    pub fn name(&self) -> &str {
        self.store.queue_name()
    }
    /// Returns the key prefix used for all collections belonging to this queue.
    #[must_use]
    pub fn prefix(&self) -> &str {
        self.store.queue_prefix()
    }
}

/// The outcome of a single [`Queue::move_to_active`] call.
#[derive(derive_more::Debug)]
pub enum MoveToActiveResult<D, R, P> {
    /// The queue is paused; no job was picked up.
    Paused,
    /// The queue is rate-limited; retry after the given number of milliseconds.
    RateLimit(u64),
    /// No job is ready yet; retry after the given Unix timestamp in milliseconds.
    DelayUntil(u64),
    /// A job is ready to be processed.
    #[debug("ProcessJob({0}) from state{1}", _0.id.unwrap_or_default(), _0.state)]
    ProcessJob(Box<Job<D, R, P>>),
}
impl<D, R, P> MoveToActiveResult<D, R, P> {
    fn from_job_state_pair((job, _state): (Job<D, R, P>, Option<JobState>)) -> Self {
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
    pub(crate) fn calculate_next_delay_ms(
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
    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(self, opts)))]
    #[allow(clippy::future_not_send)]
    pub(crate) async fn retry_job<'a, T: Into<RetryOptions<'a>>>(
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
                                .await?;
                        }
                        _ => {
                            self.store
                                .add_item(
                                    CollectionSuffix::Delayed,
                                    job_id,
                                    Some(next_delayed_timestamp),
                                    true,
                                )
                                .await?;
                        }
                    }
                }
                Ok(())
            }
        }
    }
    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(self)))]
    #[allow(clippy::future_not_send)]
    async fn retry_failed(
        &self,
        job_id: u64,
        backoff_job_opts: &BackOffJobOptions,
        attempts: u64,
    ) -> KioResult<()> {
        let ts = Utc::now();

        if let Some(next_delay) =
            self.calculate_next_delay_ms(backoff_job_opts, attempts.cast_signed())
        {
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
    #[must_use]
    pub fn is_paused(&self) -> bool {
        self.current_metrics.queue_is_paused()
    }
    /// Signals all workers attached to this queue to stop picking up new jobs.
    ///
    /// This sets an atomic flag that workers poll; jobs already being processed
    /// continue to completion.
    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(self)))]
    pub fn pause_active_workers(&self) {
        self.pause_workers.store(true);
    }
    /// Allows workers to resume picking up new jobs after a pause.
    ///
    /// Wakes any workers that are sleeping on the notifier.
    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(self)))]
    pub(crate) fn resume_workers(&self) {
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
    #[allow(clippy::future_not_send)]
    pub async fn get_metrics(&self) -> KioResult<QueueMetrics> {
        let updated = self.store.get_metrics().await?;
        self.current_metrics.update(&updated);
        Ok(updated)
    }
    /// Retrieves per-worker metrics stored in the backing store.
    ///
    /// Returns a map from worker [`Uuid`] to [`WorkerMetrics`].
    ///
    /// # Errors
    ///
    /// Returns [`KioError`] if the store lookup fails.
    #[allow(clippy::future_not_send)]
    pub async fn fetch_worker_metrics(&self) -> KioResult<BTreeMap<uuid::Uuid, WorkerMetrics>> {
        self.store.fetch_worker_metrics().await
    }
    /// Retrieves per-process metrics stored in the backing store.
    ///
    /// Returns a map from process ID (`u32`) to [`ProcessMetrics`].
    ///
    /// # Errors
    ///
    /// Returns [`KioError`] if the store lookup fails.
    #[allow(clippy::future_not_send)]
    pub async fn fetch_proess_metrics(&self) -> KioResult<BTreeMap<u32, ProcessMetrics>> {
        self.store.fetch_process_metrics().await
    }
    /// Persists the given worker metrics to the backing store with a TTL.
    ///
    /// Workers call this periodically (controlled by
    /// [`WorkerOpts::metrics_update_interval`]) so that operators can monitor
    /// per-worker task health.
    ///
    /// # Errors
    ///
    /// Returns [`KioError`] if the store write fails.
    #[allow(clippy::future_not_send)]
    pub async fn store_worker_metrics(&self, metrics: WorkerMetrics, ttl_ms: u64) -> KioResult<()> {
        self.store.store_worker_metrics(metrics, ttl_ms).await
    }
    /// Persists the given process metrics to the backing store with a TTL.
    ///
    /// # Errors
    ///
    /// Returns [`KioError`] if the store write fails.
    #[allow(clippy::future_not_send)]
    pub async fn store_process_metrics(
        &self,
        metrics: ProcessMetrics,
        ttl_ms: u64,
    ) -> KioResult<()> {
        self.store.store_process_metrics(metrics, ttl_ms).await
    }
    /// Increments or decrements the in-progress counter and publishes a
    /// `Processing` event so that listeners know a worker has started or
    /// finished a job.
    ///
    /// Returns the updated counter value.
    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(self)))]
    #[allow(clippy::future_not_send)]
    pub(crate) async fn update_processing_count(
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
        let event_mode = self.event_mode.load();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::QueueError;
    use crate::worker::MIN_DELAY_MS_LIMIT;
    use crate::{BackOffJobOptions, BackOffOptions, InMemoryStore, JobOptions};
    use std::sync::Arc;

    type D = i32;
    type R = i32;
    type P = i32;
    type TestQueue = Queue<D, R, P, InMemoryStore<D, R, P>>;

    /// Builds an isolated in-memory-backed queue with a unique name so that
    /// tests never share the global metrics collector's per-queue state.
    async fn make_queue(opts: Option<QueueOpts>) -> KioResult<TestQueue> {
        let name = Uuid::new_v4().to_string();
        let store = InMemoryStore::<D, R, P>::new(None, &name);
        Queue::new(store, opts).await
    }

    async fn make_queue_with_prefix(prefix: &str, name: &str) -> KioResult<TestQueue> {
        let store = InMemoryStore::<D, R, P>::new(Some(prefix), name);
        Queue::new(store, None).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn new_with_none_opts_applies_defaults() -> KioResult<()> {
        let queue = make_queue(None).await?;
        assert_eq!(queue.opts.attempts, 1);
        assert_eq!(queue.opts.event_mode, Some(QueueEventMode::Stream));
        assert!(!queue.is_paused());
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn new_honours_custom_attempts_and_event_mode() -> KioResult<()> {
        let opts = QueueOpts {
            attempts: 9,
            event_mode: Some(QueueEventMode::PubSub),
            ..Default::default()
        };
        let queue = make_queue(Some(opts)).await?;
        assert_eq!(queue.opts.attempts, 9);
        assert_eq!(queue.event_mode.load(), QueueEventMode::PubSub);
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn name_and_prefix_accessors_reflect_store() -> KioResult<()> {
        let queue = make_queue_with_prefix("myprefix", "orders").await?;
        assert_eq!(queue.name(), "orders");
        assert_eq!(queue.prefix(), "myprefix");
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn default_prefix_is_kio() -> KioResult<()> {
        let queue = make_queue(None).await?;
        assert_eq!(queue.prefix(), "kio");
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn add_single_job_increments_waiting_and_assigns_id() -> KioResult<()> {
        let queue = make_queue(None).await?;
        let job = queue.add_job("task", 1, None).await?;
        assert!(job.id.is_some(), "store must assign a job id");

        let metrics = queue.get_metrics().await?;
        assert_eq!(metrics.waiting.load(), 1);
        assert_eq!(metrics.last_id.load(), job.id.expect("id present"));
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_job_returns_none_for_unknown_id() -> KioResult<()> {
        let queue = make_queue(None).await?;
        // Nothing was ever enqueued, so any id must be absent.
        assert!(queue.get_job(999_999).await.is_none());
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn added_job_is_retrievable_by_id() -> KioResult<()> {
        let queue = make_queue(None).await?;
        let job = queue.add_job("fetch-me", 77, None).await?;
        let id = job.id.expect("id present");
        let fetched = queue.get_job(id).await.expect("job should be retrievable");
        assert_eq!(fetched.id, job.id);
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn add_job_accepts_empty_name() -> KioResult<()> {
        let queue = make_queue(None).await?;
        let job = queue.add_job("", 1, None).await?;
        assert!(job.id.is_some());
        assert_eq!(queue.get_metrics().await?.waiting.load(), 1);
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn add_job_accepts_very_long_name() -> KioResult<()> {
        let queue = make_queue(None).await?;
        let long_name = "n".repeat(10_000);
        let job = queue.add_job(&long_name, 1, None).await?;
        assert!(job.id.is_some());
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn duplicate_named_jobs_receive_distinct_ids() -> KioResult<()> {
        let queue = make_queue(None).await?;
        let first = queue.add_job("dup", 1, None).await?;
        let second = queue.add_job("dup", 1, None).await?;
        assert_ne!(
            first.id, second.id,
            "identically named jobs must not collide on id"
        );
        assert_eq!(queue.get_metrics().await?.waiting.load(), 2);
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn zero_delay_job_goes_straight_to_waiting() -> KioResult<()> {
        let queue = make_queue(None).await?;
        let opts = JobOptions {
            delay: 0i64.into(),
            ..Default::default()
        };
        queue.add_job("immediate", 1, Some(opts)).await?;
        let metrics = queue.get_metrics().await?;
        assert_eq!(metrics.waiting.load(), 1);
        assert_eq!(metrics.delayed.load(), 0);
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delay_just_below_limit_is_rejected() -> KioResult<()> {
        let queue = make_queue(None).await?;
        let below = MIN_DELAY_MS_LIMIT.saturating_sub(1); // 49 ms
        let opts = JobOptions {
            delay: below.cast_signed().into(),
            ..Default::default()
        };
        let err = queue
            .add_job("too-soon", 1, Some(opts))
            .await
            .expect_err("sub-limit delay must be rejected");
        assert!(
            matches!(
                err,
                KioError::QueueError(QueueError::DelayBelowAllowedLimit { limit_ms, current_ms })
                    if limit_ms == MIN_DELAY_MS_LIMIT && current_ms == below
            ),
            "unexpected error variant: {err:?}"
        );
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delay_exactly_at_limit_is_accepted() -> KioResult<()> {
        let queue = make_queue(None).await?;
        let opts = JobOptions {
            delay: MIN_DELAY_MS_LIMIT.cast_signed().into(), // 50 ms — the boundary
            ..Default::default()
        };
        queue.add_job("boundary", 1, Some(opts)).await?;
        let metrics = queue.get_metrics().await?;
        assert_eq!(metrics.delayed.load(), 1);
        assert!(metrics.has_delayed());
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn comfortably_delayed_job_lands_in_delayed_set() -> KioResult<()> {
        let queue = make_queue(None).await?;
        let opts = JobOptions {
            delay: 500i64.into(),
            ..Default::default()
        };
        let job = queue.add_job("later", 1, Some(opts)).await?;
        let id = job.id.expect("id present");
        let fetched = queue.get_job(id).await.expect("delayed job present");
        assert_eq!(fetched.state, JobState::Delayed);
        assert_eq!(queue.get_metrics().await?.delayed.load(), 1);
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prioritised_job_enters_prioritized_state() -> KioResult<()> {
        let queue = make_queue(None).await?;
        let opts = JobOptions {
            priority: 5,
            ..Default::default()
        };
        let job = queue.add_job("vip", 1, Some(opts)).await?;
        let id = job.id.expect("id present");
        let fetched = queue.get_job(id).await.expect("prioritised job present");
        assert_eq!(fetched.priority, 5);
        assert_eq!(fetched.state, JobState::Prioritized);
        assert_eq!(queue.get_metrics().await?.prioritized.load(), 1);
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bulk_add_empty_iterator_adds_nothing() -> KioResult<()> {
        let queue = make_queue(None).await?;
        let empty: Vec<(String, Option<JobOptions>, D)> = Vec::new();
        let jobs = queue.bulk_add(empty.into_iter()).await?;
        assert!(jobs.is_empty());
        assert_eq!(queue.get_metrics().await?.waiting.load(), 0);
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bulk_add_large_batch_counts_all_jobs() -> KioResult<()> {
        let queue = make_queue(None).await?;
        let batch = (0..250).map(|i| (format!("job-{i}"), None, i));
        let jobs = queue.bulk_add(batch).await?;
        assert_eq!(jobs.len(), 250);
        assert_eq!(queue.get_metrics().await?.waiting.load(), 250);
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bulk_add_only_still_enqueues_jobs() -> KioResult<()> {
        let queue = make_queue(None).await?;
        let batch = (0..8).map(|i| (format!("j{i}"), None, i));
        queue.bulk_add_only(batch).await?;
        assert_eq!(queue.get_metrics().await?.waiting.load(), 8);
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fetch_jobs_ignores_missing_ids() -> KioResult<()> {
        let queue = make_queue(None).await?;
        let job = queue.add_job("real", 1, None).await?;
        let id = job.id.expect("id present");
        // Mix one real id with ids that never existed.
        let fetched = queue.fetch_jobs(&[id, 424_242, 999_999]).await?;
        assert_eq!(fetched.len(), 1, "only the real job should come back");
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_job_ids_in_state_lists_waiting_jobs() -> KioResult<()> {
        let queue = make_queue(None).await?;
        queue
            .bulk_add((0..3).map(|i| (format!("w{i}"), None, i)))
            .await?;
        let ids = queue
            .get_job_ids_in_state(JobState::Wait, None, None)
            .await?;
        assert_eq!(ids.len(), 3);
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pause_or_resume_moves_jobs_between_wait_and_paused() -> KioResult<()> {
        let opts = QueueOpts {
            event_mode: Some(QueueEventMode::PubSub),
            ..Default::default()
        };
        let queue = make_queue(Some(opts)).await?;
        queue.add_job("p", 1, None).await?;
        assert_eq!(queue.get_metrics().await?.waiting.load(), 1);

        queue.pause_or_resume().await?;
        // Assert on the fresh store-returned metrics, which are the reliable
        // signal. (The in-memory `queue.is_paused()` flag is intentionally not
        // asserted here — see the ignored test below documenting its staleness.)
        let paused = queue.get_metrics().await?;
        assert!(paused.is_paused.load());
        assert_eq!(paused.waiting.load(), 0);
        assert_eq!(paused.paused.load(), 1);

        queue.pause_or_resume().await?;
        let resumed = queue.get_metrics().await?;
        assert!(!resumed.is_paused.load());
        assert_eq!(resumed.waiting.load(), 1);
        queue.obliterate().await?;
        Ok(())
    }

    #[ignore = "SUSPECTED BUG: QueueMetrics::update never swaps the `is_paused` \
                flag, so the in-memory `queue.is_paused()` value is never refreshed \
                by get_metrics(), contradicting the is_paused() docs which say to \
                call get_metrics() first for a fresh value."]
    #[tokio::test(flavor = "multi_thread")]
    async fn is_paused_flag_is_refreshed_by_get_metrics() -> KioResult<()> {
        let queue = make_queue(None).await?;
        queue.add_job("p", 1, None).await?;
        queue.pause_or_resume().await?;
        // Fresh store metrics report paused == true.
        let refreshed = queue.get_metrics().await?;
        assert!(refreshed.is_paused.load(), "store reports paused");
        // But the in-memory flag remains stale (this assertion currently fails).
        assert!(
            queue.is_paused(),
            "in-memory is_paused() should reflect the store after get_metrics()"
        );
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn obliterate_clears_all_counters() -> KioResult<()> {
        let queue = make_queue(None).await?;
        queue
            .bulk_add((0..5).map(|i| (format!("o{i}"), None, i)))
            .await?;
        assert_eq!(queue.get_metrics().await?.waiting.load(), 5);

        queue.obliterate().await?;
        assert_eq!(queue.current_metrics.waiting.load(), 0);
        assert_eq!(queue.current_metrics.last_id.load(), 0);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn calculate_next_delay_ms_zero_number_yields_none() -> KioResult<()> {
        let queue = make_queue(None).await?;
        // A fixed backoff of 0 ms normalises to "no backoff" → None.
        assert_eq!(
            queue.calculate_next_delay_ms(&BackOffJobOptions::Number(0), 3),
            None
        );
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn calculate_next_delay_ms_fixed_is_constant() -> KioResult<()> {
        let queue = make_queue(None).await?;
        let opts = BackOffJobOptions::Number(120);
        assert_eq!(queue.calculate_next_delay_ms(&opts, 1), Some(120));
        assert_eq!(queue.calculate_next_delay_ms(&opts, 9), Some(120));
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn calculate_next_delay_ms_exponential_grows() -> KioResult<()> {
        let queue = make_queue(None).await?;
        let opts = BackOffJobOptions::Opts(BackOffOptions {
            type_: Some("exponential".into()),
            delay: Some(100),
        });
        // Exponential backoff is 2^attempt * delay: 2^2 * 100 = 400, 2^3 * 100 = 800.
        let attempt_2 = queue.calculate_next_delay_ms(&opts, 2);
        let attempt_3 = queue.calculate_next_delay_ms(&opts, 3);
        assert_eq!(attempt_2, Some(400));
        assert_eq!(attempt_3, Some(800));
        assert!(
            attempt_3 > attempt_2,
            "delay must strictly grow with attempts ({attempt_3:?} > {attempt_2:?})"
        );
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn calculate_next_delay_ms_unknown_strategy_yields_none() -> KioResult<()> {
        let queue = make_queue(None).await?;
        let opts = BackOffJobOptions::Opts(BackOffOptions {
            type_: Some("does-not-exist".into()),
            delay: Some(100),
        });
        assert_eq!(queue.calculate_next_delay_ms(&opts, 2), None);
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn register_backoff_strategy_adds_a_new_named_strategy() -> KioResult<()> {
        let queue = make_queue(None).await?;
        queue.register_backoff_strategy("triple", |delay| {
            Arc::new(move |attempts: i64| attempts.saturating_mul(delay))
        });
        let opts = BackOffJobOptions::Opts(BackOffOptions {
            type_: Some("triple".into()),
            delay: Some(10),
        });
        assert_eq!(queue.calculate_next_delay_ms(&opts, 4), Some(40));
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn register_backoff_strategy_does_not_replace_builtin() -> KioResult<()> {
        let queue = make_queue(None).await?;
        // Attempt to shadow the built-in "exponential" with a constant.
        queue.register_backoff_strategy("exponential", |_delay| Arc::new(|_attempts: i64| 999));
        let opts = BackOffJobOptions::Opts(BackOffOptions {
            type_: Some("exponential".into()),
            delay: Some(100),
        });
        // The original exponential formula (2^2 * 100 = 400) must survive.
        assert_eq!(queue.calculate_next_delay_ms(&opts, 2), Some(400));
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn event_listener_can_be_registered_then_removed_once() -> KioResult<()> {
        let queue = make_queue(None).await?;
        let id = queue.on(JobState::Completed, |_evt| async move {});
        // First removal succeeds and returns the same id.
        assert_eq!(queue.remove_event_listener(id), Some(id));
        // A second removal finds nothing.
        assert_eq!(queue.remove_event_listener(id), None);
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn removing_unknown_listener_returns_none() -> KioResult<()> {
        let queue = make_queue(None).await?;
        assert_eq!(queue.remove_event_listener(Uuid::new_v4()), None);
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn on_all_events_returns_a_handle() -> KioResult<()> {
        let queue = make_queue(None).await?;
        // The emitter permits only a single catch-all listener, so a distinct-id
        // check is impossible. Instead prove the handle is meaningful by showing
        // the listener actually fires — a bare non-nil v4 UUID could never fail.
        let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let hits_in_cb = std::sync::Arc::clone(&hits);
        let id = queue.on_all_events(move |_evt: EventParameters<R, P>| {
            let hits = std::sync::Arc::clone(&hits_in_cb);
            async move {
                hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        });
        assert!(
            !id.is_nil(),
            "catch-all registration must yield a non-nil handle"
        );

        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            queue
                .emitter
                .emit(JobState::Completed, EventParameters::Void),
        )
        .await
        .expect("emit must not hang");
        assert_eq!(
            hits.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the catch-all listener must fire for the emitted event"
        );

        queue.obliterate().await?;
        Ok(())
    }

    #[ignore = "SUSPECTED BUG: on_all_events() docs promise the returned Uuid \
                'can later be passed to remove_event_listener', but removal of a \
                catch-all listener returns None (per-event listeners remove fine)."]
    #[tokio::test(flavor = "multi_thread")]
    async fn on_all_events_listener_is_removable() -> KioResult<()> {
        let queue = make_queue(None).await?;
        let id = queue.on_all_events(|_evt| async move {});
        assert_eq!(queue.remove_event_listener(id), Some(id));
        queue.obliterate().await?;
        Ok(())
    }

    #[test]
    fn move_to_active_result_debug_renders_simple_variants() {
        let paused: MoveToActiveResult<D, R, P> = MoveToActiveResult::Paused;
        let rate: MoveToActiveResult<D, R, P> = MoveToActiveResult::RateLimit(250);
        let delay: MoveToActiveResult<D, R, P> = MoveToActiveResult::DelayUntil(1_000);
        assert!(format!("{paused:?}").contains("Paused"));
        assert!(format!("{rate:?}").contains("250"));
        assert!(format!("{delay:?}").contains("1000"));
    }
}
