use crate::{
    stores::Store, timers::DelayQueueTimer, utils::processor_types::SharedStore,
    worker::processor_types::SyncFn, Job, JobState, JobToken, KioError, KioResult, Queue,
};

use crate::utils::main_loop;
use derive_more::Debug;
use futures::future::{Future, FutureExt};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::atomic::AtomicBool;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize},
    Arc,
};
use uuid::Uuid;
mod metrics;
mod worker_opts;
pub use metrics::*;

use crate::error::WorkerError;
use crate::events::EventParameters;
use arc_swap::ArcSwapOption;
use crossbeam_skiplist::SkipMap;
use hdrhistogram::Histogram;
use tokio::{sync::Notify, task::JoinHandle};
use tokio_metrics::TaskMonitor;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use xutex::AsyncMutex;
type JobMeta<D, R, P> = (
    Job<D, R, P>,
    JobToken,
    TaskHandle,
    TaskMonitor,
    AsyncMutex<Histogram<u64>>,
);
pub type JobMap<D, R, P> = Arc<SkipMap<u64, JobMeta<D, R, P>>>;
pub type Task = JoinHandle<KioResult<()>>;
pub type TaskHandle = ArcSwapOption<Task>;
pub type SharedTaskHandle = Arc<TaskHandle>;
/// Alias for the `processing_queue`. changed from (`Futures::FuturesUnordered` -> `TaskTracker`)
pub type ProcessingQueue = TaskTracker;
use atomig::{Atom, Atomic};
use derive_more::IsVariant;
pub use worker_opts::WorkerOpts;
/// The current lifecycle state of a [`Worker`].
#[derive(Atom, IsVariant, Default, Debug)]
#[repr(u8)]
pub enum WorkerState {
    /// The worker is actively polling and processing jobs.
    Active,
    /// The worker is running but has no jobs to process (idle / sleeping).
    #[default]
    Idle,
    /// The worker has been shut down via [`Worker::close`].
    Closed,
}
#[cfg(feature = "tracing")]
use tracing::{debug, instrument, warn, Instrument, Span};

pub use worker_opts::MIN_DELAY_MS_LIMIT;
/// A job processor that consumes jobs from a [`Queue`].
///
/// Each `Worker` runs an internal async loop that fetches jobs from the queue
/// and invokes your processor function.  Multiple workers can be attached to
/// the same queue to increase throughput.
///
/// # Type parameters
///
/// | Parameter | Description |
/// |-----------|-------------|
/// | `D` | Job input data type |
/// | `R` | Job return / result type |
/// | `P` | Job progress type |
/// | `S` | Backing [`Store`] implementation |
///
/// # Lifecycle
///
/// 1. Create with [`Worker::new_async`] or [`Worker::new_sync`].
/// 2. Call [`run`](Worker::run) to start the processing loop.
/// 3. Call [`close`](Worker::close) to stop the worker (idempotent—calling
///    `close` on an already-closed worker is a no-op).
///
/// # Examples
///
/// ```rust
/// # #[tokio::main]
/// # async fn main() -> kiomq::KioResult<()> {
/// use std::sync::Arc;
/// use kiomq::{InMemoryStore, Job, KioError, Queue, Worker, WorkerOpts};
///
/// let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "worker-demo");
/// let queue = Queue::new(store, None).await?;
///
/// let worker = Worker::new_async(
///     &queue,
///     |_store: Arc<_>, job: Job<u64, u64, ()>| async move {
///         Ok::<u64, KioError>(job.data.unwrap_or_default() * 2)
///     },
///     Some(WorkerOpts::default()),
/// )?;
///
/// worker.run()?;
/// worker.close();
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Worker<D, R, P, S> {
    /// Unique identifier for this worker instance.
    pub id: Uuid,
    #[cfg(feature = "tracing")]
    resource_span: Span,
    queue: Arc<Queue<D, R, P, S>>,
    jobs_in_progress: JobMap<D, R, P>,
    #[debug(skip)]
    processor: WorkerCallback<D, R, P, S>,
    /// Configuration options for this worker.
    pub opts: WorkerOpts,
    cancellation_token: Arc<CancellationToken>,
    /// Current lifecycle state of the worker.
    pub state: Arc<Atomic<WorkerState>>,
    processing: ProcessingQueue,
    promoting: ProcessingQueue,
    timer_pauser: Arc<AtomicBool>,
    timers: DelayQueueTimer<D, R, P, S>,
    block_until: Arc<AtomicU64>,
    active_job_count: Arc<AtomicUsize>,
    continue_notifier: Arc<Notify>,
    main_task: SharedTaskHandle,
}
use crate::utils::processor_types;
use processor_types::Callback;
/// A callback definition alias for the worker
pub type WorkerCallback<D, R, P, S> = Callback<D, R, P, S>;

impl<
        D: Clone + DeserializeOwned + 'static + Send + Sync + Serialize,
        R: Clone + DeserializeOwned + 'static + Serialize + Send + Sync,
        P: Clone + DeserializeOwned + 'static + Send + Sync + Serialize,
        S: Clone + Store<D, R, P> + Send + 'static + Sync,
    > Worker<D, R, P, S>
{
    /// Creates a worker with a **sync** (blocking) processor function.
    ///
    /// The processor runs on a dedicated OS thread via
    /// [`tokio::task::spawn_blocking`](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html),
    /// so heavy CPU-bound or blocking work will not starve Tokio's async executor threads.
    ///
    /// # Errors
    ///
    /// Returns [`KioError`] if the worker cannot be initialised (e.g. if
    /// `WorkerOpts::autorun` is `true` and the initial [`run`](Worker::run) call
    /// fails).
    ///
    /// # Examples
    ///
    /// ```rust
    /// # #[tokio::main]
    /// # async fn main() -> kiomq::KioResult<()> {
    /// use std::sync::Arc;
    /// use kiomq::{InMemoryStore, Job, KioError, Queue, Worker, WorkerOpts};
    ///
    /// let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "sync-worker");
    /// let queue = Queue::new(store, None).await?;
    ///
    /// let worker = Worker::new_sync(
    ///     &queue,
    ///     |_store: Arc<_>, job: Job<u64, u64, ()>| {
    ///         Ok::<u64, KioError>(job.data.unwrap_or_default() * 2)
    ///     },
    ///     Some(WorkerOpts::default()),
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    #[track_caller]
    pub fn new_sync<C, E>(
        queue: &Queue<D, R, P, S>,
        processor: C,
        worker_opts: Option<WorkerOpts>,
    ) -> KioResult<Self>
    where
        KioError: From<E>,
        C: Fn(SharedStore<S>, Job<D, R, P>) -> Result<R, E> + Send + Sync + 'static,
        P: Send + Sync + 'static,
        R: Send + Sync + 'static,
        D: Send + Sync + 'static,
        S: Sync + Store<D, R, P> + Send + 'static,
        E: std::error::Error + Send + 'static,
    {
        Self::new::<C, SyncFn<C, D, R, P, S, E>, E>(queue, processor, worker_opts)
    }
    /// Creates a worker with an **async** processor function.
    ///
    /// The processor runs directly on the Tokio runtime; it is best suited for
    /// I/O-bound work.  For CPU-intensive or blocking workloads prefer
    /// [`new_sync`](Worker::new_sync).
    ///
    /// # Errors
    ///
    /// Returns [`KioError`] if initialisation fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # #[tokio::main]
    /// # async fn main() -> kiomq::KioResult<()> {
    /// use std::sync::Arc;
    /// use kiomq::{InMemoryStore, Job, KioError, Queue, Worker, WorkerOpts};
    ///
    /// let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "async-worker");
    /// let queue = Queue::new(store, None).await?;
    ///
    /// let worker = Worker::new_async(
    ///     &queue,
    ///     |_store: Arc<_>, job: Job<u64, u64, ()>| async move {
    ///         Ok::<u64, KioError>(job.data.unwrap_or_default() * 2)
    ///     },
    ///     None,
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    #[track_caller]
    pub fn new_async<C, Fut, E>(
        queue: &Queue<D, R, P, S>,
        processor: C,
        worker_opts: Option<WorkerOpts>,
    ) -> KioResult<Self>
    where
        KioError: From<E>,
        C: Fn(SharedStore<S>, Job<D, R, P>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, E>> + Send + 'static,
        P: Send + Sync + 'static,
        R: Send + Sync + 'static,
        S: Sync + Store<D, R, P> + Send + 'static,
        D: Send + Sync + 'static,
        E: std::error::Error + Send + 'static,
    {
        use processor_types::AsyncFn;
        Self::new::<C, AsyncFn<C, D, R, P, S, E>, E>(queue, processor, worker_opts)
    }
    #[track_caller]
    fn new<C, F, E>(
        queue: &Queue<D, R, P, S>,
        processor: C,
        worker_opts: Option<WorkerOpts>,
    ) -> KioResult<Self>
    where
        KioError: From<E>,
        C: Into<F>,
        Callback<D, R, P, S>: From<F>,
        P: Send + Sync + 'static,
        R: Send + Sync + 'static,
        D: Send + Sync + 'static,
        S: Store<D, R, P> + Send + Sync + 'static,
        E: std::error::Error + Send + 'static,
    {
        let queue = Arc::new(queue.clone());
        let jobs_in_progress: JobMap<_, _, _> = Arc::new(SkipMap::new());
        let f: F = processor.into();
        let callback = Callback::from(f);

        let id = Uuid::new_v4();
        let opts = worker_opts.unwrap_or_default();
        let jobs = jobs_in_progress.clone();
        let cancellation_token: Arc<CancellationToken> = Arc::default();
        let continue_notifier = queue.worker_notifier.clone();
        let notifier = continue_notifier.clone();
        let state: Arc<Atomic<WorkerState>> = Arc::default();
        let worker_state = state.clone();
        let timer_pauser: Arc<AtomicBool> = Arc::default();
        let processing = TaskTracker::new();
        let promoting = TaskTracker::new();
        let timers = DelayQueueTimer::new(
            jobs,
            id,
            opts,
            queue.clone(),
            cancellation_token.clone(),
            worker_state,
            notifier,
            timer_pauser.clone(),
            promoting.clone(),
            processing.clone(),
        );

        #[cfg(feature = "tracing")]
        let resource_span = {
            let callback_type = match &callback {
                Callback::Async(_) => "Async",
                Callback::Sync(_) => "Sync",
            };
            {
                let location = std::panic::Location::caller().to_string();
                let queue_name = queue.name();
                let worker_type = format!(
                    "{}-Worker({},{queue_name})",
                    callback_type,
                    id.as_u64_pair().0,
                );
                tracing::info_span!(parent:None, "",worker_type, ?location)
            }
        };
        let main_task = Arc::default();
        let worker = Self {
            state,
            timer_pauser,
            main_task,
            #[cfg(feature = "tracing")]
            resource_span,
            timers,
            continue_notifier,
            block_until: Arc::default(),
            opts,
            id,
            queue,
            jobs_in_progress,
            promoting,
            processing,
            processor: callback,
            cancellation_token,
            active_job_count: Arc::default(),
        };
        if worker.opts.autorun {
            worker.run()?;
        }

        Ok(worker)
    }

    /// Returns `true` if the worker is actively processing jobs.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.state
            .load(std::sync::atomic::Ordering::Acquire)
            .is_active()
            && !self.cancellation_token.is_cancelled()
    }
    /// Returns `true` if the worker is idle (started but waiting for work).
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.state
            .load(std::sync::atomic::Ordering::Acquire)
            .is_idle()
    }
    /// Starts the worker's job-processing loop.
    ///
    /// # Errors
    ///
    /// | Condition | Error |
    /// |-----------|-------|
    /// | Worker is already running | `WorkerAlreadyRunning` |
    /// | Worker has been closed | `WorkerAlreadyClosed` |
    ///
    /// Calling `run` on a closed worker (after [`close`](Worker::close)) is an
    /// error; create a new worker instead.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # #[tokio::main]
    /// # async fn main() -> kiomq::KioResult<()> {
    /// use std::sync::Arc;
    /// use kiomq::{InMemoryStore, Job, KioError, Queue, Worker};
    ///
    /// let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "run-demo");
    /// let queue = Queue::new(store, None).await?;
    /// let worker = Worker::new_async(
    ///     &queue,
    ///     |_: Arc<_>, job: Job<u64, u64, ()>| async move { Ok::<u64, KioError>(0) },
    ///     None,
    /// )?;
    ///
    /// worker.run()?;
    /// assert!(worker.is_running());
    /// worker.close();
    /// # Ok(())
    /// # }
    /// ```
    pub fn run(&self) -> KioResult<()> {
        let prev = self.state.compare_exchange(
            WorkerState::Idle,
            WorkerState::Active,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        );
        if let Err(current) = prev {
            if current.is_active() && !self.cancellation_token.is_cancelled() {
                return Err(WorkerError::WorkerAlreadyRunningWithId(self.id).into());
            }
            // if closed or canceled, return another error
            if current.is_closed() || self.cancellation_token.is_cancelled() {
                return Err(WorkerError::WorkerAlreadyClosed(self.id).into());
            }
        }
        #[cfg(not(feature = "tracing"))]
        let params = (
            self.id,
            self.cancellation_token.clone(),
            self.processing.clone(),
            self.promoting.clone(),
            self.opts,
            self.block_until.clone(),
            self.jobs_in_progress.clone(),
            self.active_job_count.clone(),
            self.processor.clone(),
            self.queue.clone(),
            self.state.clone(),
            self.continue_notifier.clone(),
            self.timers.clone(),
            self.timer_pauser.clone(),
        );
        #[cfg(feature = "tracing")]
        let params = (
            self.resource_span.clone(),
            self.id,
            self.cancellation_token.clone(),
            self.processing.clone(),
            self.promoting.clone(),
            self.opts,
            self.block_until.clone(),
            self.jobs_in_progress.clone(),
            self.active_job_count.clone(),
            self.processor.clone(),
            self.queue.clone(),
            self.state.clone(),
            self.continue_notifier.clone(),
            self.timers.clone(),
            self.timer_pauser.clone(),
        );
        #[cfg(feature = "tracing")]
        let main = main_loop(params).instrument(self.resource_span.clone());
        #[cfg(not(feature = "tracing"))]
        let main = main_loop(params);
        let main_task = tokio::spawn(main.boxed());
        self.main_task.swap(Some(main_task.into()));
        Ok(())
    }
    /// Returns `true` if the worker has been closed (cancelled).
    #[must_use]
    pub fn closed(&self) -> bool {
        self.cancellation_token.is_cancelled()
            || self
                .state
                .load(std::sync::atomic::Ordering::Acquire)
                .is_closed()
    }

    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(self)))]
    /// Stops the worker's processing loop.
    ///
    /// Signals the internal cancellation token and waits for the main loop
    /// task to finish.  Already-running jobs are allowed to complete.
    ///
    /// Calling `close` on a worker that is not running is a no-op (idempotent).
    ///
    /// # Note
    ///
    /// After calling `close` the worker **cannot** be restarted.  Create a new
    /// worker if you need to resume processing.
    pub fn close(&self) {
        if !self.is_running() {
            return;
        }
        #[cfg(feature = "tracing")]
        debug!(
            "cancel the worker's engine_loop, current_state: {:#?}",
            self.state.load(std::sync::atomic::Ordering::Acquire)
        );
        self.processing.close();
        self.promoting.close();

        self.timers.close();
        self.queue.resume_workers();
        self.queue.worker_notifier.notify_waiters();
        self.queue
            .pause_workers
            .store(false, std::sync::atomic::Ordering::Release);
        self.cancellation_token.cancel();
        // TODO: work on gracefully shutdown later; currently we just cancel the token but
        // don't check whether the main_task has closed too. Handle this later
        let mut main_task = self.main_task.load_full();
        if let Some(handle) = main_task.take() {
            // wait for handle to finishd
            #[cfg(feature = "tracing")]
            {
                let running_tasks = self.processing.len() + self.promoting.len();
                warn!("waiting for all {running_tasks} tasks to complete or abort");
            }
            // wait for the main loop to close
            while !handle.is_finished() {}
        }
    }

    /// Registers a listener for a specific job-state event on the underlying queue.
    ///
    /// This is a convenience wrapper around [`Queue::on`].  Returns a listener
    /// ID that can be passed to [`remove_event_listener`](Worker::remove_event_listener).
    pub fn on<F, C>(&self, event: JobState, callback: C) -> Uuid
    where
        C: Fn(EventParameters<R, P>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        self.queue.on(event, callback)
    }
    /// Registers a listener for **all** job-state events on the underlying queue.
    ///
    /// This is a convenience wrapper around [`Queue::on_all_events`].
    pub fn on_all_events<F, C>(&self, callback: C) -> Uuid
    where
        C: Fn(EventParameters<R, P>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        self.queue.on_all_events(callback)
    }
    /// Removes a previously registered event listener from the underlying queue.
    ///
    /// Returns the listener ID if found and removed, or `None` otherwise.
    #[must_use]
    pub fn remove_event_listener(&self, id: Uuid) -> Option<Uuid> {
        self.queue.remove_event_listener(id)
    }
}
