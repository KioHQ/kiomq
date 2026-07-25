use crate::{
    Job, JobState, JobToken, KioError, KioResult, Queue, stores::Store,
    utils::processor_types::SharedStore, worker::processor_types::SyncFn,
};

use crate::utils::main_loop;
use chrono::Utc;
use derive_more::Debug;
use futures::future::{Future, FutureExt};
use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;
use uuid::Uuid;
mod worker_opts;
use crate::Dt;

use crate::Counter;
use crate::error::WorkerError;
use crate::events::EventParameters;
use arc_swap::ArcSwapOption;
use hdrhistogram::Histogram;
use parking_lot::Mutex;
use serde::Deserialize;
use tokio::{sync::Notify, task::JoinHandle};
use tokio_metrics::TaskMonitor;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
type JobMeta<D, R, P> = (
    Job<D, R, P>,
    JobToken,
    TaskHandle,
    TaskMonitor,
    Mutex<Histogram<u64>>,
    WorkerOpts,
);
use crossbeam::atomic::AtomicCell;
use crossbeam_skiplist::SkipMap;
pub type JobMap<D, R, P> = Arc<SkipMap<u64, JobMeta<D, R, P>>>;
pub type Task = JoinHandle<KioResult<()>>;
pub type TaskHandle = ArcSwapOption<Task>;
pub type SharedTaskHandle = Arc<TaskHandle>;
/// Alias for the `processing_queue`. changed from (`Futures::FuturesUnordered` -> `TaskTracker`)
pub type ProcessingQueue = TaskTracker;
use derive_more::IsVariant;
pub use worker_opts::WorkerOpts;
/// The current lifecycle state of a [`Worker`].
#[derive(IsVariant, Default, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
use compact_str::ToCompactString;
#[cfg(feature = "tracing")]
use tracing::{Instrument, Span, debug, instrument, warn};

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
    /// The creation datetime of this worker
    pub created_at: Dt,
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
    pub state: Arc<AtomicCell<WorkerState>>,
    processing: ProcessingQueue,
    block_until: Counter,
    active_job_count: Arc<AtomicCell<usize>>,
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
        let f: F = processor.into();
        let callback = Callback::from(f);
        let id = Uuid::new_v4();
        let opts = worker_opts.unwrap_or_default();
        let jobs_in_progress = queue.jobs_in_progress.clone();
        let cancellation_token: Arc<CancellationToken> = Arc::default();
        let continue_notifier = queue.worker_notifier.clone();
        let state: Arc<AtomicCell<WorkerState>> = Arc::default();
        let processing = TaskTracker::new();
        #[cfg(feature = "tracing")]
        let resource_span = {
            let callback_type = match &callback {
                Callback::Async(_) => "Async",
                Callback::Sync(_) => "Sync",
            };
            {
                let location = std::panic::Location::caller().to_compact_string();
                let queue_name = queue.name();
                let worker_type = format!(
                    "{}-Worker({},{queue_name})",
                    callback_type,
                    id.as_u64_pair().0,
                );
                tracing::info_span!(parent:None, "",worker_type, ?location)
            }
        };

        let created_at = Utc::now();
        queue.add_worker(id, processing.clone(), state.clone(), opts, created_at);
        let main_task = Arc::default();
        let worker = Self {
            created_at,
            state,
            main_task,
            #[cfg(feature = "tracing")]
            resource_span,
            continue_notifier,
            block_until: Arc::default(),
            opts,
            id,
            queue,
            jobs_in_progress,
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
    ///
    /// A worker counts as running while its main-loop task is still live and it
    /// has not been fully shut down — i.e. it is either actively processing or
    /// has not yet been cancelled. Once [`close`](Worker::close) has taken the
    /// main-loop handle, this returns `false`, which also makes a second
    /// `close()` a cheap no-op.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.main_task.load().as_ref().is_some()
            && (self.state.load().is_active() || !self.cancellation_token.is_cancelled())
    }
    /// Returns `true` if the worker is idle (started but waiting for work).
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.state.load().is_idle()
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
        let prev = self
            .state
            .compare_exchange(WorkerState::Idle, WorkerState::Active);
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
            self.opts,
            self.block_until.clone(),
            self.jobs_in_progress.clone(),
            self.active_job_count.clone(),
            self.processor.clone(),
            self.queue.clone(),
            self.state.clone(),
            self.continue_notifier.clone(),
        );
        #[cfg(feature = "tracing")]
        let params = (
            self.resource_span.clone(),
            self.id,
            self.cancellation_token.clone(),
            self.processing.clone(),
            self.opts,
            self.block_until.clone(),
            self.jobs_in_progress.clone(),
            self.active_job_count.clone(),
            self.processor.clone(),
            self.queue.clone(),
            self.state.clone(),
            self.continue_notifier.clone(),
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
        self.cancellation_token.is_cancelled() || self.state.load().is_closed()
    }

    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(self)))]
    /// Stops the worker's processing loop.
    ///
    /// This is a **synchronous, blocking** call: it signals cancellation and
    /// then blocks the caller until the main loop has observed it, drained every
    /// in-flight job (`processing.wait().await`) and transitioned to
    /// [`WorkerState::Closed`]. When `close` returns, the worker is fully
    /// stopped and deregistered from the queue. Because the worker stays
    /// registered until the drain completes, in-flight jobs keep having their
    /// locks extended and cannot be re-claimed as stalled by another worker
    /// while they finish.
    ///
    /// Workers require a multi-threaded Tokio runtime. `close` may be called
    /// either from ordinary synchronous code (its intended use for long-lived
    /// singleton workers) or from within the runtime — in the latter case it
    /// hands the worker thread back to the scheduler via
    /// [`block_in_place`](tokio::task::block_in_place) so the main loop can be
    /// polled to completion while the caller blocks.
    ///
    /// Calling `close` on a worker that is not running is a no-op (idempotent),
    /// as is a second concurrent call once the first has taken the main-loop
    /// handle.
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
            self.state.load()
        );
        self.processing.close();

        self.queue.resume_workers();
        self.queue.worker_notifier.notify_waiters();
        self.queue.pause_workers.store(false);
        self.cancellation_token.cancel();

        // Take the main-loop handle out of the shared slot. A second (or
        // concurrent) `close()` then observes `None` here and skips straight to
        // deregistration instead of blocking on an already-finishing shutdown.
        if let Some(handle) = self.main_task.swap(None) {
            #[cfg(feature = "tracing")]
            {
                let running_tasks = self.processing.len();
                warn!("waiting for {running_tasks} in-flight task(s) to drain");
            }
            // On cancellation the main loop drains its in-flight jobs and sets
            // `Closed` before returning, so blocking on its handle turns
            // `close()` into a "stopped and drained" barrier.
            match Arc::try_unwrap(handle) {
                // Sole owner: await the handle. Awaiting parks the caller rather
                // than busy-spinning a CPU core while the jobs finish.
                Ok(task) => {
                    let wait = async {
                        let _ = task.await;
                    };
                    if tokio::runtime::Handle::try_current().is_ok() {
                        // Inside the (multi-threaded) runtime: give the worker
                        // thread back so the main loop can be polled elsewhere.
                        tokio::task::block_in_place(|| {
                            tokio::runtime::Handle::current().block_on(wait);
                        });
                    } else {
                        // Plain sync code: drive the handle on a local executor.
                        futures::executor::block_on(wait);
                    }
                }
                // A concurrent reader momentarily holds the handle too, so we
                // cannot take ownership to `await` it. Fall back to cooperatively
                // waiting for it to finish.
                Err(shared) => {
                    let backoff = crossbeam::utils::Backoff::new();
                    while !shared.is_finished() {
                        backoff.snooze();
                    }
                }
            }
        }
        self.queue.remove_worker(self.id);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EventParameters, InMemoryStore, Job, JobState, KioError, Queue, WorkerError};
    use crossbeam::queue::ArrayQueue;
    use std::time::Duration;

    type TestStore = InMemoryStore<i32, i32, i32>;
    type TestQueue = Queue<i32, i32, i32, TestStore>;

    async fn make_queue() -> KioResult<TestQueue> {
        let name = Uuid::new_v4().to_string();
        let store = InMemoryStore::<i32, i32, i32>::new(None, &name);
        Queue::new(store, None).await
    }

    fn doubling_worker(
        queue: &TestQueue,
        opts: Option<WorkerOpts>,
    ) -> KioResult<Worker<i32, i32, i32, TestStore>> {
        Worker::new_async(
            queue,
            |_conn, job: Job<i32, i32, i32>| async move {
                Ok::<i32, KioError>(job.data.unwrap_or_default() * 2)
            },
            opts,
        )
    }

    async fn wait_until<F: Fn() -> bool + Send + Sync>(condition: F, label: &str) {
        let outcome = tokio::time::timeout(Duration::from_secs(10), async {
            while !condition() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(outcome.is_ok(), "timed out waiting for: {label}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn new_worker_starts_idle_and_not_running() -> KioResult<()> {
        let queue = make_queue().await?;
        let worker = doubling_worker(&queue, None)?;

        assert!(worker.is_idle());
        assert!(!worker.is_running());
        assert!(!worker.closed());
        assert_eq!(worker.state.load(), WorkerState::Idle);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_transitions_worker_to_running() -> KioResult<()> {
        let queue = make_queue().await?;
        let worker = doubling_worker(&queue, None)?;

        worker.run()?;
        assert!(worker.is_running());
        assert!(!worker.is_idle());
        assert!(!worker.closed());

        worker.close();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn double_run_returns_already_running_error() -> KioResult<()> {
        let queue = make_queue().await?;
        let worker = doubling_worker(&queue, None)?;
        let id = worker.id;

        worker.run()?;
        let second = worker.run();
        match second {
            Err(KioError::WorkerError(WorkerError::WorkerAlreadyRunningWithId(err_id))) => {
                assert_eq!(err_id, id);
            }
            other => panic!("expected WorkerAlreadyRunningWithId, got {other:?}"),
        }

        worker.close();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_after_close_returns_already_closed_error() -> KioResult<()> {
        let queue = make_queue().await?;
        let worker = doubling_worker(&queue, None)?;
        let id = worker.id;

        worker.run()?;
        worker.close();
        assert!(worker.closed());

        match worker.run() {
            Err(KioError::WorkerError(WorkerError::WorkerAlreadyClosed(err_id))) => {
                assert_eq!(err_id, id);
            }
            other => panic!("expected WorkerAlreadyClosed, got {other:?}"),
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn close_on_a_worker_that_never_ran_is_a_noop() -> KioResult<()> {
        let queue = make_queue().await?;
        let worker = doubling_worker(&queue, None)?;

        worker.close();
        assert!(
            !worker.closed(),
            "an unstarted worker stays open after close()"
        );
        assert!(worker.is_idle());
        assert!(!worker.is_running());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn close_is_idempotent_after_running() -> KioResult<()> {
        let queue = make_queue().await?;
        let worker = doubling_worker(&queue, None)?;

        worker.run()?;
        worker.close();
        assert!(worker.closed());
        assert!(!worker.is_running());

        worker.close();
        assert!(worker.closed());
        assert!(!worker.is_running());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn autorun_starts_the_worker_inside_the_constructor() -> KioResult<()> {
        let queue = make_queue().await?;
        let opts = WorkerOpts {
            autorun: true,
            ..Default::default()
        };
        let worker = doubling_worker(&queue, Some(opts))?;

        assert!(
            worker.is_running(),
            "autorun=true must start the loop during construction"
        );

        worker.close();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn handler_error_moves_job_to_failed() -> KioResult<()> {
        let queue = make_queue().await?;

        let failed: Arc<ArrayQueue<u64>> = Arc::new(ArrayQueue::new(4));
        let failed_sink = failed.clone();
        queue.on(JobState::Failed, move |state: EventParameters<i32, i32>| {
            let failed = failed_sink.clone();
            async move {
                if let EventParameters::Failed { job_id, .. } = state {
                    failed.push(job_id).expect("event sink capacity exceeded");
                }
            }
        });

        let worker = Worker::new_async(
            &queue,
            |_conn, _job: Job<i32, i32, i32>| async move {
                Err::<i32, KioError>(std::io::Error::other("handler failed").into())
            },
            None,
        )?;
        worker.run()?;

        queue.add_job("boom", 1, None).await?;
        wait_until(|| !failed.is_empty(), "the failing job to be marked failed").await;

        worker.close();
        assert_eq!(failed.len(), 1);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn handler_panic_moves_job_to_failed() -> KioResult<()> {
        let queue = make_queue().await?;

        let failed: Arc<ArrayQueue<u64>> = Arc::new(ArrayQueue::new(4));
        let failed_sink = failed.clone();
        queue.on(JobState::Failed, move |state: EventParameters<i32, i32>| {
            let failed = failed_sink.clone();
            async move {
                if let EventParameters::Failed { job_id, .. } = state {
                    failed.push(job_id).expect("event sink capacity exceeded");
                }
            }
        });

        let worker = Worker::new_async(
            &queue,
            |_conn, _job: Job<i32, i32, i32>| async move {
                panic!("processor panicked");
                #[allow(unreachable_code)]
                Ok::<i32, KioError>(0)
            },
            None,
        )?;
        worker.run()?;

        queue.add_job("panic", 1, None).await?;
        wait_until(
            || !failed.is_empty(),
            "the panicking job to be caught and marked failed",
        )
        .await;

        worker.close();
        assert_eq!(
            failed.len(),
            1,
            "a handler panic must not crash the worker; the job fails"
        );
        assert!(worker.closed());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn clone_shares_lifecycle_state_with_the_original() -> KioResult<()> {
        let queue = make_queue().await?;
        let worker = doubling_worker(&queue, None)?;
        let clone = worker.clone();

        assert_eq!(worker.id, clone.id);
        assert!(!clone.is_running());

        worker.run()?;
        assert!(
            clone.is_running(),
            "clone observes the shared running state"
        );

        clone.close();
        assert!(worker.closed(), "close through a clone closes the original");
        assert!(!worker.is_running());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn state_and_running_flags_track_the_full_lifecycle() -> KioResult<()> {
        let queue = make_queue().await?;
        let worker = doubling_worker(&queue, None)?;

        assert_eq!(worker.state.load(), WorkerState::Idle);
        worker.run()?;
        assert_eq!(worker.state.load(), WorkerState::Active);
        worker.close();
        wait_until(
            || worker.state.load() == WorkerState::Closed,
            "worker state to become Closed",
        )
        .await;
        assert!(worker.closed());
        assert!(!worker.is_running());
        Ok(())
    }

    #[test]
    fn worker_state_variants_and_default() {
        assert_eq!(WorkerState::default(), WorkerState::Idle);
        assert!(WorkerState::Active.is_active());
        assert!(WorkerState::Idle.is_idle());
        assert!(WorkerState::Closed.is_closed());
        assert!(!WorkerState::Idle.is_active());
    }

    #[cfg(feature = "redis-store")]
    #[test]
    fn worker_state_serde_round_trip() {
        for state in [WorkerState::Active, WorkerState::Idle, WorkerState::Closed] {
            let mut bytes = simd_json::to_vec(&state).expect("state must serialise");
            let restored: WorkerState =
                simd_json::from_slice(&mut bytes).expect("state must deserialise");
            assert_eq!(restored, state);
        }
    }
}
