use crate::{
    error::{BacktraceCatcher, CaughtError, CaughtPanicInfo},
    job, queue,
    stores::Store,
    timers::DelayQueueTimer,
    utils::processor_types::SharedStore,
    worker::processor_types::SyncFn,
    Job, JobOptions, JobState, JobToken, KioError, KioResult, Queue,
};

use crate::utils::{get_next_job, main_loop};
use atomic_refcell::AtomicRefCell;
use chrono::Utc;
use derive_more::Debug;
use futures::future::{BoxFuture, Future, FutureExt, Shared, TryFutureExt};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize},
        Arc,
    },
    time::Duration,
};
use uuid::Uuid;
mod metrics;
mod worker_opts;
pub use metrics::*;

use crate::error::WorkerError;
use crate::events::{EventEmitter, EventParameters};
use crossbeam_skiplist::SkipMap;
use tokio::{
    sync::Notify,
    task::{AbortHandle, JoinHandle},
};
use tokio_metrics::TaskMonitor;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
type JobMeta<D, R, P> = (Job<D, R, P>, JobToken, TaskHandle, TaskMonitor);
pub(crate) type JobMap<D, R, P> = Arc<SkipMap<u64, JobMeta<D, R, P>>>;
type Task = JoinHandle<KioResult<()>>;
pub(crate) type TaskHandle = AtomicRefCell<Option<Task>>;
pub(crate) type SharedTaskHandle = Arc<TaskHandle>;
use tokio::task::Id;
pub(crate) type ProcessingQueue = TaskTracker;
use atomig::{Atom, Atomic};
use derive_more::IsVariant;
pub use worker_opts::WorkerOpts;
#[derive(Atom, IsVariant, Default, Debug)]
#[repr(u8)]
pub enum WorkerState {
    Active,
    #[default]
    Idle,
    Closed,
}
#[cfg(feature = "tracing")]
use tracing::{debug, instrument, trace, warn, Instrument, Span};

pub(crate) use worker_opts::MIN_DELAY_MS_LIMIT;
#[derive(Clone, Debug)]
pub struct Worker<D, R, P, S> {
    pub id: Uuid,
    #[cfg(feature = "tracing")]
    resource_span: Span,
    queue: Arc<Queue<D, R, P, S>>,
    jobs_in_progress: JobMap<D, R, P>,
    #[debug(skip)]
    processor: WorkerCallback<D, R, P, S>,
    pub opts: WorkerOpts,
    cancellation_token: CancellationToken,
    pub state: Arc<Atomic<WorkerState>>,
    processing: ProcessingQueue,
    timers: DelayQueueTimer<D, R, P, S>,
    block_until: Arc<AtomicU64>,
    mini_block_timout: u64,
    active_job_count: Arc<AtomicUsize>,
    continue_notifier: Arc<Notify>,
    main_task: SharedTaskHandle,
    timers_task: SharedTaskHandle,
}
use crate::utils::processor_types;
use processor_types::Callback;
pub(crate) type WorkerCallback<D, R, P, S> = Callback<D, R, P, S>;

impl<
        D: Clone + DeserializeOwned + 'static + Send + Sync + Serialize,
        R: Clone + DeserializeOwned + 'static + Serialize + Send + Sync,
        P: Clone + DeserializeOwned + 'static + Send + Sync + Serialize,
        S: Clone + Store<D, R, P> + Send + 'static + Sync,
    > Worker<D, R, P, S>
{
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
        let callback_type = match &callback {
            Callback::Async(_) => "Async",
            Callback::Sync(_) => "Sync",
        };

        let id = Uuid::new_v4();
        let mut opts = worker_opts.unwrap_or_default();
        let queue_clone = queue.clone();

        let jobs = jobs_in_progress.clone();
        let now = tokio::time::Instant::now();
        let queue_clone = queue.clone();

        let timers = DelayQueueTimer::new(jobs.clone(), id, opts, queue.clone());
        let continue_notifier = queue.worker_notifier.clone();

        #[cfg(feature = "tracing")]
        let resource_span = {
            let location = std::panic::Location::caller();
            let queue_name = queue.name();
            let worker_type = format!(
                "{}-Worker({},{queue_name})",
                callback_type,
                id.as_u64_pair().0,
            );
            tracing::info_span!(parent:None, "",worker_type)
        };
        let main_task = Arc::default();
        let timers_task = Arc::default();
        let worker = Self {
            timers_task,
            main_task,
            #[cfg(feature = "tracing")]
            resource_span,
            timers,
            continue_notifier,
            block_until: Arc::default(),
            opts,
            state: Arc::default(),
            id,
            queue,
            jobs_in_progress,
            processor: callback,
            cancellation_token: CancellationToken::new(),
            processing: TaskTracker::new(),
            mini_block_timout: 10000, // 10s
            active_job_count: Arc::default(),
        };
        if worker.opts.autorun {
            worker.run()?;
        }

        Ok(worker)
    }

    pub fn is_running(&self) -> bool {
        self.state
            .load(std::sync::atomic::Ordering::Acquire)
            .is_active()
            && !self.cancellation_token.is_cancelled()
    }
    pub fn is_idle(&self) -> bool {
        self.state
            .load(std::sync::atomic::Ordering::Acquire)
            .is_idle()
    }
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
            self.opts,
            self.block_until.clone(),
            self.jobs_in_progress.clone(),
            self.active_job_count.clone(),
            self.processor.clone(),
            self.queue.clone(),
            self.state.clone(),
            self.continue_notifier.clone(),
            self.timers.clone(),
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
            self.timers.clone(),
        );
        #[cfg(feature = "tracing")]
        let main = main_loop(params).instrument(self.resource_span.clone());
        #[cfg(not(feature = "tracing"))]
        let main = main_loop(params);
        let main_task = tokio::spawn(main.boxed());
        self.main_task.borrow_mut().replace(main_task);
        Ok(())
    }
    pub fn closed(&self) -> bool {
        self.cancellation_token.is_cancelled()
            || self
                .state
                .load(std::sync::atomic::Ordering::Acquire)
                .is_closed()
    }

    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(self)))]
    /// Stops the worker from running (adding more jobs to run)
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

        self.timers.close();
        self.queue.resume_workers();
        self.queue.worker_notifier.notify_waiters();
        self.queue
            .pause_workers
            .store(false, std::sync::atomic::Ordering::Release);
        self.cancellation_token.cancel();
        // TODO: work on gracefully shutdown later; currently we just cancel the token but
        // don't check whether the main_task has closed too. Handle this later
        let mut main_task = self.main_task.borrow_mut();
        if let (Some(handle)) = main_task.take() {
            // wait for handle to finishd
            let running_tasks = self.processing.len();
            #[cfg(feature = "tracing")]
            warn!("waiting for all {running_tasks} tasks to complete or abort");
            // wait for the main loop to close
            while !handle.is_finished() {}
        }
    }

    pub fn on<F, C>(&self, event: JobState, callback: C) -> Uuid
    where
        C: Fn(EventParameters<R, P>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        self.queue.on(event, callback)
    }
    pub fn on_all_events<F, C>(&self, callback: C) -> Uuid
    where
        C: Fn(EventParameters<R, P>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        self.queue.on_all_events(callback)
    }
    pub fn remove_event_listener(&self, id: Uuid) -> Option<Uuid> {
        self.queue.remove_event_listener(id)
    }
}
