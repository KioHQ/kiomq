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
mod worker_opts;
use crate::error::WorkerError;
use crate::events::{EventEmitter, EventParameters};
use crossbeam_skiplist::SkipMap;
use tokio::{
    sync::Notify,
    task::{AbortHandle, JoinHandle},
};
use tokio_util::sync::CancellationToken;
pub(crate) type JobMeta<D, R, P> = (Job<D, R, P>, JobToken, AtomicU64);
pub(crate) type JobMap<D, R, P> = Arc<SkipMap<u64, JobMeta<D, R, P>>>;
type Task = JoinHandle<KioResult<()>>;
use tokio::task::Id;
pub(crate) type ProcessingQueue = Arc<SkipMap<u64, Task>>;
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
pub(crate) use worker_opts::MIN_DELAY_MS_LIMIT;
#[derive(Clone, Debug)]
pub struct Worker<D, R, P, S> {
    pub id: Uuid,
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
}
use crate::utils::processor_types;
use deadpool_redis::Connection;
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

        let id = Uuid::new_v4();
        let mut opts = worker_opts.unwrap_or_default();
        let queue_clone = queue.clone();

        let jobs = jobs_in_progress.clone();
        let now = tokio::time::Instant::now();
        let queue_clone = queue.clone();

        let timers = DelayQueueTimer::new(jobs.clone(), opts.clone(), queue.clone());
        let continue_notifier = queue.worker_notifier.clone();
        let worker = Self {
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
            processing: Arc::default(),
            mini_block_timout: 10000, // 10s
            active_job_count: Arc::default(),
        };
        if worker.opts.autorun {
            worker.run()?;
        }

        Ok(worker)
    }

    pub fn is_running(&self) -> bool {
        (self
            .state
            .load(std::sync::atomic::Ordering::Acquire)
            .is_active()
            || self.is_idle())
            && !self.cancellation_token.is_cancelled()
    }
    pub fn is_idle(&self) -> bool {
        self.state
            .load(std::sync::atomic::Ordering::Acquire)
            .is_idle()
    }
    pub fn run(&self) -> KioResult<()> {
        if self.is_running() && !self.is_idle() {
            return Err(WorkerError::WorkerAlreadyRunningWithId(self.id).into());
        }
        let params = (
            self.id,
            self.cancellation_token.clone(),
            self.processing.clone(),
            self.opts.clone(),
            self.block_until.clone(),
            self.jobs_in_progress.clone(),
            self.active_job_count.clone(),
            self.processor.clone(),
            self.queue.clone(),
            self.state.clone(),
            self.continue_notifier.clone(),
            self.timers.clone(),
        );
        let main = main_loop(params);
        tokio::spawn(main.boxed());
        self.state
            .store(WorkerState::Active, std::sync::atomic::Ordering::Release);

        Ok(())
    }
    pub fn closed(&self) -> bool {
        self.cancellation_token.is_cancelled()
            || self
                .state
                .load(std::sync::atomic::Ordering::Acquire)
                .is_closed()
    }
    /// Stops the worker from running (adding more jobs to run)
    /// If true is passed as an argument, all actively running jobs are stopped too.
    pub fn close(&self, stop_active_jobs: bool) {
        if !self.is_running() {
            return;
        }
        self.cancellation_token.cancel();
        self.timers.close();

        self.state
            .store(WorkerState::Closed, std::sync::atomic::Ordering::Release);
        if stop_active_jobs {
            self.jobs_in_progress.iter().for_each(|pair| {
                let (job, _, current_handle) = pair.value();
                if let Some(entry) = self
                    .processing
                    .get(&current_handle.load(std::sync::atomic::Ordering::Acquire))
                {
                    entry.value().abort();
                }
            });

            self.jobs_in_progress.clear();
        }
    }

    pub fn on<F, C>(&self, event: JobState, callback: C) -> Uuid
    where
        C: Fn(EventParameters<D, R, P>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        self.queue.on(event, callback)
    }
    pub fn on_all_events<F, C>(&self, callback: C) -> Uuid
    where
        C: Fn(EventParameters<D, R, P>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        self.queue.on_all_events(callback)
    }
    pub fn remove_event_listener(&self, id: Uuid) -> Option<Uuid> {
        self.queue.remove_event_listener(id)
    }
}
