use crate::{
    error::{BacktraceCatcher, CaughtError, CaughtPanicInfo},
    job, queue,
    timer::Timer,
    Job, JobState, KioError, KioResult, Queue,
};

use crate::utils::{get_next_job, main_loop};
use chrono::Utc;
use deadpool_redis::Pool;
use derive_more::Debug;
use futures::future::{BoxFuture, Future, FutureExt, TryFutureExt};
use redis::aio::ConnectionLike;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::json;
use std::{
    fmt::format,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize},
        Arc,
    },
    thread::sleep,
    time::Duration,
};
use uuid::Uuid;
mod worker_opts;
use crate::error::WorkerError;
use crossbeam_skiplist::SkipMap;
use tokio::task::{AbortHandle, JoinHandle};
use tokio_util::sync::CancellationToken;
mod worker_events;
use crate::events::{EventEmitter, EventParameters};
type JobMeta<D, R, P> = (Job<D, R, P>, String, AtomicU64);
pub(crate) type JobMap<D, R, P> = Arc<SkipMap<String, JobMeta<D, R, P>>>;
type Task = JoinHandle<KioResult<()>>;
use tokio::task::Id;
pub(crate) type ProcessingQueue = Arc<SkipMap<u64, Task>>;
pub use worker_opts::WorkerOpts;
pub(crate) use worker_opts::MIN_DELAY_MS_LIMIT;
#[derive(Clone, Debug)]
pub struct Worker<D, R, P> {
    id: Uuid,
    queue: Arc<Queue<D, R, P>>,
    jobs_in_progress: JobMap<D, R, P>,
    #[debug(skip)]
    processor: Arc<WorkerCallback<D, R, P>>,
    pub opts: WorkerOpts,
    pub cancellation_token: CancellationToken,
    active: Arc<AtomicBool>,
    processing: ProcessingQueue,
    stalled_check_timer: Timer,
    extend_lock_timer: Timer,
    block_until: Arc<AtomicU64>,
    mini_block_timout: u64,
    active_job_count: Arc<AtomicUsize>,
}
use deadpool_redis::Connection;
pub(crate) type WorkerCallback<D, R, P> =
    dyn Fn(Connection, Job<D, R, P>) -> BoxFuture<'static, KioResult<R>> + Send + 'static + Sync;

impl<
        D: Clone + DeserializeOwned + 'static + Send + Sync + Serialize,
        R: Clone + DeserializeOwned + 'static + Serialize + Send + Sync,
        P: Clone + DeserializeOwned + 'static + Send + Sync + Serialize,
    > Worker<D, R, P>
{
    pub fn new<C, F, E>(
        queue: &Queue<D, R, P>,
        processor: C,
        worker_opts: Option<WorkerOpts>,
    ) -> KioResult<Self>
    where
        KioError: From<E>,
        C: Fn(Connection, Job<D, R, P>) -> F + Send + 'static + Sync,
        F: Future<Output = Result<R, E>> + Send + 'static,
        P: Send + Sync + 'static,
        R: Send + Sync + 'static,
        D: Send + Sync + 'static,
        E: std::error::Error + Send + 'static,
    {
        let queue = Arc::new(queue.clone());
        let pool = queue.conn_pool.clone();
        let jobs_in_progress: JobMap<_, _, _> = Arc::new(SkipMap::new());
        let callback = move |conn: Connection, job: Job<D, R, P>| {
            let fut = async_backtrace::frame!(processor(conn, job));
            fut.map_err(|e| e.into()).boxed()
        };
        let id = Uuid::new_v4();
        let mut opts = worker_opts.unwrap_or_default();
        let queue_clone = queue.clone();

        let jobs = jobs_in_progress.clone();
        let now = tokio::time::Instant::now();
        let queue_clone = queue.clone();

        let opts_clone = opts.clone();
        let extend_lock_timer = Timer::new(opts.lock_duration, move || {
            let queue = queue_clone.clone();
            let jobs = jobs.clone();
            let opts = opts_clone.clone();
            async move {
                for pair in jobs.iter() {
                    let (job, token, handle) = pair.value();

                    if let Some(id) = job.id.as_ref() {
                        let done = queue.extend_lock(id, opts.lock_duration, token).await;
                    }
                }
            }
        });
        let opts_clone = opts.clone();
        let queue_clone = queue.clone();
        let jobs = jobs_in_progress.clone();
        let stalled_check_timer = Timer::new(opts.stalled_interval, move || {
            let queue = queue_clone.clone();
            let jobs = jobs.clone();
            let opts = opts_clone.clone();
            async move {
                if let Ok((failed, stalled)) = queue.make_stalled_jobs_wait(&opts).await {
                    // do something with results
                    //dbg!(failed, stalled);
                }
            }
        });

        let worker = Self {
            block_until: Arc::default(),
            stalled_check_timer,
            extend_lock_timer,
            opts,
            active: Arc::default(),
            id,
            queue,
            jobs_in_progress,
            processor: Arc::new(callback),
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
        self.active.load(std::sync::atomic::Ordering::Acquire)
            && !self.cancellation_token.is_cancelled()
    }
    pub fn run(&self) -> KioResult<()> {
        if self.is_running() {
            return Err(WorkerError::WorkerAlreadyRunningWithId(self.id).into());
        }
        let handle = self.stalled_check_timer.run();
        self.extend_lock_timer.run();
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
            self.active.clone(),
        );
        let main = main_loop(params);
        tokio::spawn(main.boxed());
        self.active
            .store(true, std::sync::atomic::Ordering::Release);

        Ok(())
    }
    pub fn closed(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }
    /// Stops the worker from running (adding more jobs to run)
    /// If true is passed as an argument, all actively running jobs are stopped too.
    pub fn close(&self, stop_active_jobs: bool) {
        if !self.is_running() {
            return;
        }
        self.stalled_check_timer.stop();
        self.extend_lock_timer.stop();
        self.cancellation_token.cancel();

        self.active
            .store(false, std::sync::atomic::Ordering::Release);
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

    pub async fn on<F, C>(&self, event: JobState, callback: C) -> Uuid
    where
        C: Fn(EventParameters<D, R, P>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        self.queue.on(event, callback).await
    }
    pub async fn on_all_events<F, C>(&self, callback: C) -> Uuid
    where
        C: Fn(EventParameters<D, R, P>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        self.queue.on_all_events(callback).await
    }
    pub fn remove_event_listener(&self, id: Uuid) -> Option<Uuid> {
        self.queue.remove_event_listener(id)
    }
}
