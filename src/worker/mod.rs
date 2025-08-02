use crate::{
    error::{BacktraceCatcher, CaughtError, CaughtPanicInfo},
    queue,
    timer::Timer,
    Job, JobState, KioError, KioResult, Queue,
};

use chrono::Utc;
use dashmap::DashMap;
use deadpool_redis::Pool;
use derive_more::Debug;
use futures::{
    future::{BoxFuture, Future, FutureExt, TryFutureExt},
    stream::FuturesUnordered,
};
use futures::{lock::Mutex, stream::TryReadyChunksError};
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
use tokio::task::AbortHandle;
use tokio_util::sync::CancellationToken;
mod worker_events;
pub(crate) use worker_events::EventEmitter;
pub use worker_events::EventParameters;

use tokio::task::JoinSet;
type JobMap<D, R, P> = Arc<DashMap<String, (Job<D, R, P>, String, Option<AbortHandle>)>>;
type ProcessingQueue = Arc<Mutex<JoinSet<KioResult<()>>>>;
pub use worker_opts::WorkerOpts;
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
    ) -> Self
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
        let jobs_in_progress: JobMap<_, _, _> = Arc::new(DashMap::new());
        let callback = move |conn: Connection, job: Job<D, R, P>| {
            let fut = async_backtrace::frame!(processor(conn, job));
            fut.map_err(|e| e.into()).boxed()
        };
        let id = Uuid::new_v4();
        let mut opts = worker_opts.unwrap_or_default();
        opts.concurrency = 6;
        let queue_clone = queue.clone();

        let jobs = jobs_in_progress.clone();

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

        Self {
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
        }
    }

    fn is_running(&self) -> bool {
        self.active.load(std::sync::atomic::Ordering::Acquire)
            && !self.cancellation_token.is_cancelled()
    }
    pub async fn run(&self) -> KioResult<()> {
        if self.is_running() {
            return Err(WorkerError::WorkerAlreadyRunningWithId(self.id).into());
        }
        let handle = self.stalled_check_timer.run();
        self.main_loop().await;

        Ok(())
    }
    pub fn closed(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }
    pub fn close(&self) {
        self.stalled_check_timer.stop();
        self.extend_lock_timer.stop();
        self.cancellation_token.cancel();
        self.active
            .store(false, std::sync::atomic::Ordering::Release);
    }
    async fn main_loop(&self) -> KioResult<()> {
        self.extend_lock_timer.run();
        while !self.closed() {
            while !self.closed() && self.processing.lock().await.len() < self.opts.concurrency {
                let token_prefix = self
                    .active_job_count
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                let token = format!("{}:{token_prefix}", self.id);
                let block_delay = self.block_until.load(std::sync::atomic::Ordering::Acquire);
                if let Some(job) =
                    get_next_job(&self.queue, &token, block_delay, self.closed(), &self.opts)
                        .await?
                {
                    let id = job.id.clone().unwrap();
                    self.jobs_in_progress.insert(
                        job.id.clone().unwrap_or_default(),
                        (job.clone(), token.clone(), None),
                    );
                    let jobs_in_progress = self.jobs_in_progress.clone();
                    let queue = self.queue.clone();
                    let callback = self.processor.clone();

                    let task = async_backtrace::frame!(process_job(
                        job,
                        token,
                        jobs_in_progress,
                        queue,
                        callback
                    ));
                    let handle = self.processing.lock().await.spawn(task);
                    // task handle
                    if let Some(mut re) = self.jobs_in_progress.get_mut(&id) {
                        let (_, _, stored_handle) = re.value_mut();
                        stored_handle.replace(handle);
                    }
                }
            }

            if self.processing.lock().await.len() == self.opts.concurrency {
                // we wait for current jobs to complete before adding others;
                if let Some(done) = self.processing.lock().await.join_next().await {}
            }
        }
        Ok(())
    }
    pub async fn on<F, C>(&self, event: JobState, callback: C) -> String
    where
        C: Fn(EventParameters<D, R, P>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        self.queue.on(event, callback).await
    }
    pub async fn on_all_events<F, C>(&self, callback: C) -> String
    where
        C: Fn(EventParameters<D, R, P>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        self.queue.on_all_events(callback).await
    }
    pub fn remove_event_listener(&self, id: &str) -> Option<String> {
        self.queue.remove_event_listener(id)
    }
}

// ---- UTIL FUNCTIONS

#[async_backtrace::framed]
async fn process_job<D, R, P>(
    job: Job<D, R, P>,
    token: String,
    jobs_in_progress: JobMap<D, R, P>,
    queue: Arc<Queue<D, R, P>>,
    callback: Arc<WorkerCallback<D, R, P>>,
) -> KioResult<()>
where
    R: Serialize + Send + Clone + DeserializeOwned,
    D: Clone + Serialize + DeserializeOwned,
    P: Clone + Serialize + DeserializeOwned,
{
    use crate::JobState;
    let job_id = job.id.clone();
    let conn = queue.conn_pool.get().await?;
    let callback = async_backtrace::frame!(callback(conn, job));
    let returned = BacktraceCatcher::catch(callback).await;
    match returned {
        Ok(result) => {
            // move the job to failed state here;
            if let (Ok(result_str), Some(job_id)) =
                (serde_json::to_string(&result), job_id.as_ref())
            {
                let ts = Utc::now().timestamp_millis();
                let move_to_state = JobState::Completed;
                let completed = queue
                    .move_job_to_finished_or_failed(
                        job_id,
                        ts,
                        &token,
                        move_to_state,
                        &result_str,
                        None,
                    )
                    .await?;
                if let Some((_, (job, _, Some(handle)))) = jobs_in_progress.remove(job_id) {
                    //handle.abort(); // remove task from the queue
                    queue
                        .emit(
                            JobState::Completed,
                            EventParameters::Completed {
                                prev_state: Some(job.state),
                                job: completed,
                                result,
                            },
                        )
                        .await;
                }
                // Todo emit event here
                //
            }
        }
        Err(err) => {
            let (failed_reason, backtrace) = match err {
                CaughtError::Panic(CaughtPanicInfo {
                    backtrace,
                    payload,
                    location,
                }) => (payload, backtrace),
                CaughtError::Error(error, backtrace) => (error.to_string(), backtrace),
            };
            let backtrace: Option<Vec<String>> =
                backtrace.map(|trace| trace.iter().map(|loc| loc.to_string()).collect());

            let frames = backtrace.and_then(|frames| serde_json::to_string(&frames).ok());
            // move job to failed_state
            if let Some(job_id) = job_id.as_ref() {
                let ts = Utc::now().timestamp_millis();
                let move_to_state = JobState::Failed;
                let failed_job = queue
                    .move_job_to_finished_or_failed(
                        job_id,
                        ts,
                        &token,
                        move_to_state,
                        &serde_json::to_string(&failed_reason)?,
                        frames,
                    )
                    .await?;
                if let Some((_, (job, _, Some(handle)))) = jobs_in_progress.remove(job_id) {
                    queue
                        .emit(
                            JobState::Failed,
                            EventParameters::Failed {
                                prev_state: job.state,
                                job: failed_job,
                                error: failed_reason,
                            },
                        )
                        .await
                }
            }
        }
    }
    Ok(())
}
async fn get_next_job<D, R, P>(
    queue: &Queue<D, R, P>,
    token: &str,
    block_delay: u64,
    closed: bool,
    opts: &WorkerOpts,
) -> KioResult<Option<Job<D, R, P>>>
where
    D: DeserializeOwned + Clone + Serialize,
    R: DeserializeOwned + Clone + Serialize,
    P: DeserializeOwned + Clone + Serialize,
{
    // handle pausing or closing;
    if closed {
        return Ok(None);
    }
    //let waiting = queue.wait_for_job(block_delay as i64).await?;
    queue.move_to_active(token, opts).await
}
