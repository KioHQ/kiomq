use crate::{queue, timer::Timer, Job, KioError, KioResult, Queue};

use dashmap::DashMap;
use deadpool_redis::Pool;
use derive_more::Debug;
use futures::{
    future::{BoxFuture, Future, FutureExt, TryFutureExt},
    stream::FuturesUnordered,
};
use redis::aio::ConnectionLike;
use serde::de::DeserializeOwned;
use std::sync::{atomic::AtomicBool, Arc, Mutex};
use uuid::Uuid;
mod worker_opts;
use crate::error::WorkerError;
use tokio_util::sync::CancellationToken;

pub use worker_opts::WorkerOpts;
#[derive(Clone, Debug)]
pub struct Worker<D, R, P> {
    id: Uuid,
    queue: Arc<Queue<D, R, P>>,
    jobs_in_progress: DashMap<u64, (Job<D, R, P>, String)>,
    #[debug(skip)]
    processor: Arc<WorkerCallback<D, R, P>>,
    pub opts: WorkerOpts,
    cancellation_token: CancellationToken,
    active: Arc<AtomicBool>,
    processing: Arc<Mutex<FuturesUnordered<KioResult<()>>>>,
    stalled_check_timer: Timer,
    extend_lock_timer: Timer,
}
use deadpool_redis::Connection;
pub(crate) type WorkerCallback<D, R, P> =
    dyn Fn(Connection, Job<D, R, P>) -> BoxFuture<'static, KioResult<R>> + Send;

impl<D: Clone + DeserializeOwned, R: Clone + DeserializeOwned, P: Clone + DeserializeOwned>
    Worker<D, R, P>
{
    pub fn new<C, F>(queue: &Queue<D, R, P>, processor: C, worker_opts: Option<WorkerOpts>) -> Self
    where
        C: Fn(Connection, Job<D, R, P>) -> F + Send + 'static,
        F: Future<Output = Result<R, Box<dyn std::error::Error + Send>>> + Send + 'static,
        P: Send + Sync + 'static,
        R: Send + Sync + 'static,
        D: Send + Sync + 'static,
    {
        let queue = Arc::new(queue.clone());
        let pool = queue.conn_pool.clone();
        let jobs_in_progress: DashMap<u64, (Job<D, R, P>, String)> = DashMap::new();
        let callback = move |conn: Connection, job: Job<D, R, P>| {
            let fut = processor(conn, job);
            fut.map_err(|e| e.into()).boxed()
        };
        let id = Uuid::new_v4();
        let opts = worker_opts.unwrap_or_default();
        let queue_clone = queue.clone();

        let jobs = jobs_in_progress.clone();

        let opts_clone = opts.clone();
        let extend_lock_timer = Timer::new(opts.lock_duration, move || {
            let queue = queue_clone.clone();
            let jobs = jobs.clone();
            let opts = opts_clone.clone();
            async move {
                for pair in jobs.iter() {
                    let (job, token) = pair.value();
                    if let Some(id) = job.id.as_ref() {
                        let _ = queue.extend_lock(id, opts.lock_duration, token).await;
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
                    dbg!(failed, stalled);
                }
            }
        });

        Self {
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
    async fn main_loop(&self) {

    }
}
