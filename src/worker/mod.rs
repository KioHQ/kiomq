use crate::{queue, Job, KioError, KioResult, Queue};

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
    jobs_in_progress: DashMap<u64, Job<D, R, P>>,
    #[debug(skip)]
    processor: Arc<WorkerCallback<D, R, P>>,
    pub opts: WorkerOpts,
    cancellation_token: CancellationToken,
    active: Arc<AtomicBool>,
    processing: Arc<Mutex<FuturesUnordered<KioResult<()>>>>,
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
    {
        let queue = Arc::new(queue.clone());
        let pool = queue.conn_pool.clone();
        let jobs_in_progress = DashMap::new();
        let callback = move |conn: Connection, job: Job<D, R, P>| {
            let fut = processor(conn, job);
            fut.map_err(|e| e.into()).boxed()
        };
        let id = Uuid::new_v4();
        let opts = worker_opts.unwrap_or_default();

        Self {
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
}
