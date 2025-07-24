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
use std::sync::Arc;
use uuid::Uuid;
mod worker_opts;
pub use worker_opts::WorkerOpts;
#[derive(Clone, Debug)]
pub struct Worker<D, R, P> {
    id: Uuid,
    queue: Arc<Queue<D, R, P>>,
    jobs: DashMap<u64, Job<D, R, P>>,
    #[debug(skip)]
    processor: Arc<WorkerCallback<D, R, P>>,
    opts: WorkerOpts,
    processing: Arc<FuturesUnordered<KioResult<()>>>,
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
        let jobs = DashMap::new();
        let callback = move |conn: Connection, job: Job<D, R, P>| {
            let fut = processor(conn, job);
            fut.map_err(|e| e.into()).boxed()
        };
        let id = Uuid::new_v4();
        let opts = worker_opts.unwrap_or_default();

        Self {
            opts,
            id,
            queue,
            jobs,
            processor: Arc::new(callback),
            processing: Arc::default(),
        }
    }

    pub async fn run(&self) -> KioResult<()> {
        // populate our jobs with waiting jobs;
        self.queue
            .fetch_waiting_jobs()
            .await?
            .into_iter()
            .for_each(|job| {
                if let Some(job_id) = job.id {
                    if !self.jobs.contains_key(&job_id) {
                        self.jobs.insert(job_id, job);
                    }
                }
            });

        Ok(())
    }
}
