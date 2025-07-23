use crate::{queue, Job, KioError, KioResult, Queue};

use dashmap::DashMap;
use deadpool_redis::Pool;
use futures::{
    future::{BoxFuture, Future, FutureExt, TryFutureExt},
    stream::FuturesUnordered,
};
use redis::aio::ConnectionLike;
use std::{process::Output, sync::Arc};
#[derive(Clone)]
pub struct Worker<D, R, P> {
    queue: Arc<Queue>,
    jobs: DashMap<u64, Job<D, R, P>>,
    processor: Arc<WorkerCallback<D, R, P>>,
    running_queue: Arc<FuturesUnordered<Result<(), KioError>>>,
}
use deadpool_redis::Connection;
pub(crate) type WorkerCallback<D, R, P> =
    dyn Fn(Connection, Job<D, R, P>) -> BoxFuture<'static, Result<R, KioError>> + Send;

impl<D, R, P> Worker<D, R, P> {
    pub fn new<C, F>(queue: &Queue, procesor: C) -> Self
    where
        C: Fn(Connection, Job<D, R, P>) -> F + Send + 'static,
        F: Future<Output = Result<R, Box<dyn std::error::Error + Send>>> + Send + 'static,
    {
        let queue = Arc::new(queue.clone());
        let pool = queue.conn_pool.clone();
        let jobs = DashMap::new();
        let callback = move |conn: Connection, job: Job<D, R, P>| {
            let fut = procesor(conn, job);
            fut.map_err(|e| e.into()).boxed()
        };

        Self {
            queue,
            jobs,
            processor: Arc::new(callback),
            running_queue: Arc::default(),
        }
    }

    async fn run(&self) -> KioResult<()> {
        // finished waiting jobs;
        //self.queue.
        Ok(())
    }
}
