use crate::KioError;
use futures::{
    future::{BoxFuture, Future, FutureExt},
    TryFutureExt,
};
use std::marker::PhantomData;

use crate::{worker::WorkerCallback, Job, KioResult};
use deadpool_redis::Connection as AsyncConnection;
use redis::Connection;
use std::sync::Arc;
type SyncCallback<D, R, P> =
    dyn Fn(Connection, Job<D, R, P>) -> KioResult<R> + Send + Sync + 'static;
type AsyncCallback<D, R, P> = dyn Fn(AsyncConnection, Job<D, R, P>) -> BoxFuture<'static, KioResult<R>>
    + Send
    + Sync
    + 'static;

/// An enum representing both sync and async processors
#[derive(Clone)]
pub(crate) enum Callback<D, R, P> {
    Async(Arc<AsyncCallback<D, R, P>>),
    Sync(Arc<SyncCallback<D, R, P>>),
}
pub(crate) struct SyncFn<F, D, R, P, E>(pub F, PhantomData<(D, R, P, E)>);
pub(crate) struct AsyncFn<F, D, R, P, E>(pub F, PhantomData<(D, R, P, E)>);

impl<F, D, R, P, E> From<SyncFn<F, D, R, P, E>> for Callback<D, R, P>
where
    F: Fn(Connection, Job<D, R, P>) -> Result<R, E> + Send + Sync + 'static,
    KioError: From<E>,
    E: std::error::Error + Send + 'static,
{
    fn from(SyncFn(f, _): SyncFn<F, D, R, P, E>) -> Self {
        let callback = move |con: Connection, job: Job<_, _, _>| f(con, job).map_err(|e| e.into());
        Self::Sync(Arc::new(callback))
    }
}

impl<F, Fut, D, R, P, E> From<AsyncFn<F, D, R, P, E>> for Callback<D, R, P>
where
    F: Fn(AsyncConnection, Job<D, R, P>) -> Fut + Send + Sync + 'static,
    KioError: From<E>,
    Fut: Future<Output = Result<R, E>> + Send + 'static,
    E: std::error::Error + Send + 'static,
{
    fn from(AsyncFn(f, _): AsyncFn<F, D, R, P, E>) -> Self {
        let callback = move |conn: AsyncConnection, job: Job<D, R, P>| {
            let fut = async_backtrace::frame!(f(conn, job));
            fut.map_err(|e| e.into()).boxed()
        };
        Self::Async(Arc::new(callback))
    }
}
impl<F, D, R, P, E> From<F> for SyncFn<F, D, R, P, E>
where
    F: Fn(Connection, Job<D, R, P>) -> Result<R, E> + Send + Sync + 'static,
{
    fn from(value: F) -> Self {
        Self(value, PhantomData)
    }
}
impl<F, Fut, D, R, P, E> From<F> for AsyncFn<F, D, R, P, E>
where
    F: Fn(AsyncConnection, Job<D, R, P>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, E>> + Send + 'static,
{
    fn from(value: F) -> Self {
        Self(value, PhantomData)
    }
}
