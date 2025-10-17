//pub(crate) type WorkerCallback<D, R, P> =
//    dyn Fn(Connection, Job<D, R, P>) -> BoxFuture<'static, KioResult<R>> + Send + 'static + Sync;
use futures::future::{BoxFuture, Future, FutureExt};

use crate::{worker::WorkerCallback, Job};
use deadpool_redis::Connection;
type SyncCallback<D, R, P, E> =
    dyn Fn(Connection, Job<D, R, P>) -> Result<R, E> + Send + Sync + 'static;
type AsyncCallback<D, R, P, E> =
    dyn Fn(Connection, Job<D, R, P>) -> BoxFuture<'static, Result<R, E>> + Send + Sync + 'static;
/// An enum representing both sync and async processors
pub(crate) enum Callback<D, R, P, E> {
    ASync(Box<AsyncCallback<D, R, P, E>>),
    Sync(Box<SyncCallback<D, R, P, E>>),
}
impl<T, D, R, P, E> From<T> for Callback<D, R, P, E>
where
    T: Fn(Connection, Job<D, R, P>) -> Result<R, E> + Send + Sync + 'static,
{
    fn from(value: T) -> Self {
        Self::Sync(Box::new(value))
    }
}
