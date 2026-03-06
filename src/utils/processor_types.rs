use crate::KioError;
use futures::{
    future::{BoxFuture, Future, FutureExt},
    TryFutureExt,
};
use std::marker::PhantomData;

use crate::{Job, KioResult};
use std::sync::Arc;
pub type SharedStore<S> = Arc<S>;
type SyncCallback<D, R, P, S> =
    dyn Fn(SharedStore<S>, Job<D, R, P>) -> KioResult<R> + Send + Sync + 'static;
type AsyncCallback<D, R, P, S> = dyn Fn(SharedStore<S>, Job<D, R, P>) -> BoxFuture<'static, KioResult<R>>
    + Send
    + Sync
    + 'static;

/// An enum representing both sync and async processors
#[derive(Clone)]
pub enum Callback<D, R, P, S> {
    Async(Arc<AsyncCallback<D, R, P, S>>),
    Sync(Arc<SyncCallback<D, R, P, S>>),
}
pub struct SyncFn<F, D, R, P, S, E>(pub F, PhantomData<(D, R, P, S, E)>);
pub struct AsyncFn<F, D, R, P, S, E>(pub F, PhantomData<(D, R, P, S, E)>);

impl<F, D, R, P, S, E> From<SyncFn<F, D, R, P, S, E>> for Callback<D, R, P, S>
where
    F: Fn(SharedStore<S>, Job<D, R, P>) -> Result<R, E> + Send + Sync + 'static,
    KioError: From<E>,
    E: std::error::Error + Send + 'static,
{
    fn from(SyncFn(f, _): SyncFn<F, D, R, P, S, E>) -> Self {
        let callback = move |store: SharedStore<S>, job: Job<_, _, _>| {
            f(store, job).map_err(std::convert::Into::into)
        };
        Self::Sync(Arc::new(callback))
    }
}

impl<F, Fut, D, R, P, S, E> From<AsyncFn<F, D, R, P, S, E>> for Callback<D, R, P, S>
where
    F: Fn(SharedStore<S>, Job<D, R, P>) -> Fut + Send + Sync + 'static,
    KioError: From<E>,
    Fut: Future<Output = Result<R, E>> + Send + 'static,
    E: std::error::Error + Send + 'static,
{
    fn from(AsyncFn(f, _): AsyncFn<F, D, R, P, S, E>) -> Self {
        let callback = move |store: SharedStore<S>, job: Job<D, R, P>| {
            let fut = async_backtrace::frame!(f(store, job));
            fut.map_err(std::convert::Into::into).boxed()
        };
        Self::Async(Arc::new(callback))
    }
}
impl<F, D, R, P, S, E> From<F> for SyncFn<F, D, R, P, S, E>
where
    F: Fn(SharedStore<S>, Job<D, R, P>) -> Result<R, E> + Send + Sync + 'static,
{
    fn from(value: F) -> Self {
        Self(value, PhantomData)
    }
}
impl<F, Fut, D, R, P, S, E> From<F> for AsyncFn<F, D, R, P, S, E>
where
    F: Fn(SharedStore<S>, Job<D, R, P>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, E>> + Send + 'static,
{
    fn from(value: F) -> Self {
        Self(value, PhantomData)
    }
}
