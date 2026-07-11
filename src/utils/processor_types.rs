use crate::KioError;
use futures::{
    TryFutureExt,
    future::{BoxFuture, Future, FutureExt},
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

#[cfg(test)]
mod callback_conversion_tests {
    #![allow(clippy::cast_possible_truncation)]
    use super::{AsyncFn, Callback, SharedStore, SyncFn};
    use crate::{Job, JobError, KioError};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Concrete monomorphisation used throughout: data `u64`, return `u64`,
    // progress `()`, and a trivial unit store so no backing store is required.
    type TestStore = ();
    type TestCallback = Callback<u64, u64, (), TestStore>;

    /// Builds a `Job` carrying the supplied input payload.
    fn job_with_data(data: u64) -> Job<u64, u64, ()> {
        debug_assert!(
            Job::<u64, u64, ()>::default().id.is_none(),
            "a default job must not carry an id before insertion"
        );
        Job {
            data: Some(data),
            ..Default::default()
        }
    }

    #[test]
    fn sync_fn_converts_to_sync_variant() {
        let cb: TestCallback = SyncFn::from(|_s: SharedStore<TestStore>, _j: Job<u64, u64, ()>| {
            Ok::<u64, JobError>(1)
        })
        .into();
        assert!(
            matches!(cb, Callback::Sync(_)),
            "SyncFn must convert into the Sync callback variant"
        );
    }

    #[test]
    fn async_fn_converts_to_async_variant() {
        let cb: TestCallback =
            AsyncFn::from(|_s: SharedStore<TestStore>, _j: Job<u64, u64, ()>| async {
                Ok::<u64, JobError>(1)
            })
            .into();
        assert!(
            matches!(cb, Callback::Async(_)),
            "AsyncFn must convert into the Async callback variant"
        );
    }

    #[test]
    fn sync_callback_returns_processor_result() {
        let cb: TestCallback = SyncFn::from(|_s: SharedStore<TestStore>, j: Job<u64, u64, ()>| {
            Ok::<u64, JobError>(j.data.unwrap_or_default() * 2)
        })
        .into();
        let Callback::Sync(f) = cb else {
            panic!("expected a Sync callback");
        };
        let result = f(Arc::new(()), job_with_data(21));
        let value = result.expect("sync processor must succeed");
        assert_eq!(value, 42, "sync callback must receive the job payload");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn async_callback_returns_processor_result() {
        let cb: TestCallback = AsyncFn::from(
            |_s: SharedStore<TestStore>, j: Job<u64, u64, ()>| async move {
                Ok::<u64, JobError>(j.data.unwrap_or_default() + 100)
            },
        )
        .into();
        let Callback::Async(f) = cb else {
            panic!("expected an Async callback");
        };
        let value = f(Arc::new(()), job_with_data(5))
            .await
            .expect("async processor must succeed");
        assert_eq!(value, 105, "async callback must receive the job payload");
    }

    #[test]
    fn sync_callback_maps_error_into_kioerror() {
        let cb: TestCallback = SyncFn::from(|_s: SharedStore<TestStore>, _j: Job<u64, u64, ()>| {
            Err::<u64, JobError>(JobError::JobNotFound)
        })
        .into();
        let Callback::Sync(f) = cb else {
            panic!("expected a Sync callback");
        };
        let err =
            f(Arc::new(()), job_with_data(0)).expect_err("callback must propagate the failure");
        assert!(
            matches!(err, KioError::JobError(JobError::JobNotFound)),
            "the domain error must be mapped into KioError, got {err:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn async_callback_maps_error_into_kioerror() {
        let cb: TestCallback =
            AsyncFn::from(|_s: SharedStore<TestStore>, _j: Job<u64, u64, ()>| async {
                Err::<u64, JobError>(JobError::JobLockMismatch)
            })
            .into();
        let Callback::Async(f) = cb else {
            panic!("expected an Async callback");
        };
        let err = f(Arc::new(()), job_with_data(0))
            .await
            .expect_err("async callback must propagate the failure");
        assert!(
            matches!(err, KioError::JobError(JobError::JobLockMismatch)),
            "the domain error must be mapped into KioError, got {err:?}"
        );
    }

    #[test]
    fn cloning_a_callback_shares_the_same_processor() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_in_closure = Arc::clone(&counter);
        let cb: TestCallback =
            SyncFn::from(move |_s: SharedStore<TestStore>, _j: Job<u64, u64, ()>| {
                counter_in_closure.fetch_add(1, Ordering::SeqCst);
                Ok::<u64, JobError>(0)
            })
            .into();
        let clone = cb.clone();

        for handle in [cb, clone] {
            let Callback::Sync(f) = handle else {
                panic!("expected a Sync callback");
            };
            f(Arc::new(()), job_with_data(0)).expect("processor must succeed");
        }
        assert_eq!(
            counter.load(Ordering::SeqCst),
            2,
            "a cloned callback must invoke the very same shared closure"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn shared_async_callback_runs_every_concurrent_invocation() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_in_closure = Arc::clone(&counter);
        let cb: TestCallback =
            AsyncFn::from(move |_s: SharedStore<TestStore>, j: Job<u64, u64, ()>| {
                let counter = Arc::clone(&counter_in_closure);
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok::<u64, JobError>(j.data.unwrap_or_default())
                }
            })
            .into();
        let Callback::Async(shared) = cb else {
            panic!("expected an Async callback");
        };

        let invocations = 200u64;
        let mut handles = Vec::new();
        for i in 0..invocations {
            let f = Arc::clone(&shared);
            handles.push(tokio::spawn(async move {
                f(Arc::new(()), job_with_data(i))
                    .await
                    .expect("async processor must succeed")
            }));
        }
        let mut sum = 0u64;
        for handle in handles {
            sum += handle.await.expect("invocation task must not panic");
        }

        assert_eq!(
            counter.load(Ordering::SeqCst),
            invocations as usize,
            "every concurrent invocation of the shared callback must run"
        );
        assert_eq!(
            sum,
            (0..invocations).sum::<u64>(),
            "each invocation must return its own job payload"
        );
    }
}
