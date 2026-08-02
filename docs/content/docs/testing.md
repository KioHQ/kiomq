---
title: "Testing"
linkTitle: "Testing"
group: "Project"
weight: 120
lead: "Run the suite with cargo-nextest, and write tests against your own queues without touching Redis."
---

## Running the suite

Use [`cargo nextest`](https://nexte.st/):

```bash
cargo nextest run
```

> [!WARNING]
> Use `cargo nextest run`, **not** `cargo test`. The integration tests share process-global state (the
> metrics collector), so they must each run in their own process — which nextest does by default.
> Under `cargo test` (a single process) they interfere with one another and fail. CI runs nextest;
> local development should too.

The [`.config/nextest.toml`](https://github.com/KioHQ/kiomq/blob/main/.config/nextest.toml) profile
also terminates any hung test, so a stall fails fast instead of wedging the run:

```toml
[profile.default]
retries = 2
failure-output = "immediate-final"
fail-fast = false
slow-timeout = { period = "3s", terminate-after = 7 }
status-level = "skip"
```

Nextest skips doctests, so run those separately:

```bash
cargo test --doc --features tracing,redis-store
```

## Testing your own jobs

`InMemoryStore` needs no services and no cleanup, which makes it the natural fixture. Give each test
its own queue name so parallel tests never share state.

```rust
use std::sync::Arc;
use kiomq::{InMemoryStore, Job, KioError, Queue, Worker, WorkerOpts};

#[tokio::test(flavor = "multi_thread")]
async fn doubles_the_payload() -> kiomq::KioResult<()> {
    let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "test-doubles");
    let queue = Queue::new(store, None).await?;

    let worker = Worker::new_async(
        &queue,
        |_s: Arc<_>, job: Job<u64, u64, ()>| async move {
            Ok::<u64, KioError>(job.data.unwrap_or_default() * 2)
        },
        Some(WorkerOpts { concurrency: 2, ..Default::default() }),
    )?;
    worker.run()?;

    let job = queue.add_job("double", 21u64, None).await?;

    let metrics = queue.current_metrics.clone();
    while !metrics.all_jobs_completed() {
        tokio::task::yield_now().await;
    }

    let done = queue.get_job(job.id.expect("id assigned")).await.expect("job exists");
    assert_eq!(done.returned_value, Some(42));

    worker.close();
    Ok(())
}
```

The `flavor = "multi_thread"` attribute matters: worker concurrency and `spawn_blocking` both assume
more than one runtime thread.

## Waiting deterministically

Busy-waiting on `all_jobs_completed()` works but spins. For tighter tests, wait on the event instead:

```rust
use kiomq::JobState;
use tokio::sync::oneshot;

let (tx, rx) = oneshot::channel();
let tx = std::sync::Arc::new(std::sync::Mutex::new(Some(tx)));

queue.on(JobState::Completed, move |_evt| {
    let tx = tx.clone();
    async move {
        if let Some(tx) = tx.lock().unwrap().take() {
            let _ = tx.send(());
        }
    }
});

queue.add_job("job", payload, None).await?;
tokio::time::timeout(std::time::Duration::from_secs(5), rx).await??;
```

Always wrap the wait in a `timeout` — a test that hangs is worse than a test that fails.

## Testing failure paths

Set `attempts` to more than one and assert on the recorded reason:

```rust
use kiomq::{JobOptions, QueueOpts};

let queue = Queue::new(store, Some(QueueOpts { attempts: 3, ..Default::default() })).await?;

let worker = Worker::new_async(&queue, |_s: Arc<_>, _j: Job<u64, u64, ()>| async move {
    Err::<u64, KioError>(std::io::Error::other("nope").into())
}, None)?;
worker.run()?;

let job = queue.add_job("always-fails", 1u64, None).await?;
// … wait for completion …

let failed = queue.get_job(job.id.unwrap()).await.unwrap();
assert_eq!(failed.attempts_made, 3);
assert!(failed.failed_reason.is_some());
```

Panicking processors can be asserted the same way — the worker catches the panic and records it as a
failure, so the test process survives.

## Redis-backed tests

If you need to exercise `RedisStore`, start a throwaway server and namespace by prefix:

```bash
docker run --rm -p 6379:6379 redis:latest
```

```rust
let store = RedisStore::new(Some("test-run-1"), "orders", &redis_conn).await?;
let queue = Queue::new(store, None).await?;
// … assertions …
queue.obliterate().await?; // clean up after yourself
```

Prefer `InMemoryStore` for logic and keep Redis tests for the handful of behaviours that genuinely
depend on the backend.
