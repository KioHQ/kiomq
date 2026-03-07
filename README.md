<p align="center">
  <img src="assets/logo-dark.png" alt="KioMQ logo" width="320" />
</p>

<p align="center">
  <b>A task queue &amp; orchestration library for Rust</b><br/>
  Built for <b>Tokio</b> · Scale up (many workers per machine) · Scale out (Redis + workers across machines)
</p>

<p align="center">
  <a href="https://crates.io/crates/kiomq"><img alt="crates.io" src="https://img.shields.io/crates/v/kiomq.svg" /></a>
  <a href="https://github.com/KioHQ/kiomq/actions/workflows/ci.yml"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/KioHQ/kiomq/ci.yml?branch=main" /></a>
  <a href="https://docs.rs/kiomq"><img alt="docs.rs" src="https://img.shields.io/docsrs/kiomq" /></a>
  <a href= "https://opensource.org/licenses/MIT"> <img alt ="LICENSE" src="https://img.shields.io/badge/License-MIT-yellow.svg"/> </a>
</p>

---

**KioMQ** provides the core building blocks to run background work inside your Tokio services:

- A **Queue** to enqueue tasks/jobs.
- One or more **Workers** to process jobs concurrently.
- Pluggable **Stores**: `InMemoryStore` (ephemeral), `RedisStore` (durable, distributed), RocksDB _(under construction)_.
- **Scheduling** – delays, cron expressions, repeat policies.
- **Reliability** – retries, backoff strategies, stalled-job detection.
- **Observability** – events, progress updates, per-worker metrics.

Inspired by [BullMQ](https://docs.bullmq.io/)'s ergonomics, implemented as an embeddable Rust library.

---

**Contents:** [Key features](#key-features) · [Tokio runtime](#tokio-runtime-requirements) · [Installation](#installation) · [Quick-start](#quick-start) · [Panics & errors](#panics--errors-in-the-processor) · [Configuration](#configuration) · [Events & observability](#events--observability) · [Progress updates](#progress-updates) · [Backends](#backends) · [Benchmarks](#benchmarks) · [Testing](#testing) · [License](#license)

---

### Key features

- **Async & sync processors** – async for I/O-bound work, sync (`spawn_blocking`) for CPU-bound.
- **Configurable concurrency** – defaults to CPU count.
- **Event-driven idle workers** – near-zero CPU when empty, using lock-free atomics and `tokio::sync::Notify`.
- **Bulk enqueue** – `Queue::bulk_add` / `Queue::bulk_add_only`.
- **Priority & delayed jobs** – by score or after _N_ ms / cron schedule.
- **Repeat policies** – cron, backoff-driven, fixed interval, immediate.

---

### Tokio runtime requirements

Multi-thread runtime is recommended:

```toml
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

For tests:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn my_test() { /* ... */ }
```

---

### Installation

```toml
[dependencies]
kiomq = "0.1"
```

Cargo features: `redis-store` _(default)_, `rocksdb-store`, `tracing`.

---

### Quick-start

#### Async worker

```rust
use std::sync::Arc;
use kiomq::{InMemoryStore, Job, KioError, Queue, Worker, WorkerOpts};

#[tokio::main]
async fn main() -> kiomq::KioResult<()> {
    let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "demo");
    let queue = Queue::new(store, None).await?;

    let processor = |_store: Arc<_>, job: Job<u64, u64, ()>| async move {
        Ok::<u64, KioError>(job.data.unwrap_or_default() * 2)
    };

    let worker = Worker::new_async(&queue, processor, Some(WorkerOpts::default()))?;
    worker.run()?;
   
    queue.bulk_add_only((0..10u64).map(|i| (format!("job-{i}"), None, i))).await?;

    let updating_metrics = queue.current_metrics.clone();
    // while for all jobs to complete
    while !updating_metrics.all_jobs_completed()  {
        tokio::task::yield_now().await;
    }
    worker.close();
    Ok(())
}
```

#### Sync worker

Sync processors run on a blocking thread via `tokio::task::spawn_blocking` — suitable for
heavy computation, hashing, blocking FFI, etc.

```rust
use std::sync::Arc;
use kiomq::{InMemoryStore, Job, KioError, Queue, Worker, WorkerOpts};

#[tokio::main]
async fn main() -> kiomq::KioResult<()> {
    let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "demo-sync");
    let queue = Queue::new(store, None).await?;

    let processor = |_store: Arc<_>, job: Job<u64, u64, ()>| {
        Ok::<u64, KioError>(job.data.unwrap_or_default() * 2)
    };

    let worker = Worker::new_sync(&queue, processor, Some(WorkerOpts::default()))?;
    worker.run()?;

    queue.add_job("compute", 42u64, None).await?;

    let updating_metrics = queue.current_metrics.clone();
    // while for all jobs to complete
    while !updating_metrics.all_jobs_completed()  {
        tokio::task::yield_now().await;
    }
    worker.close();
    Ok(())
}
```

---

### Panics & errors in the processor

A processor signals a job failure by **returning `Err`**. The worker catches the error,
marks the job as failed, and — depending on the `attempts` configuration — retries it
with the configured backoff.

Panics inside a processor are also caught by the worker and treated as failures, so a
rogue job cannot bring down the whole process.

#### Async backtrace with `#[framed]`

Annotate your processor with `#[framed]` (re-exported from
[`async_backtrace`](https://docs.rs/async-backtrace) as `kiomq::framed`) for richer async
stack traces:

```rust
use std::sync::Arc;
use kiomq::{framed, InMemoryStore, Job, KioError, Queue, Store, Worker, WorkerOpts};

#[framed]
async fn my_processor<S: Store<u64, u64, ()>>(
    _store: Arc<S>,
    job: Job<u64, u64, ()>,
) -> Result<u64, KioError> {
    let data = job.data.unwrap_or_default();
    if data == 0 {
        // Returning Err marks the job as failed and triggers a retry
        // (up to `attempts` times, as set in QueueOpts / JobOptions).
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "zero input").into());
    }
    Ok(data * 2)
}

#[tokio::main]
async fn main() -> kiomq::KioResult<()> {
    let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "framed-demo");
    let queue = Queue::new(store, None).await?;

    let worker = Worker::new_async(&queue, |s, j| my_processor(s, j), Some(WorkerOpts::default()))?;
    worker.run()?;

    queue.add_job("job-1", 42u64, None).await?;

    let updating_metrics = queue.current_metrics.clone();
    // while for all jobs to complete
    while !updating_metrics.all_jobs_completed()  {
        tokio::task::yield_now().await;
    }
    worker.close();
    Ok(())
}
```

---

### Configuration

#### Queue options (`QueueOpts`)

```rust
use kiomq::{BackOffJobOptions, BackOffOptions, KeepJobs, QueueEventMode, QueueOpts,
            RemoveOnCompletionOrFailure};

let queue_opts = QueueOpts {
    attempts: 2,
    default_backoff: Some(BackOffJobOptions::Opts(BackOffOptions {
        type_: Some("exponential".to_owned()),
        delay: Some(200),
    })),
    remove_on_fail: Some(RemoveOnCompletionOrFailure::Opts(KeepJobs {
        age: Some(3600), // keep for 1 hour
        count: None,
    })),
    event_mode: Some(QueueEventMode::PubSub),
    ..Default::default()
};
```

#### Per-job options (`JobOptions`)

```rust
use kiomq::JobOptions;

let opts = JobOptions { attempts: 5, ..Default::default() };
```

#### Worker options (`WorkerOpts`)

```rust
use kiomq::WorkerOpts;

let opts = WorkerOpts { concurrency: 8, ..Default::default() };
```

---

### Events & observability

Subscribe to job-state events on the queue:

```rust
use kiomq::{EventParameters, InMemoryStore, JobState, Queue};

// Subscribe to a specific state.
let _listener_id = queue.on(JobState::Completed, |evt| async move { let _ = evt; });

// Subscribe to all events.
let _listener_id2 = queue.on_all_events(|evt: EventParameters<u64, ()>| async move { let _ = evt; });

// Remove a listener when no longer needed.
queue.remove_event_listener(_listener_id);
```

---

### Progress updates

Report progress from inside your processor using `job.update_progress`:

```rust
use std::sync::Arc;
use kiomq::{Job, KioError, Store};

async fn processor<S: Store<u64, u64, u8>>(
    store: Arc<S>,
    mut job: Job<u64, u64, u8>,
) -> Result<u64, KioError> {
    // update_progress persists to the store and emits a progress event.
    job.update_progress(50u8, store.as_ref())?; // 50% done
    Ok(job.data.unwrap_or_default() * 2)
}
```

---

### Backends

#### In-memory

`InMemoryStore` – ideal for tests, dev, and short-lived tasks. No external dependencies.

#### Redis _(default feature)_

Durable, distributed workloads. Requires a running Redis instance:

```bash
docker run --rm -p 6379:6379 redis:latest
```

#### RocksDB _(under construction)_

Embedded persistence – work in progress.

---

### Benchmarks

```bash
cargo bench
```

### Testing

```bash
cargo test
```

### License

MIT — see [LICENSE](LICENSE)
