<p align="center">
  <!-- Cropped (bottom-right) logo variant.
       Replace `assets/logo-dark.png` with your actual path if different. -->
  <img src="assets/logo-dark.png" alt="KioMQ logo" width="320" />
</p>

<p align="center">
  <b>A task queue & orchestration library for Rust</b><br/>
  Built for <b>Tokio</b> (<code>rt-multi-thread</code> recommended) • Scale up (many workers per machine) • Scale out (Redis + workers across machines)
</p>

---

## What is KioMQ?

**KioMQ is a task-queue and orchestration library for Rust**. It provides the core building blocks to run background work inside your Tokio services:

- A **Queue** to enqueue tasks/jobs
- One or more **Workers** to process jobs concurrently
- Pluggable **Stores**:
  - **Redis** (default) for durable, distributed workloads
  - **In-memory** for **ephemeral** workloads (tests, dev, short-lived tasks)
  - **RocksDB** _(under construction)_ for embedded persistence
- **Scheduling** (delays, cron, repeat)
- **Reliability** (attempts, retries, backoff, stalled job recovery)
- **Observability** (events, progress updates, metrics)

Inspired by BullMQ’s ergonomics, implemented as an embeddable Rust library for Tokio apps.

---

## Key features

### Reliability & lifecycle

- Retries via **attempts** (queue defaults + per-job overrides)
- Backoff strategies (**fixed**, **exponential**, plus custom hooks)
- Stalled job detection + recovery (lock duration, renew time, max stalled count)
- Job state transitions (waiting/active/delayed/completed/failed/paused, etc.)

### Scheduling & orchestration

- Delayed jobs (milliseconds)
- Cron-based scheduling (delay/repeat)
- Repeat policies (cron, backoff-driven, fixed interval, immediate repeat with max)

### Throughput & performance

- Bulk enqueue APIs
- Configurable worker concurrency (default: CPU count)

### Observability

- Event subscriptions (completed/failed/progress/etc)
- Progress updates from inside processors
- Queue metrics (counts per state, last id, etc.)

### Efficient idle workers (event-driven)

When queue **events** are enabled, workers can become effectively **idle** (near-zero CPU usage) while the queue has no work.

This is achieved by coordinating wakeups with a combination of:

- lock-free **atomics** to track state
- Tokio synchronization primitives such as
  [`tokio::sync::Notify`](https://docs.rs/tokio/latest/tokio/sync/struct.Notify.html)

The result is an event-driven “sleep until work arrives” behavior, rather than tight polling loops, which helps reduce CPU burn in low-traffic queues.

#### Suitable for low-powered devices

This event-driven idle behavior makes KioMQ a good fit for low-powered devices and low-traffic services:

- minimal CPU usage between jobs helps reduce power draw / improve battery life
- less heat and fewer wasted cycles on small CPUs

---

## Built for scale

KioMQ is designed to scale both **vertically** and **horizontally**:

### Vertical scale (single machine)

- Run **multiple workers** on the same machine (same process or multiple processes)
- Each worker supports **configurable concurrency** (defaults to CPU count)
- Best performance when using Tokio’s **multi-thread runtime** (`rt-multi-thread`)

### Horizontal scale (multiple machines)

With the **Redis store** you can:

- Run workers across **multiple machines/containers** against the same queue
- Add/remove workers without changing producer code
- Use Redis-backed persistence + coordination for distributed processing

---

## Table of contents

- [Tokio runtime requirements](#tokio-runtime-requirements)
- [Key features](#key-features)
- [Installation](#installation)
- [Usage](#usage)
  - [Panics & errors in the processor](#panics--errors-in-the-processor)
- [Configuration](#configuration)
  - [Queue configuration (QueueOpts)](#queue-configuration-queueopts)
  - [Per-job configuration (JobOptions)](#per-job-configuration-joboptions)
  - [Worker configuration (WorkerOpts)](#worker-configuration-workeropts)
  - [Event delivery (QueueEventMode)](#event-delivery-queueeventmode)
- [Events & observability](#events--observability)
- [Progress updates](#progress-updates)
- [Scheduling](#scheduling)
- [Backends](#backends)
- [Benchmarks](#benchmarks)
- [Testing](#testing)
- [License](#license)

---

## Tokio runtime requirements

KioMQ is built on **Tokio**.

- For best throughput and “scale up” behavior, **Tokio’s multi-thread runtime is recommended** (`rt-multi-thread`).
- You don’t need to specify the runtime flavor in most application code—`#[tokio::main]` is fine.

If you configure Tokio in your app, ensure you have:

```toml
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

### Tests (explicit multi-thread runtime)

```rust
#[tokio::test(flavor = "multi_thread")]
async fn my_test() {
    // ...
}
```

---

## Installation

```toml
[dependencies]
kio-mq = "0.1"
```

### Cargo features

- `redis-store` (default): Redis backend (recommended for distributed workers)
- `rocksdb-store`: RocksDB backend _(under construction)_
- `tracing`: tracing instrumentation

---

## Usage

Start with the repo examples:

- [`examples/basic.rs`](examples/basic.rs) — queue + worker + options + events + bulk enqueue
- [`examples/video_transcoding.rs`](examples/video_transcoding.rs) — real workload + progress reporting

KioMQ supports both **async** and **sync** worker processors.

### Async worker

Async processors run directly on the Tokio runtime and are best for I/O-bound work.

```rust
use std::sync::Arc;
use kiomq::{InMemoryStore, Job, KioError, KioResult, Queue, Worker, WorkerOpts};

#[tokio::main]
async fn main() -> KioResult<()> {
    // InMemoryStore is ideal for ephemeral workloads (tests, dev, short-lived tasks)
    let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "demo");
    let queue = Queue::new(store, None).await?;

    let processor = |_store: Arc<_>, job: Job<u64, u64, ()>| async move {
        Ok::<u64, KioError>(job.data.unwrap_or_default() * 2)
    };

    let worker = Worker::new_async(&queue, processor, Some(WorkerOpts::default()))?;
    worker.run()?;

    let jobs = (0..100u64).map(|i| (format!("job-{i}"), None, i));
    queue.bulk_add_only(jobs).await?;

    worker.close();

    Ok(())
}
```

### Sync worker

Sync processors are designed for CPU-bound or blocking workloads.

Under the hood, a sync worker runs your processor on a dedicated blocking thread via
[`tokio::task::spawn_blocking`](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html),
so it won’t block Tokio’s async executor threads.

This is suitable for:

- heavy computation (hashing, image processing, compression, ML inference, etc.)
- blocking libraries that aren’t async-friendly

Future direction:

- a Rayon integration is a natural fit here (e.g. using a Rayon thread pool for CPU work),
  and may be added as an optional execution backend.

```rust
use std::sync::Arc;
use kiomq::{InMemoryStore, Job, KioError, KioResult, Queue, Worker, WorkerOpts};

#[tokio::main]
async fn main() -> KioResult<()> {
    let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "demo");
    let queue = Queue::new(store, None).await?;

    let processor = |_store: Arc<_>, job: Job<u64, u64, ()>| {
        Ok::<u64, KioError>(job.data.unwrap_or_default() * 2)
    };

    let worker = Worker::new_sync(&queue, processor, Some(WorkerOpts::default()))?;
    worker.run()?;

    let jobs = (0..100u64).map(|i| (format!("job-{i}"), None, i));
    queue.bulk_add_only(jobs).await?;

    worker.close();

    Ok(())
}
```

---

## Panics & errors in the processor

A processor signals a job failure by **returning `Err`**. The worker catches the error,
marks the job as failed, and — depending on the `attempts` configuration — retries it
with the configured backoff.

Panics inside a processor are also caught by the worker and treated as failures, so a
rogue job cannot bring down the whole process.

### Async backtrace with `#[framed]`

Annotate your processor function with the `#[framed]` attribute macro (re-exported from
[`async_backtrace`](https://docs.rs/async-backtrace) as `kiomq::framed`) to capture
richer async stack traces when a panic or error occurs:

```rust
use std::sync::Arc;
use kiomq::{framed, InMemoryStore, Job, KioError, KioResult, Queue, Store, Worker, WorkerOpts};

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
async fn main() -> KioResult<()> {
    let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "framed-demo");
    let queue = Queue::new(store, None).await?;

    let worker = Worker::new_async(&queue, |s, j| my_processor(s, j), Some(WorkerOpts::default()))?;
    worker.run()?;

    queue.add_job("job-1", 42u64, None).await?;

    worker.close();
    Ok(())
}
```

---

## Configuration

### Queue configuration (QueueOpts)

```rust
use kiomq::{
    BackOffJobOptions, BackOffOptions, KeepJobs, QueueEventMode, QueueOpts,
    RemoveOnCompletionOrFailure,
};

let remove_opts = RemoveOnCompletionOrFailure::Opts(KeepJobs {
    age: Some(60 * 60), // keep for 1 hour
    count: None,
});

let backoff_opts = BackOffJobOptions::Opts(BackOffOptions {
    type_: Some("exponential".to_owned()),
    delay: Some(200),
});

let queue_opts = QueueOpts {
    attempts: 2,
    default_backoff: Some(backoff_opts.clone()),
    remove_on_fail: Some(remove_opts),
    remove_on_complete: Some(remove_opts),

    // Stream (default) or PubSub
    event_mode: Some(QueueEventMode::PubSub),

    ..Default::default()
};
```

### Per-job configuration (JobOptions)

```rust
use kiomq::JobOptions;

let mut job_opts = JobOptions {
    // delay: 500.into(), // delay by ms (or cron-based delay)
    // priority: 10,
    ..Default::default()
};

job_opts.attempts = 5;
```

### Worker configuration (WorkerOpts)

```rust
use kiomq::WorkerOpts;

let worker_opts = WorkerOpts {
    concurrency: 100,

    // stalled_interval: 30_000,
    // lock_duration: 30_000,
    // lock_renew_time: 15_000,
    // max_stalled_count: 1,

    // metrics_update_interval: 100,
    ..Default::default()
};
```

### Event delivery (QueueEventMode)

KioMQ can emit events using:

- `QueueEventMode::Stream` (default)
- `QueueEventMode::PubSub`

---

## Events & observability

```rust
use kiomq::{EventParameters, JobState};

queue.on(JobState::Completed, |_evt| async move {
    // handle completed
});

queue.on_all_events(|evt: EventParameters<_, _>| async move {
    let _ = evt;
});
```

See:

- [`JobState` (event type)](src/job_state.rs)
- [`EventParameters`](src/events/event_parameters.rs)

---

## Progress updates

```rust
use std::sync::Arc;
use kiomq::{Job, KioError, Store};

async fn processor<S: Store<MyData, MyReturn, MyProgress>>(
    store: Arc<S>,
    mut job: Job<MyData, MyReturn, MyProgress>,
) -> Result<MyReturn, KioError> {
    job.update_progress(MyProgress { /* ... */ }, store.as_ref())?;
    Ok(MyReturn { /* ... */ })
}
```

---

## Scheduling

See:

- [`JobDelay`](src/job/delay.rs)
- [`Repeat`](src/job/repeat.rs)

---

## Backends

### In-memory — best for ephemeral tasks

Use `InMemoryStore` when you want fast local execution and don’t need persistence across restarts.

### Redis (default) — scale out across machines

```bash
docker run --rm -p 6379:6379 redis:latest
```

```rust
use kiomq::{Config, KioResult, Queue, RedisStore};

#[tokio::main]
async fn main() -> KioResult<()> {
    // `Config` can be imported from `kiomq` or from `deadpool_redis`
    // (if you already use it in your app).
    let config = Config::default();

    let store = RedisStore::new(None, "my-queue", &config).await?;
    let queue = Queue::new(store, None).await?;
    Ok(())
}
```

### RocksDB _(under construction)_

The RocksDB store is a work-in-progress and may change.

---

## Benchmarks

Criterion benchmarks live in [`benches/`](benches/):

```bash
cargo bench
```

---

## Testing

```bash
cargo test
```

---

## License

MIT — see [`LICENSE`](LICENSE)

