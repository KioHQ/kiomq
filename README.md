<p align="center">
  <img src="assets/logo-dark.png" alt="KioMQ logo" width="220" />
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
- Pluggable **Stores** (Redis default; optional RocksDB; in-memory for ephemeral workloads)
- **Scheduling** (delays, cron, repeat)
- **Reliability** (attempts, retries, backoff, stalled job recovery)
- **Observability** (events, progress updates, metrics)

Inspired by BullMQ’s ergonomics, implemented as an embeddable Rust library for Tokio apps.

---

## Built for scale

KioMQ is designed to scale both **vertically** and **horizontally**:

### Vertical scale (single machine)

- Run **multiple worker instances** in the same process or across multiple processes on one machine
- Each worker supports **configurable concurrency** (defaults to CPU count)
- Best performance when using Tokio’s **multi-thread runtime** (`rt-multi-thread`)

### Horizontal scale (multiple machines)

With the **Redis store** you can:

- Run workers on **multiple machines** (or containers) against the same queue
- Add/remove workers without changing producer code
- Use Redis-backed persistence + coordination for distributed processing

---

## Table of contents

- [Tokio runtime requirements](#tokio-runtime-requirements)
- [Key features](#key-features)
- [Installation](#installation)
- [Quickstart](#quickstart)
- [Configuration](#configuration)
  - [Queue configuration (`QueueOpts`)](#queue-configuration-queueopts)
  - [Per-job configuration (`JobOptions`)](#per-job-configuration-joboptions)
  - [Worker configuration (`WorkerOpts`)](#worker-configuration-workeropts)
  - [Event delivery (`QueueEventMode`)](#event-delivery-queueeventmode)
- [Events & observability](#events--observability)
- [Progress updates](#progress-updates)
- [Scheduling: delays, cron, repeat](#scheduling-delays-cron-repeat)
- [Backends](#backends)
- [Examples](#examples)
- [Benchmarks](#benchmarks)
- [Testing](#testing)
- [License](#license)

---

## Tokio runtime requirements

KioMQ is built on **Tokio** and is intended to be used with the **multi-thread runtime**.

### Recommended

Use `#[tokio::main(flavor = "multi_thread")]`:

```rust
#[tokio::main(flavor = "multi_thread")]
async fn main() {
    // create queue, spawn worker(s), etc...
}
```

If you configure Tokio in your app, ensure you have:

```toml
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

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

### Control plane

- Pause/resume processing
- Cleanup policies for completed/failed jobs (keep by age/count)

---

## Installation

```toml
[dependencies]
kio-mq = "0.1"
```

### Cargo features

- `redis-store` (default): Redis backend (recommended for distributed workers)
- `rocksdb-store`: RocksDB backend
- `tracing`: tracing instrumentation

---

## Quickstart

```rust
use std::sync::Arc;
use kio_mq::{InMemoryStore, Job, KioError, KioResult, Queue, Worker, WorkerOpts};

#[tokio::main(flavor = "multi_thread")]
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

    Ok(())
}
```

---

## Configuration

This section summarizes the main knobs you’ll use in production.

### Queue configuration (`QueueOpts`)

Configure defaults that apply across jobs enqueued into a queue.

```rust
use kio_mq::{
    BackOffJobOptions, BackOffOptions, KeepJobs, QueueEventMode, QueueOpts,
    RemoveOnCompletionOrFailure,
};

let cleanup = RemoveOnCompletionOrFailure::Opts(KeepJobs {
    age: Some(60 * 60), // keep for 1 hour
    count: None,
});

let backoff = BackOffJobOptions::Opts(BackOffOptions {
    type_: Some("exponential".to_owned()),
    delay: Some(200),
});

let queue_opts = QueueOpts {
    /// Default max attempts if job doesn't override it
    attempts: 3,

    /// Default backoff policy used for retries (and optionally repeat policies)
    default_backoff: Some(backoff),

    /// Cleanup policies for terminal states
    remove_on_complete: Some(cleanup),
    remove_on_fail: Some(cleanup),

    /// Event delivery mode: Stream (default) or PubSub
    event_mode: Some(QueueEventMode::PubSub),

    /// Optional queue-level repeat policy
    repeat: None,

    ..Default::default()
};
```

### Per-job configuration (`JobOptions`)

Override queue defaults per job (e.g. delay, priority, attempts, repeat).

```rust
use kio_mq::JobOptions;

let opts = JobOptions {
    // delay: 500.into(), // delay by ms
    // priority: 10,
    // attempts: 5,
    ..Default::default()
};
```

> Note: `delay` can be based on milliseconds or cron schedules (see `JobDelay` and `Repeat`).

### Worker configuration (`WorkerOpts`)

Worker options control concurrency, lock timing, stalled detection, and metrics intervals.

```rust
use kio_mq::WorkerOpts;

let worker_opts = WorkerOpts {
    /// Number of tasks processed concurrently by this worker
    concurrency: 100,

    /// Stall checks and locking behavior (tune for your workload)
    // stalled_interval: 30_000,
    // lock_duration: 30_000,
    // lock_renew_time: 15_000,
    // max_stalled_count: 1,

    /// How often worker metrics are produced
    // metrics_update_interval: 100,

    ..Default::default()
};
```

### Event delivery (`QueueEventMode`)

KioMQ can emit queue events through:

- `QueueEventMode::Stream` (default): Redis Streams-based eventing
- `QueueEventMode::PubSub`: Redis PubSub-based eventing

Pick `PubSub` when you want low-latency “live” events, and `Stream` when you want stream semantics and replay-friendly behavior.

---

## Events & observability

```rust
use kio_mq::{EventParameters, JobState};

queue.on(JobState::Completed, |_evt| async move {
    // handle completed
});

queue.on_all_events(|evt: EventParameters<_, _>| async move {
    let _ = evt;
});
```

---

## Progress updates

```rust
use std::sync::Arc;
use kio_mq::{Job, KioError, Store};

async fn processor<S: Store<MyData, MyReturn, MyProgress>>(
    store: Arc<S>,
    mut job: Job<MyData, MyReturn, MyProgress>,
) -> Result<MyReturn, KioError> {
    job.update_progress(MyProgress { /* ... */ }, store.as_ref())?;
    Ok(MyReturn { /* ... */ })
}
```

---

## Scheduling: delays, cron, repeat

KioMQ supports:

- Delay by milliseconds
- Delay by cron expressions
- Repeat policies

See:

- `JobDelay`
- `Repeat`

---

## Backends

### In-memory — best for ephemeral tasks

Use `InMemoryStore` when you want:

- fast local execution
- tests and CI
- dev-mode background work
- ephemeral queues where persistence across restarts is not required

If you need durability or distributed workers, use **Redis** (default) instead.

### Redis (default) — scale out across machines

```bash
docker run --rm -p 6379:6379 redis:latest
```

```rust
use kio_mq::{Config, KioResult, Queue, RedisStore};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> KioResult<()> {
    let config = Config::default();
    let store = RedisStore::new(None, "my-queue", &config).await?;
    let queue = Queue::new(store, None).await?;
    Ok(())
}
```

### RocksDB

Embedded persistence. Enable with `rocksdb-store`.

---

## Examples

- `examples/basic.rs`
- `examples/video_transcoding.rs`

```bash
cargo run --example basic
```

---

## Benchmarks

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

MIT — see [LICENSE](LICENSE)
