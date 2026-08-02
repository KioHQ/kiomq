---
title: "Quick start"
linkTitle: "Quick start"
group: "Start here"
weight: 20
lead: "Enqueue jobs and process them with an async or a sync worker, end to end, in a single file."
---

## Async worker

The processor is a closure that receives an `Arc<Store>` and a `Job`, and returns
`Result<R, KioError>`.

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

    queue
        .bulk_add_only((0..10u64).map(|i| (format!("job-{i}"), None, i)))
        .await?;

    let updating_metrics = queue.current_metrics.clone();
    // wait for all jobs to complete
    while !updating_metrics.all_jobs_completed() {
        tokio::task::yield_now().await;
    }

    worker.close();
    Ok(())
}
```

What each step does:

1. **`InMemoryStore::new(None, "demo")`** — creates the state container for a queue named `demo`.
2. **`Queue::new(store, None)`** — wraps the store and applies default [`QueueOpts`](../configuration/).
3. **`Worker::new_async`** — registers the processor. Nothing runs yet.
4. **`worker.run()`** — starts the worker's main loop. (Set `WorkerOpts::autorun` to skip this call.)
5. **`bulk_add_only`** — enqueues ten jobs in one batch, discarding the returned `Job` values.
6. **`worker.close()`** — drains in-flight jobs and shuts the worker down.

> [!NOTE]
> `worker.run()` returns immediately — it spawns the loop rather than blocking. In a real service
> you keep the process alive with your server's own future instead of the busy-wait above.

## Sync worker

Sync processors run on a blocking thread via
[`tokio::task::spawn_blocking`](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html) —
use them for heavy computation, hashing, image work, or blocking FFI.

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
    while !updating_metrics.all_jobs_completed() {
        tokio::task::yield_now().await;
    }

    worker.close();
    Ok(())
}
```

The only differences are `Worker::new_sync` and the absence of `async move` — the closure returns a
`Result` directly.

## Enqueueing

```rust
// One job, queue defaults.
let job = queue.add_job("resize-avatar", payload, None).await?;
println!("queued as {:?}", job.id); // id is Option<u64>, assigned by the store

// One job with its own options.
use kiomq::JobOptions;
let opts = JobOptions { attempts: 5, priority: 1, ..Default::default() };
queue.add_job("charge-card", payload, Some(opts)).await?;

// Many jobs in one round trip: (name, options, data)
queue
    .bulk_add((0..1_000u64).map(|i| (format!("tile-{i}"), None, i)))
    .await?;

// Same, but skip allocating the returned Vec<Job>.
queue
    .bulk_add_only((0..1_000u64).map(|i| (format!("tile-{i}"), None, i)))
    .await?;
```

`bulk_add` returns the created `Job` values (including their assigned IDs);
`bulk_add_only` returns `()` and is the cheaper choice for fire-and-forget batches.

## Inspecting the queue

```rust
use kiomq::JobState;

// Live counters, cheap to read.
let metrics = queue.current_metrics.clone();
metrics.has_active_jobs();
metrics.is_idle();
metrics.has_delayed();

// Refresh the counters from the store.
let snapshot = queue.get_metrics().await?;

// Look jobs up by id or by state.
let job = queue.get_job(42).await;
let mut failed_ids = queue.get_job_ids_in_state(JobState::Failed, None, None).await?;
let failed = queue.fetch_jobs(failed_ids.make_contiguous()).await?;
```

## Pausing and draining

```rust
// Toggles between paused and running.
queue.pause_or_resume().await?;
assert!(queue.is_paused());

// Stop workers from reserving new jobs without touching the queue state.
queue.pause_active_workers();

// Delete every job and collection belonging to this queue.
queue.obliterate().await?;
```

> [!CAUTION]
> `obliterate()` is irreversible — it removes the queue's jobs, locks, and event log from the store.

## Where to go next

- [Core concepts](../core-concepts/) — job lifecycle and how workers reserve work.
- [Configuration](../configuration/) — queue, job, and worker options.
- [Scheduling](../scheduling/) — delays, priorities, cron, and repeat policies.
- The [`examples/`](https://github.com/KioHQ/kiomq/tree/main/examples) directory contains a runnable
  video-transcoding pipeline with progress reporting.
