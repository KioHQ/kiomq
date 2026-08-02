---
title: "Errors & retries"
linkTitle: "Errors & retries"
group: "Guides"
weight: 60
lead: "How failures are signalled, what happens to a panicking processor, and how backoff decides when to try again."
---

## Signalling failure

A processor reports failure by **returning `Err`**. The worker catches it, records the reason, and
either schedules a retry or moves the job to `Failed` depending on the attempt limit.

```rust
use std::sync::Arc;
use kiomq::{Job, KioError, Store};

async fn charge(store: Arc<impl Store<Invoice, Receipt, ()>>, job: Job<Invoice, Receipt, ()>)
    -> Result<Receipt, KioError>
{
    let invoice = job.data.ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "missing payload")
    })?;

    // Any error that converts into KioError works — including io::Error.
    let receipt = billing::charge(&invoice).await?;
    Ok(receipt)
}
```

`KioError` has `From` conversions for `io::Error`, `ParseIntError`, `SystemTimeError`, JSON and Redis
errors, so `?` usually just works. For anything else, box it into `GeneralError`:

```rust
use kiomq::KioError;

let receipt = billing::charge(&invoice)
    .await
    .map_err(|e| KioError::GeneralError(Box::new(e)))?;
```

## Panics are contained

A panic inside a processor is caught by the worker and treated as a failure — the job is marked
failed (and retried, if attempts remain) rather than taking the process down with it. A single rogue
job cannot kill your service.

The captured panic message and backtrace are stored on the job:

```rust
if let Some(job) = queue.get_job(42).await {
    if let Some(details) = &job.failed_reason {
        eprintln!("failed: {}", details.reason);
    }
    for trace in &job.stack_trace {
        eprintln!("{trace:?}");
    }
}
```

> [!TIP]
> Panics are contained, not free — each one costs an unwind and a retry. Return `Err` for expected
> failures and reserve panics for genuine bugs.

## Attempts

`attempts` is the **maximum number of times the processor may run** for a job, counting the first
attempt. `1` (the queue default) means no retries.

```rust
use kiomq::{JobOptions, QueueOpts};

// Queue-wide: two tries for everything.
let queue = Queue::new(store, Some(QueueOpts { attempts: 2, ..Default::default() })).await?;

// This job gets five.
queue.add_job("flaky-api-call", payload, Some(JobOptions {
    attempts: 5,
    ..Default::default()
})).await?;
```

`job.attempts_made` tracks how many invocations have happened so far, and is available inside the
processor — useful for logging or for giving up early on a specific error class.

## Backoff strategies

Between attempts, the retry delay comes from a backoff strategy. Two are built in:

| Name | Formula |
|---|---|
| `"exponential"` | `2^attempt * delay` milliseconds |
| `"fixed"` | `delay` milliseconds, constant |

```rust
use kiomq::{BackOffJobOptions, BackOffOptions, JobOptions};

// Shorthand: fixed 500 ms.
let fixed = JobOptions {
    backoff: Some(BackOffJobOptions::Number(500)),
    ..Default::default()
};

// Exponential, starting at 200 ms → 400, 800, 1600…
let exponential = JobOptions {
    backoff: Some(BackOffJobOptions::Opts(BackOffOptions {
        type_: Some("exponential".into()),
        delay: Some(200),
    })),
    ..Default::default()
};
```

Set `QueueOpts::default_backoff` to apply one strategy to the whole queue, and override per job where
it matters. Custom strategies are registered by name — see
[Configuration](../configuration/#custom-backoff-strategies).

## Stalled jobs

If a worker dies mid-job, its lock expires and the job is recovered into `Wait` by the next stalled
check. That recovery is bounded by `max_stalled_count` (default `1`); beyond it the job is failed.

```rust
use kiomq::WorkerOpts;

let opts = WorkerOpts {
    lock_duration: 120_000,     // jobs may take up to two minutes
    lock_renew_time: 60_000,
    stalled_interval: 30_000,   // scan twice a minute
    max_stalled_count: 2,
    ..Default::default()
};
```

Stalled recovery is *at-least-once* delivery: a job that stalls has already partially run. Make
processors idempotent — key writes by `job.id`, or check for an existing result before doing the work
again.

## Async backtraces with `#[framed]`

`kiomq::framed` re-exports [`async_backtrace::framed`](https://docs.rs/async-backtrace). Annotating a
processor gives you readable async stack traces when things go wrong.

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
    while !updating_metrics.all_jobs_completed() {
        tokio::task::yield_now().await;
    }

    worker.close();
    Ok(())
}
```

## Watching failures

Subscribe to the `Failed` state to alert, log, or push to a dead-letter queue:

```rust
use kiomq::{EventParameters, JobState};

queue.on(JobState::Failed, |evt| async move {
    if let EventParameters::Failed { job_id, reason, prev_state } = evt {
        tracing::error!(job_id, ?prev_state, reason = %reason.reason, "job failed");
    }
});
```

See [Events](../events/) for the full payload list.
