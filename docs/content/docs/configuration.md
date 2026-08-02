---
title: "Configuration"
linkTitle: "Configuration"
group: "Guides"
weight: 40
lead: "Queue defaults, per-job overrides, and worker tuning — every option, with its default value."
---

Options are layered: `QueueOpts` sets the defaults for the whole queue, `JobOptions` overrides them
per job, and `WorkerOpts` tunes the process that consumes them.

## Queue options

`QueueOpts` is passed to `Queue::new` and applies to every job that does not override it.

```rust
use kiomq::{BackOffJobOptions, BackOffOptions, KeepJobs, QueueEventMode, QueueOpts,
            RemoveOnCompletionOrFailure};

let queue_opts = QueueOpts {
    attempts: 2,
    default_backoff: Some(BackOffJobOptions::Opts(BackOffOptions {
        type_: Some("exponential".into()),
        delay: Some(200),
    })),
    remove_on_fail: Some(RemoveOnCompletionOrFailure::Opts(KeepJobs {
        age: Some(3600), // keep for 1 hour
        count: None,
    })),
    event_mode: Some(QueueEventMode::PubSub),
    ..Default::default()
};

let queue = Queue::new(store, Some(queue_opts)).await?;
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `attempts` | `u64` | `1` | Default attempt limit for jobs that don't set their own |
| `default_backoff` | `Option<BackOffJobOptions>` | `None` | Retry delay strategy for the whole queue |
| `remove_on_complete` | `Option<RemoveOnCompletionOrFailure>` | `None` | Retention for completed jobs; `None` keeps them |
| `remove_on_fail` | `Option<RemoveOnCompletionOrFailure>` | `None` | Retention for permanently failed jobs |
| `event_mode` | `Option<QueueEventMode>` | `Stream` | Stream (replayable) or pub/sub (broadcast-only) delivery |
| `repeat` | `Option<Repeat>` | `None` | Default repeat policy for every job on the queue |

## Job options

`JobOptions` is the third argument to `add_job`, and the second element of each `bulk_add` tuple.
Anything left at its default falls back to the queue.

```rust
use kiomq::{JobDelay, JobOptions};

let opts = JobOptions {
    attempts: 5,
    priority: 1,                          // lower runs first; 0 = no priority
    delay: JobDelay::TimeMilis(30_000),   // eligible in 30s
    ..Default::default()
};

queue.add_job("send-invoice", payload, Some(opts)).await?;
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `priority` | `u64` | `0` | Lower values run first. `0` means "no priority" |
| `delay` | `JobDelay` | `TimeMilis(0)` | Run now, after *N* ms, or from a cron expression |
| `id` | `Option<u64>` | `None` | Explicit job ID; the store assigns one when `None` |
| `attempts` | `u64` | `0` → queue default | Maximum attempts before permanent failure |
| `remove_on_complete` | `Option<…>` | inherit | Overrides the queue retention policy |
| `remove_on_fail` | `Option<…>` | inherit | Overrides the queue retention policy |
| `backoff` | `Option<BackOffJobOptions>` | inherit | Per-job retry delay strategy |
| `repeat` | `Option<Repeat>` | inherit | Re-enqueue the job after each run |

## Worker options

```rust
use kiomq::WorkerOpts;

let opts = WorkerOpts {
    concurrency: 8,
    lock_duration: 120_000,   // long-running jobs
    lock_renew_time: 60_000,  // roughly half of lock_duration
    ..Default::default()
};

let worker = Worker::new_async(&queue, processor, Some(opts))?;
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `concurrency` | `usize` | logical CPU count | Jobs processed simultaneously per worker |
| `lock_duration` | `u64` | `30_000` | Milliseconds a job lock is held before the job counts as stalled |
| `lock_renew_time` | `u64` | `15_000` | When the lock is renewed. Half of `lock_duration` suits most cases |
| `stalled_interval` | `u64` | `30_000` | How often the worker scans for stalled jobs |
| `max_stalled_count` | `u64` | `1` | Stalled recoveries allowed before the job is failed |
| `autorun` | `bool` | `false` | Call `run()` from the constructor instead of by hand |
| `metrics_update_interval` | `u64` | `100` | How often per-worker metrics are published to the store |

> [!NOTE]
> `autorun` defaults to `false` so that constructing a worker never implicitly starts consuming jobs.
> Call `worker.run()` when your service is ready.

## Retention policies

`RemoveOnCompletionOrFailure` accepts three shapes:

```rust
use kiomq::{KeepJobs, RemoveOnCompletionOrFailure};

// Delete the record as soon as the job settles.
RemoveOnCompletionOrFailure::Bool(true);

// Keep it forever (the default).
RemoveOnCompletionOrFailure::Bool(false);

// Keep at most 1 000 records, pruning the oldest.
RemoveOnCompletionOrFailure::Int(1_000);

// Keep for an hour, and at most 500 records.
RemoveOnCompletionOrFailure::Opts(KeepJobs {
    age: Some(3_600),
    count: Some(500),
});
```

A common production pairing is to drop completed jobs immediately and keep failures for a day so you
can inspect them:

```rust
let queue_opts = QueueOpts {
    remove_on_complete: Some(RemoveOnCompletionOrFailure::Bool(true)),
    remove_on_fail: Some(RemoveOnCompletionOrFailure::Opts(KeepJobs {
        age: Some(86_400),
        count: None,
    })),
    ..Default::default()
};
```

## Event delivery mode

```rust
use kiomq::QueueEventMode;

QueueEventMode::Stream;  // default — persistent, replayable by late subscribers
QueueEventMode::PubSub;  // broadcast-only — listeners miss events fired before they attached
```

Use `Stream` when a consumer needs to catch up after a restart; use `PubSub` when you only care about
live events and want to avoid retaining them. See [Events](../events/).

## Custom backoff strategies

Two strategies are registered out of the box: `"exponential"` (`2^attempt * delay`) and `"fixed"`
(constant `delay`). Register your own by name on the queue:

```rust
use std::sync::Arc;

queue.register_backoff_strategy("decorrelated", |_attempt| {
    Arc::new(|attempt: i64| {
        // your formula: (attempt) -> delay in milliseconds
        (attempt * 250).min(30_000)
    })
});

// Then reference it by name from job or queue options.
let opts = JobOptions {
    backoff: Some(BackOffJobOptions::Opts(BackOffOptions {
        type_: Some("decorrelated".into()),
        delay: Some(250),
    })),
    ..Default::default()
};
```

Registration is idempotent: a name that already exists is **not** replaced.
