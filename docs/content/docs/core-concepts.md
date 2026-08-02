---
title: "Core concepts"
linkTitle: "Core concepts"
group: "Start here"
weight: 30
lead: "How a job moves through the queue, who owns which piece of state, and what happens when a worker dies."
---

## Job states

Every job is in exactly one state at a time. `JobState` also doubles as the event key you subscribe
to — see [Events](../events/).

| State | Meaning |
|---|---|
| `Wait` | Ready to be picked up by a worker. The default. |
| `Prioritized` | In the priority sorted-set, waiting to be moved to active. |
| `Delayed` | Scheduled to run at a future timestamp. |
| `Active` | Currently being processed by a worker. |
| `Processing` | A worker has started executing the processor function. |
| `Stalled` | The worker holding the lock disappeared; pending recovery. |
| `Completed` | The processor returned `Ok`. |
| `Failed` | The processor returned `Err`, panicked, or stalled too many times. |
| `Paused` | The queue is paused; the job waits in the paused list. |
| `Resumed` | The queue resumed; the job is transitioning back to `Wait`. |
| `Progress` | A progress-update event — not a persistent state. |
| `Obliterated` | The queue was obliterated and the job was deleted. |

The happy path is `Wait → Active → Completed`. A prioritised job starts in `Prioritized`; a delayed
or cron job starts in `Delayed`.

## The lifecycle of one job

1. **Enqueue.** `add_job` / `bulk_add` writes the job to the store. If `priority` is non-zero it
   lands in the prioritized set; if `delay` is set it lands in the delayed set with a timestamp.
2. **Promotion.** The queue's timer subsystem moves delayed jobs to `Wait` once their timestamp
   passes, and notifies idle workers.
3. **Reservation.** A worker with a free concurrency slot claims the job, takes a **lock** with a
   token, and moves it to `Active`.
4. **Execution.** Your processor runs — as a future on the runtime, or on a blocking thread for sync
   processors. It may report [progress](../progress/) as it goes.
5. **Lock renewal.** While the job runs, the worker renews the lock every `lock_renew_time`
   milliseconds so the job is not mistaken for stalled.
6. **Settlement.** `Ok` moves the job to `Completed` and stores the return value; `Err` (or a panic)
   either schedules a retry or moves the job to `Failed`. See
   [Errors & retries](../errors-and-retries/).
7. **Retention.** `remove_on_complete` / `remove_on_fail` decide whether the record is kept, pruned
   by age or count, or deleted immediately.

## Locks and stalled jobs

A worker holds a lock for `lock_duration` milliseconds and renews it at `lock_renew_time`. If the
process crashes, the lock expires and the next stalled check — every `stalled_interval` — moves the
job back to `Wait` so another worker can pick it up.

`max_stalled_count` bounds how many times that recovery may happen for a single job. Once exceeded,
the job goes to `Failed` instead, which prevents a job that reliably kills its worker from cycling
forever.

> [!TIP]
> If your jobs routinely run longer than 30 seconds, raise `lock_duration` (and let
> `lock_renew_time` sit at roughly half of it) so healthy jobs are never mistaken for stalled.

## Scale up vs. scale out

**Scale up** — raise `WorkerOpts::concurrency`, or construct several workers against the same queue
in one process. Concurrency defaults to the logical CPU count.

```rust
let worker = Worker::new_async(&queue, processor, Some(WorkerOpts {
    concurrency: 32,
    ..Default::default()
}))?;
```

**Scale out** — point processes on other machines at the same Redis prefix and queue name. The store
owns the job set, the locks, and the event log, so coordination is automatic.

```rust
// Machine A and machine B — identical code, one shared queue.
let store = RedisStore::new(None, "transcode", &redis).await?;
let queue = Queue::new(store, None).await?;
```

## Who owns what

| Component | Owns |
|---|---|
| `Store` | Job records, state sets, locks, event log, stored metrics |
| `Queue` | Queue-wide defaults, event emitter, timer subsystem, live counters |
| `Worker` | Concurrency slots, lock renewal, stalled detection, per-worker metrics |
| `Job` | Payload, options, attempt counters, progress, result, stack traces |

Because state lives in the store, a `Queue` handle is cheap and disposable — clone it, or build a new
one pointing at the same name, and you are looking at the same jobs.
