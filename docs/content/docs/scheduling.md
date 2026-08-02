---
title: "Scheduling & priorities"
linkTitle: "Scheduling"
group: "Guides"
weight: 50
lead: "Delays, priority scores, cron expressions, and repeat policies — everything about when a job runs."
---

## Priorities

`priority` is a score where **lower runs first**, and `0` means "no priority" — the job takes the
ordinary `Wait` path.

```rust
use kiomq::JobOptions;

let urgent   = JobOptions { priority: 1,  ..Default::default() };
let ordinary = JobOptions { priority: 50, ..Default::default() };

queue.add_job("password-reset", payload, Some(urgent)).await?;
queue.add_job("weekly-digest", payload, Some(ordinary)).await?;
```

Prioritised jobs live in a sorted set and move to `Active` ahead of the plain wait list.

## Delays

`JobDelay` decides when a job first becomes eligible.

```rust
use kiomq::{JobDelay, JobOptions};

// Run as soon as a worker is free (the default).
JobDelay::TimeMilis(0);

// Run in five minutes.
let later = JobOptions {
    delay: JobDelay::TimeMilis(5 * 60 * 1_000),
    ..Default::default()
};

// Run at the next match of a cron expression.
use croner::Cron;
use std::str::FromStr;

let nightly = JobOptions {
    delay: JobDelay::FromCron(Box::new(Cron::from_str("0 3 * * *")?)),
    ..Default::default()
};
```

Delayed jobs sit in the delayed set until the queue's timer subsystem promotes them to `Wait`.

## Repeat policies

`Repeat` re-enqueues a job after each run. Four shapes are available, each with a constructor:

```rust
use kiomq::{BackOffJobOptions, JobOptions, Repeat};

// 1. Cron — every weekday at 07:30.
let cron = Repeat::from_cron_str("30 7 * * 1-5")?;

// 2. Fixed interval — every 30 seconds, at most 100 times.
//    Pass None for max_attempts to repeat forever.
let every = Repeat::repeat_every_for_times(30_000, Some(100));

// 3. Backoff-driven — the gap grows with each run.
let backing_off = Repeat::from_back_off(BackOffJobOptions::Number(1_000));

// 4. As fast as possible, bounded by a run count.
let hot_loop = Repeat::Immediately(10);

queue.add_job("sync-inventory", payload, Some(JobOptions {
    repeat: Some(cron),
    ..Default::default()
})).await?;
```

Set `QueueOpts::repeat` instead if every job on the queue should repeat by default.

| Variant | Constructor | Behaviour |
|---|---|---|
| `WithCron(Cron)` | `Repeat::from_cron_str("…")` | Re-run at the next cron occurrence |
| `Every { delay_ms, max_attempts }` | `Repeat::repeat_every_for_times(ms, max)` | Fixed interval, optionally bounded |
| `WithBackOff(BackOffJobOptions)` | `Repeat::from_back_off(opts)` | Interval computed by a backoff strategy |
| `Immediately(u64)` | — | Re-run at once, up to *n* times |

> [!WARNING]
> `Repeat::Immediately` re-enqueues with no delay. Always give it a bound, and prefer
> `Repeat::Every` when you actually want a polling loop.

## Cron expressions

Cron parsing is provided by [`croner`](https://docs.rs/croner). Standard five-field expressions work
as you would expect:

| Expression | Meaning |
|---|---|
| `* * * * *` | Every minute |
| `*/5 * * * *` | Every five minutes |
| `0 * * * *` | Hourly, on the hour |
| `30 7 * * 1-5` | 07:30, Monday to Friday |
| `0 0 1 * *` | Midnight on the first of the month |

`Repeat::from_cron_str` returns a `Result` — a malformed expression is a `CronError`, not a panic, so
validate operator-supplied schedules at the edge:

```rust
match Repeat::from_cron_str(&user_input) {
    Ok(repeat) => { /* enqueue */ }
    Err(err) => tracing::warn!(%err, "rejected schedule"),
}
```

## Delay vs. repeat

They compose, and they answer different questions:

- **`delay`** — *when does this job first become eligible?*
- **`repeat`** — *what happens after it finishes?*

A job with `delay: FromCron(…)` and no `repeat` runs once, at the next cron match. A job with
`repeat: WithCron(…)` keeps re-scheduling itself after every run. Use the latter for recurring work.
