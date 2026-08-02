---
title: "Events & observability"
linkTitle: "Events"
group: "Guides"
weight: 70
lead: "Subscribe to job state transitions, match on typed payloads, and choose between streamed and broadcast delivery."
---

## Subscribing

`Queue::on` listens to a single state; `Queue::on_all_events` listens to everything. Both return a
`Uuid` you can use to detach the listener later.

```rust
use kiomq::{EventParameters, JobState};

// One state.
let listener_id = queue.on(JobState::Completed, |evt| async move {
    if let EventParameters::Completed { job_id, job_metrics, .. } = evt {
        tracing::info!(job_id, ?job_metrics, "completed");
    }
});

// Everything. The type parameters are <R, P> — the processor's return type
// and the progress type.
let all_id = queue.on_all_events(|evt: EventParameters<u64, u8>| async move {
    tracing::debug!(?evt, "queue event");
});

// Detach when you no longer care.
queue.remove_event_listener(listener_id);
queue.remove_event_listener(all_id);
```

Callbacks are async: they return a future, which the queue drives. Keep them short — a slow listener
delays event delivery, so hand heavy work off to a spawned task.

## Event payloads

`EventParameters<R, P>` is an enum, one variant per transition:

| Variant | Fields |
|---|---|
| `Added` | `job_id`, `name` |
| `Prioritized` | `job_id`, `name`, `priority` |
| `Delayed` | `job_id`, `delay: Duration` |
| `WaitingToRun` | `job_id`, `prev_state` |
| `Active` | `job_id`, `prev_state` |
| `Processing` | `worker_id`, `job_id`, `status` |
| `Progress` | `job_id`, `data: P` |
| `Completed` | `job_id`, `job_metrics`, `expected_delay`, `prev_state`, `result: R` |
| `Failed` | `job_id`, `reason: FailedDetails`, `prev_state` |
| `Stalled` | `job_id`, `prev_state` |
| `Void` | — (placeholder, e.g. queue drained) |

A typical match:

```rust
queue.on_all_events(|evt: EventParameters<Receipt, u8>| async move {
    match evt {
        EventParameters::Completed { job_id, result, job_metrics, .. } => {
            // JobMetrics: ran_for, delayed_for, attempt, delay, id
            tracing::info!(job_id, ran_for = ?job_metrics.ran_for, ?result, "charged");
        }
        EventParameters::Failed { job_id, reason, .. } => {
            tracing::error!(job_id, reason = %reason.reason, "charge failed");
        }
        EventParameters::Stalled { job_id, prev_state } => {
            tracing::warn!(job_id, ?prev_state, "recovered a stalled job");
        }
        EventParameters::Progress { job_id, data } => {
            tracing::trace!(job_id, percent = data, "progress");
        }
        _ => {}
    }
});
```

> [!NOTE]
> `Completed` carries the processor's return value by value. If `R` is large, prefer reading what you
> need inside the listener over cloning the whole thing.

## Delivery modes

`QueueOpts::event_mode` picks how events reach listeners.

| Mode | Behaviour | Use when |
|---|---|---|
| `Stream` *(default)* | Persistent, append-only. Late subscribers can replay past events. | A consumer must catch up after a restart |
| `PubSub` | Broadcast only. Events fired before a listener attached are lost. | You only care about live events and don't want retention |

```rust
use kiomq::{QueueEventMode, QueueOpts};

let queue = Queue::new(store, Some(QueueOpts {
    event_mode: Some(QueueEventMode::PubSub),
    ..Default::default()
})).await?;
```

With `RedisStore`, `Stream` maps onto a Redis stream and `PubSub` onto Redis pub/sub — so a separate
process (a dashboard, an alerting service) can consume the same events without holding a `Worker`.

## Emitting your own events

`Queue::emit` publishes an event as if the queue had produced it. This is mostly useful in tests and
in bridge code that mirrors external state into the queue's event log.

```rust
use kiomq::{EventParameters, JobState};

queue.emit(JobState::Progress, EventParameters::Progress {
    job_id: 42,
    data: 75u8,
}).await;
```

## Tracing integration

Enable the `tracing` feature to get spans and events from the internals — worker loops, lock renewal,
stalled recovery, and store calls:

```toml
[dependencies]
kiomq = { version = "0.2.1", features = ["tracing"] }
```

```rust
tracing_subscriber::fmt()
    .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
    .init();
```

```bash
RUST_LOG=kiomq=debug cargo run
```

For live task inspection, KioMQ's examples pair well with
[`console-subscriber`](https://docs.rs/console-subscriber) and `tokio-console`, since the crate builds
Tokio with the `tracing` feature.
