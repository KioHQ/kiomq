---
title: "Getting started"
linkTitle: "Getting started"
group: "Start here"
weight: 10
lead: "Install KioMQ, satisfy the Tokio runtime requirements, and pick the feature flags you need."
---

## Requirements

- **Rust 2024 edition** (the crate is built with `edition = "2024"`).
- A **Tokio multi-thread runtime**. Workers spawn tasks and, for sync processors, blocking threads.
- **Redis** only if you use the `redis-store` backend.

## Installation

```bash
cargo add kiomq
```

Or add it by hand:

```toml
[dependencies]
kiomq = "0.2.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

## Cargo features

| Feature | Default | What it enables |
|---|---|---|
| `redis-store` | ✅ | `RedisStore`, `SharedRedis`, `Config` — durable, distributed queues |
| `tracing` | — | Emits [`tracing`](https://docs.rs/tracing) spans and events from the internals |

`InMemoryStore` is always available, with no feature flag and no external services.

To build without Redis:

```toml
[dependencies]
kiomq = { version = "0.2.1", default-features = false }
```

## Tokio runtime requirements

The multi-thread runtime is strongly recommended. Sync processors use
[`spawn_blocking`](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html), and worker
concurrency defaults to the logical CPU count — both of which assume more than one worker thread.

```toml
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

```rust
#[tokio::main] // defaults to the multi-thread runtime
async fn main() -> kiomq::KioResult<()> {
    // ...
    Ok(())
}
```

In tests, opt in explicitly:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn my_test() {
    // ...
}
```

> [!WARNING]
> On a current-thread runtime a long-running sync processor can starve the queue's timer
> subsystem. If you must use `flavor = "current_thread"`, stick to async processors.

## The three moving parts

**Store** — owns job state, locks, and the event log. Choose `InMemoryStore` for tests and
`RedisStore` for production. See [Backends](../backends/).

**Queue** — the handle you enqueue through. It holds the queue-wide defaults
(`attempts`, backoff, retention, event mode) and exposes metrics and event subscriptions.

**Worker** — reserves jobs from the store and runs your processor, up to `concurrency` at a time.
Many workers can share one queue, in one process or across machines.

```rust
use kiomq::{InMemoryStore, Queue};

// D = job payload, R = processor return value, P = progress type
//                            prefix (defaults to "kio")  queue name
let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "demo");
let queue = Queue::new(store, None).await?;
```

Both store constructors take an optional key **prefix** and a queue **name**. The prefix defaults to
`kio`, so the example above namespaces its collections under `kio:demo`. Two queues with the same
prefix and name share jobs — that is exactly how you scale out.

The three generic parameters flow through everything: `Job<D, R, P>`, `Queue<D, R, P, S>`, and
`EventParameters<R, P>`. Pick your own types once and the compiler keeps producers, processors, and
listeners in agreement.

Next: [build a working queue and worker](../quick-start/).
