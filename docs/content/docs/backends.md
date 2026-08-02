---
title: "Backends"
linkTitle: "Backends"
group: "Guides"
weight: 100
lead: "In-memory for tests, Redis for production fleets — both behind one Store trait."
---

Every store implements the same `Store<D, R, P>` trait, so switching backends is a change to two
lines of setup and nothing else.

| Store | Feature flag | Durable | Multi-process | Best for |
|---|---|---|---|---|
| `InMemoryStore` | always available | No | No | Tests, dev loops, short-lived in-process work |
| `RedisStore` | `redis-store` *(default)* | Yes | Yes | Production fleets spread across machines |

## In-memory

Ideal for tests, development, and short-lived tasks. No external dependencies, no serialisation
round-trip.

```rust
use kiomq::{InMemoryStore, Queue};

let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "demo");
let queue = Queue::new(store, None).await?;
```

State lives in the process, so jobs do not survive a restart and a second process sees an entirely
separate queue.

## Redis

The default feature. Durable, and shared by every process pointing at the same prefix and queue name.

Start a server:

```bash
docker run --rm -p 6379:6379 redis:latest
```

```rust
use kiomq::{Config, KioResult, Queue, RedisStore, SharedRedis};

#[tokio::main]
async fn main() -> KioResult<()> {
    // `Config` can be imported from `kiomq` or from `deadpool_redis`
    // (if you already use it in your app).
    let config = Config::default();
    let redis_conn = SharedRedis::create(&config)?;
    let store = RedisStore::new(None, "my-queue", &redis_conn).await?;
    let queue: Queue<(), (), (), _> = Queue::new(store, None).await?;
    // ... worker logic below here
    Ok(())
}
```

`Config` is re-exported from [`deadpool-redis`](https://docs.rs/deadpool-redis), so an app that
already builds a `deadpool_redis::Config` can hand the same value to KioMQ and share one pool.
`Config::default()` targets a local Redis on the default port; point it somewhere else with
`from_url`:

```rust
let mut cfg = Config::from_url("redis://127.0.0.1/");
let redis_conn = SharedRedis::create(&cfg)?;
```

For credentials, `kiomq::fetch_redis_pass()` reads the `REDIS_PASSWORD` environment variable, loading
a `.env` file via `dotenvy` first if one is present:

```rust
use kiomq::{Config, SharedRedis, fetch_redis_pass};

let url = match fetch_redis_pass() {
    Some(pass) => format!("redis://:{pass}@10.0.0.4:6379/0"),
    None => "redis://127.0.0.1/".to_string(),
};
let redis_conn = SharedRedis::create(&Config::from_url(url))?;
```

Note that `RedisStore` itself carries **no** generic parameters — the payload, result, and progress
types are pinned by the `Queue` it is handed to.

### Scaling out

There is no extra coordination step. Run the same binary on more machines with the same prefix and
queue name, and the store arbitrates:

```rust
// Every process: identical setup, one shared queue.
let store = RedisStore::new(Some("prod"), "transcode", &redis_conn).await?;
let queue = Queue::new(store, None).await?;
let worker = Worker::new_async(&queue, processor, Some(WorkerOpts {
    concurrency: 16,
    ..Default::default()
}))?;
worker.run()?;
```

Locks, stalled recovery, and the event log are all store-side, so a machine that dies has its
in-flight jobs recovered by the others — see [Core concepts](../core-concepts/#locks-and-stalled-jobs).

> [!TIP]
> Give each environment its own prefix (`Some("staging")`, `Some("prod")`). It keeps keys tidy and
> makes an accidental cross-environment `obliterate()` impossible.

### Events over Redis

With `QueueEventMode::Stream` (the default) events go to a Redis stream and can be replayed by a
consumer that attaches later; `PubSub` broadcasts without retention. Either way, a dashboard process
can subscribe without running a worker. See [Events](../events/#delivery-modes).

## Choosing a store per environment

A small type alias plus `cfg` keeps the switch in one place:

```rust
#[cfg(test)]
type AppStore = kiomq::InMemoryStore<Payload, Outcome, u8>;
#[cfg(not(test))]
type AppStore = kiomq::RedisStore; // types come from the Queue, not the store
```

Because processors are generic over `S: Store<D, R, P>`, the rest of your code — including the
processor itself — needs no changes.
