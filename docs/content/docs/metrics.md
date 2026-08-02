---
title: "Process & worker metrics"
linkTitle: "Metrics"
group: "Guides"
weight: 90
lead: "Queue counters, process snapshots, and per-worker task timings — collected automatically, with no extra setup."
---

KioMQ collects three kinds of metrics in the background. Queue counters are in-memory and free to
read; process and worker metrics are published to the store with a TTL and refreshed by the queue's
timer subsystem.

## Queue counters

`queue.current_metrics` is a cheap, always-live handle. The counters are refreshed from the store
whenever `get_metrics()` is called, so between calls they may be slightly stale — prefer the helper
methods over reading raw fields.

```rust
let metrics = queue.current_metrics.clone();

metrics.all_jobs_completed(); // nothing left to do
metrics.is_idle();            // no work and no active jobs
metrics.has_active_jobs();    // at least one job in flight
metrics.has_delayed();        // something is scheduled for later
metrics.queue_has_work();     // jobs waiting or prioritised
metrics.queue_is_paused();    // paused flag
metrics.workers_idle();       // every worker is parked

// Pull fresh counts from the store.
let snapshot = queue.get_metrics().await?;
```

These are the right tool for graceful shutdown and for tests that need to wait until the queue drains.

## Process metrics

A process-level snapshot, refreshed every `PROCESS_METRIC_UPDATE_INTERVAL` milliseconds and stored
with a TTL. Snapshots are keyed by **PID**, so a multi-process deployment stays distinguishable.

```rust
// keyed by PID so multi-process deployments can be distinguished
let snapshots: std::collections::BTreeMap<u32, kiomq::ProcessMetrics> =
    queue.fetch_proess_metrics().await?;

if let Some(m) = snapshots.values().next() {
    println!(
        "cpu: {:.1}%  mem: {} MB  tokio workers: {}",
        m.process_cpu_usage,
        m.memory_usage / 1_024 / 1_024,
        m.rt_metrics.workers_count,
    );
}
```

Each snapshot captures:

| Field | What it holds |
|---|---|
| `process_cpu_usage` | CPU usage of the process tree, as a percentage |
| `memory_usage` | Resident set size in bytes |
| `memory_stats` | Heap allocator statistics via the built-in `Heapster` instrumented allocator |
| `rt_metrics` | Tokio runtime counters — thread count, live tasks, park counts, busy durations (`RawRuntimeMetrics`) |
| `workers` | `Vec<WorkerMeta>`: state, options, and active-job count for every registered worker |
| `hostname` / `pid` | Process identity |

> [!NOTE]
> `fetch_proess_metrics` is spelled with that typo in the public API. It is kept as-is for
> backwards compatibility.

## Worker metrics

Fine-grained, per-worker timing data for every in-flight job — useful for latency profiling and
capacity planning. Workers publish theirs every `metrics_update_interval` milliseconds (default
`100`).

```rust
let worker_metrics = queue.fetch_worker_metrics().await?;

for (worker_id, wm) in &worker_metrics {
    for task in &wm.tasks {
        println!(
            "worker {worker_id}  polls={}  idle={:?}",
            task.metrics.total_poll_count,
            task.metrics.total_idle_duration,
        );
    }
}
```

The per-task numbers come from [`tokio-metrics`](https://docs.rs/tokio-metrics), so you get poll
counts, idle durations, and scheduling delays for each job the worker is running.

## Exporting

Both `fetch_*` methods return plain maps, which makes wiring them into an exporter straightforward —
scrape them from a background task and publish to whatever your stack uses:

```rust
tokio::spawn({
    let queue = queue.clone();
    async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(15));
        loop {
            tick.tick().await;
            if let Ok(snapshots) = queue.fetch_proess_metrics().await {
                for (pid, m) in snapshots {
                    gauge!("kiomq.cpu", "pid" => pid.to_string()).set(m.process_cpu_usage);
                    gauge!("kiomq.rss", "pid" => pid.to_string()).set(m.memory_usage as f64);
                }
            }
            if let Ok(counters) = queue.get_metrics().await {
                gauge!("kiomq.idle").set(u8::from(counters.is_idle()));
            }
        }
    }
});
```

With `RedisStore`, metrics land in Redis with a TTL — so a separate dashboard process can read the
fleet's numbers without running a worker of its own.

## Storing your own snapshots

The store methods are public, should you want to publish metrics from code the workers don't own:

```rust
queue.store_worker_metrics(metrics, 30_000).await?;   // ttl in ms
queue.store_process_metrics(snapshot, 30_000).await?;
```
