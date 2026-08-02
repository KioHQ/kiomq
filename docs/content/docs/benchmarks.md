---
title: "Benchmarks"
linkTitle: "Benchmarks"
group: "Project"
weight: 110
lead: "Run the Criterion suite locally — the only numbers worth trusting are the ones from your own hardware."
---

## Running the suite

Benchmarks use [Criterion](https://docs.rs/criterion):

```bash
cargo bench
```

Criterion writes an HTML report with plots and regression analysis:

```
target/criterion/report/index.html
```

## What is measured

The `queue_bench` harness covers the hot paths:

| Benchmark | What it exercises |
|---|---|
| `queue_bulk_add` | Enqueue throughput via `bulk_add` at several batch sizes |
| `queue_end_to_end_throughput` | Enqueue → reserve → process → settle, with a worker running |
| `queue_single_job_latency` | Time for one job to travel from `add_job` to `Completed` |

Run one group at a time by name:

```bash
cargo bench -- queue_bulk_add
```

## Comparing changes

Criterion stores a baseline between runs, so a second invocation reports the delta. To compare a
change against `main`:

```bash
git switch main && cargo bench -- --save-baseline main
git switch my-branch && cargo bench -- --baseline main
```

## Interpreting results

A few things to keep in mind before quoting any number:

- **The store dominates.** `InMemoryStore` benchmarks measure KioMQ's own overhead; `RedisStore`
  numbers are mostly network and Redis round-trips.
- **Concurrency is not throughput.** Raising `concurrency` past the point where your processor
  saturates CPU or I/O adds contention, not speed.
- **Idle cost is a feature.** Workers park on `Notify` rather than polling, so an empty queue should
  barely register in a CPU profile. That is worth measuring too.

> [!NOTE]
> Benchmarks that involve Redis need a server on `localhost:6379`. Start one with
> `docker run --rm -p 6379:6379 redis:latest` before running the suite.

## Profiling a real workload

For latency questions, the built-in metrics usually beat a microbenchmark — `fetch_worker_metrics`
gives you per-task poll counts and idle durations straight from `tokio-metrics`. See
[Metrics](../metrics/#worker-metrics).

The crate also builds Tokio with the `tracing` feature, so
[`tokio-console`](https://docs.rs/console-subscriber) can attach and show you exactly where worker
tasks spend their time.
