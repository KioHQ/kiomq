---
title: "Progress updates"
linkTitle: "Progress"
group: "Guides"
weight: 80
lead: "Report typed progress from inside a processor — persisted to the store and emitted as an event."
---

## Reporting progress

The third generic parameter of `Job<D, R, P>` is the **progress type**. It can be anything
serialisable: a percentage, a struct, an enum of named phases.

```rust
use std::sync::Arc;
use kiomq::{Job, KioError, Store};

async fn processor<S: Store<u64, u64, u8>>(
    store: Arc<S>,
    mut job: Job<u64, u64, u8>,
) -> Result<u64, KioError> {
    // update_progress persists to the store and emits a progress event.
    job.update_progress(50u8, store.as_ref()).await?; // 50% done
    Ok(job.data.unwrap_or_default() * 2)
}
```

Two things to note: the job must be **`mut`**, and the store handed to your processor is exactly what
`update_progress` needs — pass it as `store.as_ref()`.

In a sync processor, use the blocking variant:

```rust
fn processor<S: Store<u64, u64, u8>>(
    store: Arc<S>,
    mut job: Job<u64, u64, u8>,
) -> Result<u64, KioError> {
    job.update_progress_sync(50u8, store.as_ref())?;
    Ok(job.data.unwrap_or_default() * 2)
}
```

## Structured progress

A percentage is rarely enough for long jobs. Any `Serialize + Deserialize` type works, which lets you
report rich state:

```rust
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
struct Transcode {
    percentage: f64,
    fps: f32,
    bitrate_kbps: f32,
    size_kb: u32,
}

async fn transcode<S: Store<ProcessData, ReturnData, Transcode>>(
    store: Arc<S>,
    mut job: Job<ProcessData, ReturnData, Transcode>,
) -> Result<ReturnData, KioError> {
    while let Some(frame) = encoder.next().await {
        job.update_progress(Transcode {
            percentage: frame.percent,
            fps: frame.fps,
            bitrate_kbps: frame.bitrate,
            size_kb: frame.size_kb,
        }, store.as_ref()).await?;
    }
    Ok(ReturnData { /* … */ })
}
```

The [`video_transcoding` example](https://github.com/KioHQ/kiomq/tree/main/examples) does exactly
this with real ffmpeg output.

> [!TIP]
> Every call writes to the store. For a job emitting thousands of updates, throttle — report on
> whole-percentage changes, or at most every few hundred milliseconds.

## Consuming progress

Progress is delivered as an event, and the latest value is also persisted on the job itself.

```rust
use kiomq::{EventParameters, JobState};

// Live, as it happens.
queue.on(JobState::Progress, |evt| async move {
    if let EventParameters::Progress { job_id, data } = evt {
        tracing::info!(job_id, percent = data.percentage, "transcoding");
    }
});

// Or read the last known value on demand — e.g. from an HTTP handler.
if let Some(job) = queue.get_job(job_id).await {
    if let Some(progress) = job.progress {
        println!("{:.1}%", progress.percentage);
    }
}
```

That second form is what you want behind a status endpoint: no subscription to manage, and it works
from any process that can reach the store.

## Job logs

Alongside progress, each job carries a `logs: Vec<CompactString>` — arbitrary lines appended during
processing and kept with the record. They survive alongside the job for as long as your retention
policy keeps it, which makes them handy for post-mortems on failed jobs.
