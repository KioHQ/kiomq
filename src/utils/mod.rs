use crate::Counter;
use crate::error::{BacktraceCatcher, CaughtError, CaughtPanicInfo, JobError, QueueError};
use crate::events::QueueStreamEvent;
use crate::stores::Store;
use crate::timers::{TimerSender, TimerType};
use crate::worker::{
    JobMap, MIN_DELAY_MS_LIMIT, ProcessingQueue, TaskHandle, WorkerCallback, WorkerState,
};
use crate::{
    EventEmitter, EventParameters, FailedDetails, JobOptions, JobState, JobToken, KioError,
    QueueEventMode, QueueOpts, Trace, WorkerOpts,
};
use chrono::Utc;
use compact_str::ToCompactString;
#[cfg(feature = "redis-store")]
use compact_str::{CompactString, format_compact};
use crossbeam::atomic::AtomicCell;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use parking_lot::Mutex;
#[cfg(feature = "redis-store")]
use redis::ParsingError;
#[cfg(feature = "redis-store")]
use redis::aio::ConnectionLike;
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio_metrics::TaskMonitor;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "tracing")]
use tracing::{debug, info};
use uuid::Uuid;
mod concurrent_structures;
pub use concurrent_structures::ConcurrentDeque;
pub mod processor_types;
use crate::KioResult;
use crate::MoveToActiveResult;
use crate::{Job, ProcessedResult, Queue};

use crate::metrics::{HISTOGRAM_MAX_NS, HISTOGRAM_SIGFIG};
use hdrhistogram::Histogram;
use std::sync::Arc;

#[cfg(feature = "redis-store")]
/// Reads the Redis password from the `REDIS_PASSWORD` environment variable.
///
/// Loads a `.env` file via `dotenvy` if one is present before reading the
/// variable.  Returns `None` when the variable is unset.
#[must_use]
pub fn fetch_redis_pass() -> Option<String> {
    // A missing `.env` is expected (env vars may be set directly), so ignore
    // that specific error rather than propagate it.
    if let Err(_err) = dotenvy::dotenv() {
        // no-op: continue with the process environment
    }
    std::env::var("REDIS_PASSWORD").ok()
}

#[cfg(feature = "redis-store")]
/// A utily function thats serializes an Object/Map  into  a Vector of key-value pair strings.
pub fn serialize_into_pairs<V: Serialize>(item: &V) -> Vec<(String, String)> {
    use simd_json::BorrowedValue;
    if let Ok(BorrowedValue::Object(obj)) = simd_json::serde::to_borrowed_value(item) {
        return obj
            .into_iter()
            .flat_map(|(key, val)| {
                simd_json::to_string_pretty(&val).map(|val| (key.to_string(), val))
            })
            .collect();
    }
    vec![]
}
pub const fn calculate_next_priority_score(priority: u64, prio_counter: u64) -> u64 {
    // Priority occupies the high 32 bits; the FIFO tie-break counter the low 32.
    // The mask must stay within 32 bits or the counter bleeds into the priority
    // band and corrupts ordering.
    (priority << 32) + (prio_counter & 0xffff_ffff)
}

use crate::{CollectionSuffix, QueueMetrics};
#[cfg(feature = "redis-store")]
/// Reads all queue-state counters from Redis in a single atomic pipeline call.
///
/// Queries the number of jobs in each state (active, waiting, delayed,
/// completed, failed, paused, prioritized, stalled) as well as the current
/// highest job ID and processing count, then returns them as a [`QueueMetrics`]
/// snapshot.
///
/// # Errors
///
/// Returns [`KioError`] if the pipeline execution fails.
pub async fn get_queue_metrics<C: redis::aio::ConnectionLike>(
    prefix: &str,
    name: &str,
    conn: &mut C,
) -> KioResult<QueueMetrics> {
    let [
        job_id_key,
        stalled_key,
        active_key,
        completed_key,
        meta_key,
        delayed_key,
        _priority_counter_key,
        waiting_key,
        paused_key,
        prioritized_key,
        failed_key,
    ] = [
        CollectionSuffix::Id,
        CollectionSuffix::Stalled,
        CollectionSuffix::Active,
        CollectionSuffix::Completed,
        CollectionSuffix::Meta,
        CollectionSuffix::Delayed,
        CollectionSuffix::PriorityCounter,
        CollectionSuffix::Wait,
        CollectionSuffix::Paused,
        CollectionSuffix::Prioritized,
        CollectionSuffix::Failed,
    ]
    .map(|key| key.to_collection_name(prefix, name));
    let mut pipeline = redis::pipe();
    pipeline.atomic();
    pipeline.zcard(completed_key.as_str());
    pipeline.zcard(failed_key.as_str());
    pipeline.zcard(prioritized_key.as_str());
    pipeline.llen(active_key.as_str());
    pipeline.scard(stalled_key.as_str());
    pipeline.zcard(delayed_key.as_str());
    pipeline.llen(waiting_key.as_str());
    pipeline.llen(paused_key.as_str());
    pipeline.get(job_id_key.as_str());
    pipeline.hget(meta_key.as_str(), "processing");
    pipeline.hget(meta_key.as_str(), "event_mode");
    pipeline.hexists(meta_key.as_str(), JobState::Paused);
    let (
        completed,
        failed,
        prioritized,
        active,
        stalled,
        delayed,
        waiting,
        paused,
        last_id,
        processing,
        event_mode,
        is_paused,
    ): (
        Option<u64>,
        Option<u64>,
        Option<u64>,
        Option<u64>,
        Option<u64>,
        Option<u64>,
        Option<u64>,
        Option<u64>,
        Option<u64>,
        Option<u64>,
        Option<QueueEventMode>,
        bool,
    ) = pipeline.query_async(conn).await?;

    Ok(QueueMetrics::new(
        last_id.unwrap_or_default(),
        processing.unwrap_or_default(),
        active.unwrap_or_default(),
        stalled.unwrap_or_default(),
        completed.unwrap_or_default(),
        delayed.unwrap_or_default(),
        prioritized.unwrap_or_default(),
        paused.unwrap_or_default(),
        failed.unwrap_or_default(),
        waiting.unwrap_or_default(),
        is_paused,
        event_mode.unwrap_or_default(),
    ))
}

// ---- UTIL FUNCTIONS for the worker
pub async fn process_job<D, R, P, S>(
    job: Job<D, R, P>,
    token: JobToken,
    jobs_in_progress: JobMap<D, R, P>,
    queue: Arc<Queue<D, R, P, S>>,
    callback: WorkerCallback<D, R, P, S>,
    _permit: OwnedSemaphorePermit,
    worker_id: Uuid,
    current_job_current: Arc<AtomicCell<usize>>,
) -> KioResult<()>
where
    R: Serialize + Send + Clone + DeserializeOwned + 'static + Sync,
    D: Clone + Serialize + DeserializeOwned + Send + 'static + Sync,
    P: Clone + Serialize + DeserializeOwned + Send + 'static + Sync,
    S: Clone + Store<D, R, P> + Send + 'static + Sync,
{
    use crate::JobState;
    use crate::worker::WorkerCallback;
    let job_id = job.id.unwrap_or_default();
    let job_added_at = job.ts;
    let processed_on = job.processed_on;
    let attempts_made = job.attempts_made + 1;
    let mut metrics = job.get_metrics().unwrap_or_default();
    metrics.attempt = attempts_made;
    let mut task_queue = None;

    let returned = match callback {
        WorkerCallback::Sync(cb) => {
            let store = queue.store.clone();

            BacktraceCatcher::catch(tokio::task::spawn_blocking(move || cb(store, job)))
                .await
                .and_then(|e| {
                    e.map_err(|err| {
                        let backtrace = async_backtrace::backtrace();
                        CaughtError::Error(Box::new(err), backtrace)
                    })
                })
        }
        WorkerCallback::Async(cb) => {
            let store = queue.store.clone();
            let callback = cb(store, job);
            BacktraceCatcher::catch(callback).await
        }
    };
    match returned {
        Ok(result) => {
            let now = Utc::now();
            let ts = now.timestamp_micros();
            let move_to_state = JobState::Completed;

            if let Some(processed_at) = processed_on {
                metrics.id = job_id;
                let delayed_for = (processed_at - job_added_at).to_std().unwrap_or_default();
                let ran_for = (now - processed_at).to_std().unwrap_or_default();
                metrics.delayed_for = delayed_for;
                metrics.ran_for = ran_for;
            }

            let completed = queue
                .move_job_to_finished_or_failed(
                    job_id,
                    ts,
                    token,
                    move_to_state,
                    crate::ProcessedResult::Success(result, metrics),
                    None,
                )
                .await?;
            if let Some(entry) = jobs_in_progress.remove(&job_id) {
                let (job, _, handle, _, _, _) = entry.value();
                if completed.attempts_made < job.opts.attempts {
                    if let Some(repeat_opts) = completed.opts.repeat.as_ref() {
                        //dbg!("job here", job_id, &repeat_opts);
                        queue
                            .retry_job(job_id, repeat_opts, completed.attempts_made - 1)
                            .await?;
                    }
                } else {
                    queue
                        .clean_up_job(job_id, job.opts.remove_on_complete)
                        .await?;
                }
                let stored_handle = handle.load_full();
                if let Some(handle) = stored_handle {
                    let _handle_id = task_queue.replace((handle.id(), job_id, move_to_state));
                }
            }
        }
        Err(err) => {
            let (failed_reason, backtrace) = match err {
                CaughtError::Panic(CaughtPanicInfo { backtrace, payload }) => (payload, backtrace),
                CaughtError::Error(error, backtrace) => (error.to_compact_string(), backtrace),
                CaughtError::JoinError(join_error) => (join_error.to_compact_string(), None),
            };
            let backtrace: Option<Vec<CompactString>> = backtrace.map(|trace| {
                trace
                    .iter()
                    .map(|trace| trace.to_compact_string())
                    .collect()
            });
            let reason = failed_reason.clone();
            let frames = backtrace.map(|frames| Trace {
                run: attempts_made,
                reason,
                frames,
            });
            let failed_reason = FailedDetails {
                run: attempts_made,
                reason: failed_reason,
            };
            // move job to failed_state

            let ts = Utc::now().timestamp_micros();
            let move_to_state = JobState::Failed;
            let failed_job = queue
                .move_job_to_finished_or_failed(
                    job_id,
                    ts,
                    token,
                    move_to_state,
                    ProcessedResult::Failed(failed_reason),
                    frames,
                )
                .await?;
            if let Some(entry) = jobs_in_progress.remove(&job_id) {
                let (job, _, handle, _, _, _) = entry.value();
                // retry failed jobs
                if failed_job.attempts_made < job.opts.attempts
                    && let Some(backoff_job_opts) = job.opts.backoff.as_ref()
                {
                    queue
                        .retry_job(job_id, backoff_job_opts, failed_job.attempts_made - 1)
                        .await?;
                }
                // clean up if the number of attempts is exhausted
                if failed_job.attempts_made == job.opts.attempts {
                    queue.clean_up_job(job_id, job.opts.remove_on_fail).await?;
                }

                let stored_handle = handle.load_full();
                if let Some(handle) = stored_handle {
                    task_queue.replace((handle.id(), job_id, move_to_state));
                }
            }
        }
    }

    if let Some((key, job_id, state)) = task_queue.take() {
        let _ = current_job_current.fetch_sub(1);
        queue
            .update_processing_count(false, worker_id, job_id, state)
            .await?;

        #[cfg(feature = "tracing")]
        debug!("processed job {job_id} in task({key}) to state: {state}");
        let _ = key;
    }
    Ok(())
}
pub async fn get_next_job<D, R, P, S>(
    queue: &Queue<D, R, P, S>,
    token: JobToken,
    _block_delay: u64,
    closed: bool,
    opts: &WorkerOpts,
    passed_id: Option<u64>,
) -> KioResult<Option<Job<D, R, P>>>
where
    D: DeserializeOwned + Clone + Serialize + Send + 'static + Sync,
    R: DeserializeOwned + Clone + Serialize + Send + 'static + Sync,
    P: DeserializeOwned + Clone + Serialize + Send + 'static + Sync,
    S: Clone + Store<D, R, P> + Send + 'static + Sync,
{
    if closed {
        return Ok(None);
    }
    if let Some(job_id) = passed_id {
        let ts = Utc::now().timestamp_micros().cast_unsigned();
        let prev_state = JobState::Wait;
        let job = queue
            .prepare_job_for_processing(token, job_id, ts, opts, prev_state)
            .await?;
        return Ok(Some(job));
    }

    if let MoveToActiveResult::ProcessJob(job) = queue.move_to_active(token, opts).await? {
        return Ok(Some(*job));
    }

    Ok(None)
}
#[cfg(feature = "tracing")]
type MainLoopParams<D, R, P, S> = (
    tracing::Span,
    Uuid,
    Arc<CancellationToken>,
    ProcessingQueue,
    WorkerOpts,
    Counter,
    JobMap<D, R, P>,
    Arc<AtomicCell<usize>>,
    WorkerCallback<D, R, P, S>,
    Arc<Queue<D, R, P, S>>,
    Arc<AtomicCell<WorkerState>>,
    Arc<Notify>,
);

#[cfg(not(feature = "tracing"))]
type MainLoopParams<D, R, P, S> = (
    Uuid,
    Arc<CancellationToken>,
    ProcessingQueue,
    WorkerOpts,
    Counter,
    JobMap<D, R, P>,
    Arc<AtomicCell<usize>>,
    WorkerCallback<D, R, P, S>,
    Arc<Queue<D, R, P, S>>,
    Arc<AtomicCell<WorkerState>>,
    Arc<Notify>,
);
#[async_backtrace::framed]
pub async fn main_loop<D, R, P, S>(params: MainLoopParams<D, R, P, S>) -> KioResult<()>
where
    D: Clone + DeserializeOwned + 'static + Send + Sync + Serialize,
    R: Clone + DeserializeOwned + 'static + Serialize + Send + Sync,
    P: Clone + DeserializeOwned + 'static + Send + Sync + Serialize,
    S: Clone + Store<D, R, P> + 'static + Send + Sync,
{
    #[cfg(feature = "tracing")]
    let (
        _resource_span,
        id,
        cancellation_token,
        processing,
        opts,
        block_until,
        jobs_in_progress,
        active_job_count,
        processor,
        queue,
        worker_state,
        paused_here,
    ) = params;

    #[cfg(not(feature = "tracing"))]
    let (
        id,
        cancellation_token,
        processing,
        opts,
        block_until,
        jobs_in_progress,
        active_job_count,
        processor,
        queue,
        worker_state,
        paused_here,
    ) = params;

    #[cfg(feature = "tracing")]
    info!(
        "Worker Starting with concurrency set to {}",
        opts.concurrency
    );
    let semaphore = Arc::new(Semaphore::new(opts.concurrency));
    queue.register_worker_timers(opts).await;

    while !cancellation_token.is_cancelled() {
        if queue.pause_workers.load() {
            #[cfg(feature = "tracing")]
            info!(
                "pausing Worker ({id}) with  {delayed} delayed_jobs and {processing} running_jobs",
                delayed = queue.current_metrics.delayed.load(),
                processing = processing.len(),
            );
            worker_state.store(WorkerState::Idle);
            if cancellation_token
                .run_until_cancelled(paused_here.notified())
                .await
                .is_none()
            {
                #[cfg(feature = "tracing")]
                info!("... breaking loop to close paused worker");
                break;
            }
            worker_state.store(WorkerState::Active);
            #[cfg(feature = "tracing")]
            {
                info!("resumed worker");
            }
        }
        if semaphore.available_permits() == 0 || processing.len() >= opts.concurrency {
            tokio::task::yield_now().await;
            continue;
        }

        let Ok(permit) = semaphore.clone().acquire_owned().await else {
            break; // semaphore is closed or dropped
        };

        let token_prefix = active_job_count.load();
        let next_id = Uuid::new_v4();
        let token = JobToken(id, next_id, token_prefix as u64);
        let worker_id = id;
        let block_delay = block_until.load();

        let next_job_result = get_next_job(
            queue.as_ref(),
            token,
            block_delay,
            cancellation_token.is_cancelled(),
            &opts,
            None,
        )
        .await;

        if let Ok(Some(job)) = next_job_result {
            if let Some(id) = job.id {
                let monitor = TaskMonitor::new();
                let state = job.state;
                let callback = processor.clone();

                queue
                    .update_processing_count(true, worker_id, id, state)
                    .await?;

                let process_fn = process_job(
                    job.clone(),
                    token,
                    jobs_in_progress.clone(),
                    queue.clone(),
                    callback,
                    permit,
                    worker_id,
                    active_job_count.clone(),
                );
                let poll_histogram = Mutex::new(
                    Histogram::new_with_max(HISTOGRAM_MAX_NS, HISTOGRAM_SIGFIG).unwrap(),
                );

                jobs_in_progress.insert(
                    id,
                    (
                        job,
                        token,
                        TaskHandle::default(),
                        monitor.clone(),
                        poll_histogram,
                        opts,
                    ),
                );
                let task = processing
                    .spawn(monitor.instrument(async_backtrace::frame!(process_fn.boxed())));

                if let Some(re) = jobs_in_progress.get(&id) {
                    let (_, _, stored_handle, _, _, _) = re.value();
                    stored_handle.swap(Some(task.into()));
                }
            }
            tokio::task::yield_now().await;
        } else {
            drop(permit);
            // BACKOFF: Prevents the loop from immediately spinning at 100% CPU when the queue is empty.
            tokio::time::sleep(tokio::time::Duration::from_millis(MIN_DELAY_MS_LIMIT)).await;
        }
    }
    if cancellation_token.is_cancelled() {
        processing.wait().await;
        worker_state.store(WorkerState::Closed);
    }
    #[cfg(feature = "tracing")]
    info!("Worker Closed");
    Ok(())
}
use crate::Dt;
use chrono::TimeDelta;

pub async fn promote_jobs<D, R, P, S: Store<D, R, P> + Send + 'static + Clone + Sync>(
    queue: &Queue<D, R, P, S>,
    date_time: Dt,
    interval_ms: i64,
    timer_sender: &TimerSender,
) -> KioResult<()>
where
    D: Clone + Serialize + DeserializeOwned + Send + 'static + Sync,
    R: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
    P: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
{
    if !queue.current_metrics.as_ref().has_delayed() {
        return Ok(());
    }
    let start = date_time.timestamp_millis();
    let stop = (date_time + TimeDelta::milliseconds(interval_ms)).timestamp_millis();
    let (jobs, missed_deadline): (Vec<u64>, Vec<u64>) =
        queue.store.get_delayed_at(start, stop).await?;
    if !jobs.is_empty() {
        for job_id in jobs {
            timer_sender
                .send(TimerType::PromotedDelayed(job_id, queue.id))
                .await;
        }
    }
    if !missed_deadline.is_empty() {
        let ts = date_time.timestamp_micros();
        let move_to_state = JobState::Failed;
        let mut task_queue: FuturesUnordered<_> = FuturesUnordered::new();
        for job_id in &missed_deadline {
            let queue_clone = queue.clone();
            task_queue.push(
                async move {
                    let mut reason = FailedDetails {
                        run: 0,
                        reason: JobError::MissedDelayDeadline.to_compact_string(),
                    };
                    let attempts = queue
                        .store
                        .get_counter(CollectionSuffix::Job(*job_id), Some("attemptsMade"))
                        .await;
                    let token = queue.store.get_token(*job_id).await;
                    let attempts = attempts.unwrap_or_default();
                    let token = token.unwrap_or_default();
                    reason.run = attempts;
                    queue_clone
                        .move_job_to_finished_or_failed(
                            *job_id,
                            ts,
                            token,
                            move_to_state,
                            ProcessedResult::Failed(reason),
                            None,
                        )
                        .await?;
                    Ok::<(), KioError>(())
                }
                .boxed(),
            );
        }
        while let Some(_err) = task_queue.next().await {}
    }
    Ok(())
}
#[cfg(feature = "redis-store")]
/// Utilily function for pipelining
pub fn prepare_for_insert<D: Serialize, R: Serialize, P: Serialize>(
    queue_name: &str,
    event_mode: QueueEventMode,
    is_paused: bool,
    id: u64,
    prior_counter: u64,
    opts: JobOptions,
    job: &mut Job<D, R, P>,
    name: &str,
    pipeline: &mut redis::Pipeline,
) -> KioResult<()> {
    let JobOptions {
        priority,
        ref delay,
        id: _,
        attempts: _,
        remove_on_fail: _,
        remove_on_complete: _,
        backoff: _,
        repeat: _,
    } = opts;
    let dt = Utc::now();
    let expected_dt_ts = delay.next_occurrance_timestamp_ms();
    let delay = delay.as_diff_ms(dt).cast_unsigned();
    job.add_opts(opts);
    if delay > 0 && delay < MIN_DELAY_MS_LIMIT {
        return Err(QueueError::DelayBelowAllowedLimit {
            limit_ms: MIN_DELAY_MS_LIMIT,
            current_ms: delay,
        }
        .into());
    }
    //queue.job_count.
    let job_key = format_compact!("{queue_name}:{id}");
    let events_keys = format_compact!("{queue_name}:events");

    let waiting_or_paused = if is_paused {
        CollectionSuffix::Paused
    } else {
        CollectionSuffix::Wait
    };
    let to_delay = delay > 0;
    let to_priorize = priority > 0 && !to_delay;
    let waiting_key = format_compact!("{queue_name}:{waiting_or_paused}").to_lowercase();
    pipeline.atomic();
    if to_delay {
        let delayed_key = format_compact!("{queue_name}:delayed");
        if let Some(expected_active_time) = expected_dt_ts {
            pipeline.zadd(delayed_key.as_str(), id, expected_active_time);
            job.state = JobState::Delayed;
        }
    }
    // handle prioritized_jobs
    else if to_priorize {
        let prioritized_key =
            format_compact!("{queue_name}:{}", CollectionSuffix::Prioritized).to_lowercase();
        let score = calculate_next_priority_score(priority, prior_counter);
        pipeline.zadd(prioritized_key.as_str(), id, score);
        job.state = JobState::Prioritized;
    } else {
        pipeline.lpush(waiting_key.as_str(), id.to_compact_string().as_str());
    }
    job.id = Some(id);
    let fields = serialize_into_pairs(&job);
    pipeline.hset_multiple(job_key.as_str(), &fields);
    let event = if to_delay {
        JobState::Delayed
    } else if to_priorize {
        JobState::Prioritized
    } else {
        JobState::Wait
    };
    match event_mode {
        QueueEventMode::PubSub => {
            let mut event = QueueStreamEvent::<R, P> {
                job_id: id,
                event,
                name: Some(name.to_compact_string()),
                ..Default::default()
            };
            if to_delay {
                event.delay = Some(delay);
            }
            if to_priorize {
                event.priority = Some(priority);
            }
            pipeline.publish(events_keys.as_str(), event);
        }
        QueueEventMode::Stream => {
            let mut items = vec![
                ("event", event.to_string().to_lowercase()),
                ("job_id", id.to_string()),
                ("name", name.to_string()),
            ];
            if to_delay {
                items.push(("delay", delay.to_string()));
            }
            if to_priorize {
                items.push(("priority", priority.to_string()));
            }
            pipeline.xadd(events_keys.as_str(), "*", &items);
        }
    }
    Ok(())
}

pub type ReadStreamArgs<'a, R, P> = (
    QueueEventMode,
    usize,
    &'a EventEmitter<R, P>,
    Arc<QueueMetrics>,
);
// Helper function to process events from our queue-redis-stream
pub async fn process_queue_events<D, R, P, S: Store<D, R, P> + Send>(
    (event_mode, block_interval, emitter, metrics): ReadStreamArgs<'_, R, P>,
    store: &S,
) -> KioResult<()>
where
    D: DeserializeOwned + Clone + Send + 'static,
    R: DeserializeOwned + Clone + Send + Sync + 'static,
    P: DeserializeOwned + Clone + Send + Sync + 'static,
{
    store
        .listen_to_events(
            event_mode,
            Some(u64::try_from(block_interval).unwrap_or(u64::MAX)),
            emitter,
            &metrics,
        )
        .await
}
pub async fn process_each_event<D, R, P>(
    event: QueueStreamEvent<R, P>,
    emitter: &EventEmitter<R, P>,
    store: &(impl Store<D, R, P> + Send),
    metrics: &QueueMetrics,
) -> KioResult<()>
where
    D: DeserializeOwned + Clone + Send + 'static,
    R: DeserializeOwned + Clone + Send + Sync + 'static,
    P: DeserializeOwned + Clone + Send + Sync + 'static,
{
    let state = event.event;
    let param = EventParameters::<R, P>::from_queue_event(event)?;
    emitter.emit(state, param).await;
    if let Ok(updated) = store.get_metrics().await {
        metrics.update(&updated);
    }
    Ok(())
}
pub fn resume_helper(
    current_metrics: &QueueMetrics,
    pause_workers: &AtomicCell<bool>,
    worker_notifier: &Notify,
) {
    let workers_paused = pause_workers.load();
    if current_metrics.queue_has_work() && workers_paused {
        worker_notifier.notify_waiters();
        pause_workers.store(false);
    }
}

#[cfg(feature = "redis-store")]
use redis::Pipeline;
#[cfg(feature = "redis-store")]
fn split_pipeline(mut p: Pipeline, chunk_size: usize) -> Vec<Pipeline> {
    // Take ownership of the internal command list
    let cmds = unsafe {
        // Access private field via raw pointer trick
        let cmds_ptr = (&raw mut p).cast::<Vec<redis::Cmd>>();
        std::mem::take(&mut *cmds_ptr)
    };
    cmds.chunks(chunk_size)
        .map(|chunk| {
            let mut p = redis::Pipeline::with_capacity(chunk_size);
            for c in chunk {
                p.add_command(c.clone());
            }
            p
        })
        .collect()
}
#[cfg(feature = "redis-store")]
pub async fn query_all_batched<C: ConnectionLike + Clone>(
    conn: &C,
    p: Pipeline,
) -> redis::RedisResult<()>
where
{
    let chunk_size = 10000;
    let pipelines = split_pipeline(p, chunk_size);
    let futs = pipelines.into_iter().map(|p| {
        let mut c = conn.clone();
        async move { p.query_async::<()>(&mut c).await }
    });
    for res in futs {
        res.await?;
    }
    Ok(())
}
pub fn update_job_opts(queue_opts: &QueueOpts, opts: &mut JobOptions) {
    if opts.remove_on_complete.is_none() {
        opts.remove_on_complete = queue_opts.remove_on_complete;
    }
    if opts.remove_on_fail.is_none() {
        opts.remove_on_fail = queue_opts.remove_on_fail;
    }
    if opts.attempts < queue_opts.attempts {
        opts.attempts = queue_opts.attempts;
    }
    if opts.backoff.is_none() {
        opts.backoff.clone_from(&queue_opts.default_backoff);
    }
    if opts.repeat.is_none() {
        opts.repeat.clone_from(&queue_opts.repeat);
    }
}
/// utily function to create `stream_handles`
pub async fn create_listener_handle<D, R, P, S>(
    store: &S,
    emitter: EventEmitter<R, P>,
    notifier: Arc<Notify>,
    metrics: Arc<QueueMetrics>,
    pause_workers: Arc<AtomicCell<bool>>,
    event_mode: QueueEventMode,
) -> BoxFuture<'static, KioResult<()>>
where
    D: Clone + Serialize + DeserializeOwned + Send + 'static,
    R: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
    S: Clone + Store<D, R, P> + Send + 'static + Sync,
    P: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
{
    let store = store.clone();

    async move {
        let block_interval = 5000; // 100 seconds
        let notifier = notifier.clone();
        let is_inital: AtomicCell<bool> = AtomicCell::new(true);

        loop {
            let args: ReadStreamArgs<R, P> =
                (event_mode, block_interval, &emitter, metrics.clone());
            #[cfg(feature = "tracing")]
            let event_processing_task = {
                use tracing::{Instrument, info_span};
                let queue_name = format!("{}:{}", store.queue_prefix(), store.queue_name());
                let span = info_span!(
                    parent: None,
                    "",
                    queue_name
                );
                process_queue_events(args, &store).instrument(span)
            };
            #[cfg(feature = "tracing")]
            event_processing_task.await?;
            #[cfg(not(feature = "tracing"))]
            process_queue_events(args, &store).await?;
            pause_or_resume_workers(&notifier, &metrics, &pause_workers, &is_inital);
            tokio::task::yield_now().await;
        }

        #[allow(unreachable_code)]
        Ok(())
    }
    .boxed()
}
pub fn pause_or_resume_workers(
    notifier: &Notify,
    metrics: &QueueMetrics,
    pause_workers: &AtomicCell<bool>,
    is_initial: &AtomicCell<bool>,
) {
    if is_initial.load() {
        let _ = is_initial.compare_exchange(true, false);
        return;
    }

    if metrics.is_idle() {
        if pause_workers.compare_exchange(false, true).is_ok() {
            #[cfg(feature = "tracing")]
            info!("sent pause signal to workers");
        }
    } else {
        resume_helper(metrics, pause_workers, notifier);
    }
}

#[cfg(feature = "redis-store")]
#[allow(clippy::needless_pass_by_value)]
pub fn to_redis_parsing_error(err: impl ToString) -> ParsingError {
    ParsingError::from(err.to_string())
}

#[cfg(test)]
mod priority_score_tests {
    use super::calculate_next_priority_score;

    #[test]
    fn tie_break_counter_never_bleeds_into_priority() {
        // Priority packs into the high 32 bits, the FIFO tie-break counter into
        // the low 32 bits. A job's tie-break counter must never change which
        // priority band it lands in, otherwise a lower-priority job with a large
        // counter can outrank a higher-priority job.
        let high_priority = calculate_next_priority_score(2, 0);
        // Counter just past the 32-bit boundary on the lower priority.
        let low_priority_big_counter = calculate_next_priority_score(1, u64::from(u32::MAX) + 1);

        assert!(
            low_priority_big_counter < high_priority,
            "priority 1 (counter {}) must always sort below priority 2, got {low_priority_big_counter} >= {high_priority}",
            u64::from(u32::MAX) + 1,
        );
    }

    #[test]
    fn priority_occupies_the_high_thirty_two_bits() {
        // With a zero counter the score is exactly `priority << 32`.
        assert_eq!(calculate_next_priority_score(0, 0), 0);
        assert_eq!(calculate_next_priority_score(1, 0), 1u64 << 32);
        assert_eq!(calculate_next_priority_score(7, 0), 7u64 << 32);
    }

    #[test]
    fn counter_occupies_the_low_thirty_two_bits() {
        // With zero priority the score is exactly the masked counter.
        assert_eq!(calculate_next_priority_score(0, 123), 123);
        assert_eq!(
            calculate_next_priority_score(0, u64::from(u32::MAX)),
            u64::from(u32::MAX)
        );
    }

    #[test]
    fn counter_wraps_within_its_thirty_two_bit_band() {
        // The counter is masked with 0xffff_ffff, so exactly 2^32 wraps to 0 and
        // 2^32 + 1 wraps to 1 within the same priority band.
        let base = calculate_next_priority_score(3, 0);
        assert_eq!(
            calculate_next_priority_score(3, u64::from(u32::MAX) + 1),
            base,
            "counter 2^32 must wrap to 0 within the priority band"
        );
        assert_eq!(
            calculate_next_priority_score(3, u64::from(u32::MAX) + 2),
            base + 1,
            "counter 2^32 + 1 must wrap to 1 within the priority band"
        );
    }

    #[test]
    fn higher_priority_always_outranks_lower_regardless_of_counter() {
        // Exhaustively assert the ordering invariant across a spread of
        // priorities and counters, including counters that exceed 32 bits.
        let counters = [
            0u64,
            1,
            u64::from(u32::MAX) - 1,
            u64::from(u32::MAX),
            u64::from(u32::MAX) + 1,
            u64::MAX,
        ];
        for priority in 0..8u64 {
            let higher = calculate_next_priority_score(priority + 1, 0);
            for &counter in &counters {
                let lower = calculate_next_priority_score(priority, counter);
                assert!(
                    lower < higher,
                    "priority {priority} (counter {counter}) must sort below priority {}",
                    priority + 1
                );
            }
        }
    }

    #[test]
    fn within_a_priority_band_a_larger_counter_sorts_later() {
        let earlier = calculate_next_priority_score(5, 10);
        let later = calculate_next_priority_score(5, 11);
        assert!(
            earlier < later,
            "within a band the FIFO counter must break ties in insertion order"
        );
    }
}

#[cfg(feature = "redis-store")]
#[cfg(test)]
mod serialize_into_pairs_tests {
    use super::serialize_into_pairs;
    use serde::Serialize;
    use std::collections::BTreeMap;

    #[derive(Serialize)]
    struct Flat {
        name: String,
        count: u64,
        enabled: bool,
    }

    #[test]
    fn flat_struct_yields_one_pair_per_field() {
        let item = Flat {
            name: "widget".to_owned(),
            count: 3,
            enabled: true,
        };
        let mut pairs = serialize_into_pairs(&item);
        pairs.sort();
        assert_eq!(pairs.len(), 3, "each field must become exactly one pair");
        let keys: Vec<&str> = pairs.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["count", "enabled", "name"]);
        // Values are serialised as JSON, so the string field keeps its quotes.
        let by_key: BTreeMap<_, _> = pairs.into_iter().collect();
        assert_eq!(by_key["count"], "3");
        assert_eq!(by_key["enabled"], "true");
        assert_eq!(by_key["name"], "\"widget\"");
    }

    #[test]
    fn empty_map_yields_no_pairs() {
        let empty: BTreeMap<String, u64> = BTreeMap::new();
        let pairs = serialize_into_pairs(&empty);
        assert!(pairs.is_empty(), "an empty object must produce no pairs");
    }

    #[test]
    fn non_object_scalar_yields_no_pairs() {
        // A bare scalar is not a JSON object, so the helper must return nothing
        // rather than panicking or inventing a key.
        assert!(
            serialize_into_pairs(&42u64).is_empty(),
            "a scalar has no fields"
        );
        assert!(
            serialize_into_pairs(&"lonely").is_empty(),
            "a string has no fields"
        );
    }

    #[test]
    fn array_yields_no_pairs() {
        let values = vec![1u64, 2, 3];
        assert!(
            serialize_into_pairs(&values).is_empty(),
            "a top-level array is not an object and must yield no pairs"
        );
    }

    #[test]
    fn nested_object_values_are_serialised_as_json() {
        #[derive(Serialize)]
        struct Nested {
            inner: Inner,
        }
        #[derive(Serialize)]
        struct Inner {
            a: u64,
        }
        let pairs = serialize_into_pairs(&Nested {
            inner: Inner { a: 1 },
        });
        assert_eq!(pairs.len(), 1, "one top-level field");
        let (key, value) = &pairs[0];
        assert_eq!(key, "inner");
        assert!(
            value.contains("\"a\"") && value.contains('1'),
            "nested object value must be JSON-encoded, got {value}"
        );
    }
}

#[cfg(test)]
mod update_job_opts_tests {
    use super::update_job_opts;
    use crate::{BackOffJobOptions, JobOptions, QueueOpts, RemoveOnCompletionOrFailure, Repeat};

    #[test]
    fn unset_job_fields_inherit_queue_defaults() {
        let queue_opts = QueueOpts {
            remove_on_fail: Some(RemoveOnCompletionOrFailure::Bool(true)),
            remove_on_complete: Some(RemoveOnCompletionOrFailure::Int(5)),
            attempts: 4,
            default_backoff: Some(BackOffJobOptions::Number(1_000)),
            repeat: Some(Repeat::Immediately(2)),
            ..Default::default()
        };
        let mut opts = JobOptions::default();
        update_job_opts(&queue_opts, &mut opts);

        assert_eq!(opts.attempts, 4, "attempts must rise to the queue default");
        assert_eq!(
            opts.remove_on_complete,
            Some(RemoveOnCompletionOrFailure::Int(5))
        );
        assert_eq!(
            opts.remove_on_fail,
            Some(RemoveOnCompletionOrFailure::Bool(true))
        );
        assert_eq!(opts.backoff, Some(BackOffJobOptions::Number(1_000)));
        assert_eq!(opts.repeat, Some(Repeat::Immediately(2)));
    }

    #[test]
    fn explicitly_set_job_fields_are_not_overridden() {
        let queue_opts = QueueOpts {
            remove_on_fail: Some(RemoveOnCompletionOrFailure::Bool(true)),
            remove_on_complete: Some(RemoveOnCompletionOrFailure::Bool(true)),
            default_backoff: Some(BackOffJobOptions::Number(1_000)),
            repeat: Some(Repeat::Immediately(9)),
            attempts: 2,
            ..Default::default()
        };
        let mut opts = JobOptions {
            remove_on_fail: Some(RemoveOnCompletionOrFailure::Bool(false)),
            remove_on_complete: Some(RemoveOnCompletionOrFailure::Int(1)),
            backoff: Some(BackOffJobOptions::Number(50)),
            repeat: Some(Repeat::Immediately(1)),
            ..Default::default()
        };
        update_job_opts(&queue_opts, &mut opts);

        assert_eq!(
            opts.remove_on_fail,
            Some(RemoveOnCompletionOrFailure::Bool(false)),
            "an explicit job-level policy must survive"
        );
        assert_eq!(
            opts.remove_on_complete,
            Some(RemoveOnCompletionOrFailure::Int(1))
        );
        assert_eq!(opts.backoff, Some(BackOffJobOptions::Number(50)));
        assert_eq!(opts.repeat, Some(Repeat::Immediately(1)));
    }

    #[test]
    fn attempts_takes_the_maximum_never_lowering_the_job_value() {
        let queue_opts = QueueOpts {
            attempts: 3,
            ..Default::default()
        };
        let mut opts = JobOptions {
            attempts: 10,
            ..Default::default()
        };
        update_job_opts(&queue_opts, &mut opts);
        assert_eq!(
            opts.attempts, 10,
            "a higher job-level attempts must not be lowered"
        );

        let mut opts = JobOptions {
            attempts: 1,
            ..Default::default()
        };
        update_job_opts(&queue_opts, &mut opts);
        assert_eq!(
            opts.attempts, 3,
            "a lower job-level attempts must rise to the default"
        );

        let mut opts = JobOptions {
            attempts: 3,
            ..Default::default()
        };
        update_job_opts(&queue_opts, &mut opts);
        assert_eq!(opts.attempts, 3, "equal attempts must be left untouched");
    }
}

#[cfg(test)]
mod pause_resume_tests {
    use super::{pause_or_resume_workers, resume_helper};
    use crate::QueueMetrics;
    use crossbeam::atomic::AtomicCell;
    use std::sync::Arc;
    use tokio::sync::Notify;

    /// Builds metrics with a single waiting job so the queue reports work.
    fn metrics_with_waiting_work() -> QueueMetrics {
        let metrics = QueueMetrics::default();
        metrics.waiting.store(1);
        debug_assert!(
            metrics.queue_has_work(),
            "test fixture must report queued work"
        );
        metrics
    }

    #[test]
    fn resume_clears_pause_flag_when_paused_with_work() {
        let metrics = metrics_with_waiting_work();
        let pause = AtomicCell::new(true);
        let notify = Notify::new();
        resume_helper(&metrics, &pause, &notify);
        assert!(
            !pause.load(),
            "a paused worker with pending work must be resumed"
        );
    }

    #[test]
    fn resume_is_a_noop_when_paused_without_work() {
        let metrics = QueueMetrics::default();
        debug_assert!(!metrics.queue_has_work(), "fixture must be idle");
        let pause = AtomicCell::new(true);
        let notify = Notify::new();
        resume_helper(&metrics, &pause, &notify);
        assert!(pause.load(), "with no work the pause flag must stay set");
    }

    #[test]
    fn resume_is_a_noop_when_not_paused() {
        use std::future::Future;
        use std::pin::pin;
        use std::task::{Context, Poll, Waker};

        let metrics = metrics_with_waiting_work();
        let pause = AtomicCell::new(false);
        let notify = Notify::new();

        // Register a waiter up front. `resume_helper` signals via
        // `notify_waiters()`, which only wakes waiters registered before the
        // call, so a genuine no-op must leave this future pending. Without this
        // the test could not fail: `resume_helper` only ever clears the flag,
        // so asserting `!pause.load()` alone holds regardless of behaviour.
        let mut cx = Context::from_waker(Waker::noop());
        let mut notified = pin!(notify.notified());
        assert!(
            matches!(notified.as_mut().poll(&mut cx), Poll::Pending),
            "the waiter must start pending before any resume signal"
        );

        resume_helper(&metrics, &pause, &notify);

        assert!(
            !pause.load(),
            "an already-running worker must remain running"
        );
        assert!(
            matches!(notified.as_mut().poll(&mut cx), Poll::Pending),
            "a no-op resume must not wake any waiter"
        );
    }

    #[test]
    fn resume_triggers_on_any_kind_of_pending_work() {
        // queue_has_work is true for waiting, delayed, stalled or prioritized;
        // any single one must be enough to resume paused workers.
        let builders: [fn() -> QueueMetrics; 4] = [
            metrics_with_waiting_work,
            || {
                let m = QueueMetrics::default();
                m.delayed.store(1);
                m
            },
            || {
                let m = QueueMetrics::default();
                m.stalled.store(1);
                m
            },
            || {
                let m = QueueMetrics::default();
                m.prioritized.store(1);
                m
            },
        ];
        for build in builders {
            let metrics = build();
            let pause = AtomicCell::new(true);
            let notify = Notify::new();
            resume_helper(&metrics, &pause, &notify);
            assert!(
                !pause.load(),
                "any pending-work signal must resume the workers"
            );
        }
    }

    #[test]
    fn first_call_is_skipped_by_the_initial_guard() {
        // The very first invocation only flips the initial guard and must leave
        // the pause flag untouched, even though an idle queue would otherwise
        // trigger a pause.
        let metrics = QueueMetrics::default();
        let pause = AtomicCell::new(false);
        let notify = Notify::new();
        let is_initial = AtomicCell::new(true);
        pause_or_resume_workers(&notify, &metrics, &pause, &is_initial);
        assert!(!pause.load(), "the initial call must not pause");
        assert!(!is_initial.load(), "the initial guard must be consumed");
    }

    #[test]
    fn idle_queue_pauses_workers_after_the_initial_call() {
        let metrics = QueueMetrics::default();
        debug_assert!(metrics.is_idle(), "default metrics must be idle");
        let pause = AtomicCell::new(false);
        let notify = Notify::new();
        let is_initial = AtomicCell::new(false);
        pause_or_resume_workers(&notify, &metrics, &pause, &is_initial);
        assert!(
            pause.load(),
            "an idle queue past the initial call must pause workers"
        );
    }

    #[test]
    fn queued_work_resumes_paused_workers_after_the_initial_call() {
        let metrics = metrics_with_waiting_work();
        let pause = AtomicCell::new(true);
        let notify = Notify::new();
        let is_initial = AtomicCell::new(false);
        pause_or_resume_workers(&notify, &metrics, &pause, &is_initial);
        assert!(!pause.load(), "pending work must resume a paused worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_resume_calls_settle_on_running() {
        // Many threads racing to resume a paused-with-work queue must converge
        // deterministically on the unpaused state, never leaving it paused.
        let metrics = Arc::new(metrics_with_waiting_work());
        let pause = Arc::new(AtomicCell::new(true));
        let notify = Arc::new(Notify::new());

        let mut handles = Vec::new();
        for _ in 0..16 {
            let metrics = Arc::clone(&metrics);
            let pause = Arc::clone(&pause);
            let notify = Arc::clone(&notify);
            handles.push(tokio::spawn(async move {
                for _ in 0..1_000 {
                    resume_helper(&metrics, &pause, &notify);
                }
            }));
        }
        for handle in handles {
            handle.await.expect("resume task must not panic");
        }
        assert!(
            !pause.load(),
            "concurrent resume calls must converge on the running state"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_idle_pause_calls_settle_on_paused() {
        // pause_or_resume_workers uses compare_exchange(false -> true); under a
        // stampede of concurrent idle calls the flag must converge on paused and
        // stay there idempotently, regardless of interleaving.
        let metrics = Arc::new(QueueMetrics::default());
        let pause = Arc::new(AtomicCell::new(false));
        let notify = Arc::new(Notify::new());

        let mut handles = Vec::new();
        for _ in 0..16 {
            let metrics = Arc::clone(&metrics);
            let pause = Arc::clone(&pause);
            let notify = Arc::clone(&notify);
            handles.push(tokio::spawn(async move {
                let is_initial = AtomicCell::new(false);
                for _ in 0..1_000 {
                    pause_or_resume_workers(&notify, &metrics, &pause, &is_initial);
                    // Once idle-paused, repeated calls must not flip it back.
                    assert!(pause.load(), "idle calls must never unpause the queue");
                }
            }));
        }
        for handle in handles {
            handle.await.expect("pause task must not panic");
        }
        assert!(pause.load(), "an idle queue must end up paused");
    }
}
