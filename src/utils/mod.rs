use crate::error::{BacktraceCatcher, CaughtError, CaughtPanicInfo, JobError, QueueError};
use crate::events::QueueStreamEvent;
use crate::stores::Store;
use crate::timers::{DelayQueueTimer, TimerSender};
use crate::worker::{
    JobMap, ProcessingQueue, TaskHandle, WorkerCallback, WorkerState, MIN_DELAY_MS_LIMIT,
};
use crate::Counter;
use crate::{
    EventEmitter, EventParameters, FailedDetails, JobOptions, JobState, JobToken, KioError,
    QueueEventMode, QueueOpts, Trace, WorkerOpts,
};
use chrono::Utc;
use crossbeam::atomic::AtomicCell;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
#[cfg(feature = "redis-store")]
use redis::aio::ConnectionLike;
#[cfg(feature = "redis-store")]
use redis::ParsingError;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio_metrics::TaskMonitor;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "tracing")]
use tracing::{debug, info};
use uuid::Uuid;

pub mod processor_types;
use crate::KioResult;
use crate::MoveToActiveResult;
use crate::{Job, ProcessedResult, Queue};

use crate::worker::{HISTOGRAM_MAX_NS, HISTOGRAM_SIGFIG};
use hdrhistogram::Histogram;
use std::sync::Arc;

#[cfg(feature = "redis-store")]
/// Reads the Redis password from the `REDIS_PASSWORD` environment variable.
///
/// Loads a `.env` file via `dotenv` if one is present before reading the
/// variable.  Returns `None` when the variable is unset.
#[must_use]
pub fn fetch_redis_pass() -> Option<String> {
    if let Err(_err) = dotenv::dotenv() {
        // dothing; continue
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
    (priority << 32) + (prio_counter & 0xffff_ffff_ffff)
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
    let [job_id_key, stalled_key, active_key, completed_key, meta_key, delayed_key, _priority_counter_key, waiting_key, paused_key, prioritized_key, failed_key] =
        [
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
    pipeline.zcard(completed_key);
    pipeline.zcard(failed_key);
    pipeline.zcard(prioritized_key);
    pipeline.llen(active_key);
    pipeline.scard(stalled_key);
    pipeline.zcard(delayed_key);
    pipeline.llen(waiting_key);
    pipeline.llen(paused_key);
    pipeline.get(job_id_key);
    pipeline.hget(&meta_key, "processing");
    pipeline.hget(&meta_key, "event_mode");
    pipeline.hexists(&meta_key, JobState::Paused);
    #[allow(clippy::type_complexity)]
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
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
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
    use crate::worker::WorkerCallback;
    use crate::JobState;
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
                let (_, (job, _, handle, _, _)) = entry;
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
                CaughtError::Error(error, backtrace) => (error.to_string(), backtrace),
                CaughtError::JoinError(join_error) => (join_error.to_string(), None),
            };
            let backtrace: Option<Vec<String>> =
                backtrace.map(|trace| trace.iter().map(std::string::ToString::to_string).collect());
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
                let (_, (job, _, handle, _, _)) = entry;
                // retry failed jobs
                if failed_job.attempts_made < job.opts.attempts {
                    if let Some(backoff_job_opts) = job.opts.backoff.as_ref() {
                        queue
                            .retry_job(job_id, backoff_job_opts, failed_job.attempts_made - 1)
                            .await?;
                    }
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
    // handle pausing or closing;
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
    DelayQueueTimer<D, R, P, S>,
    Arc<AtomicCell<bool>>,
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
    DelayQueueTimer<D, R, P, S>,
    Arc<AtomicCell<bool>>,
);
use tokio::task::JoinHandle;
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
        timers,
        to_pause,
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
        timers,
        to_pause,
    ) = params;

    #[cfg(feature = "tracing")]
    info!(
        "Worker Starting with concurrency set to {}",
        opts.concurrency
    );
    timers.start_timers();
    let semaphore = Arc::new(Semaphore::new(opts.concurrency));
    while !cancellation_token.is_cancelled() {
        while !cancellation_token.is_cancelled()
            && semaphore.available_permits() > 0
            && processing.len() < opts.concurrency
        {
            if let Ok(permit) = semaphore.clone().acquire_owned().await {
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
                );

                if let Ok(Some(job)) = next_job_result.await {
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
                        let poll_histogram =
                            Histogram::new_with_max(HISTOGRAM_MAX_NS, HISTOGRAM_SIGFIG).unwrap();

                        jobs_in_progress.insert(
                            id,
                            (
                                job,
                                token,
                                TaskHandle::default(),
                                monitor.clone(),
                                poll_histogram,
                            ),
                        );
                        let task = processing
                            .spawn(monitor.instrument(async_backtrace::frame!(process_fn.boxed())));
                        if let Some(re) = jobs_in_progress.get(&id) {
                            let (_, _, stored_handle, _, _) = re.value();

                            stored_handle.swap(Some(task.into()));
                        }
                    }
                }
                tokio::task::yield_now().await;
            }

            if queue.pause_workers.load() && processing.is_empty() {
                #[cfg(feature = "tracing")]
                info!(
                    "pausing job_schedular_loop with  {delayed} delayed_jobs and {processing} running_jobs",
                    delayed = queue.current_metrics.delayed.load(),
                    processing = processing.len(),
                );
                to_pause.store(true);
                if cancellation_token
                    .run_until_cancelled(paused_here.notified())
                    .await
                    .is_none()
                {
                    #[cfg(feature = "tracing")]
                    info!("... breaking loop");
                    break;
                }
                #[cfg(feature = "tracing")]
                info!("resumed job_schedular_loop");

                to_pause.store(false);
            }
            // yield, so other tasks run
            tokio::task::yield_now().await;
        }

        tokio::task::yield_now().await;
    }

    if cancellation_token.is_cancelled() {
        // wait for all running jobs to finish
        processing.wait().await;
        timers.close().await;
        let _ = worker_state.compare_exchange(WorkerState::Active, WorkerState::Closed);
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
            timer_sender.send(crate::timers::TimerType::PromotedDelayed(job_id));
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
                        reason: JobError::MissedDelayDeadline.to_string(),
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
#[allow(clippy::too_many_arguments)]
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
    let job_key = format!("{queue_name}:{id}");
    let events_keys = format!("{queue_name}:events");

    let waiting_or_paused = if is_paused {
        CollectionSuffix::Paused
    } else {
        CollectionSuffix::Wait
    };
    let to_delay = delay > 0;
    let to_priorize = priority > 0 && !to_delay;
    let waiting_key = format!("{queue_name}:{waiting_or_paused}").to_lowercase();
    pipeline.atomic();
    if to_delay {
        let delayed_key = format!("{queue_name}:delayed");
        if let Some(expected_active_time) = expected_dt_ts {
            pipeline.zadd(delayed_key, id, expected_active_time);
            job.state = JobState::Delayed;
        }
    }
    // handle prioritized_jobs
    else if to_priorize {
        let prioritized_key =
            format!("{queue_name}:{}", CollectionSuffix::Prioritized).to_lowercase();
        let score = calculate_next_priority_score(priority, prior_counter);
        pipeline.zadd(&prioritized_key, id, score);
        job.state = JobState::Prioritized;
    } else {
        pipeline.lpush(&waiting_key, id.to_string());
    }
    job.id = Some(id);
    let fields = serialize_into_pairs(&job);
    pipeline.hset_multiple(&job_key, &fields);
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
                name: Some(name.to_owned()),
                ..Default::default()
            };
            if to_delay {
                event.delay = Some(delay);
            }
            if to_priorize {
                event.priority = Some(priority);
            }
            pipeline.publish(events_keys, event);
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
            pipeline.xadd(events_keys, "*", &items);
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
#[allow(clippy::future_not_send)]
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
#[allow(clippy::future_not_send)]
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
#[allow(clippy::future_not_send)]
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
) -> KioResult<JoinHandle<KioResult<()>>>
where
    D: Clone + Serialize + DeserializeOwned + Send + 'static,
    R: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
    S: Clone + Store<D, R, P> + Send + 'static + Sync,
    P: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
{
    let store = store.clone();

    let task: JoinHandle<KioResult<()>> = tokio::spawn(
        async move {
            let block_interval = 5000; // 100 seconds
            let notifier = notifier.clone();
            let is_inital: AtomicCell<bool> = AtomicCell::new(true);

            loop {
                let args: ReadStreamArgs<R, P> =
                    (event_mode, block_interval, &emitter, metrics.clone());
                #[cfg(feature = "tracing")]
                {
                    use tracing::{info_span, Instrument};
                    let queue_name = format!("{}:{}", store.queue_prefix(), store.queue_name());
                    let span = info_span!( parent:None, "", queue_name);
                    process_queue_events(args, &store)
                        .instrument(span.clone())
                        .await?;
                    let _enter = span.enter();
                    pause_or_resume_workers(&notifier, &metrics, &pause_workers, &is_inital);
                }
                #[cfg(not(feature = "tracing"))]
                {
                    process_queue_events(args, &store).await?;
                    pause_or_resume_workers(&notifier, &metrics, &pause_workers, &is_inital);
                }
                tokio::task::yield_now().await;
            }
            #[allow(unreachable_code)]
            Ok(())
        }
        .boxed(),
    );
    Ok(task)
}
pub fn pause_or_resume_workers(
    notifier: &Notify,
    metrics: &QueueMetrics,
    pause_workers: &AtomicCell<bool>,
    is_inital: &AtomicCell<bool>,
) {
    if metrics.is_idle() && !is_inital.load() && pause_workers.compare_exchange(false, true).is_ok()
    {
        #[cfg(feature = "tracing")]
        info!("sent pause signal to workers ");
    } else {
        resume_helper(metrics, pause_workers, notifier);
    }
    let _ = is_inital.compare_exchange(true, false);
}

#[cfg(feature = "redis-store")]
#[allow(clippy::needless_pass_by_value)]
pub fn to_redis_parsing_error(err: impl ToString) -> ParsingError {
    ParsingError::from(err.to_string())
}
