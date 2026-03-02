use crate::error::{BacktraceCatcher, CaughtError, CaughtPanicInfo, JobError, QueueError};
use crate::events::QueueStreamEvent;
use crate::stores::Store;
use crate::timers::{DelayQueueTimer, TimerType};
use crate::worker::{
    JobMap, ProcessingQueue, TaskHandle, WorkerCallback, WorkerState, MIN_DELAY_MS_LIMIT,
};
use crate::{
    utils, EventEmitter, EventParameters, FailedDetails, JobMetrics, JobOptions, JobState,
    JobToken, KioError, QueueEventMode, QueueOpts, Trace, WorkerOpts,
};
use chrono::{Month, Utc};
use crossbeam::queue::{ArrayQueue, SegQueue};
use futures::future::OkInto;
use futures::{FutureExt, StreamExt};
#[cfg(feature = "redis-store")]
use redis::aio::PubSubStream;
#[cfg(feature = "redis-store")]
use redis::streams::{StreamReadOptions, StreamReadReply};
use serde::ser;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::{format, Write};
use std::num::NonZero;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task_local;
use tokio_metrics::TaskMonitor;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "tracing")]
use tracing::{debug, info, info_span, Instrument};
use uuid::Uuid;

pub(crate) mod processor_types;
use crate::KioResult;
use crate::MoveToActiveResult;
use crate::{Job, ProcessedResult, Queue};

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};

use std::sync::Arc;

#[cfg(feature = "redis-store")]
// ---------------- REDIS FUNCTION here
pub fn fetch_redis_pass() -> Option<String> {
    use dotenv;
    if let Err(err) = dotenv::dotenv() {
        // dothing; continue
    }
    std::env::var("REDIS_PASSWORD").ok()
}

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
pub fn calculate_next_priority_score(priority: u64, prio_counter: u64) -> u64 {
    (priority << 32) + (prio_counter & 0xffffffffffff)
}

use crate::{CollectionSuffix, QueueMetrics};
#[cfg(feature = "redis-store")]
pub async fn get_queue_metrics<C: redis::aio::ConnectionLike>(
    prefix: &str,
    name: &str,
    conn: &mut C,
) -> KioResult<QueueMetrics> {
    let [job_id_key, stalled_key, active_key, completed_key, meta_key, delayed_key, priority_counter_key, waiting_key, paused_key, prioritized_key, failed_key] =
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
pub(crate) async fn process_job<D, R, P, S>(
    job: Job<D, R, P>,
    token: JobToken,
    jobs_in_progress: JobMap<D, R, P>,
    queue: Arc<Queue<D, R, P, S>>,
    callback: WorkerCallback<D, R, P, S>,
    permit: OwnedSemaphorePermit,
    worker_id: Uuid,
    current_job_current: Arc<AtomicUsize>,
) -> KioResult<()>
where
    R: Serialize + Send + Clone + DeserializeOwned + 'static + Sync,
    D: Clone + Serialize + DeserializeOwned + Send + 'static,
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
                let (job, _, handle, _) = entry.value();
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
                    let handle_id = task_queue.replace((handle.id(), job_id, move_to_state));
                }
            }
        }
        Err(err) => {
            let (failed_reason, backtrace) = match err {
                CaughtError::Panic(CaughtPanicInfo {
                    backtrace,
                    payload,
                    location,
                }) => (payload, backtrace),
                CaughtError::Error(error, backtrace) => (error.to_string(), backtrace),
                CaughtError::JoinError(join_error) => (join_error.to_string(), None),
            };
            let backtrace: Option<Vec<String>> =
                backtrace.map(|trace| trace.iter().map(|loc| loc.to_string()).collect());
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
                let (job, _, handle, _) = entry.value();
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
                    let handle_id = task_queue.replace((handle.id(), job_id, move_to_state));
                }
            }
        }
    }

    while let Some((key, job_id, state)) = task_queue.take() {
        let count = current_job_current.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        queue
            .update_processing_count(false, worker_id, job_id, state)
            .await?;

        #[cfg(feature = "tracing")]
        debug!("processed job {job_id} in task({key}) to state: {state}");
    }
    Ok(())
}
pub(crate) async fn get_next_job<D, R, P, S>(
    queue: &Queue<D, R, P, S>,
    token: JobToken,
    block_delay: u64,
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
        let ts = Utc::now().timestamp_micros() as u64;
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
    CancellationToken,
    ProcessingQueue,
    WorkerOpts,
    Arc<AtomicU64>,
    JobMap<D, R, P>,
    Arc<AtomicUsize>,
    WorkerCallback<D, R, P, S>,
    Arc<Queue<D, R, P, S>>,
    Arc<atomig::Atomic<WorkerState>>,
    Arc<Notify>,
    DelayQueueTimer<D, R, P, S>,
);

#[cfg(not(feature = "tracing"))]
type MainLoopParams<D, R, P, S> = (
    Uuid,
    CancellationToken,
    ProcessingQueue,
    WorkerOpts,
    Arc<AtomicU64>,
    JobMap<D, R, P>,
    Arc<AtomicUsize>,
    WorkerCallback<D, R, P, S>,
    Arc<Queue<D, R, P, S>>,
    Arc<atomig::Atomic<WorkerState>>,
    Arc<Notify>,
    DelayQueueTimer<D, R, P, S>,
);
use tokio::task::{Id, JoinHandle};
type TaskToRemove = ArrayQueue<(Id, u64, JobState)>;
pub(crate) type JobQueue = Arc<SegQueue<u64>>;
#[async_backtrace::framed]
pub(crate) async fn main_loop<D, R, P, S>(params: MainLoopParams<D, R, P, S>) -> KioResult<()>
where
    D: Clone + DeserializeOwned + 'static + Send + Sync + Serialize,
    R: Clone + DeserializeOwned + 'static + Serialize + Send + Sync,
    P: Clone + DeserializeOwned + 'static + Send + Sync + Serialize,
    S: Clone + Store<D, R, P> + 'static + Send + Sync,
{
    use std::sync::atomic::Ordering;
    use tokio_util::time::FutureExt;
    #[cfg(feature = "tracing")]
    let (
        resource_span,
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
    ) = params;

    #[cfg(feature = "tracing")]
    info!(
        "Worker Starting with concurrency set to {}",
        opts.concurrency
    );
    let delayed = JobQueue::default();
    let running = processing.clone();
    let cancel_token = cancellation_token.clone();
    let job_count = active_job_count.clone();
    let queue_clone = queue.clone();
    let job_queue = delayed.clone();
    let notifer = paused_here.clone();
    let worker_id = id;
    let to_pause = Arc::new(AtomicBool::default());
    let pause_schedular = to_pause.clone();
    let worker_state_clone = worker_state.clone();
    let current_job_current = active_job_count.clone();
    #[cfg(feature = "tracing")]
    timers
        .start_timers()
        .instrument(resource_span.clone())
        .await;
    #[cfg(not(feature = "tracing"))]
    timers.start_timers().await;
    let t_task = async move {
        while !cancel_token.is_cancelled() {
            // promote jobs here;
            let date_time = Utc::now();
            let interval_ms = (MIN_DELAY_MS_LIMIT) as i64;
            let metrics = queue_clone.get_metrics().await?;
            if queue_clone.current_metrics.has_delayed() {
                queue_clone
                    .promote_delayed_jobs(date_time, interval_ms, &timers)
                    .await?;
            }

            timers.run(&job_queue).await?;
            queue_clone.store.purge_expired().await;
            if pause_schedular.load(Ordering::Acquire) && running.is_empty() {
                #[cfg(feature = "tracing")]
                debug!("pausing ... ");
                worker_state_clone.store(WorkerState::Idle, Ordering::Release);
                // wait for all running jobs to completed
                timers.pause().await;
                if cancel_token
                    .run_until_cancelled(notifer.notified())
                    .await
                    .is_none()
                {
                    // handle cancellation here too
                    break;
                }
                #[cfg(feature = "tracing")]
                debug!("resumed");
                worker_state_clone.store(WorkerState::Active, Ordering::Release);
                timers.resume().await;
            }
            tokio::task::yield_now().await;
        }
        #[cfg(feature = "tracing")]
        info!("cancelled");

        Ok::<(), KioError>(())
    };
    #[cfg(feature = "tracing")]
    let sub_span = info_span!(parent: &resource_span, "timer_and_clean_up_task");
    #[cfg(feature = "tracing")]
    let timers_and_clean_up_task = tokio::spawn(t_task.instrument(sub_span).boxed());
    #[cfg(not(feature = "tracing"))]
    let timers_and_clean_up_task = tokio::spawn(t_task.boxed());
    let now = Instant::now();
    let semaphore = Arc::new(Semaphore::new(opts.concurrency));
    while !cancellation_token.is_cancelled() {
        while !cancellation_token.is_cancelled()
            && semaphore.available_permits() > 0
            && processing.len() < opts.concurrency
        {
            if let Ok(permit) = semaphore.clone().acquire_owned().await {
                let token_prefix = active_job_count.load(std::sync::atomic::Ordering::Acquire);
                let next_id = Uuid::new_v4();
                let token = JobToken(id, next_id, token_prefix as u64);
                let worker_id = id;
                let block_delay = block_until.load(std::sync::atomic::Ordering::Acquire);
                let passed_id = delayed.pop();

                if let Ok(Some(job)) = get_next_job(
                    queue.as_ref(),
                    token,
                    block_delay,
                    cancellation_token.is_cancelled(),
                    &opts,
                    passed_id,
                )
                .await
                {
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
                        jobs_in_progress
                            .insert(id, (job, token, TaskHandle::default(), monitor.clone()));
                        let task = processing
                            .spawn(monitor.instrument(async_backtrace::frame!(process_fn.boxed())));
                        if let Some(mut re) = jobs_in_progress.get(&id) {
                            let (_, _, stored_handle, _) = re.value();

                            stored_handle.swap(Some(task.into()));
                        }
                    }
                }
                tokio::task::yield_now().await;
            }

            if queue.pause_workers.load(Ordering::Acquire)
                && delayed.is_empty()
                && processing.is_empty()
            {
                #[cfg(feature = "tracing")]
                info!(
                    "pausing job_schedular_loop with  {delayed} delayed_jobs and {processing} running_jobs",
                    delayed = delayed.len(),
                    processing = processing.len(),
                );
                to_pause.store(true, Ordering::Release);
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

                to_pause.store(false, Ordering::Release);
            }
            // yield, so other tasks run
            tokio::task::yield_now().await;
        }

        tokio::task::yield_now().await;
    }

    if cancellation_token.is_cancelled() {
        // wait for all running jobs to finish
        processing.wait().await;
        timers_and_clean_up_task.await?;

        worker_state.compare_exchange(
            WorkerState::Active,
            WorkerState::Closed,
            Ordering::AcqRel,
            Ordering::SeqCst,
        );
    }
    #[cfg(feature = "tracing")]
    info!("Worker Closed");
    Ok(())
}
use crate::Dt;
use chrono::TimeDelta;
use std::time::{Duration, Instant};
#[allow(clippy::too_many_arguments)]
pub async fn promote_jobs<D, R, P, S: Store<D, R, P> + Send + 'static + Clone>(
    queue: &Queue<D, R, P, S>,
    date_time: Dt,
    mut interval_ms: i64,
    timers: &DelayQueueTimer<D, R, P, S>,
) -> KioResult<()>
where
    D: Clone + Serialize + DeserializeOwned + Send + 'static,
    R: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
    P: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
{
    let start = date_time.timestamp_millis();
    let stop = (date_time + TimeDelta::milliseconds(interval_ms)).timestamp_millis();
    let (jobs, missed_deadline): (Vec<u64>, Vec<u64>) =
        queue.store.get_delayed_at(start, stop).await?;
    if !jobs.is_empty() {
        for job in jobs {
            let timer = TimerType::PromotedDelayed(job);
            timers.insert(timer).await;
        }
    }
    if !missed_deadline.is_empty() {
        let queue_clone = queue.clone();
        let ts = date_time.timestamp_micros();
        let move_to_state = JobState::Failed;
        for job_id in missed_deadline.iter() {
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
        }
        //let _: () = conn.zrem(&delayed_key, missed_deadline).await?;
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
        attempts,
        remove_on_fail,
        remove_on_complete,
        ref backoff,
        repeat: _,
    } = opts;
    let dt = Utc::now();
    let expected_dt_ts = delay.next_occurrance_timestamp_ms();
    let delay_dt = delay.clone();
    let delay = delay.as_diff_ms(dt) as u64;
    job.add_opts(opts);
    if delay > 0 && delay < MIN_DELAY_MS_LIMIT {
        return Err(QueueError::DelayBelowAllowedLimit {
            limit_ms: MIN_DELAY_MS_LIMIT,
            current_ms: delay,
        }
        .into());
    };
    //queue.job_count.
    let job_key = format!("{queue_name}:{id}");
    let events_keys = format!("{queue_name}:events");

    let waiting_or_paused = if !is_paused {
        CollectionSuffix::Wait
    } else {
        CollectionSuffix::Paused
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
        let priority_counter_key =
            format!("{queue_name}:{}", CollectionSuffix::PriorityCounter).to_lowercase();
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
                event.delay = Some(delay)
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

pub(crate) type ReadStreamArgs<'a, R, P> = (
    QueueEventMode,
    usize,
    &'a EventEmitter<R, P>,
    Arc<QueueMetrics>,
);
// Helper function to process events from our queue-redis-stream
pub async fn process_queue_events<'a, D, R, P, S: Store<D, R, P> + Send>(
    (event_mode, block_interval, emitter, metrics): ReadStreamArgs<'a, R, P>,
    store: &S,
) -> KioResult<()>
where
    D: DeserializeOwned + Clone + Send + 'static,
    R: DeserializeOwned + Clone + Send + Sync + 'static,
    P: DeserializeOwned + Clone + Send + Sync + 'static,
{
    store
        .listen_to_events(event_mode, Some(block_interval as u64), emitter, &metrics)
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
    let param = EventParameters::<R, P>::from_queue_event(event).await?;
    emitter.emit(state, param).await;
    if let Ok(updated) = store.get_metrics().await {
        metrics.update(&updated);
    }
    Ok(())
}
pub fn resume_helper(
    current_metrics: &QueueMetrics,
    pause_workers: &AtomicBool,
    worker_notifier: &Notify,
) {
    use std::sync::atomic::Ordering;
    let workers_paused = pause_workers.load(Ordering::Acquire);
    if current_metrics.queue_has_work() && workers_paused {
        worker_notifier.notify_waiters();
        pause_workers.store(false, Ordering::Release);
    }
}

#[cfg(feature = "redis-store")]
use redis::{Cmd, Pipeline};
#[cfg(feature = "redis-store")]
fn split_pipeline(mut p: Pipeline, chunk_size: usize) -> Vec<Pipeline> {
    // Take ownership of the internal command list
    let cmds = unsafe {
        // Access private field via raw pointer trick
        let cmds_ptr = &mut p as *mut Pipeline as *mut Vec<redis::Cmd>;
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
pub async fn query_all_batched(
    conn: deadpool_redis::Connection,
    mut p: Pipeline,
) -> redis::RedisResult<()>
where
{
    let chunk_size = 10000;
    let pipelines = split_pipeline(p, chunk_size);
    let futs = pipelines.into_iter().map(|mut p| {
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
        opts.backoff = queue_opts.default_backoff.clone();
    }
    if opts.repeat.is_none() {
        opts.repeat = queue_opts.repeat.clone();
    }
}
/// utily function to create stream_handles
pub async fn create_listener_handle<D, R, P, S>(
    store: &S,
    emitter: EventEmitter<R, P>,
    notifier: Arc<Notify>,
    metrics: Arc<QueueMetrics>,
    pause_workers: Arc<AtomicBool>,
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
            let is_inital = AtomicBool::new(true);

            loop {
                let args: ReadStreamArgs<R, P> =
                    (event_mode, block_interval, &emitter, metrics.clone());
                process_queue_events(args, &store).await?;
                pause_or_resume_workers(&notifier, &metrics, &pause_workers, &is_inital);
                tokio::task::yield_now().await;
            }

            Ok(())
        }
        .boxed(),
    );
    Ok(task)
}
pub(crate) fn pause_or_resume_workers(
    notifier: &Notify,
    metrics: &QueueMetrics,
    pause_workers: &AtomicBool,
    is_inital: &AtomicBool,
) {
    use std::sync::atomic::Ordering;
    if metrics.is_idle()
        && !is_inital.load(Ordering::Acquire)
        && pause_workers
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    {
        #[cfg(feature = "tracing")]
        info!("sent pause signal to workers ");
    } else {
        resume_helper(metrics, pause_workers, notifier);
    }
    is_inital.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire);
}
