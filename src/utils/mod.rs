use crate::error::{BacktraceCatcher, CaughtError, CaughtPanicInfo, JobError, QueueError};
use crate::events::QueueStreamEvent;
use crate::stores::Store;
use crate::timer::Timer;
use crate::worker::{JobMap, ProcessingQueue, WorkerCallback, WorkerState, MIN_DELAY_MS_LIMIT};
use crate::{
    utils, EventEmitter, EventParameters, FailedDetails, JobOptions, JobState, JobToken, KioError,
    QueueEventMode, QueueOpts, Trace, WorkerOpts,
};
use chrono::Utc;
use crossbeam_queue::SegQueue;
use futures::future::OkInto;
use futures::{FutureExt, StreamExt};
use redis::aio::PubSubStream;
use redis::streams::{StreamReadOptions, StreamReadReply};
use serde::ser;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::{format, Write};
use std::num::NonZero;
use tokio::sync::Notify;
use tokio::task_local;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub(crate) mod connection_types;
pub(crate) mod processor_types;
use crate::KioResult;
use crate::MoveToActiveResult;
use crate::{Job, ProcessedResult, Queue};

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};

use std::sync::Arc;

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

use crate::{CollectionSuffix, JobMetrics};
pub async fn get_job_metrics<C: redis::aio::ConnectionLike>(
    prefix: &str,
    name: &str,
    conn: &mut C,
) -> KioResult<JobMetrics> {
    let [job_id_key, stalled_key, active_key, completed_key, meta_key, delayed_key, priority_counter_key, waiting_key] =
        [
            CollectionSuffix::Id,
            CollectionSuffix::Stalled,
            CollectionSuffix::Active,
            CollectionSuffix::Completed,
            CollectionSuffix::Meta,
            CollectionSuffix::Delayed,
            CollectionSuffix::PriorityCounter,
            CollectionSuffix::Wait,
        ]
        .map(|key| key.to_collection_name(prefix, name));
    let mut pipeline = redis::pipe();
    pipeline.atomic();
    pipeline.zcard(completed_key);
    pipeline.llen(active_key);
    pipeline.scard(stalled_key);
    pipeline.zcard(delayed_key);
    pipeline.llen(waiting_key);
    pipeline.get(job_id_key);
    pipeline.hget(&meta_key, "processing");
    pipeline.hget(&meta_key, "event_mode");
    pipeline.hexists(&meta_key, JobState::Paused);
    #[allow(clippy::type_complexity)]
    let (completed, active, stalled, delayed, waiting, last_id, processing, event_mode, paused): (
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

    Ok(JobMetrics::new(
        last_id.unwrap_or_default(),
        processing.unwrap_or_default(),
        active.unwrap_or_default(),
        stalled.unwrap_or_default(),
        completed.unwrap_or_default(),
        delayed.unwrap_or_default(),
        waiting.unwrap_or_default(),
        paused,
        event_mode.unwrap_or_default(),
    ))
}

// ---- UTIL FUNCTIONS for the worker

#[async_backtrace::framed]
pub(crate) async fn process_job<D, R, P>(
    task_sender: TaskToRemove,
    job: Job<D, R, P>,
    token: JobToken,
    jobs_in_progress: JobMap<D, R, P>,
    queue: Arc<Queue<D, R, P>>,
    callback: WorkerCallback<D, R, P>,
) -> KioResult<()>
where
    R: Serialize + Send + Clone + DeserializeOwned + 'static + Sync,
    D: Clone + Serialize + DeserializeOwned + Send + 'static,
    P: Clone + Serialize + DeserializeOwned + Send + 'static + Sync,
{
    use crate::worker::WorkerCallback;
    use crate::JobState;
    let job_id = job.id.unwrap_or_default();
    let conn = queue.conn_pool.get().await?;
    let attempts_made = job.attempts_made;
    let returned = match callback {
        WorkerCallback::Sync(cb) => {
            let blocking_conn = queue.get_blocking_connection()?;
            async_backtrace::frame!(tokio::task::spawn_blocking(move || cb(blocking_conn, job)))
                .await?
                .map_err(|e| {
                    let backtrace = async_backtrace::backtrace();
                    CaughtError::Error(Box::new(e), backtrace)
                })
        }
        WorkerCallback::Async(cb) => {
            let callback = async_backtrace::frame!(cb(conn, job));
            BacktraceCatcher::catch(callback).await
        }
    };
    match returned {
        Ok(result) => {
            let ts = Utc::now().timestamp_micros();
            let move_to_state = JobState::Completed;
            let completed = queue
                .move_job_to_finished_or_failed(
                    job_id,
                    ts,
                    token,
                    move_to_state,
                    crate::ProcessedResult::Success(result),
                    None,
                )
                .await?;
            if let Some(entry) = jobs_in_progress.remove(&job_id) {
                let (job, _, handle) = entry.value();
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

                let handle_id = handle.load(std::sync::atomic::Ordering::Acquire);
                task_sender.push((handle_id, job_id, move_to_state));
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
                run: attempts_made + 1,
                reason,
                frames,
            });
            let failed_reason = FailedDetails {
                run: attempts_made + 1,
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
                let (job, _, handle) = entry.value();
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
                let handle_id = handle.load(std::sync::atomic::Ordering::Acquire);

                task_sender.push((handle_id, job_id, move_to_state));
            }
        }
    }
    Ok(())
}
pub(crate) async fn get_next_job<D, R, P>(
    queue: &Queue<D, R, P>,
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

type MainLoopParams<D, R, P> = (
    Uuid,
    CancellationToken,
    ProcessingQueue,
    WorkerOpts,
    Arc<AtomicU64>,
    JobMap<D, R, P>,
    Arc<AtomicUsize>,
    WorkerCallback<D, R, P>,
    Arc<Queue<D, R, P>>,
    Arc<atomig::Atomic<WorkerState>>,
    Arc<Notify>,
    Timer,
    Timer,
);
use tokio::task::{Id, JoinHandle};
type TaskToRemove = Arc<SegQueue<(u64, u64, JobState)>>;
pub(crate) type JobQueue = Arc<SegQueue<u64>>;
#[async_backtrace::framed]
pub(crate) async fn main_loop<D, R, P>(params: MainLoopParams<D, R, P>) -> KioResult<()>
where
    D: Clone + DeserializeOwned + 'static + Send + Sync + Serialize,
    R: Clone + DeserializeOwned + 'static + Serialize + Send + Sync,
    P: Clone + DeserializeOwned + 'static + Send + Sync + Serialize,
{
    use std::sync::atomic::Ordering;
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
        extend_lock_timer,
        stall_timer,
    ) = params;

    let to_remove = TaskToRemove::default();
    let task_queue = to_remove.clone();
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
    tokio::spawn(
        async move {
            while !cancel_token.is_cancelled() {
                // promote jobs here;
                let date_time = Utc::now();
                let interval_ms = (MIN_DELAY_MS_LIMIT) as i64;
                let metrics = queue_clone.get_metrics().await?;
                if queue_clone.current_metrics.has_delayed() {
                    queue_clone
                        .promote_delayed_jobs(date_time, interval_ms, job_queue.clone())
                        .await?;
                }
                while let Some((key, job_id, state)) = task_queue.pop() {
                    if let Some(entry) = running.remove(&key) {
                        let handle = entry.value();
                        let id = entry.key();
                        handle.abort();
                        let count = current_job_current.fetch_sub(1, Ordering::AcqRel);
                        queue_clone
                            .update_processing_count(false, worker_id, job_id, state)
                            .await?;
                    }
                }
                if pause_schedular.load(Ordering::Acquire) && running.is_empty() {
                    println!("pausing scheduler loop");
                    extend_lock_timer.pause();
                    stall_timer.pause();
                    worker_state_clone.store(WorkerState::Idle, Ordering::Release);
                    notifer.notified().await;
                    extend_lock_timer.resume();
                    stall_timer.resume();
                    worker_state_clone.store(WorkerState::Active, Ordering::Release);
                }
                tokio::task::yield_now().await;
            }

            Ok::<(), KioError>(())
        }
        .boxed(),
    );

    let now = Instant::now();
    while !cancellation_token.is_cancelled() {
        while !cancellation_token.is_cancelled()
            && active_job_count.load(Ordering::Acquire) < opts.concurrency
        {
            let token_prefix = active_job_count.load(std::sync::atomic::Ordering::Acquire);
            let next_id = Uuid::new_v4();
            let token = JobToken(id, next_id, token_prefix as u64);
            let worker_id = id;
            let block_delay = block_until.load(std::sync::atomic::Ordering::Acquire);
            let passed_id = delayed.pop();
            if let Some(job) = get_next_job(
                queue.as_ref(),
                token,
                block_delay,
                cancellation_token.is_cancelled(),
                &opts,
                passed_id,
            )
            .await?
            {
                let id = job.id.unwrap();
                jobs_in_progress.insert(id, (job.clone(), token, AtomicU64::default()));

                let state = job.state;
                let callback = processor.clone();
                let count = active_job_count.fetch_add(1, Ordering::AcqRel);
                queue
                    .update_processing_count(true, worker_id, id, state)
                    .await?;
                let task = tokio::spawn(async_backtrace::frame!(process_job(
                    to_remove.clone(),
                    job,
                    token,
                    jobs_in_progress.clone(),
                    queue.clone(),
                    callback
                )
                .boxed()));
                let task_id: u64 = task.id().to_string().parse()?;
                let handle = processing.insert(task_id, task);
                if let Some(mut re) = jobs_in_progress.get(&id) {
                    let (_, _, stored_handle) = re.value();

                    stored_handle.swap(task_id, Ordering::AcqRel);
                }
            }
            if queue.pause_workers.load(Ordering::Acquire)
                && delayed.is_empty()
                && processing.is_empty()
                && to_remove.is_empty()
            {
                println!(
                    "pause main loop after processing all jobs, worker_delayed: {} processing:{} task_pending_removal:{}",
                    delayed.len(),
                    processing.len(),
                    to_remove.len()
                );
                to_pause.store(true, Ordering::Relaxed);
                paused_here.notified().await;
                to_pause.store(false, Ordering::Relaxed);
            }
        }
        tokio::task::yield_now().await;
    }
    if cancellation_token.is_cancelled() {
        worker_state.compare_exchange(
            WorkerState::Active,
            WorkerState::Closed,
            Ordering::Acquire,
            Ordering::Relaxed,
        );
    }
    Ok(())
}
use crate::Dt;
use chrono::TimeDelta;
use redis::AsyncCommands;
use std::time::{Duration, Instant};
#[allow(clippy::too_many_arguments)]
pub async fn promote_jobs<D, R, P>(
    queue: &Queue<D, R, P>,
    date_time: Dt,
    mut interval_ms: i64,
    job_queue: JobQueue,
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
        tokio::time::sleep(Duration::from_millis(interval_ms as u64)).await;
        for job in jobs {
            job_queue.push(job);
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
        job.state = JobState::Priorized;
    } else {
        pipeline.lpush(&waiting_key, id.to_string());
    }
    job.id = Some(id);
    let fields = serialize_into_pairs(&job);
    pipeline.hset_multiple(&job_key, &fields);
    let event = if to_delay {
        JobState::Delayed
    } else if to_priorize {
        JobState::Priorized
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

pub(crate) type ReadStreamArgs<'a, D, R, P> = (
    QueueEventMode,
    usize,
    &'a EventEmitter<D, R, P>,
    Arc<JobMetrics>,
);
// Helper function to process events from our queue-redis-stream
pub async fn process_queue_events<'a, D, R, P, S: Store<D, R, P> + Send>(
    (event_mode, block_interval, emitter, metrics): ReadStreamArgs<'a, D, R, P>,
    store: &S,
) -> KioResult<()>
where
    D: DeserializeOwned + Clone + Send + 'static,
    R: DeserializeOwned + Clone + Send + Sync + 'static,
    P: DeserializeOwned + Clone + Send + Sync + 'static,
{
    store
        .listener_to_events(event_mode, Some(block_interval as u64), emitter, &metrics)
        .await
}
pub async fn process_each_event<D, R, P>(
    event: QueueStreamEvent<R, P>,
    emitter: &EventEmitter<D, R, P>,
    store: &(impl Store<D, R, P> + Send),
    metrics: &JobMetrics,
) -> KioResult<()>
where
    D: DeserializeOwned + Clone + Send + 'static,
    R: DeserializeOwned + Clone + Send + Sync + 'static,
    P: DeserializeOwned + Clone + Send + Sync + 'static,
{
    let state = event.event;
    let param = EventParameters::<D, R, P>::from_queue_event(event, store).await?;
    emitter.emit(state, param).await;
    if let Ok(updated) = store.get_metrics().await {
        metrics.update(&updated);
    }
    Ok(())
}
pub fn resume_helper(
    current_metrics: &JobMetrics,
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

use redis::{Cmd, Pipeline};
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
