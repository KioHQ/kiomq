use crate::error::{BacktraceCatcher, CaughtError, CaughtPanicInfo, QueueError};
use crate::timer::Timer;
use crate::worker::{JobMap, ProcessingQueue, WorkerCallback, MIN_DELAY_MS_LIMIT};
use crate::{utils, EventParameters, FailedDetails, JobOptions, JobState, Trace, WorkerOpts};
use chrono::Utc;
use crossbeam_queue::SegQueue;
use futures::FutureExt;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Write;
use std::num::NonZero;
use tokio::task_local;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::KioResult;
use crate::MoveToActiveResult;
use crate::{Job, Queue};
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
    if let Ok(value) = serde_json::to_value(item) {
        if let Some(obj) = value.as_object() {
            return obj
                .into_iter()
                .map(|(key, val)| (key.to_owned(), val.to_string()))
                .collect();
        }
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
    let [job_id_key, stalled_key, active_key, completed_key, meta_key, delayed_key, priority_counter_key] =
        [
            CollectionSuffix::Id,
            CollectionSuffix::Stalled,
            CollectionSuffix::Active,
            CollectionSuffix::Completed,
            CollectionSuffix::Meta,
            CollectionSuffix::Delayed,
            CollectionSuffix::PriorityCounter,
        ]
        .map(|key| key.to_collection_name(prefix, name));
    let mut pipeline = redis::pipe();
    pipeline.atomic();
    pipeline.zcard(completed_key);
    pipeline.llen(active_key);
    pipeline.scard(stalled_key);
    pipeline.zcard(delayed_key);
    pipeline.get(job_id_key);
    pipeline.hexists(&meta_key, JobState::Paused);
    #[allow(clippy::type_complexity)]
    let (completed, active, stalled, delayed, last_id, paused): (
        Option<u64>,
        Option<u64>,
        Option<u64>,
        Option<u64>,
        u64,
        bool,
    ) = pipeline.query_async(conn).await?;

    Ok(JobMetrics::new(
        last_id,
        active.unwrap_or_default(),
        stalled.unwrap_or_default(),
        completed.unwrap_or_default(),
        delayed.unwrap_or_default(),
        paused,
    ))
}

// ---- UTIL FUNCTIONS for the worker

#[async_backtrace::framed]
pub(crate) async fn process_job<D, R, P>(
    job: Job<D, R, P>,
    token: String,
    jobs_in_progress: JobMap<D, R, P>,
    queue: Arc<Queue<D, R, P>>,
    callback: Arc<WorkerCallback<D, R, P>>,
    active_job_count: Arc<AtomicUsize>,
) -> KioResult<()>
where
    R: Serialize + Send + Clone + DeserializeOwned + 'static + Sync,
    D: Clone + Serialize + DeserializeOwned + Send + 'static,
    P: Clone + Serialize + DeserializeOwned + Send + 'static + Sync,
{
    use crate::JobState;
    let job_id = job.id.clone();
    let conn = queue.conn_pool.get().await?;
    let attempts_made = job.attempts_made;
    let callback = async_backtrace::frame!(callback(conn, job));
    let returned = BacktraceCatcher::catch(callback).await;
    match returned {
        Ok(result) => {
            if let (Ok(result_str), Some(job_id)) =
                (serde_json::to_string(&result), job_id.as_ref())
            {
                let ts = Utc::now().timestamp_micros();
                let move_to_state = JobState::Completed;
                let completed = queue
                    .move_job_to_finished_or_failed(
                        job_id,
                        ts,
                        &token,
                        move_to_state,
                        &result_str,
                        attempts_made,
                        None,
                    )
                    .await?;
                if let Some(entry) = jobs_in_progress.remove(job_id) {
                    //handle.abort(); // remove task from the queue
                    let (job, _, handle) = entry.1;
                    queue
                        .clean_up_job(job_id, job.opts.remove_on_complete)
                        .await?;
                    let handle_id = handle.load(std::sync::atomic::Ordering::Acquire);
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
            };
            let backtrace: Option<Vec<String>> =
                backtrace.map(|trace| trace.iter().map(|loc| loc.to_string()).collect());
            let reason = failed_reason.clone();
            let frames = backtrace.map(|frames| Trace {
                run: attempts_made + 1,
                reason,
                frames,
            });
            let failed_reason = serde_json::to_string(&FailedDetails {
                run: attempts_made + 1,
                reason: failed_reason,
            })?;
            // move job to failed_state
            if let Some(job_id) = job_id.as_ref() {
                let ts = Utc::now().timestamp_micros();

                let move_to_state = JobState::Failed;
                let failed_job = queue
                    .move_job_to_finished_or_failed(
                        job_id,
                        ts,
                        &token,
                        move_to_state,
                        &failed_reason,
                        attempts_made,
                        frames,
                    )
                    .await?;
                if let Some(entry) = jobs_in_progress.remove(job_id) {
                    let (job, _, handle) = entry.1;
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
                }
            }
        }
    }
    active_job_count.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    Ok(())
}
pub(crate) async fn get_next_job<D, R, P>(
    queue: &Queue<D, R, P>,
    token: &str,
    block_delay: u64,
    closed: bool,
    opts: &WorkerOpts,
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
    Arc<WorkerCallback<D, R, P>>,
    Arc<Queue<D, R, P>>,
    Arc<AtomicBool>,
    Arc<Timer>,
);
use tokio::task::Id;
type TaskToRemove = Arc<SegQueue<u64>>;
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
        is_active,
        job_scheduling_timer,
    ) = params;

    let now = Instant::now();
    while !cancellation_token.is_cancelled() {
        while !cancellation_token.is_cancelled() && processing.len() < opts.concurrency {
            let token_prefix = active_job_count.load(std::sync::atomic::Ordering::Acquire);
            let token = format!("{id}:{token_prefix}");
            let block_delay = block_until.load(std::sync::atomic::Ordering::Acquire);
            if let Some(job) = get_next_job(
                queue.as_ref(),
                &token,
                block_delay,
                cancellation_token.is_cancelled(),
                &opts,
            )
            .await?
            {
                let id = job.id.clone().unwrap();
                jobs_in_progress.insert(
                    job.id.clone().unwrap_or_default(),
                    (job.clone(), token.clone(), AtomicU64::default()),
                );

                let queue = queue.clone();
                let callback = processor.clone();
                active_job_count.fetch_add(1, Ordering::AcqRel);

                let task = tokio::spawn(async_backtrace::frame!(process_job(
                    job,
                    token,
                    jobs_in_progress.clone(),
                    queue,
                    callback,
                    active_job_count.clone()
                )
                .boxed()));
                let task_id: u64 = task.id().to_string().parse()?;
                let handle = processing.insert(task_id, task);
                if let Some(mut re) = jobs_in_progress.get(&id) {
                    let (_, _, stored_handle) = re.value();

                    stored_handle.swap(task_id, Ordering::AcqRel);
                }
            }
        }
        processing.retain(|_, handle| !handle.is_finished());
        tokio::task::yield_now().await;
    }
    if cancellation_token.is_cancelled() {
        is_active.compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed);
    }
    Ok(())
}
use crate::Dt;
use chrono::TimeDelta;
use redis::AsyncCommands;
use std::time::{Duration, Instant};

pub async fn promote_jobs(
    prefix: &str,
    name: &str,
    date_time: Dt,
    paused: bool,
    target_state: JobState,
    mut interval_ms: i64,
    mut conn: deadpool_redis::Connection,
) -> KioResult<Vec<String>> {
    let [delayed_key, marker_key, events_stream_key, prioritized_key, priority_counter_key] = [
        CollectionSuffix::Delayed,
        CollectionSuffix::Marker,
        CollectionSuffix::Events,
        CollectionSuffix::Prioritized,
        CollectionSuffix::PriorityCounter,
    ]
    .map(|collection| collection.to_collection_name(prefix, name));
    let target_key = CollectionSuffix::from(target_state).to_collection_name(prefix, name);
    let stop = (date_time + TimeDelta::milliseconds(interval_ms)).timestamp_millis();
    let start = date_time.timestamp_millis();
    let mut pipeline = redis::pipe();
    let jobs: Vec<String> = conn
        .zrangebyscore_limit(&delayed_key, start, stop, 0, 1000)
        .await?;
    if !jobs.is_empty() {
        let removed: redis::Value = conn.zrem(&delayed_key, &jobs).await?;
    }
    pipeline.atomic();
    if !jobs.is_empty() {
        for job_id in jobs.iter() {
            let job_key = CollectionSuffix::Job(job_id.to_owned()).to_collection_name(prefix, name);
            let priority: u64 = conn.hget(&job_key, "priority").await?;
            if priority == 0 {
                pipeline.lpush(&target_key, job_id);
            } else {
                let prior_counter: u64 = conn.incr(&priority_counter_key, 1).await?;
                let score = calculate_next_priority_score(priority, prior_counter);
                pipeline.zadd(&prioritized_key, score, job_id);
            }

            //self.add_base_marker(paused, &mut pipeline);
            let items = [("event", "wait"), ("job_id", job_id), ("prev", "delayed")];
            pipeline.xadd(&events_stream_key, "*", &items);
            pipeline.hset(job_key, "delay", 0);
        }
    }
    if !pipeline.is_empty() {
        tokio::time::sleep(Duration::from_millis((interval_ms) as u64)).await;
        pipeline.query_async::<()>(&mut conn).await?;
    }
    Ok(jobs)
}
/// Utilily function for pipelining
pub async fn prepare_for_insert<D: Serialize, R: Serialize, P: Serialize>(
    opts: JobOptions,
    queue: &Queue<D, R, P>,
    job: &mut Job<D, R, P>,
    name: &str,
    pipeline: &mut redis::Pipeline,
    conn: &mut deadpool_redis::Connection,
) -> KioResult<()> {
    let JobOptions {
        priority,
        delay,
        id,
        attempts,
        remove_on_fail,
        remove_on_complete,
        ref backoff,
    } = opts;
    job.add_opts(opts);
    let queue_name = format!("{}:{}", &queue.prefix, &queue.name);
    let id = queue.fetch_id().await?;
    if delay > 0 && delay < MIN_DELAY_MS_LIMIT {
        return Err(QueueError::DelayBelowAllowedLimit {
            limit_ms: MIN_DELAY_MS_LIMIT,
            current_ms: delay,
        }
        .into());
    };
    //queue.job_count.
    let prefix = &queue.prefix;
    let job_key =
        CollectionSuffix::Job(id.to_string()).to_collection_name(&queue.prefix, &queue.name);
    let events_keys = CollectionSuffix::Events.to_collection_name(&queue.prefix, &queue.name);

    let waiting_or_paused = if !queue.is_paused() {
        CollectionSuffix::Wait
    } else {
        CollectionSuffix::Paused
    };
    let to_delay = delay > 0;
    let to_priorize = priority > 0 && !to_delay;
    let waiting_key = waiting_or_paused.to_collection_name(&queue.prefix, &queue.name);
    pipeline.atomic();
    if to_delay {
        let delayed_key = CollectionSuffix::Delayed.to_collection_name(&queue.prefix, &queue.name);
        let expected_active_time = job.ts + TimeDelta::milliseconds(delay as i64);
        pipeline.zadd(delayed_key, id, expected_active_time.timestamp_millis());
        job.state = JobState::Delayed;
    }
    // handle prioritized_jobs
    else if to_priorize {
        let priority_counter_key =
            CollectionSuffix::PriorityCounter.to_collection_name(&queue.prefix, &queue.name);
        let prioritized_key =
            CollectionSuffix::Prioritized.to_collection_name(&queue.prefix, &queue.name);
        let prior_counter: u64 = conn.incr(&priority_counter_key, 1).await?;
        let score = calculate_next_priority_score(priority, prior_counter);
        pipeline.zadd(&prioritized_key, id, score);
        job.state = JobState::Priorized;
    } else {
        pipeline.lpush(&waiting_key, id.to_string());
    }
    job.id = Some(id.to_string());
    let fields = serialize_into_pairs(&job);
    pipeline.hset_multiple(&job_key, &fields);
    let event = if to_delay {
        JobState::Delayed
    } else if to_priorize {
        JobState::Priorized
    } else {
        JobState::Wait
    };
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
    Ok(())
}
