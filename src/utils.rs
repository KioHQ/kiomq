use crate::error::{BacktraceCatcher, CaughtError, CaughtPanicInfo};
use crate::worker::{JobMap, ProcessingQueue, WorkerCallback};
use crate::{utils, EventParameters, JobState, WorkerOpts};
use chrono::Utc;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Write;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::KioResult;
use crate::MoveToActiveResult;
use crate::{Job, Queue};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};

use std::sync::{Arc, Mutex};

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
    let [job_id_key, stalled_key, active_key, completed_key] = [
        CollectionSuffix::Id,
        CollectionSuffix::Stalled,
        CollectionSuffix::Active,
        CollectionSuffix::Completed,
    ]
    .map(|key| key.to_collection_name(prefix, name));
    let mut pipeline = redis::pipe();
    pipeline.atomic();
    pipeline.zcard(completed_key);
    pipeline.llen(active_key);

    pipeline.scard(stalled_key);
    pipeline.get(job_id_key);
    let (completed, active, stalled, last_id) = pipeline.query_async(conn).await?;

    Ok(JobMetrics {
        active,
        last_id,
        stalled,
        completed,
    })
}

// ---- UTIL FUNCTIONS for the worker

use futures_concurrency::future::future_group::Key;
use tokio::sync::mpsc::UnboundedSender;
#[async_backtrace::framed]
pub(crate) async fn process_job<D, R, P>(
    task_sender: UnboundedSender<Key>,
    job: Job<D, R, P>,
    token: String,
    jobs_in_progress: JobMap<D, R, P>,
    queue: Arc<Queue<D, R, P>>,
    callback: Arc<WorkerCallback<D, R, P>>,
) -> KioResult<()>
where
    R: Serialize + Send + Clone + DeserializeOwned,
    D: Clone + Serialize + DeserializeOwned,
    P: Clone + Serialize + DeserializeOwned,
{
    use crate::JobState;
    let job_id = job.id.clone();
    let conn = queue.conn_pool.get().await?;
    let callback = async_backtrace::frame!(callback(conn, job));
    let returned = BacktraceCatcher::catch(callback).await;
    match returned {
        Ok(result) => {
            // move the job to failed state here;
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
                        None,
                    )
                    .await?;
                if let Some((_, (job, _, Some(handle)))) = jobs_in_progress.remove(job_id) {
                    //handle.abort(); // remove task from the queue

                    queue
                        .emit(
                            JobState::Completed,
                            EventParameters::Completed {
                                prev_state: Some(job.state),
                                job: completed,
                                result,
                            },
                        )
                        .await;
                    task_sender.send(handle);
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

            let frames = backtrace.and_then(|frames| serde_json::to_string(&frames).ok());
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
                        &serde_json::to_string(&failed_reason)?,
                        frames,
                    )
                    .await?;
                if let Some((_, (job, _, Some(handle)))) = jobs_in_progress.remove(job_id) {
                    queue
                        .emit(
                            JobState::Failed,
                            EventParameters::Failed {
                                prev_state: job.state,
                                job: failed_job,
                                error: failed_reason,
                            },
                        )
                        .await;

                    task_sender.send(handle);
                }
            }
        }
    }
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
    D: DeserializeOwned + Clone + Serialize,
    R: DeserializeOwned + Clone + Serialize,
    P: DeserializeOwned + Clone + Serialize,
{
    // handle pausing or closing;
    if closed {
        return Ok(None);
    }

    let date_time = Utc::now();
    let interval_ms = 10;
    if let Ok(promoted_jobs) = queue.promote_delayed_jobs(date_time, interval_ms).await {
        if !promoted_jobs.is_empty() {
            dbg!(promoted_jobs);
        }
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
);

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
    ) = params;

    let now = tokio::time::Instant::now();
    use futures_concurrency::future::future_group::Key;
    let (sender, mut reciver) = tokio::sync::mpsc::unbounded_channel::<Key>();

    while !cancellation_token.is_cancelled() {
        while !cancellation_token.is_cancelled()
            && processing.clone().lock().await.len() < opts.concurrency
        {
            let token_prefix = active_job_count.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
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
                    (job.clone(), token.clone(), None),
                );

                let queue = queue.clone();
                let callback = processor.clone();

                let task = tokio::spawn(async_backtrace::frame!(process_job(
                    sender.clone(),
                    job,
                    token,
                    jobs_in_progress.clone(),
                    queue,
                    callback
                )));
                let handle = processing.lock().await.insert(task);
                // task handle
                if let Some(mut re) = jobs_in_progress.get_mut(&id) {
                    let (_, _, stored_handle) = re.value_mut();
                    stored_handle.replace(handle);
                }
            }
        }
        //if processing.lock().await.len() == opts.concurrency {
        //    // we wait for current jobs to complete before adding others;
        //    while let Some(done) = processing.lock().await.join_next().await {
        //    }
        //}
        if let Some(key) = reciver.recv().await {
            processing.clone().lock().await.remove(key);
            active_job_count.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            dbg!(key, processing.lock().await.len());
        }
    }
    if cancellation_token.is_cancelled() {
        is_active.compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed);
    }
    Ok(())
}
use crate::Dt;
use chrono::TimeDelta;
use redis::AsyncCommands;
use std::time::Duration;

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
    let removed: redis::Value = conn.zrem(&delayed_key, &jobs).await?;
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
