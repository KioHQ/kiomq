use crossbeam_queue::SegQueue;
use futures::future::Future;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::{FutureExt, StreamExt};
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::error::{JobError, KioError, QueueError};
use crate::events::{QueueStreamEvent, StreamEventId};
use crate::job::{Job, JobState};
use crate::utils::{
    calculate_next_priority_score, prepare_for_insert, process_queue_events, promote_jobs,
    query_all_batched, resume_helper, serialize_into_pairs, JobQueue, ReadStreamArgs,
};
use crate::worker::{WorkerOpts, MIN_DELAY_MS_LIMIT};
use crate::{
    get_job_metrics, queue, BackOff, BackOffJobOptions, BackOffOptions, Dt, FailedDetails,
    JobOptions, JobToken, KeepJobs, KioResult, RemoveOnCompletionOrFailure, Repeat, StoredFn,
    Trace,
};
use async_backtrace::backtrace;
use chrono::{TimeDelta, Utc};
use deadpool_redis::{Config, Pool, Runtime};
use serde::de::{value, DeserializeOwned, Error};
use serde::{ser, Deserialize, Serialize};
mod options;

pub(crate) use options::{CollectionSuffix, ProcessedResult};
pub use options::{JobMetrics, QueueEventMode, QueueOpts, RetryOptions};
use redis::{
    self, pipe, AsyncCommands, FromRedisValue, JsonAsyncCommands, LposOptions, Pipeline,
    RedisResult, ToRedisArgs, Value,
};
/// a counter for adding b/ulk jobs,
static START: AtomicU64 = AtomicU64::new(0);
static PC_COUNTER: AtomicU64 = AtomicU64::new(0);
use crate::{EventEmitter, EventParameters};
use atomig::Atomic;
use derive_more::Debug;
#[derive(Debug, Clone)]
pub struct Queue<D, R, P> {
    pub prefix: String,
    pub name: String,
    pub paused: Arc<AtomicBool>,
    pub job_count: Arc<AtomicU64>,
    pub current_metrics: Arc<JobMetrics>,
    pub opts: QueueOpts,
    pub(crate) event_mode: Arc<Atomic<QueueEventMode>>,
    emitter: EventEmitter<D, R, P>,
    #[debug(skip)]
    pub stream_listener: Arc<JoinHandle<KioResult<()>>>,
    #[debug(skip)]
    pub(crate) conn_pool: Arc<Pool>,
    pub(crate) backoff: BackOff,
    pub(crate) worker_notifier: Arc<Notify>,
    pub pause_workers: Arc<AtomicBool>,
}

impl<
        D: Clone + Serialize + DeserializeOwned + Send + 'static,
        R: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
        P: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
    > Queue<D, R, P>
{
    pub async fn new(
        prefix: Option<&str>,
        name: &str,
        cfg: &Config,
        queue_opts: Option<QueueOpts>,
    ) -> KioResult<Self> {
        use redis::streams::{StreamReadOptions, StreamReadReply};
        use typed_emitter::TypedEmitter;
        let opts = queue_opts.unwrap_or_default();
        let pool = cfg.create_pool(Some(Runtime::Tokio1))?;
        let prefix = prefix.unwrap_or("{kio}").to_lowercase();
        let meta_key = CollectionSuffix::Meta.to_collection_name(&prefix, name);
        let name = name.to_lowercase();
        let emitter = Arc::new(TypedEmitter::new());
        let mut conn = pool.get().await?;
        let mut metrics = JobMetrics::default();
        if let Ok(current_metrics) = get_job_metrics(&prefix, &name, &mut conn).await {
            metrics = current_metrics;
        }
        let meta_key = CollectionSuffix::Meta.to_collection_name(&prefix, &name);
        let events_mode_exits: bool = conn.hexists(&meta_key, "event_mode").await?;
        let event_mode = metrics.event_mode.clone();
        if let Some(passed_mode) = opts.event_mode {
            if !events_mode_exits && passed_mode != event_mode.load(Ordering::Acquire) {
                conn.hset::<_, _, _, ()>(&meta_key, "event_mode", passed_mode)
                    .await;
                event_mode.swap(passed_mode, Ordering::AcqRel);
            }
        }
        let worker_notifier: Arc<Notify> = Arc::default();
        let current_metrics = Arc::new(metrics);
        let is_paused = current_metrics.paused.load(Ordering::Relaxed);
        let mut connection = pool.get().await?;
        let stream_key = CollectionSuffix::Events.to_collection_name(&prefix, &name);
        let queue_name = CollectionSuffix::Prefix.to_collection_name(&prefix, &name);
        let emitter_clone = emitter.clone();
        let metrics = current_metrics.clone();
        let prefix_clone = prefix.clone();
        let name_clone = name.clone();
        let notifier = worker_notifier.clone();
        let pause_workers: Arc<AtomicBool> = Arc::default();
        let pause_workers_clone = pause_workers.clone();
        let event_mode_clone = event_mode.clone();
        let connection_info = cfg.connection.clone().unwrap();

        let task: JoinHandle<KioResult<()>> = tokio::spawn(
            async move {
                let block_interval = 5000; // 100 seconds
                let notifier = notifier.clone();
                let pause_workers = pause_workers_clone.clone();
                let is_inital = AtomicBool::new(true);
                let id = Uuid::new_v4();
                let consumer_group = format!("{prefix_clone}-{name_clone}-group-{id}",);
                let consumer_name = format!("consumer-{id}");
                let c_group: () = connection
                    .xgroup_create_mkstream(&stream_key, &consumer_group, "$")
                    .await?;
                let client = redis::Client::open(connection_info)?;
                let (mut sink, mut source) = client.get_async_pubsub().await?.split();
                sink.subscribe(&stream_key).await?;

                loop {
                    let args: ReadStreamArgs<D, R, P> = (
                        event_mode_clone.load(Ordering::Acquire),
                        &stream_key,
                        &queue_name,
                        block_interval,
                        &consumer_group,
                        &consumer_name,
                        &emitter_clone,
                        metrics.clone(),
                        &prefix_clone,
                        &name_clone,
                        &mut connection,
                        &mut source,
                    );
                    process_queue_events(args).await?;

                    if metrics.is_idle()
                        && !is_inital.load(Ordering::Acquire)
                        && !pause_workers
                            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                            .unwrap_or(true)
                    {
                        println!("send pause signal ");
                    } else {
                        resume_helper(&metrics, &pause_workers, &notifier);
                    }
                    is_inital.compare_exchange(true, false, Ordering::AcqRel, Ordering::Relaxed);
                }

                Ok(())
            }
            .boxed(),
        );
        let stream_listener = Arc::new(task);
        //
        Ok(Self {
            event_mode,
            pause_workers,
            worker_notifier,
            backoff: BackOff::new(),
            opts,
            current_metrics,
            stream_listener,
            job_count: Arc::default(),
            emitter,
            prefix,
            name,
            paused: Arc::new(AtomicBool::new(is_paused)),
            conn_pool: Arc::new(pool),
        })
    }

    pub async fn bulk_add<I: Iterator<Item = (String, Option<JobOptions>, D)> + Send>(
        &self,
        iter: I,
    ) -> KioResult<Vec<Job<D, R, P>>> {
        let mut conn = self.get_connection().await?;
        let mut result = vec![];
        let id_key = CollectionSuffix::Id.to_collection_name(&self.prefix, &self.name);
        let priority_counter_key =
            CollectionSuffix::PriorityCounter.to_collection_name(&self.prefix, &self.name);
        let max_len_hint = iter.size_hint().1.unwrap_or_default();
        let mut pipeline = redis::Pipeline::with_capacity((max_len_hint * 3) + 1);
        pipeline.atomic();
        let end: usize = conn.incr(&id_key, max_len_hint).await?;
        let counter: Option<u64> = conn.get(&priority_counter_key).await?;
        let pc = counter.unwrap_or_default() + 1;
        PC_COUNTER.store(pc, Ordering::Relaxed);
        let mut start = (end - max_len_hint) + 1;
        START.store(start as u64, Ordering::Relaxed);
        let mut is_prioritized = false;
        //let mut iter = iter.par_bridge();
        for (ref name, opts, data) in iter {
            let mut opts = opts.unwrap_or_default();
            self.update_job_opts(&mut opts);
            let queue_name = format!("{}:{}", self.prefix, self.name);
            let id = START.fetch_add(1, Ordering::AcqRel);
            let prior_counter = if opts.priority > 0 {
                is_prioritized = true;
                PC_COUNTER.fetch_add(1, Ordering::AcqRel)
            } else {
                PC_COUNTER.load(Ordering::Acquire)
            };
            let event_mode = self.event_mode.load(Ordering::Acquire);
            let is_paused = self.is_paused();
            let mut job = Job::<D, R, P>::new(name, Some(data), opts.id, Some(&queue_name));
            prepare_for_insert(
                &queue_name,
                event_mode,
                is_paused,
                id,
                prior_counter,
                opts,
                &mut job,
                name,
                &mut pipeline,
            )?;
            job.id = Some(id);
            result.push(job)
        }
        if is_prioritized {
            pipeline.incr(&priority_counter_key, PC_COUNTER.load(Ordering::Acquire));
        }

        query_all_batched(conn, pipeline).await?;
        Ok(result)
    }
    pub async fn bulk_add_only<I: Iterator<Item = (String, Option<JobOptions>, D)> + Send>(
        &self,
        iter: I,
    ) -> KioResult<()> {
        let mut conn = self.get_connection().await?;
        let id_key = CollectionSuffix::Id.to_collection_name(&self.prefix, &self.name);
        let priority_counter_key =
            CollectionSuffix::PriorityCounter.to_collection_name(&self.prefix, &self.name);
        let max_len_hint = iter.size_hint().1.unwrap_or_default();
        let mut pipeline = redis::Pipeline::with_capacity((max_len_hint * 3) + 1);
        pipeline.atomic();
        let end: usize = conn.incr(&id_key, max_len_hint).await?;
        let counter: Option<u64> = conn.get(&priority_counter_key).await?;
        let pc = counter.unwrap_or_default() + 1;
        PC_COUNTER.store(pc, Ordering::Relaxed);
        let mut start = (end - max_len_hint) + 1;
        START.store(start as u64, Ordering::Relaxed);
        let mut is_prioritized = false;
        //let mut iter = iter.par_bridge();
        for (ref name, opts, data) in iter {
            let mut opts = opts.unwrap_or_default();
            self.update_job_opts(&mut opts);
            let queue_name = format!("{}:{}", self.prefix, self.name);
            let id = START.fetch_add(1, Ordering::AcqRel);
            let prior_counter = if opts.priority > 0 {
                is_prioritized = true;
                PC_COUNTER.fetch_add(1, Ordering::AcqRel)
            } else {
                PC_COUNTER.load(Ordering::Acquire)
            };
            let event_mode = self.event_mode.load(Ordering::Acquire);
            let is_paused = self.is_paused();
            let mut job = Job::<D, R, P>::new(name, Some(data), opts.id, Some(&queue_name));
            prepare_for_insert(
                &queue_name,
                event_mode,
                is_paused,
                id,
                prior_counter,
                opts,
                &mut job,
                name,
                &mut pipeline,
            )?;
        }
        if is_prioritized {
            pipeline.incr(&priority_counter_key, PC_COUNTER.load(Ordering::Acquire));
        }
        println!("sending {} commands to redis", pipeline.len());
        //pipeline.query_async::<()>(&mut conn).await?;
        query_all_batched(conn, pipeline).await?;

        Ok(())
    }

    pub async fn add_job(
        &self,
        name: &str,
        data: D,
        opts: Option<JobOptions>,
    ) -> Result<Job<D, R, P>, KioError> {
        let mut opts = opts.unwrap_or_default();
        self.update_job_opts(&mut opts);
        let queue_name = format!("{}:{}", self.prefix, self.name);
        let id = self.fetch_id().await?;
        let event_mode = self.event_mode.load(Ordering::Acquire);
        let is_paused = self.is_paused();
        let mut pipeline = redis::pipe();
        pipeline.atomic();
        let mut job = Job::<D, R, P>::new(name, Some(data), opts.id, Some(&queue_name));
        let priority_counter_key =
            CollectionSuffix::PriorityCounter.to_collection_name(&self.prefix, &self.name);
        let mut conn = self.get_connection().await?;
        let prior_counter: Option<u64> = conn.get(&priority_counter_key).await?;

        let prior_counter = prior_counter.unwrap_or_default();
        if opts.priority > 0 {
            pipeline.incr(&priority_counter_key, 1);
        }
        prepare_for_insert(
            &queue_name,
            event_mode,
            is_paused,
            id,
            prior_counter + 1,
            opts,
            &mut job,
            name,
            &mut pipeline,
        )?;
        pipeline.query_async::<()>(&mut conn).await?;
        job.id = Some(id);
        Ok(job)
    }
    pub fn current_jobs(&self) -> u64 {
        self.job_count.load(std::sync::atomic::Ordering::Acquire)
    }
    pub async fn get_job(&self, id: u64) -> Option<Job<D, R, P>> {
        use redis::Value;
        let job_key = CollectionSuffix::Job(id).to_collection_name(&self.prefix, &self.name);
        let mut conn = self.conn_pool.get().await.ok()?;
        let value: Option<Job<_, _, _>> = conn.hgetall(job_key).await.ok()?;
        value
    }

    pub async fn move_job_to_state(
        &self,
        job_id: u64,
        from: JobState,
        to: JobState,
        value: Option<ProcessedResult<R>>,
        ts: Option<i64>,
        backtrace: Option<Trace>,
    ) -> KioResult<()> {
        let move_to_failed_or_completed = matches!(to, JobState::Failed | JobState::Completed);
        // do nothing if the  queue_is_paused.
        if self.is_paused() {
            return Ok(());
        }
        use redis::Value;
        let previous_suffix = from.into();
        let next_state_suffix = to.into();
        let [job_key, events_key, prev_state_key, next_state_key] = [
            CollectionSuffix::Job(job_id),
            CollectionSuffix::Events,
            previous_suffix,
            next_state_suffix,
        ]
        .map(|s| s.to_collection_name(&self.prefix, &self.name));
        let mut conn = self.conn_pool.get().await?;
        let job_exists: bool = conn.exists(&job_key).await?;
        if !job_exists {
            return Err(JobError::JobNotFound.into());
        }
        let mut pipeline = redis::pipe();
        pipeline.atomic();
        if move_to_failed_or_completed {
            pipeline.hincr(&job_key, "attemptsMade", 1);
            pipeline.lrem(prev_state_key, 1, job_id);
            pipeline.zadd(
                next_state_key,
                job_id,
                ts.unwrap_or_else(|| Utc::now().timestamp_micros()),
            );
        } else {
            // only move the value if it doesn't exist in the target list
            let job_id_exists_in_target: Option<usize> = conn
                .lpos(&next_state_key, job_id, LposOptions::default())
                .await?;
            if job_id_exists_in_target.is_none() {
                pipeline.lrem(prev_state_key, 1, job_id);
                pipeline.rpush(next_state_key, job_id);
            }
        }
        let dst = simd_json::to_string(&to)?;
        pipeline.hset(&job_key, "state", dst);
        if let Some(backtrace) = backtrace {
            // there is an empty vect stored
            let mut previous: Vec<u8> = conn.hget(&job_key, "stackTrace").await?;
            let mut previous: Vec<Trace> = simd_json::from_slice(&mut previous)?;
            previous.push(backtrace);
            pipeline.hset(
                &job_key,
                "stackTrace",
                simd_json::to_string_pretty(&previous)?,
            );
        }

        let payload_key = if matches!(to, JobState::Failed) {
            "failedReason"
        } else {
            "returnedValue"
        };
        if let Some(data) = value.as_ref() {
            let data = simd_json::to_string_pretty(&data)?;
            pipeline.hset(&job_key, payload_key, &data);
            pipeline.hset(&job_key, "finishedOn", ts);
        }
        // check of retries_exhausion here;

        match self.event_mode.load(Ordering::Acquire) {
            QueueEventMode::PubSub => {
                let mut event: QueueStreamEvent<R, P> = QueueStreamEvent {
                    event: to,
                    prev: Some(from),
                    job_id,
                    ..Default::default()
                };
                if let Some(data) = value {
                    match data {
                        ProcessedResult::Failed(failed_details) => {
                            event.failed_reason = Some(failed_details)
                        }
                        ProcessedResult::Success(value) => event.retuned_value = Some(value),
                    }
                }

                pipeline.publish(events_key, event);
            }
            QueueEventMode::Stream => {
                let data = simd_json::to_string_pretty(&value)?;
                let mut items = vec![
                    ("event", to.to_string().to_lowercase()),
                    ("prev", from.to_string().to_lowercase()),
                    ("job_id", job_id.to_string()),
                    (payload_key, data),
                ];
                pipeline.xadd(events_key, "*", &items);
            }
        }

        let _: redis::Value = pipeline.query_async(&mut conn).await?;
        Ok(())
    }
    pub async fn fetch_waiting_jobs(&self) -> KioResult<Vec<Job<D, R, P>>> {
        if self.is_paused() {
            return Ok(vec![]);
        }
        let waiting_key = CollectionSuffix::Wait.to_collection_name(&self.prefix, &self.name);
        let mut conn = self.conn_pool.get().await?;
        let waiting: Vec<u64> = conn.lrange(waiting_key, 0, -1).await?;
        let mut pipeline = redis::pipe();

        for id in waiting {
            let job_key = CollectionSuffix::Job(id).to_collection_name(&self.prefix, &self.name);
            pipeline.hgetall(job_key);
        }
        let mut jobs: Vec<Job<D, R, P>> = pipeline.query_async(&mut conn).await?;
        jobs.sort_unstable_by(|a, b| a.id.cmp(&b.id));
        Ok(jobs)
    }
    /// pauses the queue if not resumed and vice-versa
    pub async fn pause_or_resume(&self) -> Result<(), KioError> {
        // if its paused
        let pause = !self.is_paused();
        let [wait_key, events_key, meta_key, paused_key] = [
            CollectionSuffix::Wait,
            CollectionSuffix::Events,
            CollectionSuffix::Meta,
            CollectionSuffix::Paused,
        ]
        .map(|s| s.to_collection_name(&self.prefix, &self.name));
        // Plan: rename wait collection to paused
        let mut conn = self.conn_pool.get().await?;
        let src = if pause { &wait_key } else { &paused_key };
        let dst = if pause { &paused_key } else { &wait_key };
        let mut pipeline = redis::pipe();
        pipeline.atomic();
        if conn.exists::<_, bool>(src).await.unwrap_or_default() {
            pipeline.rename(src, dst);
        }
        let state = if pause {
            JobState::Paused
        } else {
            JobState::Resumed
        };
        match pause {
            true => pipeline.hset(meta_key, CollectionSuffix::Paused, 1),
            _ => pipeline.hdel(meta_key, CollectionSuffix::Paused),
        };
        let items = [("event", state)];

        match self.event_mode.load(Ordering::Acquire) {
            QueueEventMode::PubSub => {
                let event = QueueStreamEvent::<R, P> {
                    event: state,
                    ..Default::default()
                };

                pipeline.publish(events_key, event);
            }
            QueueEventMode::Stream => {
                pipeline.xadd(events_key, "*", &items);
            }
        }
        let _: redis::Value = pipeline.query_async(&mut conn).await?;
        self.paused
            .store(pause, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
    pub async fn wait_for_job(&self, block_duration: i64) -> KioResult<u64> {
        use chrono::TimeDelta;
        if self.is_paused() {
            return Err(KioError::QueueError(
                crate::error::QueueError::CantOperateWhenPaused,
            ));
        }
        let marker_key = CollectionSuffix::Marker.to_collection_name(&self.prefix, &self.name);
        let block_until = TimeDelta::milliseconds(block_duration).as_seconds_f64();
        let mut con = self.conn_pool.get().await?;
        let (_, _, score): (String, String, u64) = con.bzpopmin(&marker_key, block_until).await?;
        Ok(score)
    }

    pub async fn extend_lock(
        &self,
        job_id: u64,
        lock_duration: u64,
        token: JobToken,
    ) -> KioResult<(bool)> {
        let [lock_key, stalled_key] = [CollectionSuffix::Lock(job_id), CollectionSuffix::Stalled]
            .map(|key| key.to_collection_name(&self.prefix, &self.name));
        let mut conn = self.conn_pool.get().await?;
        let mut pipeline = redis::pipe();
        pipeline.atomic();
        let previous: Option<JobToken> = conn.get(&lock_key).await?;
        if let Some(prev_token) = previous {
            if prev_token == token {
                pipeline.pset_ex(lock_key, token, lock_duration);
                pipeline.srem(stalled_key, job_id.to_string());
                let result: redis::Value = pipeline.query_async(&mut conn).await?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub async fn make_stalled_jobs_wait(
        &self,
        opts: &WorkerOpts,
    ) -> KioResult<(Vec<u64>, Vec<u64>)> {
        let ts = Utc::now().timestamp_micros();
        let [wait_key, active_key, events_key, meta_key, paused_key, stalled_key, stalled_check, failed_key, marker_key] =
            [
                CollectionSuffix::Wait,
                CollectionSuffix::Active,
                CollectionSuffix::Events,
                CollectionSuffix::Meta,
                CollectionSuffix::Paused,
                CollectionSuffix::Stalled,
                CollectionSuffix::StalledCheck,
                CollectionSuffix::Failed,
                CollectionSuffix::Marker,
            ]
            .map(|s| s.to_collection_name(&self.prefix, &self.name));
        let mut conn = self.conn_pool.get().await?;
        let mut failed = vec![];
        let mut stalled = vec![];
        let stalled_key_exists: bool = conn.exists(&stalled_check).await?;
        if stalled_key_exists {
            return Ok((failed, stalled));
        }
        let _: () = conn
            .pset_ex(&stalled_check, ts, opts.stalled_interval)
            .await?;
        // trim
        let stalling: Vec<u64> = conn.smembers(&stalled_key).await?;
        if !stalling.is_empty() {
            for job_id in stalling {
                let job_key =
                    CollectionSuffix::Job(job_id).to_collection_name(&self.prefix, &self.name);
                let job_lock_key =
                    CollectionSuffix::Lock(job_id).to_collection_name(&self.prefix, &self.name);
                if !conn
                    .exists::<_, bool>(&job_lock_key)
                    .await
                    .unwrap_or_default()
                {
                    let mut inner = redis::pipe();

                    // If this job has been stalled too many times, such as if it crashes the worker, then fail it.
                    inner.hincr(&job_key, "stalledCounter", 1_u64);
                    inner.hget(&job_key, "state");
                    inner.hget(&job_key, "attemptsMade");
                    let (stalled_count, from, attempts_made): (u64, JobState, u64) =
                        inner.query_async(&mut conn).await?;

                    if stalled_count > opts.max_stalled_count {
                        // Add job removal option logic here
                        let mut pipeline = redis::pipe();
                        let reason = "job stalled more than allowable limit".to_lowercase();
                        let to = JobState::Failed;
                        let failed_reason = FailedDetails {
                            run: attempts_made + 1,
                            reason,
                        };

                        self.move_job_to_state(
                            job_id,
                            from,
                            to,
                            Some(ProcessedResult::Failed(failed_reason)),
                            None,
                            None,
                        )
                        .await?;
                        failed.push(job_id);
                    } else {
                        let (is_paused, target) = self.get_target_list();
                        if !is_paused {
                            let _: () = conn.zadd(&marker_key, 0, "0").await?;
                        }
                        self.move_job_to_state(job_id, JobState::Active, target, None, None, None)
                            .await?;
                        // emit  stalled;
                        stalled.push(job_id);
                    }
                }
            }
        } else {
            // mark stalled Jobs
            let active: Vec<u64> = conn.lrange(&active_key, 0, -1).await?;
            let mut pipeline = redis::pipe();
            pipeline.atomic();
            if !active.is_empty() {
                active.chunks(2).for_each(|chunk| {
                    pipeline.sadd(&stalled_key, chunk);
                });

                let _: () = pipeline.query_async(&mut conn).await?;
            }
        }

        Ok((failed, stalled))
    }

    pub fn get_target_list(&self) -> (bool, JobState) {
        let paused = self.is_paused();
        if paused {
            return (paused, JobState::Paused);
        }
        (paused, JobState::Wait)
    }

    pub async fn move_to_active(
        &self,
        token: JobToken,
        opts: &WorkerOpts,
    ) -> KioResult<MoveToActiveResult<D, R, P>> {
        let [wait_key, active_key, events_key, meta_key, paused_key, stalled_key, stalled_check, failed_key, marker_key, delayed_key] =
            [
                CollectionSuffix::Wait,
                CollectionSuffix::Active,
                CollectionSuffix::Events,
                CollectionSuffix::Meta,
                CollectionSuffix::Paused,
                CollectionSuffix::Stalled,
                CollectionSuffix::StalledCheck,
                CollectionSuffix::Failed,
                CollectionSuffix::Marker,
                CollectionSuffix::Delayed,
            ]
            .map(|s| s.to_collection_name(&self.prefix, &self.name));
        let ts = Utc::now().timestamp_micros();
        let mut conn = self.conn_pool.get().await?;
        let (is_paused, target_state) = self.get_target_list();
        let target_key =
            CollectionSuffix::from(target_state).to_collection_name(&self.prefix, &self.name);
        let mut job_id: Option<u64> = conn.rpoplpush(&wait_key, &active_key).await?;
        let mut prepare_job = |id: u64, mut conn: deadpool_redis::Connection| async move {
            let job_id_key = CollectionSuffix::Job(id).to_collection_name(&self.prefix, &self.name);
            let prev_state: Option<JobState> = conn.hget(&job_id_key, "state").await.ok();
            let job = self
                .prepare_job_for_processing(
                    token,
                    id,
                    ts as u64,
                    opts,
                    prev_state.unwrap_or_default(),
                )
                .await?;

            Ok::<_, KioError>((job, prev_state))
        };
        let connection = self.conn_pool.get().await?;
        match job_id {
            Some(job_id) => Ok(MoveToActiveResult::from_job_state_pair(
                prepare_job(job_id, connection).await?,
            )),
            None => {
                let connection = self.conn_pool.get().await?;
                if let Some(id) = self.move_job_from_priorty_to_active().await? {
                    let (job, state) = prepare_job(id, connection).await?;
                    return Ok(MoveToActiveResult::ProcessJob(job.boxed()));
                }

                let mut next_delayed_timestamp: Vec<(u64, u64)> =
                    conn.zrange_withscores(&delayed_key, 0, 0).await?;
                let mut next_delay = next_delayed_timestamp.pop().unwrap_or_default().1;
                next_delay /= 0x1000;

                Ok(MoveToActiveResult::DelayUntil(next_delay))
            }
        }
        // fetch the next delayed_timestamp;
    }
    pub async fn prepare_job_for_processing(
        &self,
        token: JobToken,
        job_id: u64,
        ts: u64,
        opts: &WorkerOpts,
        prev_state: JobState,
    ) -> KioResult<Job<D, R, P>> {
        let [wait_key, active_key, events_key, meta_key, paused_key, stalled_key, stalled_check, failed_key, marker_key, job_key, job_lock_key] =
            [
                CollectionSuffix::Wait,
                CollectionSuffix::Active,
                CollectionSuffix::Events,
                CollectionSuffix::Meta,
                CollectionSuffix::Paused,
                CollectionSuffix::Stalled,
                CollectionSuffix::StalledCheck,
                CollectionSuffix::Failed,
                CollectionSuffix::Marker,
                CollectionSuffix::Job(job_id),
                CollectionSuffix::Lock(job_id),
            ]
            .map(|s| s.to_collection_name(&self.prefix, &self.name));
        let mut conn = self.conn_pool.get().await?;

        let _: () = conn
            .pset_ex(&job_lock_key, token, opts.lock_duration)
            .await?;
        self.move_job_to_state(job_id, prev_state, JobState::Active, None, None, None)
            .await?;
        let items = [
            ("processedOn", simd_json::to_string(&ts)?),
            ("token", simd_json::to_string(&token)?),
        ];
        let _: () = conn.hset_multiple(&job_key, &items).await?;
        let job = conn.hgetall(&job_key).await?;
        Ok(job)
    }

    pub(crate) async fn move_job_to_finished_or_failed(
        &self,
        job_id: u64,
        ts: i64,
        token: JobToken,
        move_to_state: JobState,
        processed: ProcessedResult<R>,
        backtrace: Option<Trace>,
    ) -> KioResult<Job<D, R, P>> {
        let [job_key, job_lock_key, active_key, completed_key, events_stream_key, stalled_key] = [
            CollectionSuffix::Job(job_id),
            CollectionSuffix::Lock(job_id),
            CollectionSuffix::Active,
            CollectionSuffix::Completed,
            CollectionSuffix::Events,
            CollectionSuffix::Stalled,
        ]
        .map(|e| e.to_collection_name(&self.prefix, &self.name));

        let mut conn = self.conn_pool.get().await?;
        let mut pipeline = redis::pipe();
        pipeline.atomic();
        let job_exists: bool = conn.exists(&job_key).await?;
        if !job_exists {
            return Err(JobError::JobNotFound.into());
        }
        let lock_token: Option<JobToken> = conn.get(&job_lock_key).await?;
        if let Some(local) = lock_token {
            if local != token {
                return Err(JobError::JobLockMismatch.into());
            }
            pipeline.del(&job_lock_key);
            pipeline.srem(&stalled_key, job_id);
        } else if backtrace.is_some() {
            return Err(JobError::JobLockNotExist.into());
        }
        let prev_state = conn.hget(&job_key, "state").await?;
        // Todo: remove any dependencies too here ;
        self.move_job_to_state(
            job_id,
            prev_state,
            move_to_state,
            Some(processed),
            Some(ts),
            backtrace,
        )
        .await;

        //remove element from stalled set too;
        let val: redis::Value = pipeline.query_async(&mut conn).await?;
        let job = conn.hgetall(job_key).await?;
        Ok(job)
    }
    pub async fn emit(&self, event: JobState, data: EventParameters<D, R, P>) {
        self.emitter.emit(event, data).await
    }
    pub async fn on<F, C>(&self, event: JobState, callback: C) -> Uuid
    where
        C: Fn(EventParameters<D, R, P>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        self.emitter.on(event, callback)
    }
    pub async fn on_all_events<F, C>(&self, callback: C) -> Uuid
    where
        C: Fn(EventParameters<D, R, P>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        self.emitter.on_all(callback)
    }
    pub fn remove_event_listener(&self, id: Uuid) -> Option<Uuid> {
        self.emitter.remove_listener(id)
    }

    pub async fn obliterate(&self) -> KioResult<()> {
        self.delete_all_jobs().await?;
        let events_stream_key =
            CollectionSuffix::Events.to_collection_name(&self.prefix, &self.name);

        // delete all other grouped collections;
        let mut conn = self.conn_pool.get().await?;
        let mut pipeline = redis::pipe();
        [
            CollectionSuffix::Delayed,
            CollectionSuffix::Wait,
            CollectionSuffix::Active,
            CollectionSuffix::Completed,
            CollectionSuffix::Failed,
            CollectionSuffix::Events,
            CollectionSuffix::Id,
            CollectionSuffix::Events,
            CollectionSuffix::Stalled,
            CollectionSuffix::Marker,
            CollectionSuffix::Prioritized,
            CollectionSuffix::PriorityCounter,
            CollectionSuffix::Meta,
        ]
        .iter()
        .for_each(|name| {
            let key = name.to_collection_name(&self.prefix, &self.name);
            pipeline.del(key);
        });
        let event_mode = self.event_mode.load(Ordering::Acquire);
        let event = JobState::Obliterated;
        let last_id = self.current_metrics.last_id.load(Ordering::Acquire);
        match event_mode {
            QueueEventMode::PubSub => {
                let item: QueueStreamEvent<(), ()> = QueueStreamEvent {
                    job_id: last_id,
                    event,
                    ..Default::default()
                };
                pipeline.publish(&events_stream_key, item);
            }
            QueueEventMode::Stream => {
                pipeline.xadd(
                    &events_stream_key,
                    "*",
                    &[
                        ("event", event.to_string().to_lowercase()),
                        ("job_id", last_id.to_string()),
                    ],
                );
            }
        }
        pipeline.del(&events_stream_key);
        let done: () = pipeline.query_async(&mut conn).await?;
        self.current_metrics.clear();
        Ok(done)
    }
    async fn delete_all_jobs(&self) -> KioResult<()> {
        let mut conn = self.conn_pool.get().await?;
        let id_key = CollectionSuffix::Id.to_collection_name(&self.prefix, &self.name);
        let mut pipeline = redis::pipe();
        pipeline.atomic();

        let last_id = self.current_metrics.last_id.load(Ordering::Acquire);
        (1..=last_id).for_each(|id| {
            let job_key = CollectionSuffix::Job(id).to_collection_name(&self.prefix, &self.name);
            pipeline.del(job_key);
        });

        let _: () = pipeline.query_async(&mut conn).await?;
        Ok(())
    }

    pub async fn promote_delayed_jobs(
        &self,
        date_time: Dt,
        mut interval_ms: i64,
        job_queue: JobQueue,
    ) -> KioResult<()> {
        promote_jobs(self, date_time, interval_ms, job_queue).await
    }
    fn add_base_marker(&self, is_paused: bool, pipeline: &mut Pipeline) {
        let marker_key = CollectionSuffix::Marker.to_collection_name(&self.prefix, &self.name);
        if !is_paused {
            pipeline.zadd(marker_key, 0, "0");
        }
    }

    async fn move_job_from_priorty_to_active(&self) -> KioResult<Option<u64>> {
        let [active_key, prioritized_key, priority_counter_key] = [
            CollectionSuffix::Active,
            CollectionSuffix::Prioritized,
            CollectionSuffix::PriorityCounter,
        ]
        .map(|collection| collection.to_collection_name(&self.prefix, &self.name));
        let mut conn = self.conn_pool.get().await?;
        let mut min_priority_job: Vec<(u64, u64)> = conn.zpopmin(&prioritized_key, 1).await?;
        if let Some((job_id, score)) = min_priority_job.pop() {
            let _: () = conn.lpush(&active_key, job_id).await?;
            return Ok(Some(job_id));
        }

        let _: () = conn.del(&priority_counter_key).await?;

        Ok(None)
    }

    pub async fn clean_up_job(
        &self,
        job_id: u64,
        remove_options: Option<RemoveOnCompletionOrFailure>,
    ) -> KioResult<()> {
        let id = job_id;
        let job_id_key = CollectionSuffix::Job(id).to_collection_name(&self.prefix, &self.name);
        let mut conn = self.conn_pool.get().await?;
        let mut pipeline = redis::pipe();
        pipeline.atomic();
        if let Some(remove_options) = remove_options {
            match remove_options {
                RemoveOnCompletionOrFailure::Bool(remove_immediately) => {
                    if remove_immediately {
                        pipeline.del(&job_id_key);
                    }
                }
                RemoveOnCompletionOrFailure::Int(max_to_keep) => {
                    if max_to_keep.is_positive() && (id as i64) > max_to_keep {
                        pipeline.del(&job_id_key);
                    }
                }
                RemoveOnCompletionOrFailure::Opts(KeepJobs { age, count }) => {
                    if let Some(expire_in_secs) = age {
                        pipeline.expire(&job_id_key, expire_in_secs);
                    }
                    if let Some(max_to_keep) = count {
                        if max_to_keep.is_positive() && (id as i64) > max_to_keep {
                            pipeline.del(&job_id_key);
                        }
                    }
                }
            }
        }
        if !pipeline.is_empty() {
            let done: redis::Value = pipeline.query_async(&mut conn).await?;
        }
        Ok(())
    }
}

#[derive(derive_more::Debug)]
pub enum MoveToActiveResult<D, R, P> {
    Paused,
    RateLimit(u64),
    DelayUntil(u64),
    #[debug("ProcessJob({0}) from state{1}", _0.id.unwrap_or_default(), _0.state)]
    ProcessJob(Box<Job<D, R, P>>),
}
impl<D, R, P> MoveToActiveResult<D, R, P> {
    fn from_job_state_pair((job, state): (Job<D, R, P>, Option<JobState>)) -> Self {
        Self::ProcessJob(job.boxed())
    }
}
// ----- UTILITY FUNCTIONS -------------------

impl<D, R, P> Queue<D, R, P> {
    pub fn register_backoff_strategy(
        &self,
        name: &str,
        strategy: impl Fn(i64) -> Arc<dyn Fn(i64) -> i64 + Send + Sync> + 'static + Send + Sync,
    ) {
        if !self.backoff.has_strategy(name) {
            self.backoff.register(name, strategy);
        }
    }
    pub fn calculate_next_delay_ms(
        &self,
        backoff_job_opts: &BackOffJobOptions,
        attempts: i64,
    ) -> Option<i64> {
        let backoff_opts = BackOff::normalize(Some(backoff_job_opts))?;
        self.backoff.calculate(Some(backoff_opts), attempts, None)
    }
    pub async fn retry_job<'a, T: Into<RetryOptions<'a>>>(
        &self,
        job_id: u64,
        opts: T,
        attempts: u64,
    ) -> KioResult<()> {
        let opts = opts.into();
        match opts {
            RetryOptions::Failed(backoff_job_opts) => {
                self.retry_failed(job_id, backoff_job_opts, attempts).await
            }
            RetryOptions::WithRepeat(repeat) => {
                if let Some(next_delayed_timestamp) =
                    repeat.next_occurrence(&self.backoff, attempts)
                {
                    let [delayed_key, job_id_key, wait_key] = [
                        CollectionSuffix::Delayed,
                        CollectionSuffix::Job(job_id),
                        CollectionSuffix::Wait,
                    ]
                    .map(|collection| collection.to_collection_name(&self.prefix, &self.name));
                    let mut conn = self.get_connection().await?;
                    match next_delayed_timestamp {
                        0 => conn.lpush::<_, u64, ()>(&wait_key, job_id).await?,
                        _ => {
                            conn.zadd::<_, _, _, ()>(delayed_key, job_id, next_delayed_timestamp)
                                .await?
                        }
                    };
                }
                Ok(())
            }
        }
    }
    async fn retry_failed(
        &self,
        job_id: u64,
        backoff_job_opts: &BackOffJobOptions,
        attempts: u64,
    ) -> KioResult<()> {
        let [delayed_key, job_id_key, failed_key] = [
            CollectionSuffix::Delayed,
            CollectionSuffix::Job(job_id),
            CollectionSuffix::Failed,
        ]
        .map(|collection| collection.to_collection_name(&self.prefix, &self.name));
        let mut conn = self.conn_pool.get().await?;
        let ts = Utc::now();
        let mut pipeline = redis::pipe();
        pipeline.atomic();

        if let Some(next_delay) = self.calculate_next_delay_ms(backoff_job_opts, attempts as i64) {
            let expected_active_time = ts + TimeDelta::milliseconds(next_delay);
            pipeline.zadd(delayed_key, job_id, expected_active_time.timestamp_millis());
            pipeline.zrem(failed_key, &[job_id]);
        }
        if !pipeline.is_empty() {
            let _: () = pipeline.query_async(&mut conn).await?;
        }

        Ok(())
    }
    fn update_job_opts(&self, opts: &mut JobOptions) {
        if opts.remove_on_complete.is_none() {
            opts.remove_on_complete = self.opts.remove_on_complete;
        }
        if opts.remove_on_fail.is_none() {
            opts.remove_on_fail = self.opts.remove_on_fail;
        }
        if opts.attempts < self.opts.attempts {
            opts.attempts = self.opts.attempts;
        }
        if opts.backoff.is_none() {
            opts.backoff = self.opts.default_backoff.clone();
        }
        if opts.repeat.is_none() {
            opts.repeat = self.opts.repeat.clone();
        }
    }
    pub async fn fetch_id(&self) -> KioResult<u64> {
        let mut conn = self.conn_pool.get().await?;
        let id_key = CollectionSuffix::Id.to_collection_name(&self.prefix, &self.name);
        let id = conn.incr(&id_key, 1_u64).await?;
        Ok(id)
    }
    pub fn is_paused(&self) -> bool {
        self.current_metrics.paused.load(Ordering::Acquire)
    }
    pub async fn get_connection(&self) -> KioResult<deadpool_redis::Connection> {
        let conn = self.conn_pool.get().await?;
        Ok(conn)
    }
    pub fn pause_active_workers(&self) {
        self.pause_workers.store(true, Ordering::Release);
    }
    pub fn resume_workers(&self) {
        resume_helper(
            &self.current_metrics,
            &self.pause_workers,
            &self.worker_notifier,
        );
    }
    pub async fn get_metrics(&self) -> KioResult<JobMetrics> {
        let mut conn = self.conn_pool.get().await?;
        let updated = get_job_metrics(&self.prefix, &self.name, &mut conn).await?;
        self.current_metrics.update(&updated);
        Ok(updated)
    }
    pub async fn update_processing_count(
        &self,
        increment: bool,
        worker_id: Uuid,
        job_id: u64,
        state: JobState,
    ) -> KioResult<u64> {
        let mut conn = self.get_connection().await?;
        let [meta_key, events_stream_key] = [CollectionSuffix::Meta, CollectionSuffix::Events]
            .map(|col| col.to_collection_name(&self.prefix, &self.name));

        let delta = if increment { 1_i64 } else { -1_i64 };
        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.hincr(&meta_key, "processing", delta);
        match self.event_mode.load(Ordering::Acquire) {
            QueueEventMode::PubSub => {
                // this event, doesn't have the return and progress fields
                let event = QueueStreamEvent::<(), ()> {
                    job_id,
                    event: JobState::Processing,
                    prev: Some(state),
                    worker_id: Some(worker_id),
                    ..Default::default()
                };
                pipe.publish(events_stream_key, event);
            }
            QueueEventMode::Stream => {
                let items = [
                    ("event", JobState::Processing.to_string().to_lowercase()),
                    ("prev", state.to_string().to_lowercase()),
                    ("job_id", job_id.to_string()),
                    ("worker_id", simd_json::to_string(&worker_id)?),
                ];

                pipe.xadd(events_stream_key, "*", &items);
            }
        }
        let (current, done): (u64, ()) = pipe.query_async(&mut conn).await?;
        Ok(current)
    }
}
