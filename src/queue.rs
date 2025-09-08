use futures::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::error::{JobError, KioError, QueueError};
use crate::events::QueueStreamEvent;
use crate::job::{Job, JobState};
use crate::utils::{calculate_next_priority_score, promote_jobs, serialize_into_pairs};
use crate::worker::{WorkerOpts, MIN_DELAY_MS_LIMIT};
use crate::{get_job_metrics, Dt, JobOptions, KeepJobs, KioResult, RemoveOnCompletionOrFailure};
use async_backtrace::backtrace;
use chrono::{TimeDelta, Utc};
use deadpool_redis::{Config, Pool, Runtime};
use serde::de::{DeserializeOwned, Error};
use serde::{Deserialize, Serialize};

use redis::{
    self, AsyncCommands, JsonAsyncCommands, LposOptions, Pipeline, RedisResult, ToRedisArgs, Value,
};

use derive_more::{Debug, Display};
#[derive(Display, Serialize)]
pub enum CollectionSuffix {
    Active,    // (list)
    Completed, //Sorted Set
    Delayed,   // ZSET
    Stalled,   // Set
    Prioritized,
    PriorityCounter, // (hash(number))
    Id,              // hash(number)
    Meta,            // key
    Events,          // stream
    Wait,            // LIST
    Paused,          // LIST
    Failed,          // ZSET
    Marker,
    #[display("{_0}")]
    Job(String),
    #[display("")]
    Prefix,
    #[display("{_0}:lock")]
    /// Lock(job_id)
    Lock(String),
    #[display("stalled_check")]
    StalledCheck, // key
}

impl CollectionSuffix {
    pub fn to_collection_name(&self, prefix: &str, name: &str) -> String {
        format!("{}:{}:{}", prefix, name, &self).to_lowercase()
    }
}
impl From<JobState> for CollectionSuffix {
    fn from(val: JobState) -> Self {
        match val {
            JobState::Wait => CollectionSuffix::Wait,
            JobState::Stalled => CollectionSuffix::Paused,
            JobState::Active => CollectionSuffix::Active,
            JobState::Paused => CollectionSuffix::Paused,
            JobState::Completed => CollectionSuffix::Completed,
            JobState::Resumed => CollectionSuffix::Active,
            JobState::Failed => CollectionSuffix::Failed,
            JobState::Delayed => CollectionSuffix::Delayed,
            JobState::Progress => CollectionSuffix::Prefix,
        }
    }
}

impl ToRedisArgs for CollectionSuffix {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + deadpool_redis::redis::RedisWrite,
    {
        out.write_arg_fmt(self.to_string().to_lowercase());
    }
}

#[derive(Debug, Clone, Copy)]
pub struct JobMetrics {
    pub last_id: usize,
    pub active: usize,
    pub stalled: usize,
    pub completed: usize,
}
impl JobMetrics {
    pub fn all_jobs_completed(&self) -> bool {
        self.completed == self.last_id && self.active == 0
    }
}
use crate::{EventEmitter, EventParameters};
#[derive(Debug, Clone)]
pub struct Queue<D, R, P> {
    pub prefix: String,
    pub name: String,
    pub paused: Arc<AtomicBool>,
    pub job_count: Arc<AtomicU64>,
    #[debug(skip)]
    emitter: EventEmitter<D, R, P>,
    pub stream_listener: Arc<JoinHandle<KioResult<()>>>,
    #[debug(skip)]
    pub conn_pool: Arc<Pool>,
}

impl<
        D: Clone + Serialize + DeserializeOwned + Send + 'static,
        R: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
        P: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
    > Queue<D, R, P>
{
    pub async fn get_connection(&self) -> KioResult<deadpool_redis::Connection> {
        let conn = self.conn_pool.get().await?;
        Ok(conn)
    }
    pub async fn new(prefix: Option<&str>, name: &str, cfg: &Config) -> KioResult<Self> {
        use redis::streams::{StreamReadOptions, StreamReadReply};
        use typed_emitter::TypedEmitter;
        let pool = cfg.create_pool(Some(Runtime::Tokio1))?;
        let prefix = prefix.unwrap_or("kio").to_lowercase();
        let meta_key = CollectionSuffix::Meta.to_collection_name(&prefix, name);
        let name = name.to_lowercase();
        let emitter = Arc::new(TypedEmitter::new());
        let mut conn = pool.get().await?;
        let is_paused = conn.hexists(&meta_key, JobState::Paused).await?;
        let mut connection = pool.get().await?;
        let stream_key = CollectionSuffix::Events.to_collection_name(&prefix, &name);
        let queue_name = CollectionSuffix::Prefix.to_collection_name(&prefix, &name);
        let emitter_clone = emitter.clone();
        let task: JoinHandle<KioResult<()>> = tokio::spawn(async move {
            let block_interval = 1000000; // 100 seconds
            loop {
                let options = StreamReadOptions::default().block(block_interval);
                let reply: StreamReadReply = connection
                    .xread_options(&[&stream_key], &["$"], &options)
                    .await?;
                let events: Vec<QueueStreamEvent<R, P>> =
                    QueueStreamEvent::from_stream_read_reply(&stream_key, reply);
                if !events.is_empty() {
                    for event in events.into_iter() {
                        let state = event.event;
                        let param = EventParameters::<D, R, P>::from_queue_event(
                            &queue_name,
                            event,
                            &mut connection,
                        )
                        .await?;
                        emitter_clone.emit(state, param).await;
                    }
                }
                tokio::task::yield_now().await;
            }

            Ok(())
        });
        let stream_listener = Arc::new(task);
        //
        Ok(Self {
            stream_listener,
            job_count: Arc::default(),
            emitter,
            prefix,
            name,
            paused: Arc::new(AtomicBool::new(is_paused)),
            conn_pool: Arc::new(pool),
        })
    }
    pub async fn is_paused(&self) -> KioResult<bool> {
        let mut conn = self.conn_pool.get().await?;

        let meta_key = CollectionSuffix::Meta.to_collection_name(&self.prefix, &self.name);
        let is_paused = conn.hexists(meta_key, JobState::Paused).await?;
        let done = self
            .paused
            .fetch_and(is_paused, std::sync::atomic::Ordering::AcqRel);
        Ok(done)
    }
    pub async fn add_job(
        &self,
        name: &str,
        data: D,
        opts: Option<JobOptions>,
    ) -> Result<Job<D, R, P>, KioError> {
        let opts = opts.unwrap_or_default();
        let JobOptions {
            priority,
            delay,
            id,
            remove_on_fail,
            remove_on_complete,
        } = opts;
        let queue_name = format!("{}:{}", self.prefix, self.name);
        let mut job = Job::<D, R, P>::new(name, Some(data), id, Some(&queue_name));
        job.add_opts(opts);
        let mut conn = self.conn_pool.get().await?;

        let id = self.fetch_id().await?;
        if delay > 0 && delay < MIN_DELAY_MS_LIMIT {
            return Err(QueueError::DelayBelowAllowedLimit {
                limit_ms: MIN_DELAY_MS_LIMIT,
                current_ms: delay,
            }
            .into());
        };
        //self.job_count.
        let prefix = &self.prefix;
        let job_key =
            CollectionSuffix::Job(id.to_string()).to_collection_name(&self.prefix, &self.name);
        let events_keys = CollectionSuffix::Events.to_collection_name(&self.prefix, &self.name);

        let waiting_or_paused = if !self.is_paused().await.unwrap_or_default() {
            CollectionSuffix::Wait
        } else {
            CollectionSuffix::Paused
        };
        let mut pipeline = redis::pipe();
        let to_delay = delay > 0;
        let waiting_key = waiting_or_paused.to_collection_name(&self.prefix, &self.name);
        pipeline.atomic();
        if !to_delay {
            pipeline.lpush(&waiting_key, id.to_string());
        } else {
            let delayed_key =
                CollectionSuffix::Delayed.to_collection_name(&self.prefix, &self.name);
            let expected_active_time = job.ts + TimeDelta::milliseconds(delay as i64);
            pipeline.zadd(delayed_key, id, expected_active_time.timestamp_millis());
            job.state = JobState::Delayed;
        }
        job.id = Some(id.to_string());
        let fields = serialize_into_pairs(&job);
        pipeline.hset_multiple(&job_key, &fields);
        let event = if to_delay {
            JobState::Delayed
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
        pipeline.xadd(events_keys, "*", &items);
        pipeline.query_async::<()>(&mut conn).await?;

        Ok(job)
    }
    async fn fetch_id(&self) -> KioResult<u64> {
        let mut conn = self.conn_pool.get().await?;
        let id_key = CollectionSuffix::Id.to_collection_name(&self.prefix, &self.name);
        let id = conn.incr(&id_key, 1_u64).await?;
        self.job_count.swap(id, std::sync::atomic::Ordering::AcqRel);
        Ok(id)
    }
    pub fn current_jobs(&self) -> u64 {
        self.job_count.load(std::sync::atomic::Ordering::Acquire)
    }
    pub async fn get_job(&self, id: &str) -> KioResult<Job<D, R, P>> {
        use redis::Value;
        let job_key =
            CollectionSuffix::Job(id.to_lowercase()).to_collection_name(&self.prefix, &self.name);
        let mut conn = self.conn_pool.get().await?;
        let value: Job<_, _, _> = conn.hgetall(job_key).await?;
        Ok(value)
    }
    pub async fn move_job_to_state(
        &self,
        job_id: &str,
        from: JobState,
        to: JobState,
        value: Option<&str>,
        ts: Option<i64>,
        backtrace: Option<String>,
    ) -> KioResult<()> {
        let move_to_failed_or_completed = matches!(to, JobState::Failed | JobState::Completed);
        // do nothing if the  queue_is_paused.
        if self.is_paused().await.unwrap_or_default() {
            return Ok(());
        }
        use redis::Value;
        let previous_suffix = from.into();
        let next_state_suffix = to.into();
        let [job_key, events_key, prev_state_key, next_state_key] = [
            CollectionSuffix::Job(job_id.to_lowercase()),
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
                ts.unwrap_or_else(|| Utc::now().timestamp_micros()),
                job_id,
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
        let dst = serde_json::to_string(&to)?;
        pipeline.hset(&job_key, "state", dst);
        if let Some(backtrace) = backtrace {
            pipeline.hset(&job_key, "stackTrace", backtrace);
        }

        let mut items = vec![
            ("event", to.to_string().to_lowercase()),
            ("prev", from.to_string().to_lowercase()),
            ("job_id", job_id.to_string()),
        ];
        if let Some(data) = value {
            let key = if matches!(to, JobState::Failed) {
                "failedReason"
            } else {
                "returnedValue"
            };
            pipeline.hset(&job_key, key, data);
            items.push((key, data.to_owned()));
            pipeline.hset(&job_key, "finishedOn", ts);
        }
        // check of retries_exhausion here;

        pipeline.xadd(events_key, "*", &items);
        let _: redis::Value = pipeline.query_async(&mut conn).await?;
        Ok(())
    }
    pub async fn fetch_waiting_jobs(&self) -> KioResult<Vec<Job<D, R, P>>> {
        if self.is_paused().await? {
            return Ok(vec![]);
        }
        let waiting_key = CollectionSuffix::Wait.to_collection_name(&self.prefix, &self.name);
        let mut conn = self.conn_pool.get().await?;
        let waiting: Vec<String> = conn.lrange(waiting_key, 0, -1).await?;
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
        let pause = !self.is_paused().await?;
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

        pipeline.xadd(events_key, "*", &items);
        let _: redis::Value = pipeline.query_async(&mut conn).await?;
        self.paused
            .store(pause, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
    pub async fn wait_for_job(&self, block_duration: i64) -> KioResult<u64> {
        use chrono::TimeDelta;
        if self.is_paused().await.unwrap_or_default() {
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
        job_id: &str,
        lock_duration: u64,
        token: &str,
    ) -> KioResult<(bool)> {
        let [lock_key, stalled_key] = [
            CollectionSuffix::Lock(job_id.to_lowercase()),
            CollectionSuffix::Stalled,
        ]
        .map(|key| key.to_collection_name(&self.prefix, &self.name));
        let mut conn = self.conn_pool.get().await?;
        let mut pipeline = redis::pipe();
        pipeline.atomic();
        let previous: Option<String> = conn.get(&lock_key).await?;
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
    ) -> KioResult<(Vec<String>, Vec<String>)> {
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
        let stalling: Vec<String> = conn.smembers(&stalled_key).await?;
        if !stalling.is_empty() {
            for job_id in stalling {
                if job_id.starts_with("0:") {
                    let _: () = conn.lrem(&active_key, 1, &job_id).await?;
                } else {
                    let job_key = CollectionSuffix::Job(job_id.clone())
                        .to_collection_name(&self.prefix, &self.name);
                    let job_lock_key = CollectionSuffix::Lock(job_id.to_lowercase())
                        .to_collection_name(&self.prefix, &self.name);
                    if !conn
                        .exists::<_, bool>(&job_lock_key)
                        .await
                        .unwrap_or_default()
                    {
                        let removed: isize = conn.lrem(&active_key, 1, &job_id).await?;
                        if removed > 0 {
                            // If this job has been stalled too many times, such as if it crashes the worker, then fail it.
                            let stalled_count: u64 =
                                conn.hincr(&job_key, "stalledCounter", 1).await?;
                            if stalled_count > opts.max_stalled_count {
                                // Add job removal option logic here
                                let _: () = conn.zadd(&failed_key, ts, &job_id).await?;
                                let failed_reason = "job stalled more than allowable limit";
                                let state = JobState::Failed.to_string();

                                let items = [
                                    ("failedReason", failed_reason.to_lowercase()),
                                    ("finishedOn", ts.to_string()),
                                    ("state", state),
                                ];
                                let _: () = conn.hset_multiple(&job_key, &items).await?;
                                let items = [
                                    ("event", "failed"),
                                    ("job_id", &job_id),
                                    ("failedReason", failed_reason),
                                    ("prev", "active"),
                                ];
                                let _: () = conn.xadd(&events_key, "*", &items).await?;
                                failed.push(job_id);
                            } else {
                                let (is_paused, target) = self.get_target_list().await;
                                if !is_paused {
                                    let _: () = conn.zadd(&marker_key, 0, "0").await?;
                                }
                                self.move_job_to_state(
                                    &job_id,
                                    JobState::Active,
                                    target,
                                    None,
                                    None,
                                    None,
                                )
                                .await?;
                                // emit  stalled;
                                let items = [("event", "stalled"), ("job_id", &job_id)];
                                let _: () = conn.xadd(&events_key, "*", &items).await?;
                                stalled.push(job_id);
                            }
                        }
                    }
                }
            }
        } else {
            // mark stalled Jobs
            let active: Vec<String> = conn.lrange(&active_key, 0, -1).await?;
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

    pub async fn get_target_list(&self) -> (bool, JobState) {
        let paused = self.is_paused().await.unwrap_or_default();
        if paused {
            return (paused, JobState::Paused);
        }
        (paused, JobState::Wait)
    }

    pub async fn move_to_active(
        &self,
        token: &str,
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
        let (is_paused, target_state) = self.get_target_list().await;
        let target_key =
            CollectionSuffix::from(target_state).to_collection_name(&self.prefix, &self.name);
        let mut job_id: Option<String> = conn.rpoplpush(&wait_key, &active_key).await?;
        if let Some(id) = job_id.as_ref() {
            if id.starts_with("0:") {
                let _: () = conn.lrem(&active_key, 1, id).await?;
                job_id = conn.rpoplpush(&wait_key, &active_key).await?;
            }
        }
        let mut prepare_job = |id: String, mut conn: redis::aio::MultiplexedConnection| async move {
            let job_id_key =
                CollectionSuffix::Job(id.clone()).to_collection_name(&self.prefix, &self.name);
            let prev_state: Option<JobState> = conn.hget(job_id_key, "state").await.ok();
            let job = self
                .prepare_job_for_processing(token, &id, ts as u64, opts)
                .await?;
            Ok::<_, KioError>(Some((job, prev_state)))
        };
        let current_job = match job_id {
            Some(job_id) => prepare_job(job_id.to_owned(), conn.clone()).await?,
            None => {
                if let Some(id) = self.move_job_from_priorty_to_active().await? {
                    prepare_job(id, conn.clone()).await.ok().flatten();
                }
                None
            }
        };
        if let Some((job, prev_state)) = current_job {
            let job_clone = job.clone();
            self.emit(
                JobState::Active,
                EventParameters::Active { job, prev_state },
            )
            .await;

            return Ok(MoveToActiveResult::ProcessJob(job_clone.boxed()));
        }
        // fetch the next delayed_timestamp;
        let mut next_delayed_timestamp: Vec<(u64, u64)> =
            conn.zrange_withscores(&delayed_key, 0, 0).await?;
        let mut next_delay = next_delayed_timestamp.pop().unwrap_or_default().1;
        next_delay /= 0x1000;

        Ok(MoveToActiveResult::DelayUntil(next_delay))
    }
    pub async fn prepare_job_for_processing(
        &self,
        token: &str,
        job_id: &str,
        ts: u64,
        opts: &WorkerOpts,
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
                CollectionSuffix::Job(job_id.to_lowercase()),
                CollectionSuffix::Lock(job_id.to_lowercase()),
            ]
            .map(|s| s.to_collection_name(&self.prefix, &self.name));
        let mut conn = self.conn_pool.get().await?;

        let _: () = conn
            .pset_ex(&job_lock_key, token, opts.lock_duration)
            .await?;
        self.move_job_to_state(job_id, JobState::Wait, JobState::Active, None, None, None)
            .await?;
        let items = [
            ("processedOn", serde_json::to_string(&ts)?),
            ("token", serde_json::to_string(token)?),
        ];
        let _: () = conn.hset_multiple(&job_key, &items).await?;
        let job = conn.hgetall(&job_key).await?;
        Ok(job)
    }

    pub(crate) async fn move_job_to_finished_or_failed(
        &self,
        job_id: &str,
        ts: i64,
        token: &str,
        move_to_state: JobState,
        returned_value_or_failed_reason: &str,
        backtrace: Option<String>,
    ) -> KioResult<Job<D, R, P>> {
        let [job_key, job_lock_key, active_key, completed_key, events_stream_key, stalled_key] = [
            CollectionSuffix::Job(job_id.to_owned()),
            CollectionSuffix::Lock(job_id.to_owned()),
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
        let lock_token: Option<String> = conn.get(&job_lock_key).await?;
        if let Some(local) = lock_token {
            if local != token {
                return Err(JobError::JobLockMismatch.into());
            }
            pipeline.del(&job_lock_key);
            pipeline.srem(&stalled_key, job_id);
        } else {
            return Err(JobError::JobLockNotExist.into());
        }
        // Todo: remove any dependencies too here ;
        self.move_job_to_state(
            job_id,
            JobState::Active,
            move_to_state,
            Some(returned_value_or_failed_reason),
            Some(ts),
            backtrace,
        )
        .await;

        //remove element from stalled set too;
        let _: () = pipeline.query_async(&mut conn).await?;

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
            CollectionSuffix::Meta,
            CollectionSuffix::Id,
            CollectionSuffix::Events,
            CollectionSuffix::Stalled,
            CollectionSuffix::Marker,
        ]
        .iter()
        .for_each(|name| {
            let key = name.to_collection_name(&self.prefix, &self.name);
            pipeline.del(key);
        });

        let done: () = pipeline.query_async(&mut conn).await?;
        Ok(done)
    }
    async fn delete_all_jobs(&self) -> KioResult<()> {
        let mut conn = self.conn_pool.get().await?;
        let id_key = CollectionSuffix::Id.to_collection_name(&self.prefix, &self.name);
        let mut pipeline = redis::pipe();
        pipeline.atomic();

        let last_id = self.current_jobs();
        (1..=last_id).for_each(|id| {
            let job_key =
                CollectionSuffix::Job(id.to_string()).to_collection_name(&self.prefix, &self.name);
            pipeline.del(job_key);
        });

        let _: () = pipeline.query_async(&mut conn).await?;
        Ok(())
    }

    async fn get_metrics(&self) -> KioResult<JobMetrics> {
        let mut conn = self.conn_pool.get().await?;
        get_job_metrics(&self.prefix, &self.name, &mut conn).await
    }
    pub async fn promote_delayed_jobs(
        &self,
        date_time: Dt,
        mut interval_ms: i64,
    ) -> KioResult<Vec<String>> {
        let (paused, target_state) = self.get_target_list().await;
        let conn = self.conn_pool.get().await?;
        promote_jobs(
            &self.prefix,
            &self.name,
            date_time,
            paused,
            target_state,
            interval_ms,
            conn,
        )
        .await
    }
    fn add_base_marker(&self, is_paused: bool, pipeline: &mut Pipeline) {
        let marker_key = CollectionSuffix::Marker.to_collection_name(&self.prefix, &self.name);
        if !is_paused {
            pipeline.zadd(marker_key, 0, "0");
        }
    }

    async fn move_job_from_priorty_to_active(&self) -> KioResult<Option<String>> {
        let [active_key, prioritized_key, priority_counter_key] = [
            CollectionSuffix::Active,
            CollectionSuffix::Prioritized,
            CollectionSuffix::PriorityCounter,
        ]
        .map(|collection| collection.to_collection_name(&self.prefix, &self.name));
        let mut conn = self.conn_pool.get().await?;
        let mut min_priority_job: Vec<(u64, String)> = conn.zpopmin(&prioritized_key, 1).await?;
        if let Some((score, job_id)) = min_priority_job.pop() {
            let _: () = conn.lpush(&active_key, &job_id).await?;
            return Ok(Some(job_id));
        }

        let _: () = conn.del(&priority_counter_key).await?;

        Ok(None)
    }

    pub async fn clean_up_job(
        &self,
        job_id: &str,
        remove_options: RemoveOnCompletionOrFailure,
    ) -> KioResult<()> {
        let id = job_id;
        let id_num: i64 = id.parse()?;
        let job_id_key =
            CollectionSuffix::Job(id.to_owned()).to_collection_name(&self.prefix, &self.name);
        let mut conn = self.conn_pool.get().await?;
        let mut pipeline = redis::pipe();
        match remove_options {
            RemoveOnCompletionOrFailure::Bool(remove_immediately) => {
                if remove_immediately {
                    pipeline.del(&job_id_key);
                }
            }
            RemoveOnCompletionOrFailure::Int(max_to_keep) => {
                if max_to_keep.is_positive() && id_num > max_to_keep {
                    pipeline.del(&job_id_key);
                }
            }
            RemoveOnCompletionOrFailure::Opts(KeepJobs { age, count }) => {
                if let Some(expire_in_secs) = age {
                    pipeline.expire(&job_id_key, expire_in_secs);
                }
                if let Some(max_to_keep) = count {
                    if max_to_keep.is_positive() && id_num > max_to_keep {
                        pipeline.del(&job_id_key);
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
    #[debug("ProcessJob({0})", _0.id.clone().unwrap_or_default())]
    ProcessJob(Box<Job<D, R, P>>),
}
// ----- UTILITY FUNCTIONS -------------------
