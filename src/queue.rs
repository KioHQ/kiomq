use std::any::Any;
use std::fmt::format;
use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::error::{JobError, KioError};
use crate::job::{Job, JobState};
use crate::utils::{serialize_into_pairs, Batches};
use crate::worker::WorkerOpts;
use crate::{Dt, KioResult};
use chrono::Utc;
use deadpool_redis::{Config, Pool, Runtime};
use futures::stream::FuturesUnordered;
use serde::de::{DeserializeOwned, Error};
use serde::{Deserialize, Serialize};
use serde_redis::RedisDeserialize;
use std::collections::HashMap;

use redis::{
    self, AsyncCommands, JsonAsyncCommands, LposOptions, Pipeline, RedisResult, ToRedisArgs, Value,
};

use derive_more::{Debug, Display};
#[derive(Display, Serialize)]
pub enum CollectionSuffix {
    Active,    // (list)
    Completed, //
    Delayed,
    Stalled, // Set
    Id,      // hash(number)
    Meta,    // key
    Events,  // stream
    Wait,    // LIST
    Paused,  // LIST
    Failed,  // ZSET
    #[display("{_0}")]
    Job(String),
    #[display("")]
    Prefix,
    #[display("{_0}:lock")]
    /// Lock(job_id)
    Lock(String),
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

#[derive(Debug, Clone)]
pub struct Queue<D, R, P> {
    pub(crate) prefix: String,
    pub name: String,
    pub paused: Arc<AtomicBool>,
    #[debug(skip)]
    pub conn_pool: Arc<Pool>,
    _d: PhantomData<(D, R, P)>,
}

impl<D, R, P> Queue<D, R, P> {
    pub async fn new(prefix: Option<&str>, name: &str, cfg: &Config) -> KioResult<Self> {
        let pool = cfg.create_pool(Some(Runtime::Tokio1))?;
        let prefix = prefix.unwrap_or("kio").to_lowercase();
        let meta_key = CollectionSuffix::Meta.to_collection_name(&prefix, name);
        let name = name.to_lowercase();
        // if queue exists in redis, restore its state;
        let mut conn = pool.get().await?;
        let is_paused = conn.hexists(&meta_key, JobState::Paused).await?;
        //
        Ok(Self {
            prefix,
            name,
            paused: Arc::new(AtomicBool::new(is_paused)),
            conn_pool: Arc::new(pool),
            _d: PhantomData,
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
        job_id: Option<u64>,
    ) -> Result<Job<D, R, P>, KioError>
    where
        D: Serialize,
        R: Serialize,
        P: Serialize,
    {
        let queue_name = format!("{}:{}", self.prefix, self.name);
        let mut job = Job::<D, R, P>::new(name, Some(data), job_id, Some(&queue_name));
        let mut conn = self.conn_pool.get().await?;
        let id = self.fetch_id().await?;
        let prefix = &self.prefix;
        let job_key =
            CollectionSuffix::Job(id.to_string()).to_collection_name(&self.prefix, &self.name);
        let events_keys = CollectionSuffix::Events.to_collection_name(&self.prefix, &self.name);

        let waiting_or_paused = if !self.is_paused().await.unwrap_or_default() {
            CollectionSuffix::Wait
        } else {
            CollectionSuffix::Paused
        };
        let waiting_key = waiting_or_paused.to_collection_name(&self.prefix, &self.name);
        let mut pipeline = redis::pipe();
        pipeline.atomic();
        job.id = Some(id.to_string());
        pipeline.lpush(&waiting_key, id.to_string());
        let fields = serialize_into_pairs(&job);
        pipeline.hset_multiple(&job_key, &fields);
        let items = [
            ("event", CollectionSuffix::Wait.to_string().to_lowercase()),
            ("job_id", id.to_string()),
            ("name", name.to_string()),
        ];
        pipeline.xadd(events_keys, "*", &items);
        pipeline.query_async::<()>(&mut conn).await?;

        Ok(job)
    }
    async fn fetch_id(&self) -> KioResult<u64> {
        let mut conn = self.conn_pool.get().await?;
        let id_key = CollectionSuffix::Id.to_collection_name(&self.prefix, &self.name);
        let id = conn.incr(&id_key, 1_u64).await?;
        Ok(id)
    }
    pub async fn get_job(&self, id: &str) -> KioResult<Job<D, R, P>>
    where
        D: DeserializeOwned,
        R: DeserializeOwned,
        P: DeserializeOwned,
    {
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
        returned_value: Option<String>,
    ) -> KioResult<()> {
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
            return Err(JobError::NotFound.into());
        }
        let mut pipeline = redis::pipe();
        pipeline.atomic();
        // only move the value if it doesn't exist in the target list
        let job_id_exists_in_target: Option<usize> = conn
            .lpos(&next_state_key, job_id, LposOptions::default())
            .await?;
        if job_id_exists_in_target.is_none() {
            pipeline.lrem(prev_state_key, 1, job_id);
            pipeline.rpush(next_state_key, job_id);
        }
        let dst = serde_json::to_string(&to)?;
        pipeline.hset(job_key, "state", dst);

        let mut items = vec![
            ("event", to.to_string().to_lowercase()),
            ("prev", from.to_string().to_lowercase()),
            ("job_id", job_id.to_string()),
        ];
        if let Some(data) = returned_value {
            items.push(("returned_value", data));
        }
        pipeline.xadd(events_key, "*", &items);
        let _: redis::Value = pipeline.query_async(&mut conn).await?;
        Ok(())
    }
    pub async fn fetch_waiting_jobs(&self) -> KioResult<Vec<Job<D, R, P>>>
    where
        D: DeserializeOwned,
        R: DeserializeOwned,
        P: DeserializeOwned,
    {
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
    pub async fn wait_for_job(&self, block_duration: i64) -> KioResult<String> {
        use chrono::TimeDelta;
        if self.is_paused().await.unwrap_or_default() {
            return Err(KioError::QueueError(
                crate::error::QueueError::CantOperateWhenPaused,
            ));
        }
        let [wait_key, active_key] = [CollectionSuffix::Wait, CollectionSuffix::Active]
            .map(|key| key.to_collection_name(&self.prefix, &self.name));

        let block_until = TimeDelta::milliseconds(block_duration).as_seconds_f64();
        let mut con = self.conn_pool.get().await?;
        let job_id: String = con.brpoplpush(wait_key, active_key, block_until).await?;
        self.move_job_to_state(&job_id, JobState::Wait, JobState::Active, None)
            .await?;
        Ok(job_id)
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
        let ts = Utc::now().timestamp_millis();
        let [wait_key, active_key, events_key, meta_key, paused_key, stalled_key, stalled_check, failed_key] =
            [
                CollectionSuffix::Wait,
                CollectionSuffix::Active,
                CollectionSuffix::Events,
                CollectionSuffix::Meta,
                CollectionSuffix::Paused,
                CollectionSuffix::Stalled,
                CollectionSuffix::StalledCheck,
                CollectionSuffix::Failed,
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
                                let items = [
                                    ("failedReason", failed_reason.to_lowercase()),
                                    ("finishedOn", ts.to_string()),
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
                                let is_paused = self.is_paused().await?;
                                let target = if is_paused {
                                    JobState::Paused
                                } else {
                                    JobState::Wait
                                };
                                self.move_job_to_state(&job_id, JobState::Active, target, None)
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
                for (from, to) in Batches::new(active.len(), 7000) {
                    dbg!(from, to);
                    let batch = &active[from..to];
                    pipeline.sadd(&stalled_key, batch);
                }
                let _: () = pipeline.query_async(&mut conn).await?;
            }
        }

        Ok((failed, stalled))
    }
}
