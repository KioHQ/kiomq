use std::any::Any;
use std::fmt::format;
use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::error::{JobError, KioError};
use crate::job::{Job, JobState};
use crate::utils::serialize_into_pairs;
use crate::KioResult;
use deadpool_redis::{Config, Pool, Runtime};
use futures::stream::FuturesUnordered;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_redis::RedisDeserialize;
use std::collections::HashMap;

use redis::{self, AsyncCommands, JsonAsyncCommands, Pipeline, RedisResult, ToRedisArgs, Value};

use derive_more::{Debug, Display};
#[derive(Display, Serialize)]
pub enum CollectionSuffix {
    Active,
    Completed,
    Delayed,
    Stalled,
    Id, // hash(number)
    Meta,
    Events,
    Wait,
    Paused,
    Failed,
    #[display("{_0}")]
    Job(u64),
    #[display("")]
    Prefix,
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
    pub fn is_paused(&self) -> bool {
        self.paused.load(std::sync::atomic::Ordering::Relaxed)
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
        let job_key = CollectionSuffix::Job(id).to_collection_name(&self.prefix, &self.name);
        let events_keys = CollectionSuffix::Events.to_collection_name(&self.prefix, &self.name);
        let waiting_or_paused = if !self.is_paused() {
            CollectionSuffix::Wait
        } else {
            CollectionSuffix::Paused
        };
        let waiting_key = waiting_or_paused.to_collection_name(&self.prefix, &self.name);
        let mut pipeline = redis::pipe();
        pipeline.atomic();
        job.id = Some(id);
        pipeline.rpush(&waiting_key, id);
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
    pub async fn get_job(&self, id: u64) -> KioResult<Job<D, R, P>>
    where
        D: DeserializeOwned,
        R: DeserializeOwned,
        P: DeserializeOwned,
    {
        use redis::Value;
        let job_key = CollectionSuffix::Job(id).to_collection_name(&self.prefix, &self.name);
        let mut conn = self.conn_pool.get().await?;
        let value: Job<_, _, _> = conn.hgetall(job_key).await?;
        Ok(value)
    }
    pub async fn move_job_to_state(
        &self,
        job_id: u64,
        from: JobState,
        to: JobState,
        returned_value: Option<String>,
    ) -> KioResult<()> {
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
            return Err(JobError::NotFound.into());
        }
        let mut pipeline = redis::pipe();
        pipeline.atomic();
        pipeline.lrem(prev_state_key, 1, job_id);
        pipeline.rpush(next_state_key, job_id);
        pipeline.hset(job_key, "state", to.to_string().to_lowercase());

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
        let jobs = pipeline.query_async(&mut conn).await?;
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
        pipeline.xadd(events_key, "*", &items);

        let _: redis::Value = pipeline.query_async(&mut conn).await?;
        self.paused
            .store(pause, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}
//let mut fetches: FuturesUnordered<RedisResult<Job<D, R, P>>> = FuturesUnordered::new();
