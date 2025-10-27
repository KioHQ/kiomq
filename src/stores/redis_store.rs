use super::*;
use crate::utils::{
    prepare_for_insert, process_each_event, process_queue_events, query_all_batched, resume_helper,
    update_job_opts, ReadStreamArgs,
};
use crate::{FailedDetails, JobError};
use chrono::Utc;
use deadpool_redis::{Config, Pool, Runtime};
use derive_more::Debug;
use futures::future::FutureExt;
use futures::stream::Collect;
use futures::StreamExt;
use redis::aio::{PubSubSink, PubSubStream};
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{streams::StreamPendingReply, AsyncCommands, Commands, LposOptions, Pipeline};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::num::ParseIntError;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{AcquireError, Notify};
use tokio::task::JoinHandle;
use uuid::Uuid;
/// a counter for adding b/ulk jobs,
static START: AtomicU64 = AtomicU64::new(0);
static PC_COUNTER: AtomicU64 = AtomicU64::new(0);
use tokio::sync::Mutex;
#[derive(Clone, Debug)]
pub struct RedisStore {
    prefix: String,
    name: String,
    consumer_group: String,
    consumer_name: String,
    stream_key: String,
    #[debug(skip)]
    pubsub_source: Arc<Mutex<PubSubStream>>,
    #[debug(skip)]
    pubsub_sink: Arc<Mutex<PubSubSink>>,
    subscribed: Arc<AtomicBool>,
    #[debug(skip)]
    pub(crate) conn_pool: Arc<Pool>,
    redis_client: redis::Client,
}

impl RedisStore {
    pub async fn new(prefix: Option<&str>, name: &str, cfg: &Config) -> KioResult<Self> {
        let pool = cfg.create_pool(Some(Runtime::Tokio1))?;
        let name = name.to_lowercase();
        let prefix = prefix.unwrap_or("{kio}").to_lowercase();
        let conn_pool = Arc::new(pool);
        let connection_info = cfg.connection.clone().unwrap_or_default();
        let redis_client = redis::Client::open(connection_info)?;
        let id = Uuid::new_v4();
        let consumer_group = format!("{prefix}-{prefix}-group-{id}",);
        let consumer_name = format!("consumer-{id}");
        let mut connection = conn_pool.get().await?;
        let queue_name = CollectionSuffix::Prefix.to_collection_name(&prefix, &name);
        let stream_key = CollectionSuffix::Events.to_collection_name(&prefix, &name);
        let (mut sink, source) = redis_client.get_async_pubsub().await?.split();
        let pubsub_sink = Arc::new(Mutex::new(sink));
        let pubsub_source = Arc::new(Mutex::new(source));
        let c_group: () = connection
            .xgroup_create_mkstream(&stream_key, &consumer_group, "$")
            .await?;
        let subscribed = Arc::default();

        Ok(Self {
            stream_key,
            subscribed,
            consumer_name,
            consumer_group,
            pubsub_sink,
            pubsub_source,
            conn_pool,
            name,
            prefix,
            redis_client,
        })
    }
    pub fn get_blocking_connection(&self) -> KioResult<redis::Connection> {
        let conn = self.redis_client.get_connection()?;
        Ok(conn)
    }
    pub async fn get_connection(&self) -> KioResult<deadpool_redis::Connection> {
        let conn = self.conn_pool.get().await?;
        Ok(conn)
    }
}
#[async_trait::async_trait]
impl<
        D: Clone + Serialize + DeserializeOwned + Send + 'static,
        R: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
        P: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
    > Store<D, R, P> for RedisStore
where
    Self: Send,
{
    async fn metadata_field_exists(&self, field: &str) -> KioResult<bool> {
        let mut conn = self.get_connection().await?;
        let meta_key = CollectionSuffix::Meta.to_collection_name(&self.prefix, &self.name);
        let result = conn.hexists(meta_key, field).await?;
        Ok(result)
    }
    async fn set_event_mode(&self, event_mode: QueueEventMode) -> KioResult<()> {
        let mut conn = self.get_connection().await?;
        let meta_key = CollectionSuffix::Meta.to_collection_name(&self.prefix, &self.name);
        let result = conn.hset(&meta_key, "event_mode", event_mode).await?;
        Ok(result)
    }
    fn queue_name(&self) -> &str {
        self.name.as_ref()
    }
    fn queue_prefix(&self) -> &str {
        self.prefix.as_ref()
    }
    fn update_job_progress(&self, job: &mut Job<D, R, P>, value: P) -> KioResult<()> {
        use crate::QueueEventMode;
        use redis::Commands;
        let mut conn = self.get_blocking_connection()?;
        if let Some(id) = job.id {
            let job_key = CollectionSuffix::Job(id).to_collection_name(&self.prefix, &self.name);
            let mut pipeline = redis::pipe();
            pipeline.atomic();
            let progress_str = simd_json::to_string_pretty(&value)?;
            let events_stream_key =
                CollectionSuffix::Events.to_collection_name(&self.prefix, &self.name);
            pipeline.hset(job_key, "progress", &progress_str);
            let meta_key = CollectionSuffix::Meta.to_collection_name(&self.prefix, &self.name);
            let event_mode: Option<QueueEventMode> = conn.hget(&meta_key, "event_mode")?;
            // check for the queue_event_mode
            let event_mode = event_mode.unwrap_or_default();

            match event_mode {
                QueueEventMode::PubSub => {
                    let event = QueueStreamEvent::<R, P> {
                        job_id: id,
                        event: JobState::Progress,
                        name: Some(self.name.to_owned()),
                        progress_data: Some(value.clone()),
                        ..Default::default()
                    };
                    pipeline.publish(&events_stream_key, event);
                }
                QueueEventMode::Stream => {
                    let items = [
                        ("event", JobState::Progress.to_string().to_lowercase()),
                        ("job_id", id.to_string()),
                        ("data", progress_str),
                        ("name", self.name.to_string()),
                    ];
                    pipeline.xadd(&events_stream_key, "*", &items);
                }
            }
            let _: () = pipeline.query(&mut conn)?;
            job.progress = Some(value);
        }
        Ok(())
    }
    async fn get_delayed_at(&self, start: i64, stop: i64) -> KioResult<(Vec<u64>, Vec<u64>)> {
        let [delayed_key] = [CollectionSuffix::Delayed]
            .map(|collection| collection.to_collection_name(&self.prefix, &self.name));
        let mut pipeline = redis::pipe();
        let mut conn = self.get_connection().await?;
        pipeline.atomic();
        pipeline.zrangebyscore(&delayed_key, start, stop);
        pipeline.zrangebyscore(&delayed_key, "-inf", format!("({start}"));
        pipeline.zrembyscore(&delayed_key, start, stop);
        let (jobs, missed_deadline, done): (Vec<u64>, Vec<u64>, i64) =
            pipeline.query_async(&mut conn).await?;
        Ok((jobs, missed_deadline))
    }
    async fn pop_back_push_front(
        &self,
        src: CollectionSuffix,
        dst: CollectionSuffix,
    ) -> Option<u64> {
        let [src_key, dst_key] =
            [src, dst].map(|col| col.to_collection_name(&self.prefix, &self.name));
        let mut conn = self.get_connection().await.ok()?;
        conn.rpoplpush(src_key, dst_key).await.ok()
    }
    async fn listener_to_events(
        &self,
        event_mode: QueueEventMode,
        block_interval: Option<u64>,
        emitter: &EventEmitter<D, R, P>,
        metrics: &JobMetrics,
    ) -> KioResult<()> {
        let store = Arc::new(self.clone());
        if !self.subscribed.load(Ordering::Acquire) {
            self.pubsub_sink
                .lock()
                .await
                .subscribe(&self.stream_key)
                .await?;
            self.subscribed.store(true, Ordering::Release);
        }
        match event_mode {
            QueueEventMode::PubSub => {
                if let Some(msg) = self.pubsub_source.lock().await.next().await {
                    let event: QueueStreamEvent<R, P> = msg.get_payload()?;
                    process_each_event(event, emitter, self, metrics).await?;
                }
            }
            QueueEventMode::Stream => {
                let mut connection = self.get_connection().await?;
                let mut options = StreamReadOptions::default()
                    .group(&self.consumer_group, &self.consumer_name)
                    .noack();
                if let Some(b_internal) = block_interval {
                    options = options.block(b_internal as usize);
                }
                let reply: StreamReadReply = connection
                    .xread_options(&[&self.stream_key], &[">"], &options)
                    .await?;

                let events =
                    QueueStreamEvent::<R, P>::from_stream_read_reply(&self.stream_key, reply);
                for event in events {
                    process_each_event(event, emitter, self, metrics).await?;
                }
            }
        };

        Ok(())
    }
    async fn create_stream_listener(
        &self,
        emitter: EventEmitter<D, R, P>,
        notifier: Arc<Notify>,
        metrics: Arc<JobMetrics>,
        pause_workers: Arc<AtomicBool>,
        event_mode: QueueEventMode,
    ) -> KioResult<JoinHandle<KioResult<()>>> {
        let store = self.clone();

        let task: JoinHandle<KioResult<()>> = tokio::spawn(
            async move {
                let block_interval = 5000; // 100 seconds
                let notifier = notifier.clone();
                let is_inital = AtomicBool::new(true);

                loop {
                    let args: ReadStreamArgs<D, R, P> =
                        (event_mode, block_interval, &emitter, metrics.clone());
                    process_queue_events(args, &store).await?;

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
        Ok(task)
    }
    async fn add_bulk_only(
        &self,
        iter: Box<dyn Iterator<Item = (String, Option<JobOptions>, D)> + Send>,
        queue_opts: QueueOpts,
        event_mode: QueueEventMode,
        is_paused: bool,
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
            update_job_opts(&queue_opts, &mut opts);
            let queue_name = format!("{}:{}", self.prefix, self.name);
            let id = START.fetch_add(1, Ordering::AcqRel);
            let prior_counter = if opts.priority > 0 {
                is_prioritized = true;
                PC_COUNTER.fetch_add(1, Ordering::AcqRel)
            } else {
                PC_COUNTER.load(Ordering::Acquire)
            };
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
        }
        if is_prioritized {
            pipeline.incr(&priority_counter_key, PC_COUNTER.load(Ordering::Acquire));
        }

        query_all_batched(conn, pipeline).await?;
        Ok(())
    }
    async fn add_bulk(
        &self,
        iter: Box<dyn Iterator<Item = (String, Option<JobOptions>, D)> + Send>,
        queue_opts: QueueOpts,
        event_mode: QueueEventMode,
        is_paused: bool,
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
            update_job_opts(&queue_opts, &mut opts);
            let queue_name = format!("{}:{}", self.prefix, self.name);
            let id = START.fetch_add(1, Ordering::AcqRel);
            let prior_counter = if opts.priority > 0 {
                is_prioritized = true;
                PC_COUNTER.fetch_add(1, Ordering::AcqRel)
            } else {
                PC_COUNTER.load(Ordering::Acquire)
            };
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

    async fn get_metrics(&self) -> KioResult<JobMetrics> {
        let mut conn = self.get_connection().await?;
        crate::utils::get_job_metrics(&self.prefix, &self.name, &mut conn).await
    }
    async fn get_job(&self, id: u64) -> Option<Job<D, R, P>> {
        use redis::Value;
        let job_key = CollectionSuffix::Job(id).to_collection_name(&self.prefix, &self.name);
        let mut conn = self.conn_pool.get().await.ok()?;
        let value: Option<Job<_, _, _>> = conn.hgetall(job_key).await.ok()?;
        value
    }
    async fn get_token(&self, id: u64) -> Option<JobToken> {
        let mut conn = self.get_connection().await.ok()?;
        let job_lock_key = CollectionSuffix::Lock(id).to_collection_name(&self.prefix, &self.name);

        if let Ok(result) = conn.get(job_lock_key).await {
            return Some(result);
        }
        // try fetch token from job hash;
        let job_key = CollectionSuffix::Job(id).to_collection_name(&self.prefix, &self.name);
        conn.hget(job_key, "token").await.ok()
    }
    async fn get_state(&self, id: u64) -> Option<JobState> {
        let mut conn = self.get_connection().await.ok()?;
        let job_key = CollectionSuffix::Job(id).to_collection_name(&self.prefix, &self.name);
        conn.hget(&job_key, "state").await.ok()
    }
    fn remove(&self, key: CollectionSuffix) -> KioResult<()> {
        let key = key.to_collection_name(&self.prefix, &self.name);
        let mut conn = self.get_blocking_connection()?;
        let _: () = conn.del(key)?;
        Ok(())
    }
    //async fn del
    async fn move_job_to_state(
        &self,
        job_id: u64,
        from: JobState,
        to: JobState,
        value: Option<ProcessedResult<R>>,
        ts: Option<i64>,
        backtrace: Option<Trace>,
        event_mode: QueueEventMode,
        is_paused: bool,
    ) -> KioResult<()> {
        let move_to_failed_or_completed = matches!(to, JobState::Failed | JobState::Completed);
        // do nothing if the  queue_is_paused.
        if is_paused {
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
        let _: redis::Value = pipeline.query_async(&mut conn).await?;
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
                ProcessedResult::Success(value) => event.returned_value = Some(value),
            }
        }

        <RedisStore as Store<D, R, P>>::publish_event(self, event_mode, event).await
    }
    async fn set_lock(&self, job_id: u64, token: JobToken, lock_duration: u64) -> KioResult<()> {
        let mut conn = self.get_connection().await?;
        let key = CollectionSuffix::Lock(job_id).to_collection_name(&self.prefix, &self.name);
        let _: () = conn.pset_ex(key, token, lock_duration).await?;
        Ok(())
    }
    async fn set_fields(&self, job_id: u64, fields: &[(&str, String)]) -> KioResult<()> {
        let mut conn = self.get_connection().await?;
        let job_key = CollectionSuffix::Job(job_id).to_collection_name(&self.prefix, &self.name);
        let _: () = conn.hset_multiple(&job_key, fields).await?;
        Ok(())
    }
    async fn incr(
        &self,
        key: CollectionSuffix,
        delta: i64,
        hash_key: Option<&str>,
    ) -> KioResult<()> {
        let mut conn = self.get_connection().await?;
        let key_string = key.to_collection_name(&self.prefix, &self.name);
        let pipeline = redis::pipe();

        if let Some(field_key) = hash_key {
            // we getting the processing counter;
            let _: () = conn.hincr(key_string, field_key, delta).await?;
            return Ok(());
        }

        let _: () = conn.incr(key_string, delta).await?;
        Ok(())
    }
    async fn get_counter(&self, key: CollectionSuffix, hash_key: Option<&str>) -> Option<u64> {
        let mut conn = self.get_connection().await.ok()?;
        let key = key.to_collection_name(&self.prefix, &self.name);
        if let Some(field) = hash_key {
            // we getting the processing counter;
            return conn.hget(key, field).await.ok();
        }

        conn.get(key).await.ok()
    }

    async fn publish_event(
        &self,
        event_mode: QueueEventMode,
        event: QueueStreamEvent<R, P>,
    ) -> KioResult<()> {
        let mut pipeline = redis::pipe();
        let mut conn = self.get_connection().await?;
        let events_stream_key =
            CollectionSuffix::Events.to_collection_name(&self.prefix, &self.name);
        match event_mode {
            QueueEventMode::PubSub => pipeline.publish(events_stream_key, event),
            QueueEventMode::Stream => {
                let mut items = crate::utils::serialize_into_pairs(&event);
                // remove the id field
                items.retain(|(key, _)| key != "id");
                items.retain(|(key, val)| !val.contains("null"));
                pipeline.xadd(events_stream_key, "*", &items)
            }
        };
        let done: () = pipeline.query_async(&mut conn).await?;
        Ok(())
    }
    async fn move_stalled_jobs(
        &self,
        opts: &WorkerOpts,
        (is_paused, target): (bool, JobState),
        event_mode: QueueEventMode,
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
                        <RedisStore as Store<D, R, P>>::move_job_to_state(
                            self,
                            job_id,
                            from,
                            to,
                            Some(ProcessedResult::Failed(failed_reason)),
                            None,
                            None,
                            event_mode,
                            is_paused,
                        )
                        .await?;
                        failed.push(job_id);
                    } else {
                        if !is_paused {
                            let _: () = conn.zadd(&marker_key, 0, "0").await?;
                        }
                        <RedisStore as Store<D, R, P>>::move_job_to_state(
                            self,
                            job_id,
                            JobState::Active,
                            target,
                            None,
                            None,
                            None,
                            event_mode,
                            is_paused,
                        )
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
    async fn pop_set(&self, col: CollectionSuffix, min: bool) -> KioResult<Vec<(u64, u64)>> {
        let key = col.to_collection_name(&self.prefix, &self.name);
        let mut conn = self.get_connection().await?;
        let result = if min {
            conn.zpopmin(key, 1)
        } else {
            conn.zpopmax(key, 1)
        }
        .await?;

        Ok(result)
    }
    async fn job_exist(&self, id: u64) -> bool {
        let mut conn = self
            .get_connection()
            .await
            .expect("failed to get connection");
        let job_key = CollectionSuffix::Job(id).to_collection_name(&self.prefix, &self.name);
        let result = conn.exists(job_key).await.unwrap_or_default();
        result
    }

    async fn clear_collections(&self) -> KioResult<()> {
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
        let done: () = pipeline.query_async(&mut conn).await?;
        Ok(())
    }

    async fn remove_item(&self, col: CollectionSuffix, item: u64) -> KioResult<()> {
        let mut conn = self.get_connection().await?;
        let mut pipeline = redis::pipe();
        let key = col.to_collection_name(&self.prefix, &self.name);
        match col {
            CollectionSuffix::Completed
            | CollectionSuffix::Delayed
            | CollectionSuffix::Failed
            | CollectionSuffix::Prioritized => {
                pipeline.zrem(key, item);
            }
            CollectionSuffix::Stalled => {
                pipeline.srem(key, item);
            }
            CollectionSuffix::Active | CollectionSuffix::Wait | CollectionSuffix::Wait => {
                pipeline.lrem(key, 1, item);
            }
            _ => {}
        }
        let _: () = pipeline.query_async(&mut conn).await?;
        Ok(())
    }
    async fn add_item(
        &self,
        col: CollectionSuffix,
        item: u64,
        score: Option<i64>,
        append: bool,
    ) -> KioResult<()> {
        let mut conn = self.get_connection().await?;
        let mut pipeline = redis::pipe();
        let key = col.to_collection_name(&self.prefix, &self.name);
        match col {
            CollectionSuffix::Completed
            | CollectionSuffix::Delayed
            | CollectionSuffix::Failed
            | CollectionSuffix::Prioritized => {
                let score = score.unwrap_or_default();
                pipeline.zadd(key, item, score);
            }
            CollectionSuffix::Stalled => {
                pipeline.sadd(key, item);
            }

            CollectionSuffix::Active | CollectionSuffix::Wait | CollectionSuffix::Wait => {
                if append {
                    pipeline.lpush(key, item);
                } else {
                    pipeline.rpush(key, item);
                }
            }

            _ => {}
        }
        let _: () = pipeline.query_async(&mut conn).await?;
        Ok(())
    }
    async fn expire(&self, col: CollectionSuffix, secs: i64) -> KioResult<()> {
        let mut conn = self.get_connection().await?;
        let key = col.to_collection_name(&self.prefix, &self.name);
        let _: () = conn.expire(key, secs).await?;
        Ok(())
    }
    async fn clear_jobs(&self, last_id: u64) -> KioResult<()> {
        let mut conn = self.conn_pool.get().await?;
        let id_key = CollectionSuffix::Id.to_collection_name(&self.prefix, &self.name);
        let mut pipeline = redis::pipe();
        pipeline.atomic();

        (1..=last_id).for_each(|id| {
            let job_key = CollectionSuffix::Job(id).to_collection_name(&self.prefix, &self.name);
            pipeline.del(job_key);
        });

        let _: () = pipeline.query_async(&mut conn).await?;
        Ok(())
    }
    async fn pause(&self, pause: bool, event_mode: QueueEventMode) -> KioResult<()> {
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
        match pause {
            true => pipeline.hset(meta_key, CollectionSuffix::Paused, 1),
            _ => pipeline.hdel(meta_key, CollectionSuffix::Paused),
        };
        let _: redis::Value = pipeline.query_async(&mut conn).await?;
        Ok(())
    }
}
