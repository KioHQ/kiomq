use super::{
    BTreeMap, CollectionSuffix, EventEmitter, Job, JobField, JobOptions, JobState, JobToken,
    KioResult, ProcessedResult, QueueEventMode, QueueMetrics, QueueOpts, QueueStreamEvent, Store,
    Trace, VecDeque, WorkerMetrics,
};
use crate::utils::{
    create_listener_handle, prepare_for_insert, process_each_event, query_all_batched,
    update_job_opts,
};
use chrono::Utc;
use deadpool_redis::{Config, Pool, Runtime};
use derive_more::Debug;
use futures::StreamExt;
use redis::aio::{PubSubSink, PubSubStream};
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, Commands, LposOptions};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use uuid::Uuid;
/// a counter for adding bulk jobs,
static START: AtomicU64 = AtomicU64::new(0);
static PC_COUNTER: AtomicU64 = AtomicU64::new(0);
use xutex::Mutex;
/// A [`Store`] implementation backed by Redis.
///
/// `RedisStore` uses a deadpool connection pool for async operations and a
/// separate synchronous `redis::Client` for the small number of blocking
/// operations that must run outside of async context. Events can be delivered
/// via either a Redis Stream or Pub/Sub channel depending on the
/// [`QueueEventMode`] configured on the queue.
///
/// # Examples
///
/// ```rust,no_run
/// use kiomq::{Config, Queue, QueueOpts, RedisStore, fetch_redis_pass};
///
/// #[tokio::main]
/// async fn main() -> kiomq::KioResult<()> {
///     let mut cfg = Config::from_url("redis://127.0.0.1/");
///     let store = RedisStore::new(None, "my-queue", &cfg).await?;
///     let queue: Queue<String, String, (), _> =
///         Queue::new(store, Some(QueueOpts::default())).await?;
///     Ok(())
/// }
/// ```
#[derive(Clone, Debug)]
pub struct RedisStore {
    /// The key prefix used to namespace all Redis collections for this queue.
    pub prefix: String,
    /// The name of the queue this store was created for.
    pub name: String,
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
    /// Creates a new `RedisStore` connected to the Redis instance described by `cfg`.
    ///
    /// A deadpool-redis connection pool is created for async use and a consumer
    /// group is registered on the queue's event stream so that this instance
    /// can receive events.
    ///
    /// # Arguments
    ///
    /// * `prefix` – key namespace prefix (defaults to `"{kio}"` when `None`).
    /// * `name` – queue name; converted to lowercase automatically.
    /// * `cfg` – deadpool-redis [`Config`] describing the connection URL and pool settings.
    ///
    /// # Errors
    ///
    /// Returns [`crate::KioError`] if the pool cannot be created or the initial Redis
    /// connection fails.
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
        let stream_key = CollectionSuffix::Events.to_collection_name(&prefix, &name);
        let (sink, source) = redis_client.get_async_pubsub().await?.split();
        let pubsub_sink = Arc::new(Mutex::new(sink));
        let pubsub_source = Arc::new(Mutex::new(source));
        connection
            .xgroup_create_mkstream::<_, _, _, ()>(&stream_key, &consumer_group, "$")
            .await?;
        let subscribed = Arc::default();

        Ok(Self {
            prefix,
            name,
            consumer_group,
            consumer_name,
            stream_key,
            pubsub_source,
            pubsub_sink,
            subscribed,
            conn_pool,
            redis_client,
        })
    }
    /// Returns a synchronous (blocking) Redis connection.
    ///
    /// Useful for operations that cannot be performed asynchronously. Prefer
    /// [`get_connection`](RedisStore::get_connection) for all async code paths.
    ///
    /// # Errors
    ///
    /// Returns [`crate::KioError`] if the connection cannot be established.
    pub fn get_blocking_connection(&self) -> KioResult<redis::Connection> {
        let conn = self.redis_client.get_connection()?;
        Ok(conn)
    }
    /// Borrows an async connection from the deadpool connection pool.
    ///
    /// # Errors
    ///
    /// Returns [`crate::KioError`] if the pool is exhausted or the connection fails.
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
    fn fetch_worker_metrics(&self) -> KioResult<BTreeMap<uuid::Uuid, WorkerMetrics>> {
        let mut conn = self.get_blocking_connection()?;
        let prefix = CollectionSuffix::Prefix.to_collection_name(&self.prefix, &self.name);
        let key = format!("{prefix}worker_metrics:*");
        let mut pipe = redis::pipe();
        let results = conn.scan_match::<_, redis::Value>(key)?;
        results.into_iter().for_each(|value| match value {
            redis::Value::BulkString(items) => {
                pipe.get(&items);
            }
            redis::Value::SimpleString(str) => {
                pipe.get(str.as_bytes());
            }
            _ => {}
        });

        let results: Vec<WorkerMetrics> = pipe.query(&mut conn)?;
        Ok(results
            .into_iter()
            .map(|metrics| (metrics.worker_id, metrics))
            .collect())
    }
    async fn store_worker_metrics(&self, metrics: WorkerMetrics, ttl_ms: u64) -> KioResult<()> {
        let key = CollectionSuffix::WorkerMetrics(metrics.worker_id)
            .to_collection_name(&self.prefix, &self.name);
        let mut conn = self.get_connection().await?;
        let metrics_string = simd_json::to_string(&metrics)?;
        let _: () = conn.pset_ex(key, metrics_string, ttl_ms).await?;
        Ok(())
    }
    async fn metadata_field_exists(&self, field: &str) -> KioResult<bool> {
        let mut conn = self.get_connection().await?;
        let meta_key = CollectionSuffix::Meta.to_collection_name(&self.prefix, &self.name);
        let result = conn.hexists(meta_key, field).await?;
        Ok(result)
    }
    async fn exists_in(&self, col: CollectionSuffix, item: u64) -> KioResult<bool> {
        let mut conn = self.get_connection().await?;
        let key = col.to_collection_name(&self.prefix, &self.name);
        let value = match col {
            CollectionSuffix::Completed
            | CollectionSuffix::Delayed
            | CollectionSuffix::Failed
            | CollectionSuffix::Prioritized => {
                let score: Option<u64> = conn.zscore(key, item).await?;
                score.is_some()
            }
            CollectionSuffix::Stalled => conn.sismember(key, item).await?,

            CollectionSuffix::Active | CollectionSuffix::Wait => {
                let opts = LposOptions::default();
                let pos: Option<redis::Value> = conn.lpos(key, item, opts).await?;
                pos.is_some()
            }

            _ => conn.exists(key).await?,
        };
        Ok(value)
    }
    async fn set_event_mode(&self, event_mode: QueueEventMode) -> KioResult<()> {
        let mut conn = self.get_connection().await?;
        let meta_key = CollectionSuffix::Meta.to_collection_name(&self.prefix, &self.name);
        let result = conn.hset(&meta_key, "event_mode", event_mode).await?;
        Ok(result)
    }
    fn queue_name(&self) -> &str {
        &self.name
    }
    fn queue_prefix(&self) -> &str {
        &self.prefix
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
                        name: Some(self.name.clone()),
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
                        ("name", self.name.clone()),
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
        pipeline.zrembyscore(&delayed_key, "-inf", start);
        let (jobs, missed_deadline, _done, _): (Vec<u64>, Vec<u64>, i64, i64) =
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
    async fn listen_to_events(
        &self,
        event_mode: QueueEventMode,
        block_interval: Option<u64>,
        emitter: &EventEmitter<R, P>,
        metrics: &QueueMetrics,
    ) -> KioResult<()> {
        if !self.subscribed.load(Ordering::Acquire) {
            self.pubsub_sink
                .as_async()
                .lock()
                .await
                .subscribe(&self.stream_key)
                .await?;
            self.subscribed.store(true, Ordering::Release);
        }
        match event_mode {
            QueueEventMode::PubSub => {
                use tokio_util::time::FutureExt;
                let block_interval = block_interval.unwrap_or(5000);
                let timeout = Duration::from_millis(block_interval);
                while let Ok(Some(msg)) = self
                    .pubsub_source
                    .as_async()
                    .lock()
                    .await
                    .next()
                    .timeout(timeout)
                    .await
                {
                    let event: QueueStreamEvent<R, P> = msg.get_payload()?;
                    process_each_event::<D, R, P>(event, emitter, self, metrics).await?;
                }
            }
            QueueEventMode::Stream => {
                let mut connection = self.get_connection().await?;
                let mut options = StreamReadOptions::default()
                    .group(&self.consumer_group, &self.consumer_name)
                    .noack();
                if let Some(b_internal) = block_interval {
                    options = options.block(usize::try_from(b_internal).unwrap_or(usize::MAX));
                }
                let reply: StreamReadReply = connection
                    .xread_options(&[&self.stream_key], &[">"], &options)
                    .await?;

                let events =
                    QueueStreamEvent::<R, P>::from_stream_read_reply(&self.stream_key, reply);
                for event in events {
                    process_each_event::<D, R, P>(event, emitter, self, metrics).await?;
                }
            }
        }

        Ok(())
    }
    async fn create_stream_listener(
        &self,
        emitter: EventEmitter<R, P>,
        notifier: Arc<Notify>,
        metrics: Arc<QueueMetrics>,
        pause_workers: Arc<AtomicBool>,
        event_mode: QueueEventMode,
    ) -> KioResult<JoinHandle<KioResult<()>>> {
        create_listener_handle::<D, R, P, Self>(
            self,
            emitter,
            notifier,
            metrics,
            pause_workers,
            event_mode,
        )
        .await
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
        let start = (end - max_len_hint) + 1;
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
        let start = (end - max_len_hint) + 1;
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
            result.push(job);
        }
        if is_prioritized {
            pipeline.incr(&priority_counter_key, PC_COUNTER.load(Ordering::Acquire));
        }

        query_all_batched(conn, pipeline).await?;
        Ok(result)
    }

    async fn get_metrics(&self) -> KioResult<QueueMetrics> {
        let mut conn = self.get_connection().await?;
        crate::utils::get_queue_metrics(&self.prefix, &self.name, &mut conn).await
    }
    async fn get_job(&self, id: u64) -> Option<Job<D, R, P>> {
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
    async fn set_lock(
        &self,
        col: CollectionSuffix,
        token: Option<JobToken>,
        lock_duration: u64,
    ) -> KioResult<()> {
        let mut conn = self.get_connection().await?;
        let key = col.to_collection_name(&self.prefix, &self.name);
        match col {
            CollectionSuffix::Lock(_) => {
                if let Some(token) = token {
                    let _: () = conn.pset_ex(key, token, lock_duration).await?;
                }
            }
            CollectionSuffix::StalledCheck => {
                let ts = Utc::now().timestamp_micros();
                let _: () = conn.pset_ex(key, ts, lock_duration).await?;
            }
            _ => {}
        }
        Ok(())
    }
    async fn set_fields(&self, job_id: u64, fields: Vec<JobField<R>>) -> KioResult<()> {
        let mut conn = self.get_connection().await?;
        let mut blocking_con = self.redis_client.get_connection()?;
        let job_key = CollectionSuffix::Job(job_id).to_collection_name(&self.prefix, &self.name);
        let fields: Vec<_> = fields
            .into_iter()
            .filter_map(|field| {
                let name = field.name();
                if let JobField::BackTrace(trace) = field {
                    let mut previous: Vec<u8> = blocking_con.hget(&job_key, "stackTrace").ok()?;
                    let mut previous: Vec<Trace> = simd_json::from_slice(&mut previous).ok()?;
                    previous.push(trace);
                    return simd_json::to_string(&previous)
                        .ok()
                        .map(move |result| (name, result));
                }
                if let JobField::Payload(ProcessedResult::Success(result, _)) = field {
                    return simd_json::to_string(&result)
                        .ok()
                        .map(move |result| (name, result));
                }
                simd_json::to_string(&field)
                    .ok()
                    .map(move |result| (name, result))
            })
            .collect();
        let _: () = conn.hset_multiple(&job_key, &fields).await?;
        Ok(())
    }
    async fn incr(
        &self,
        key: CollectionSuffix,
        delta: i64,
        hash_key: Option<&str>,
    ) -> KioResult<u64> {
        let mut conn = self.get_connection().await?;
        let key_string = key.to_collection_name(&self.prefix, &self.name);

        if let Some(field_key) = hash_key {
            let val = conn.hincr(key_string, field_key, delta).await?;
            return Ok(val);
        }

        let value: u64 = conn.incr(key_string, delta).await?;
        Ok(value)
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
                items.retain(|(_, val)| !val.contains("null"));
                pipeline.xadd(events_stream_key, "*", &items)
            }
        };
        let _: () = pipeline.query_async(&mut conn).await?;
        Ok(())
    }
    fn get_job_ids_in_state(
        &self,
        state: JobState,
        start: Option<usize>,
        end: Option<usize>,
    ) -> KioResult<VecDeque<u64>> {
        let mut conn = self.get_blocking_connection()?;
        let start = start.unwrap_or_default();
        let col: CollectionSuffix = state.into();
        let key = col.to_collection_name(&self.prefix, &self.name);
        match state {
            JobState::Prioritized | JobState::Completed | JobState::Failed | JobState::Delayed => {
                let list_len: usize = conn.zcard(&key)?;
                if list_len > 0 {
                    let end = end.map_or(list_len, |value| value + 1);

                    let items: Vec<u64> =
                        conn.zrange(key, start.cast_signed(), end.cast_signed())?;
                    return Ok(VecDeque::from_iter(items));
                }
            }
            JobState::Stalled => {
                let set: Vec<u64> = conn.smembers(key)?;
                if !set.is_empty() {
                    let set = VecDeque::from(set);
                    let end = end.unwrap_or(set.len());

                    return Ok(set.range(start..=end).copied().collect());
                }
            }

            JobState::Active | JobState::Wait | JobState::Paused => {
                let list_len: usize = conn.llen(&key)?;
                if list_len > 0 {
                    let end = end.map_or(list_len, |value| value + 1);
                    let items: Vec<u64> =
                        conn.lrange(key, start.cast_signed(), end.cast_signed())?;
                    return Ok(VecDeque::from_iter(items));
                }
            }
            _ => {}
        }
        Ok(VecDeque::new())
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
    async fn job_exists(&self, id: u64) -> bool {
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
        for name in [
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
            CollectionSuffix::Paused,
        ] {
            let key = name.to_collection_name(&self.prefix, &self.name);
            pipeline.del(key);
        }
        let _: () = pipeline.query_async(&mut conn).await?;
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
            CollectionSuffix::Active | CollectionSuffix::Wait | CollectionSuffix::Paused => {
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

            CollectionSuffix::Active | CollectionSuffix::Wait | CollectionSuffix::Paused => {
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
        let mut pipeline = redis::pipe();
        pipeline.atomic();

        (1..=last_id).for_each(|id| {
            let job_key = CollectionSuffix::Job(id).to_collection_name(&self.prefix, &self.name);
            pipeline.del(job_key);
        });

        let _: () = pipeline.query_async(&mut conn).await?;
        Ok(())
    }
    async fn pause(&self, pause: bool, _event_mode: QueueEventMode) -> KioResult<()> {
        let [wait_key, _events_key, meta_key, paused_key] = [
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
        if pause {
            pipeline.hset(meta_key, CollectionSuffix::Paused, 1)
        } else {
            pipeline.hdel(meta_key, CollectionSuffix::Paused)
        };
        let _: redis::Value = pipeline.query_async(&mut conn).await?;
        Ok(())
    }

    fn fetch_jobs(&self, ids: &[u64]) -> KioResult<VecDeque<Job<D, R, P>>> {
        if ids.is_empty() {
            return Ok(VecDeque::new());
        }
        let mut conn = self.get_blocking_connection()?;
        let mut pipeline = redis::pipe();
        pipeline.atomic();
        for id in ids {
            let key = CollectionSuffix::Job(*id).to_collection_name(&self.prefix, &self.name);
            pipeline.hgetall(key);
        }

        let list: Vec<Job<D, R, P>> = pipeline.query(&mut conn)?;

        Ok(VecDeque::from_iter(list))
    }
}
