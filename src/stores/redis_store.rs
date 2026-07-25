use super::{
    BTreeMap, CollectionSuffix, EventEmitter, Job, JobField, JobOptions, JobState, JobToken,
    KioResult, ProcessedResult, QueueEventMode, QueueMetrics, QueueOpts, QueueStreamEvent, Store,
    Trace, VecDeque, WorkerMetrics,
};
use crate::ProcessMetrics;
use crate::utils::{
    create_listener_handle, prepare_for_insert, process_each_event, query_all_batched,
    update_job_opts,
};
use chrono::Utc;
use compact_str::{CompactString, ToCompactString, format_compact};
use crossbeam::atomic::AtomicCell;
use deadpool_redis::{Config, Pool, Runtime};
use derive_more::Debug;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use redis::aio::{PubSubSink, PubSubStream, transaction_async};
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{
    AsyncCommands, FieldExistenceCheck, HashFieldExpirationOptions, LposOptions, SetExpiry,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, Notify};
use uuid::Uuid;
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
/// use kiomq::{Config, Queue, QueueOpts, RedisStore, fetch_redis_pass, SharedRedis};
///
/// #[tokio::main]
/// async fn main() -> kiomq::KioResult<()> {
///     let mut cfg = Config::from_url("redis://127.0.0.1/");
///     let  mut redis_conn  =  SharedRedis::create(&cfg)?;
///     let store = RedisStore::new(None, "my-queue", &redis_conn).await?;
///     let queue: Queue<String, String, (), _> =
///         Queue::new(store, Some(QueueOpts::default())).await?;
///     Ok(())
/// }
/// ```
#[derive(Clone, Debug)]
pub struct RedisStore {
    /// The key prefix used to namespace all Redis collections for this queue.
    pub prefix: CompactString,
    /// The name of the queue this store was created for.
    pub name: CompactString,
    consumer_group: CompactString,
    consumer_name: CompactString,
    stream_key: CompactString,
    #[debug(skip)]
    pubsub_source: Arc<Mutex<PubSubStream>>,
    #[debug(skip)]
    pubsub_sink: Arc<Mutex<PubSubSink>>,
    subscribed: Arc<AtomicBool>,
    #[debug(skip)]
    pub(crate) connection: SharedRedis,

    /// The redis version of redis this store is initialized with.
    pub redis_version: RedisVersion,
}

impl RedisStore {
    /// Creates a new `RedisStore` connected to the Redis instance described by `shared_conn`.
    ///
    /// A deadpool-redis connection pool is created for async use and a consumer
    /// group is registered on the queue's event stream so that this instance
    /// can receive events.
    ///
    /// # Arguments
    ///
    /// * `prefix` – key namespace prefix (defaults to `"{kio}"` when `None`).
    /// * `name` – queue name; converted to lowercase automatically.
    /// * `shared_conn` – shared redis connection [`SharedRedis`] with shared pool and  redis-client.
    ///
    /// # Errors
    ///
    /// Returns [`crate::KioError`] if the pool cannot be created or the initial Redis
    /// connection fails.
    pub async fn new(
        prefix: Option<&str>,
        name: &str,
        shared_conn: &SharedRedis,
    ) -> KioResult<Self> {
        let name = name.to_compact_string();
        let prefix = prefix.unwrap_or("{kio}").to_compact_string();
        let conn_pool = shared_conn.conn_pool.clone();
        let id = Uuid::new_v4();
        let consumer_group = format_compact!("{prefix}-{prefix}-group-{id}");
        let consumer_name = format_compact!("consumer-{id}");
        let mut connection = conn_pool.get().await?;
        let stream_key = CollectionSuffix::Events.to_collection_name(&prefix, &name);
        let (sink, source) = shared_conn.redis_client.get_async_pubsub().await?.split();
        let pubsub_sink = Arc::new(Mutex::new(sink));
        let pubsub_source = Arc::new(Mutex::new(source));
        connection
            .xgroup_create_mkstream::<_, _, _, ()>(
                stream_key.as_str(),
                consumer_group.as_str(),
                "$",
            )
            .await?;
        let subscribed = Arc::default();
        let raw: String = redis::cmd("INFO")
            .arg("server")
            .query_async(&mut connection)
            .await?;

        let redis_version = RedisVersion::parse(&raw)
            .ok_or_else(|| std::io::Error::other("failed to fetch redis-version info "))?;
        let connection = shared_conn.clone();

        Ok(Self {
            prefix,
            name,
            consumer_group,
            consumer_name,
            stream_key,
            pubsub_source,
            pubsub_sink,
            subscribed,
            connection,
            redis_version,
        })
    }

    /// Borrows an async connection from the deadpool connection pool.
    ///
    /// # Errors
    ///
    /// Returns [`crate::KioError`] if the pool is exhausted or the connection fails.
    pub async fn get_connection(&self) -> KioResult<deadpool_redis::Connection> {
        let conn = self.connection.conn_pool.get().await?;
        Ok(conn)
    }
    async fn add<
        D: Clone + Serialize + DeserializeOwned + Send + 'static,
        R: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
        P: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
    >(
        &self,
        iter: Box<dyn Iterator<Item = (String, Option<JobOptions>, D)> + Send>,
        queue_opts: QueueOpts,
        event_mode: QueueEventMode,
        is_paused: bool,
        return_jobs: bool,
    ) -> KioResult<Vec<Job<D, R, P>>> {
        let deadpool_conn = self.get_connection().await?;
        let conn = deadpool_redis::Connection::take(deadpool_conn);
        let id_key = CollectionSuffix::Id
            .to_collection_name(&self.prefix, &self.name)
            .to_string();
        let pc_key = CollectionSuffix::PriorityCounter
            .to_collection_name(&self.prefix, &self.name)
            .to_string();
        let items = iter.collect::<Vec<_>>();
        let jobs = transaction_async(
            conn,
            &[id_key.clone(), pc_key.clone()],
            move |mut conn: redis::aio::MultiplexedConnection, mut pipeline: redis::Pipeline| {
                let id_key = id_key.clone();
                let priority_counter_key = pc_key.clone();
                let queue_opts = queue_opts.clone();
                let items = items.clone();

                async move {
                    let mut jobs = Vec::with_capacity(items.len());
                    let id: u64 = conn
                        .get::<_, Option<u64>>(&id_key.as_str())
                        .await?
                        .unwrap_or_default();
                    let id_counter: AtomicCell<u64> = AtomicCell::new(id + 1);
                    let prior_counter: u64 = conn
                        .get::<_, Option<u64>>(priority_counter_key.as_str())
                        .await?
                        .unwrap_or_default();
                    let pc_counter: AtomicCell<u64> = AtomicCell::new(prior_counter + 1);
                    let mut to_priorize = false;
                    for (ref name, opts, data) in items {
                        let mut opts = opts.unwrap_or_default();
                        update_job_opts(&queue_opts, &mut opts);
                        let queue_name = format_compact!("{}:{}", self.prefix, self.name);
                        let id = id_counter.fetch_add(1);
                        let prior_counter = if opts.priority > 0 {
                            to_priorize = true;
                            pc_counter.fetch_add(1)
                        } else {
                            pc_counter.load()
                        };
                        let mut job = Job::<D, R, P>::new(
                            name,
                            Some(data.clone()),
                            opts.id,
                            Some(queue_name.as_str()),
                        );
                        let _ = prepare_for_insert(
                            queue_name.as_str(),
                            event_mode,
                            is_paused,
                            id,
                            prior_counter,
                            opts,
                            &mut job,
                            name,
                            &mut pipeline,
                        );

                        job.id = Some(id);
                        if return_jobs {
                            jobs.push(job);
                        }
                    }
                    pipeline.incr(id_key.as_str(), id_counter.load().saturating_sub(1));
                    if to_priorize {
                        pipeline.incr(
                            priority_counter_key.as_str(),
                            pc_counter.load().saturating_sub(1),
                        );
                    }
                    let _: () = query_all_batched(&conn, pipeline).await?;

                    Ok(Some(jobs))
                }
                .boxed()
            },
        )
        .await?;
        Ok(jobs)
    }

    async fn store_metrics(&self, metrics: MetricsType, ttl_ms: u64) -> KioResult<()> {
        let key = metrics
            .get_collection_suffix()
            .to_collection_name(&self.prefix, &self.name);
        let mut conn = self.get_connection().await?;
        let (field_key, metrics_string) = match metrics {
            MetricsType::Process(process_metrics) => (
                format_compact!("{}-Pid({})", process_metrics.hostname, process_metrics.pid),
                simd_json::to_string(&process_metrics)?,
            ),
            MetricsType::Worker(worker_metrics) => (
                worker_metrics.worker_id.to_compact_string(),
                simd_json::to_string(&worker_metrics)?,
            ),
        };
        let expiry_opts = HashFieldExpirationOptions::default()
            .set_existence_check(FieldExistenceCheck::FNX)
            .set_expiration(SetExpiry::PX(ttl_ms));
        if self.redis_version.is_at_least("7.4.0").unwrap_or(false) {
            let _: () = conn
                .hset_ex(
                    key.as_str(),
                    &expiry_opts,
                    &[(field_key.as_str(), metrics_string.as_str())],
                )
                .await?;
            return Ok(());
        }
        let mut pipeline = redis::pipe();
        pipeline.atomic();
        pipeline.hset(key.as_str(), field_key.as_str(), metrics_string);
        pipeline.pexpire(key.as_str(), ttl_ms.saturating_mul(100).cast_signed());
        let _: () = pipeline.query_async(&mut conn).await?;
        Ok(())
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
    async fn fetch_worker_metrics(&self) -> KioResult<BTreeMap<uuid::Uuid, WorkerMetrics>> {
        let mut conn = self.get_connection().await?;
        let key = CollectionSuffix::WorkerMetrics.to_collection_name(&self.prefix, &self.name);
        let results: Vec<WorkerMetrics> = conn.hvals(key.as_str()).await?;
        Ok(results
            .into_iter()
            .filter_map(|metrics| {
                let now = Utc::now();
                if now
                    .signed_duration_since(metrics.last_updated)
                    .num_milliseconds()
                    <= (metrics.ttl_ms.cast_signed() + 20)
                {
                    return Some((metrics.worker_id, metrics));
                }

                None
            })
            .collect())
    }
    async fn store_worker_metrics(&self, metrics: WorkerMetrics, ttl_ms: u64) -> KioResult<()> {
        self.store_metrics(MetricsType::Worker(metrics), ttl_ms)
            .await
    }
    async fn store_process_metrics(&self, metrics: ProcessMetrics, ttl_ms: u64) -> KioResult<()> {
        self.store_metrics(MetricsType::Process(metrics.into()), ttl_ms)
            .await
    }
    async fn fetch_process_metrics(&self) -> KioResult<BTreeMap<u32, ProcessMetrics>> {
        let mut conn = self.get_connection().await?;
        let key = CollectionSuffix::ProcessMetrics.to_collection_name(&self.prefix, &self.name);
        let results: Vec<ProcessMetrics> = conn.hvals(key.as_str()).await?;
        Ok(results
            .into_iter()
            .map(|metrics| (metrics.pid, metrics))
            .collect())
    }
    async fn metadata_field_exists(&self, field: &str) -> KioResult<bool> {
        let mut conn = self.get_connection().await?;
        let meta_key = CollectionSuffix::Meta.to_collection_name(&self.prefix, &self.name);
        let result = conn.hexists(meta_key.as_str(), field).await?;
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
                let score: Option<u64> = conn.zscore(key.as_str(), item).await?;
                score.is_some()
            }
            CollectionSuffix::Stalled => conn.sismember(key.as_str(), item).await?,

            CollectionSuffix::Active | CollectionSuffix::Wait => {
                let opts = LposOptions::default();
                let pos: Option<redis::Value> = conn.lpos(key.as_str(), item, opts).await?;
                pos.is_some()
            }

            _ => conn.exists(key.as_str()).await?,
        };
        Ok(value)
    }
    async fn set_event_mode(&self, event_mode: QueueEventMode) -> KioResult<()> {
        let mut conn = self.get_connection().await?;
        let meta_key = CollectionSuffix::Meta.to_collection_name(&self.prefix, &self.name);
        let result = conn
            .hset(meta_key.as_str(), "event_mode", event_mode)
            .await?;
        Ok(result)
    }
    fn queue_name(&self) -> &str {
        &self.name
    }
    fn queue_prefix(&self) -> &str {
        &self.prefix
    }
    async fn update_job_progress(&self, job: &mut Job<D, R, P>, value: P) -> KioResult<()> {
        let mut conn = self.connection.conn_pool.get().await?;
        if let Some(id) = job.id {
            let job_key = CollectionSuffix::Job(id).to_collection_name(&self.prefix, &self.name);
            let mut pipeline = redis::pipe();
            pipeline.atomic();
            let progress_str = simd_json::to_string_pretty(&value)?.to_compact_string();
            let events_stream_key =
                CollectionSuffix::Events.to_collection_name(&self.prefix, &self.name);
            pipeline.hset(job_key.as_str(), "progress", progress_str.as_str());
            let meta_key = CollectionSuffix::Meta.to_collection_name(&self.prefix, &self.name);
            let event_mode: Option<QueueEventMode> =
                conn.hget(meta_key.as_str(), "event_mode").await?;
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
                    pipeline.publish(events_stream_key.as_str(), event);
                }
                QueueEventMode::Stream => {
                    let items = [
                        ("event", JobState::Progress.to_string().to_lowercase()),
                        ("job_id", id.to_string()),
                        ("data", progress_str.to_string()),
                        ("name", self.name.to_string()),
                    ];
                    pipeline.xadd(events_stream_key.as_str(), "*", &items);
                }
            }
            let _: () = pipeline.query_async(&mut conn).await?;
            job.progress = Some(value);
        }
        Ok(())
    }
    fn update_job_progress_sync(&self, job: &mut Job<D, R, P>, value: P) -> KioResult<()> {
        use crate::QueueEventMode;
        use redis::Commands;
        let mut conn = self.connection.redis_client.get_connection()?;
        if let Some(id) = job.id {
            let job_key = CollectionSuffix::Job(id).to_collection_name(&self.prefix, &self.name);
            let mut pipeline = redis::pipe();
            pipeline.atomic();
            let progress_str = simd_json::to_string_pretty(&value)?.to_compact_string();
            let events_stream_key =
                CollectionSuffix::Events.to_collection_name(&self.prefix, &self.name);
            pipeline.hset(job_key.as_str(), "progress", progress_str.as_str());
            let meta_key = CollectionSuffix::Meta.to_collection_name(&self.prefix, &self.name);
            let event_mode: Option<QueueEventMode> = conn.hget(meta_key.as_str(), "event_mode")?;
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
                    pipeline.publish(events_stream_key.as_str(), event);
                }
                QueueEventMode::Stream => {
                    let items = [
                        ("event", JobState::Progress.to_string().to_lowercase()),
                        ("job_id", id.to_string()),
                        ("data", progress_str.to_string()),
                        ("name", self.name.to_string()),
                    ];
                    pipeline.xadd(events_stream_key.as_str(), "*", &items);
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
        pipeline.zrangebyscore(delayed_key.as_str(), start, stop);
        pipeline.zrangebyscore(
            delayed_key.as_str(),
            "-inf",
            format_compact!("({start}").as_str(),
        );
        pipeline.zrembyscore(delayed_key.as_str(), start, stop);
        pipeline.zrembyscore(delayed_key.as_str(), "-inf", start);
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
        conn.rpoplpush(src_key.as_str(), dst_key.as_str())
            .await
            .ok()
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
                .lock()
                .await
                .subscribe(self.stream_key.as_str())
                .await?;
            self.subscribed.store(true, Ordering::Release);
        }
        match event_mode {
            QueueEventMode::PubSub => {
                while let Some(msg) = self.pubsub_source.lock().await.next().await {
                    let event: QueueStreamEvent<R, P> = msg.get_payload()?;
                    process_each_event::<D, R, P>(event, emitter, self, metrics).await?;
                }
            }
            QueueEventMode::Stream => {
                let mut connection = self.get_connection().await?;
                let mut options = StreamReadOptions::default()
                    .group(self.consumer_group.as_str(), self.consumer_name.as_str())
                    .noack();
                if let Some(b_internal) = block_interval {
                    options = options.block(usize::try_from(b_internal).unwrap_or(usize::MAX));
                }
                let reply: StreamReadReply = connection
                    .xread_options(&[self.stream_key.as_str()], &[">"], &options)
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
        pause_workers: Arc<AtomicCell<bool>>,
        event_mode: QueueEventMode,
    ) -> BoxFuture<'static, KioResult<()>> {
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
        let _done = self
            .add::<D, R, P>(iter, queue_opts, event_mode, is_paused, false)
            .await?;
        Ok(())
    }
    async fn add_bulk(
        &self,
        iter: Box<dyn Iterator<Item = (String, Option<JobOptions>, D)> + Send>,
        queue_opts: QueueOpts,
        event_mode: QueueEventMode,
        is_paused: bool,
    ) -> KioResult<Vec<Job<D, R, P>>> {
        let jobs = self
            .add::<D, R, P>(iter, queue_opts, event_mode, is_paused, true)
            .await?;
        Ok(jobs)
    }

    async fn get_metrics(&self) -> KioResult<QueueMetrics> {
        let mut conn = self.get_connection().await?;
        crate::utils::get_queue_metrics(&self.prefix, &self.name, &mut conn).await
    }
    async fn get_job(&self, id: u64) -> Option<Job<D, R, P>> {
        let job_key = CollectionSuffix::Job(id).to_collection_name(&self.prefix, &self.name);
        let mut conn = self.connection.conn_pool.get().await.ok()?;
        let value: Option<Job<_, _, _>> = conn.hgetall(job_key.as_str()).await.ok()?;
        value
    }
    async fn get_token(&self, id: u64) -> Option<JobToken> {
        let mut conn = self.get_connection().await.ok()?;
        let job_lock_key = CollectionSuffix::Lock(id).to_collection_name(&self.prefix, &self.name);

        if let Ok(result) = conn.get(job_lock_key.as_str()).await {
            return Some(result);
        }
        let job_key = CollectionSuffix::Job(id).to_collection_name(&self.prefix, &self.name);
        conn.hget(job_key.as_str(), "token").await.ok()
    }
    async fn get_state(&self, id: u64) -> Option<JobState> {
        let mut conn = self.get_connection().await.ok()?;
        let job_key = CollectionSuffix::Job(id).to_collection_name(&self.prefix, &self.name);
        conn.hget(job_key.as_str(), "state").await.ok()
    }
    async fn remove(&self, key: CollectionSuffix) -> KioResult<()> {
        let key = key.to_collection_name(&self.prefix, &self.name);
        let mut conn = self.get_connection().await?;
        let _: () = conn.del(key.as_str()).await?;
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
                    let _: () = conn.pset_ex(key.as_str(), token, lock_duration).await?;
                }
            }
            CollectionSuffix::StalledCheck => {
                let ts = Utc::now().timestamp_micros();
                let _: () = conn.pset_ex(key.as_str(), ts, lock_duration).await?;
            }
            _ => {}
        }
        Ok(())
    }
    async fn set_fields(&self, job_id: u64, fields: Vec<JobField<R>>) -> KioResult<()> {
        let mut conn = self.get_connection().await?;
        let job_key = CollectionSuffix::Job(job_id).to_collection_name(&self.prefix, &self.name);
        let mut next_fields = vec![];
        for field in fields {
            let name = field.name();
            let pair = match field {
                JobField::Payload(ProcessedResult::Success(result, _)) => {
                    simd_json::to_string(&result).map(move |result| (name, result))?
                }
                JobField::BackTrace(trace) => {
                    let mut raw_bytes = conn
                        .hget::<_, _, Vec<u8>>(&job_key.as_str(), "stackTrace")
                        .await
                        .unwrap_or_default();

                    let mut previous = if raw_bytes.is_empty() {
                        Vec::new()
                    } else {
                        simd_json::from_slice::<Vec<Trace>>(&mut raw_bytes).unwrap_or_default()
                    };

                    previous.push(trace);
                    simd_json::to_string(&previous).map(move |result| (name, result))?
                }

                _ => simd_json::to_string(&field).map(move |result| (name, result))?,
            };

            next_fields.push(pair);
        }
        let _: () = conn.hset_multiple(job_key.as_str(), &next_fields).await?;
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
            let val = conn.hincr(key_string.as_str(), field_key, delta).await?;
            return Ok(val);
        }

        let value: u64 = conn.incr(key_string.as_str(), delta).await?;
        Ok(value)
    }
    async fn get_counter(&self, key: CollectionSuffix, hash_key: Option<&str>) -> Option<u64> {
        let mut conn = self.get_connection().await.ok()?;
        let key = key.to_collection_name(&self.prefix, &self.name);
        if let Some(field) = hash_key {
            // we getting the processing counter;
            return conn.hget(key.as_str(), field).await.ok();
        }

        conn.get(key.as_str()).await.ok()
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
            QueueEventMode::PubSub => pipeline.publish(events_stream_key.as_str(), event),
            QueueEventMode::Stream => {
                let mut items = crate::utils::serialize_into_pairs(&event);
                // remove the id field
                items.retain(|(key, _)| key != "id");
                items.retain(|(_, val)| !val.contains("null"));
                pipeline.xadd(events_stream_key.as_str(), "*", &items)
            }
        };
        let _: () = pipeline.query_async(&mut conn).await?;
        Ok(())
    }
    async fn get_job_ids_in_state(
        &self,
        state: JobState,
        start: Option<usize>,
        end: Option<usize>,
    ) -> KioResult<VecDeque<u64>> {
        let mut conn = self.get_connection().await?;
        let start = start.unwrap_or_default();
        let col: CollectionSuffix = state.into();
        let key = col.to_collection_name(&self.prefix, &self.name);
        match state {
            JobState::Prioritized | JobState::Completed | JobState::Failed | JobState::Delayed => {
                let list_len: usize = conn.zcard(key.as_str()).await?;
                if list_len > 0 {
                    let end = end.map_or(list_len, |value| value + 1);

                    let items: Vec<u64> = conn
                        .zrange(key.as_str(), start.cast_signed(), end.cast_signed())
                        .await?;
                    return Ok(VecDeque::from_iter(items));
                }
            }
            JobState::Stalled => {
                let set: Vec<u64> = conn.smembers(key.as_str()).await?;
                if !set.is_empty() {
                    let set = VecDeque::from(set);
                    let end = end.unwrap_or(set.len());

                    return Ok(set.range(start..=end).copied().collect());
                }
            }

            JobState::Active | JobState::Wait | JobState::Paused => {
                let list_len: usize = conn.llen(key.as_str()).await?;
                if list_len > 0 {
                    let end = end.map_or(list_len, |value| value + 1);
                    let items: Vec<u64> = conn
                        .lrange(key.as_str(), start.cast_signed(), end.cast_signed())
                        .await?;
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
            conn.zpopmin(key.as_str(), 1)
        } else {
            conn.zpopmax(key.as_str(), 1)
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

        conn.exists(job_key.as_str()).await.unwrap_or_default()
    }

    async fn clear_collections(&self) -> KioResult<()> {
        let mut conn = self.connection.conn_pool.get().await?;
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
            pipeline.del(key.as_str());
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
                pipeline.zrem(key.as_str(), item);
            }
            CollectionSuffix::Stalled => {
                pipeline.srem(key.as_str(), item);
            }
            CollectionSuffix::Active | CollectionSuffix::Wait | CollectionSuffix::Paused => {
                pipeline.lrem(key.as_str(), 1, item);
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
                pipeline.zadd(key.as_str(), item, score);
            }
            CollectionSuffix::Stalled => {
                pipeline.sadd(key.as_str(), item);
            }

            CollectionSuffix::Active | CollectionSuffix::Wait | CollectionSuffix::Paused => {
                if append {
                    pipeline.lpush(key.as_str(), item);
                } else {
                    pipeline.rpush(key.as_str(), item);
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
        let _: () = conn.expire(key.as_str(), secs).await?;
        Ok(())
    }
    async fn clear_jobs(&self, last_id: u64) -> KioResult<()> {
        let mut conn = self.connection.conn_pool.get().await?;
        let mut pipeline = redis::pipe();
        pipeline.atomic();

        (1..=last_id).for_each(|id| {
            let job_key = CollectionSuffix::Job(id).to_collection_name(&self.prefix, &self.name);
            pipeline.del(job_key.as_str());
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
        let mut conn = self.connection.conn_pool.get().await?;
        let src = if pause { &wait_key } else { &paused_key };
        let dst = if pause { &paused_key } else { &wait_key };
        let mut pipeline = redis::pipe();
        pipeline.atomic();
        if conn
            .exists::<_, bool>(src.as_str())
            .await
            .unwrap_or_default()
        {
            pipeline.rename(src.as_str(), dst.as_str());
        }
        if pause {
            pipeline.hset(meta_key.as_str(), CollectionSuffix::Paused, 1)
        } else {
            pipeline.hdel(meta_key.as_str(), CollectionSuffix::Paused)
        };
        let _: redis::Value = pipeline.query_async(&mut conn).await?;
        Ok(())
    }

    async fn fetch_jobs(&self, ids: &[u64]) -> KioResult<VecDeque<Job<D, R, P>>> {
        if ids.is_empty() {
            return Ok(VecDeque::new());
        }
        let mut conn = self.get_connection().await?;
        let mut pipeline = redis::pipe();
        pipeline.atomic();
        for id in ids {
            let key = CollectionSuffix::Job(*id).to_collection_name(&self.prefix, &self.name);
            pipeline.hgetall(key.as_str());
        }

        let list: Vec<Job<D, R, P>> = pipeline.query_async(&mut conn).await?;

        Ok(VecDeque::from_iter(list))
    }
}
/// Represents a parsed Redis server version with major, minor, and patch components.
///
/// Supports ordering and equality comparisons, allowing you to check
/// minimum version requirements before using version-specific Redis features.
///
/// # Examples
///
/// ```rust,ignore
/// use kiomq::{RedisVersion};
/// let raw = get_redis_info();
/// let version = RedisVersion::parse(&raw).expect("Failed to parse Redis version");
///
/// if version >= (RedisVersion { major: 7, minor: 2, patch: 0 }) {
///     // use Redis 7.2+ features
/// }
/// ```
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
pub struct RedisVersion {
    /// Major version number (e.g. `7` in `7.2.4`)
    pub major: u32,
    /// Minor version number (e.g. `2` in `7.2.4`)
    pub minor: u32,
    /// Patch version number (e.g. `4` in `7.2.4`), defaults to `0` if absent
    pub patch: u32,
}

impl RedisVersion {
    /// Parses a `RedisVersion` from the raw output of the `INFO server` command.
    ///
    /// Looks for the `redis_version:` field in the INFO response and splits
    /// it into major, minor, and patch components.
    ///
    /// # Arguments
    ///
    /// * `raw` - The raw string output from `INFO server`
    ///
    /// # Returns
    ///
    /// `Some(RedisVersion)` if the version field was found and parsed successfully,
    /// `None` otherwise.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kiomq::{RedisVersion};
    /// let raw = "redis_version:7.2.4\r\nos:Linux\r\n";
    /// let version = RedisVersion::parse(raw).unwrap();
    /// assert_eq!(version.major, 7);
    /// assert_eq!(version.minor, 2);
    /// assert_eq!(version.patch, 4);
    /// ```
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        let version_str = raw
            .lines()
            .find(|line| line.starts_with("redis_version:"))?
            .split_once(':')?
            .1
            .trim();

        let mut parts = version_str.splitn(3, '.');
        Some(Self {
            major: parts.next()?.parse().ok()?,
            minor: parts.next()?.parse().ok()?,
            patch: parts.next().unwrap_or("0").parse().ok()?,
        })
    }
    /// Checks if this version is greater than or equal to a version string.
    ///
    /// # Arguments
    ///
    /// * `version` - A version string in the format `"major.minor.patch"` or `"major.minor"`
    ///
    /// # Returns
    ///
    /// `Some(bool)` if the string was parsed successfully, `None` if the format is invalid.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kiomq::{RedisVersion};
    /// let version = RedisVersion { major: 7, minor: 2, patch: 4 };
    ///
    /// assert_eq!(version.is_at_least("7.2"), Some(true));
    /// assert_eq!(version.is_at_least("7.2.4"), Some(true));
    /// assert_eq!(version.is_at_least("8.0"), Some(false));
    /// ```
    #[must_use]
    pub fn is_at_least(&self, version: &str) -> Option<bool> {
        let mut parts = version.splitn(3, '.');
        let other = Self {
            major: parts.next()?.parse().ok()?,
            minor: parts.next()?.parse().ok()?,
            patch: parts.next().unwrap_or("0").parse().ok()?,
        };
        Some(self >= &other)
    }
}
/// A shared Redis connection wrapper that provides both a pooled connection manager
/// and a direct Redis client for flexible interaction with a Redis instance.
///
/// `SharedRedis` is designed to be cloned and shared across async tasks, as the
/// connection pool is wrapped in an [`Arc`].
///
/// # Fields
///
/// * `conn_pool` - A thread-safe, reference-counted connection pool for efficient
///   reuse of Redis connections across concurrent tasks.
/// * `redis_client` - A direct Redis client, used for operations that require a
///   dedicated connection outside of the pool (e.g. pub/sub, blocking commands).
#[derive(Debug, Clone)]
pub struct SharedRedis {
    /// An  connection pool managed by `deadpool-redis`.
    #[debug(skip)]
    pub conn_pool: Arc<Pool>,
    pub(crate) redis_client: redis::Client,
}
impl SharedRedis {
    /// Creates a new [`SharedRedis`] instance from the provided configuration.
    ///
    /// Initialises both the connection pool (backed by Tokio) and a standalone
    /// Redis client using the connection info in `cfg`.
    ///
    /// # Arguments
    ///
    /// * `cfg` - A reference to a [`Config`] deadpool config.
    ///
    /// # Errors
    ///
    /// Returns a [`KioResult`] error if:
    /// - The connection pool cannot be created (e.g. invalid config or runtime error).
    /// - The Redis client fails to open (e.g. malformed connection URL).
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use kiomq::{SharedRedis};
    /// let mut cfg = deadpool_redis::Config::from_url("redis://127.0.0.1/");
    /// let redis = SharedRedis::create(&cfg);
    /// ```
    pub fn create(cfg: &Config) -> KioResult<Self> {
        let pool = cfg.create_pool(Some(Runtime::Tokio1))?;
        let conn_pool = Arc::new(pool);
        let connection_info = cfg.connection.clone().unwrap_or_default();
        let redis_client = redis::Client::open(connection_info)?;

        Ok(Self {
            conn_pool,
            redis_client,
        })
    }
}

enum MetricsType {
    Process(Box<ProcessMetrics>),
    Worker(WorkerMetrics),
}
impl MetricsType {
    const fn get_collection_suffix(&self) -> CollectionSuffix {
        match &self {
            Self::Process(_) => CollectionSuffix::ProcessMetrics,
            Self::Worker(_) => CollectionSuffix::WorkerMetrics,
        }
    }
}
