use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicBool, Arc},
};

use crate::{
    events::QueueStreamEvent, worker::WorkerMetrics, CollectionSuffix, EventEmitter, Job, JobField,
    JobOptions, JobState, JobToken, KioResult, ProcessedResult, QueueEventMode, QueueMetrics,
    QueueOpts, Trace,
};
use std::collections::VecDeque;
mod inmemory_store;
#[cfg(feature = "rocksdb-store")]
mod rocksdb_store;
pub use inmemory_store::InMemoryStore;
#[cfg(feature = "redis-store")]
mod redis_store;
#[cfg(feature = "redis-store")]
pub use redis_store::RedisStore;
#[cfg(feature = "rocksdb-store")]
pub use rocksdb_store::{ivec_to_number, temporary_rocks_db, RocksDbStore};
use tokio::{sync::Notify, task::JoinHandle};
enum Lock {
    Token(JobToken),
    StallCheck,
}
use crate::events::Emitter;
use arc_swap::ArcSwapOption;
type SharedEmitter<R, P> = ArcSwapOption<Emitter<R, P>>;
/// Backend storage interface implemented by all `KioMQ` store backends.
///
/// This trait abstracts over Redis, `RocksDB`, and the in-memory store so that
/// [`Queue`](crate::Queue) and [`Worker`](crate::Worker) are generic over the
/// persistence layer. Implementors are responsible for job serialisation, TTL
/// management, event publishing, and atomic state transitions.
///
/// You typically do not implement this trait yourself; use one of the provided
/// implementations: [`InMemoryStore`], `RedisStore`, or `RocksDbStore`.
#[allow(clippy::too_many_arguments)]
#[async_trait::async_trait]
pub trait Store<D, R, P> {
    /// Returns the name of the queue this store was created for.
    fn queue_name(&self) -> &str;
    /// Returns the key prefix used to namespace all store collections.
    fn queue_prefix(&self) -> &str;
    /// Fetches a batch of jobs by their IDs.
    ///
    /// # Errors
    ///
    /// Returns [`KioResult`] error if any job lookup fails.
    fn fetch_jobs(&self, ids: &[u64]) -> KioResult<VecDeque<Job<D, R, P>>>;
    /// Removes entries whose TTL has elapsed (no-op for backends without TTL support).
    async fn purge_expired(&self) {}
    /// Returns per-worker metric snapshots stored with a TTL.
    ///
    /// # Errors
    ///
    /// Returns [`KioResult`] error if the store lookup fails.
    fn fetch_worker_metrics(&self) -> KioResult<BTreeMap<uuid::Uuid, WorkerMetrics>>;
    /// Persists a worker's metrics with a time-to-live of `ttl_ms` milliseconds.
    async fn store_worker_metrics(&self, metrics: WorkerMetrics, ttl_ms: u64) -> KioResult<()>;
    /// Returns `true` if a metadata field with the given name exists.
    async fn metadata_field_exists(&self, field: &str) -> KioResult<bool>;
    /// Returns `true` if `item` is present in the collection `col`.
    async fn exists_in(&self, col: CollectionSuffix, item: u64) -> KioResult<bool>;
    /// Persists the event-delivery mode to the store.
    async fn set_event_mode(&self, event_mode: QueueEventMode) -> KioResult<()>;
    /// Returns the IDs of jobs in the given `state`, optionally paginated.
    ///
    /// # Errors
    ///
    /// Returns [`KioResult`] error if the store lookup fails.
    fn get_job_ids_in_state(
        &self,
        state: JobState,
        start: Option<usize>,
        end: Option<usize>,
    ) -> KioResult<VecDeque<u64>>;
    /// Blocks until an event arrives on the store's event channel, then
    /// dispatches it to the given `emitter`.
    async fn listen_to_events(
        &self,
        event_mode: QueueEventMode,
        block_interval: Option<u64>,
        emitter: &EventEmitter<R, P>,
        metrics: &QueueMetrics,
    ) -> KioResult<()>;
    /// Spawns a long-running background task that forwards store events to the
    /// emitter and returns its join handle.
    async fn create_stream_listener(
        &self,
        emitter: EventEmitter<R, P>,
        notifier: Arc<Notify>,
        metrics: Arc<QueueMetrics>,
        pause_workers: Arc<AtomicBool>,
        event_mode: QueueEventMode,
    ) -> KioResult<JoinHandle<KioResult<()>>>;
    /// Enqueues multiple jobs without returning the created job records.
    async fn add_bulk_only(
        &self,
        iter: Box<dyn Iterator<Item = (String, Option<JobOptions>, D)> + Send>,
        queue_opts: QueueOpts,
        event_mode: QueueEventMode,
        is_paused: bool,
    ) -> KioResult<()>;
    /// Enqueues multiple jobs and returns the created job records.
    async fn add_bulk(
        &self,
        iter: Box<dyn Iterator<Item = (String, Option<JobOptions>, D)> + Send>,
        queue_opts: QueueOpts,
        event_mode: QueueEventMode,
        is_paused: bool,
    ) -> KioResult<Vec<Job<D, R, P>>>;
    /// Returns the IDs of delayed jobs whose score falls in `[start, stop]`,
    /// together with the IDs of any that have already exceeded their deadline.
    async fn get_delayed_at(&self, start: i64, stop: i64) -> KioResult<(Vec<u64>, Vec<u64>)>;
    /// Pops the element with the minimum or maximum score from the sorted set `col`.
    async fn pop_set(&self, col: CollectionSuffix, min: bool) -> KioResult<Vec<(u64, u64)>>;
    /// Sets an expiry of `secs` seconds on the collection `col`.
    async fn expire(&self, col: CollectionSuffix, secs: i64) -> KioResult<()>;
    /// Returns a fresh [`QueueMetrics`] snapshot by reading all state counters.
    async fn get_metrics(&self) -> KioResult<QueueMetrics>;
    /// Returns the job with the given `id`, or `None` if it does not exist.
    async fn get_job(&self, id: u64) -> Option<Job<D, R, P>>;
    /// Returns the lock token currently held on `id`, or `None` if unlocked.
    async fn get_token(&self, id: u64) -> Option<JobToken>;
    /// Returns the current state of the job with the given `id`.
    async fn get_state(&self, id: u64) -> Option<JobState>;
    /// Writes a progress update for `job` to the store in place.
    ///
    /// # Errors
    ///
    /// Returns [`KioResult`] error if the store write fails.
    fn update_job_progress(&self, job: &mut Job<D, R, P>, value: P) -> KioResult<()>;
    /// Inserts `item` into the sorted collection `col` with the given optional
    /// `score`. Pass `append = true` to push to the tail of a list.
    async fn add_item(
        &self,
        col: CollectionSuffix,
        item: u64,
        score: Option<i64>,
        append: bool,
    ) -> KioResult<()>;
    /// Atomically pops the last element of `src` and pushes it to the front
    /// of `dst`, returning the moved ID.
    async fn pop_back_push_front(
        &self,
        src: CollectionSuffix,
        dst: CollectionSuffix,
    ) -> Option<u64>;

    /// Sets (or refreshes) the lock on `col` with the given `token` and TTL.
    async fn set_lock(
        &self,
        col: CollectionSuffix,
        token: Option<JobToken>,
        lock_duration: u64,
    ) -> KioResult<()>;
    /// Writes multiple field updates to the job with the given `job_id`.
    async fn set_fields(&self, job_id: u64, fields: Vec<JobField<R>>) -> KioResult<()>;
    /// Increments (or decrements) a numeric counter stored under `key` by
    /// `delta`, optionally scoped to a hash field `hash_key`.
    async fn incr(
        &self,
        key: CollectionSuffix,
        delta: i64,
        hash_key: Option<&str>,
    ) -> KioResult<u64>;
    /// Returns the current value of a numeric counter, or `None` if absent.
    async fn get_counter(&self, key: CollectionSuffix, hash_key: Option<&str>) -> Option<u64>;
    /// Publishes an event to the store's event channel.
    async fn publish_event(
        &self,
        event_mode: QueueEventMode,
        event: QueueStreamEvent<R, P>,
    ) -> KioResult<()>;

    /// Returns `true` if a job record for `id` exists in the store.
    async fn job_exists(&self, id: u64) -> bool;
    /// Removes `item` from the sorted/list collection `col`.
    async fn remove_item(&self, col: CollectionSuffix, item: u64) -> KioResult<()>;
    /// Deletes the entire collection identified by `key`.
    ///
    /// # Errors
    ///
    /// Returns [`KioResult`] error if the store operation fails.
    fn remove(&self, key: CollectionSuffix) -> KioResult<()>;
    /// Drops all auxiliary collections (active, waiting, delayed, etc.) but
    /// leaves job records intact.
    async fn clear_collections(&self) -> KioResult<()>;
    /// Deletes all job records up to and including `last_id`.
    async fn clear_jobs(&self, last_id: u64) -> KioResult<()>;
    /// Pauses or resumes the queue in the store and emits the corresponding
    /// event.
    async fn pause(&self, pause: bool, event_mode: QueueEventMode) -> KioResult<()>;
}
