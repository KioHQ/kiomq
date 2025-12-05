use std::{
    ops::RangeBounds,
    sync::{atomic::AtomicBool, Arc},
};

use crate::{
    events::QueueStreamEvent, BackOffJobOptions, CollectionSuffix, EventEmitter, Job, JobField,
    JobMetrics, JobOptions, JobState, JobToken, KioResult, ProcessedResult, QueueEventMode,
    QueueOpts, RemoveOnCompletionOrFailure, Trace, WorkerOpts,
};
use std::collections::VecDeque;
mod inmemory_store;
#[cfg(feature = "rocksdb-store")]
mod rocksdb_store;
use async_trait::async_trait;
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
type SharedEmitter<D, R, P> = ArcSwapOption<Emitter<D, R, P>>;
#[allow(clippy::too_many_arguments)]
#[async_trait::async_trait]
pub trait Store<D, R, P> {
    fn queue_name(&self) -> &str;
    fn queue_prefix(&self) -> &str;
    fn fetch_jobs(&self, ids: &[u64]) -> KioResult<VecDeque<Job<D, R, P>>>;
    async fn purge_expired(&self) {}

    async fn metadata_field_exists(&self, field: &str) -> KioResult<bool>;
    async fn exists_in(&self, col: CollectionSuffix, item: u64) -> KioResult<bool>;
    async fn set_event_mode(&self, event_mode: QueueEventMode) -> KioResult<()>;
    fn get_job_ids_in_state(
        &self,
        state: JobState,
        start: Option<usize>,
        end: Option<usize>,
    ) -> KioResult<VecDeque<u64>>;
    async fn listen_to_events(
        &self,
        event_mode: QueueEventMode,
        block_interval: Option<u64>,
        emitter: &EventEmitter<D, R, P>,
        metrics: &JobMetrics,
    ) -> KioResult<()>;
    async fn create_stream_listener(
        &self,
        emitter: EventEmitter<D, R, P>,
        notifier: Arc<Notify>,
        metrics: Arc<JobMetrics>,
        pause_workers: Arc<AtomicBool>,
        event_mode: QueueEventMode,
    ) -> KioResult<JoinHandle<KioResult<()>>>;
    async fn add_bulk_only(
        &self,
        iter: Box<dyn Iterator<Item = (String, Option<JobOptions>, D)> + Send>,
        queue_opts: QueueOpts,
        event_mode: QueueEventMode,
        is_paused: bool,
    ) -> KioResult<()>;
    async fn add_bulk(
        &self,
        iter: Box<dyn Iterator<Item = (String, Option<JobOptions>, D)> + Send>,
        queue_opts: QueueOpts,
        event_mode: QueueEventMode,
        is_paused: bool,
    ) -> KioResult<Vec<Job<D, R, P>>>;
    async fn get_delayed_at(&self, start: i64, stop: i64) -> KioResult<(Vec<u64>, Vec<u64>)>;
    async fn pop_set(&self, col: CollectionSuffix, min: bool) -> KioResult<Vec<(u64, u64)>>;
    async fn expire(&self, col: CollectionSuffix, secs: i64) -> KioResult<()>;
    async fn get_metrics(&self) -> KioResult<JobMetrics>;
    async fn get_job(&self, id: u64) -> Option<Job<D, R, P>>;
    async fn get_token(&self, id: u64) -> Option<JobToken>;
    async fn get_state(&self, id: u64) -> Option<JobState>;
    fn update_job_progress(&self, job: &mut Job<D, R, P>, value: P) -> KioResult<()>;
    async fn add_item(
        &self,
        col: CollectionSuffix,
        item: u64,
        score: Option<i64>,
        append: bool,
    ) -> KioResult<()>;
    async fn pop_back_push_front(
        &self,
        src: CollectionSuffix,
        dst: CollectionSuffix,
    ) -> Option<u64>;

    //async fn del
    async fn set_lock(
        &self,
        col: CollectionSuffix,
        token: Option<JobToken>,
        lock_duration: u64,
    ) -> KioResult<()>;
    async fn set_fields(&self, job_id: u64, fields: Vec<JobField<R>>) -> KioResult<()>;
    async fn incr(
        &self,
        key: CollectionSuffix,
        delta: i64,
        hash_key: Option<&str>,
    ) -> KioResult<u64>;
    async fn get_counter(&self, key: CollectionSuffix, hash_key: Option<&str>) -> Option<u64>;
    async fn publish_event(
        &self,
        event_mode: QueueEventMode,
        event: QueueStreamEvent<R, P>,
    ) -> KioResult<()>;

    async fn job_exists(&self, id: u64) -> bool;
    async fn remove_item(&self, col: CollectionSuffix, item: u64) -> KioResult<()>;
    fn remove(&self, key: CollectionSuffix) -> KioResult<()>;
    async fn clear_collections(&self) -> KioResult<()>;
    async fn clear_jobs(&self, last_id: u64) -> KioResult<()>;
    async fn pause(&self, pause: bool, event_mode: QueueEventMode) -> KioResult<()>;
}
