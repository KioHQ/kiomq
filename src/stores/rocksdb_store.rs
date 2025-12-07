use super::*;
use crate::events::StreamEventId;
use crate::timers::TimedMap;
use crate::utils::{
    calculate_next_priority_score, create_listener_handle, pause_or_resume_workers,
    process_each_event, resume_helper, update_job_opts,
};
use crate::worker::MIN_DELAY_MS_LIMIT;
use crate::{
    job, CollectionSuffix, Dt, FailedDetails, JobError, KioError, KioResult, QueueError,
    QueueEventMode,
};
use chrono::Utc;
use crossbeam_queue::SegQueue;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use derive_more::Debug;
use futures::FutureExt;
use rocksdb::{
    BoundColumnFamily, ColumnFamily, DBWithThreadMode, MultiThreaded, OptimisticTransactionDB,
    Options, ReadOptions, WriteBatch, WriteBatchWithTransaction, WriteOptions,
};
use serde::Deserialize;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::convert::TryInto;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use xutex::Mutex;
#[track_caller]
pub fn ivec_to_number<T: AsRef<[u8]>>(mut src: T) -> i64 {
    let array: [u8; 8] = src.as_ref().try_into().ok().unwrap_or_default();
    i64::from_be_bytes(array)
}
fn increment(old: Option<&[u8]>, delta: i64) -> Option<Vec<u8>> {
    let mut number = match old {
        Some(bytes) => ivec_to_number(bytes) + delta,
        None => 0,
    };
    Some(number.to_be_bytes().to_vec())
}

fn leak_arc<T>(arc: Arc<T>) -> &'static T {
    let leaked: &'static Arc<T> = Box::leak(Box::new(arc));
    leaked
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Default)]
struct Meta {
    processing: u64,
    paused: bool,
}
use atomig::{Atom, Atomic};
pub type RocksDb = OptimisticTransactionDB<MultiThreaded>;
#[derive(Debug, Clone)]
pub struct RocksDbStore<D, R, P> {
    db: Arc<RocksDb>,
    /// A column family for all collections (waiting, active, delayed, failed, Priorized & completed) and
    /// also their counters (id, priority-counter)
    pub main_tree: String,
    // A separate column family for only jobs
    #[debug(skip)]
    pub jobs: String,
    #[debug(skip)]
    locks: Arc<TimedMap<u64, Lock>>,
    #[debug(skip)]
    events: Arc<SharedEmitter<R, P>>,
    pause_workers: Arc<ArcSwapOption<AtomicBool>>,
    is_inital: Arc<AtomicBool>,
    notifier: Arc<ArcSwapOption<Notify>>,
    #[debug(skip)]
    job_batch: Arc<Mutex<WriteBatchWithTransaction<true>>>,
    /// A mode for publishing events, this store only supports PubSub using an channel,
    event_mode: QueueEventMode, // Also stream, no pub
    pub prefix: String,
    pub name: String,
    _data: PhantomData<(D, R, P)>,
}
fn setup_type<T: Serialize + Default>(
    key: CollectionSuffix,
    db: &RocksDb,
    tree: &Arc<BoundColumnFamily<'_>>,
) -> KioResult<()> {
    let col = T::default();
    let value = simd_json::to_vec(&col)?;
    db.put_cf(tree, key.to_bytes(), value)?;
    Ok(())
}
fn get_collection<T: DeserializeOwned>(
    key: CollectionSuffix,
    db: &RocksDb,
    tree: &Arc<BoundColumnFamily<'_>>,
) -> Option<T> {
    db.get_cf(tree, key.to_bytes())
        .ok()
        .flatten()
        .and_then(|mut bytes| simd_json::from_slice(&mut bytes).ok())
}
impl<D, R, P> RocksDbStore<D, R, P> {
    pub fn new(prefix: Option<&str>, name: &str, db: Arc<RocksDb>) -> KioResult<Self> {
        let prefix = prefix.unwrap_or("kio").to_lowercase();
        let queue_name = format!("{prefix}:{name}");
        let jobs_collection = format!("{queue_name}:jobs");
        let event_mode = QueueEventMode::PubSub;
        let locks = Arc::default();
        let events = Arc::default();
        let notifier = Arc::default();
        let pause_workers = Arc::default();
        let is_inital = Arc::default();

        let store = Self {
            pause_workers,
            notifier,
            is_inital,
            locks,
            events,
            job_batch: Arc::new(Mutex::new(WriteBatchWithTransaction::default())),
            jobs: jobs_collection,
            main_tree: queue_name,
            prefix,
            name: name.to_lowercase(),
            db,
            event_mode,
            _data: PhantomData,
        };
        store.initial_collections()?;
        Ok(store)
    }
    fn initial_collections(&self) -> KioResult<()> {
        let opts = Options::default();
        self.db.create_cf(&self.main_tree, &opts)?;
        self.db.create_cf(&self.jobs, &opts)?;
        let main_tree = self.db.cf_handle(&self.main_tree).unwrap();
        let jobs = self.db.cf_handle(&self.jobs).unwrap();
        setup_type::<VecDeque<u64>>(CollectionSuffix::Wait, &self.db, &main_tree)?;
        setup_type::<VecDeque<u64>>(CollectionSuffix::Active, &self.db, &main_tree)?;
        setup_type::<BTreeSet<u64>>(CollectionSuffix::Stalled, &self.db, &main_tree)?;
        setup_type::<BTreeMap<u64, u64>>(CollectionSuffix::Completed, &self.db, &main_tree)?;
        setup_type::<BTreeMap<u64, u64>>(CollectionSuffix::Prioritized, &self.db, &main_tree)?;
        setup_type::<BTreeMap<u64, u64>>(CollectionSuffix::Failed, &self.db, &main_tree)?;
        setup_type::<BTreeMap<u64, u64>>(CollectionSuffix::Delayed, &self.db, &main_tree)?;
        setup_type::<u64>(CollectionSuffix::Id, &self.db, &main_tree)?;
        setup_type::<u64>(CollectionSuffix::PriorityCounter, &self.db, &main_tree)?;
        Ok(())
    }
    fn get_meta(&self) -> Option<Meta> {
        let cf = self.db.cf_handle(&self.main_tree)?;
        get_collection(CollectionSuffix::Meta, &self.db, &cf)
    }
    fn item_exists_in_list(&self, col: CollectionSuffix, item: u64) -> Option<bool> {
        let key = col.to_bytes();
        if matches!(
            col,
            CollectionSuffix::Active | CollectionSuffix::Wait | CollectionSuffix::Paused
        ) {
            let cf = self.db.cf_handle(&self.main_tree)?;
            return self
                .db
                .get_cf(&cf, key)
                .ok()
                .flatten()
                .and_then(|mut prev| {
                    let mut queue: VecDeque<u64> = simd_json::from_slice(&mut prev).ok()?;
                    Some(queue.contains(&item))
                });
        }
        None
    }
    async fn purge_expired(&self) {
        if self.locks.len_expired() > 0 {
            self.locks.purge_expired().await;
        }
    }
    async fn put<V: AsRef<[u8]>>(&self, col: CollectionSuffix, value: V) -> KioResult<()> {
        let collection = col;
        let key = col.to_bytes();
        let main_cf = self
            .db
            .cf_handle(&self.main_tree)
            .expect("failed to get handle");
        let jobs_cf = self.db.cf_handle(&self.jobs).unwrap();
        if let CollectionSuffix::Job(_) = collection {
            self.db.put_cf(&jobs_cf, key, value)?;
        } else {
            self.db.put_cf(&main_cf, key, value)?;
        }
        Ok(())
    }
    fn fetch<T: DeserializeOwned>(&self, key: CollectionSuffix) -> Option<T> {
        let main_cf = self.db.cf_handle(&self.main_tree)?;
        let jobs_cf = self.db.cf_handle(&self.jobs)?;
        let cf = if let CollectionSuffix::Job(_) = key {
            &jobs_cf
        } else {
            &main_cf
        };
        let key = key.to_bytes();
        self.db
            .get_cf(cf, key)
            .ok()
            .flatten()
            .and_then(|ref mut bytes| simd_json::from_slice(bytes).ok())
    }
    async fn commit(&self) -> KioResult<()> {
        let mut opts = WriteOptions::default();
        opts.set_sync(false);
        opts.disable_wal(true);
        let job_batch = std::mem::take(&mut *self.job_batch.lock());
        self.db.write_opt(job_batch, &opts)?;
        Ok(())
    }
    async fn submit_changes(
        &self,
        is_paused: bool,
        (mut list, mut priorized, mut delayed): (
            VecDeque<u64>,
            BTreeMap<u64, u64>,
            BTreeMap<u64, u64>,
        ),
    ) -> KioResult<()> {
        let next_list = if is_paused {
            CollectionSuffix::Paused
        } else {
            CollectionSuffix::Wait
        };
        let delayed_col = CollectionSuffix::Delayed;
        let priorized_col = CollectionSuffix::Prioritized;
        let mut queue = self.fetch::<VecDeque<u64>>(next_list).unwrap_or_default();
        let mut current_delayed = self
            .fetch::<BTreeMap<u64, u64>>(CollectionSuffix::Delayed)
            .unwrap_or_default();
        let mut current_priorized = self
            .fetch::<BTreeMap<u64, u64>>(CollectionSuffix::Prioritized)
            .unwrap_or_default();
        current_priorized.extend(priorized);
        current_delayed.extend(delayed);
        list.append(&mut queue);
        let cf = self
            .db
            .cf_handle(&self.main_tree)
            .ok_or(std::io::Error::other("failed here"))?;
        let tx = self.db.transaction();
        let list = simd_json::to_vec(&list)?;
        let delayed = simd_json::to_vec(&current_delayed)?;
        let priorized = simd_json::to_vec(&current_priorized)?;
        tx.put_cf(&cf, next_list.to_bytes(), list);
        tx.put_cf(&cf, delayed_col.to_bytes(), delayed);
        tx.put_cf(&cf, priorized_col.to_bytes(), priorized);
        self.commit().await?;
        tx.commit()?;

        Ok(())
    }
}
impl<D, R, P> RocksDbStore<D, R, P>
where
    D: Clone + Serialize + DeserializeOwned + Send + 'static + Sync,
    R: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
    P: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
{
    async fn prepare_for_insert(
        &self,
        id: u64,
        pc: u64,
        is_paused: bool,
        job: &mut Job<D, R, P>,
        (opts, list, priorized, delayed): (
            JobOptions,
            &mut VecDeque<u64>,
            &mut BTreeMap<u64, u64>,
            &mut BTreeMap<u64, u64>,
        ),
        name: &str,
    ) -> KioResult<()> {
        let JobOptions {
            priority,
            ref delay,
            id: _,
            attempts,
            remove_on_fail,
            remove_on_complete,
            ref backoff,
            repeat: _,
        } = opts;
        let dt = Utc::now();
        let expected_dt_ts = delay.next_occurrance_timestamp_ms();
        let delay_dt = delay.clone();
        let delay = delay.as_diff_ms(dt) as u64;
        job.add_opts(opts);
        if delay > 0 && delay < MIN_DELAY_MS_LIMIT {
            return Err(QueueError::DelayBelowAllowedLimit {
                limit_ms: MIN_DELAY_MS_LIMIT,
                current_ms: delay,
            }
            .into());
        };
        let mut event = JobState::Wait;
        let waiting_or_paused = if !is_paused {
            CollectionSuffix::Wait
        } else {
            event = JobState::Paused;
            CollectionSuffix::Paused
        };

        let to_delay = delay > 0;
        let to_priorize = priority > 0 && !to_delay;
        if to_delay {
            if let Some(expected_active_time) = expected_dt_ts {
                delayed.insert(expected_active_time as u64, id);
                job.state = JobState::Delayed;
                event = JobState::Delayed;
            }
        } else if to_priorize {
            let score = calculate_next_priority_score(priority, pc) as i64;
            priorized.insert(score as u64, id);
            job.state = JobState::Prioritized;
            event = JobState::Prioritized;
        } else {
            list.push_front(id);
        }
        job.id = Some(id);
        let jobs_cf = self.db.cf_handle(&self.jobs).ok_or(JobError::JobNotFound)?;
        self.job_batch.lock().put_cf(
            &jobs_cf,
            CollectionSuffix::Job(id).to_bytes(),
            simd_json::to_vec(job)?,
        );
        let mut event = QueueStreamEvent::<R, P> {
            job_id: id,
            event,
            name: Some(name.to_owned()),
            ..Default::default()
        };
        if to_delay {
            event.delay = Some(delay)
        }
        if to_priorize {
            event.priority = Some(priority);
        }
        self.publish_event(self.event_mode, event).await?;
        Ok(())
    }
}
#[async_trait::async_trait]
impl<D, R, P> Store<D, R, P> for RocksDbStore<D, R, P>
where
    D: Clone + Serialize + DeserializeOwned + Send + 'static + Sync,
    R: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
    P: Clone + DeserializeOwned + Serialize + Send + 'static + Sync,
{
    fn queue_name(&self) -> &str {
        &self.name
    }
    fn queue_prefix(&self) -> &str {
        &self.prefix
    }
    async fn exists_in(&self, col: CollectionSuffix, item: u64) -> KioResult<bool> {
        let result = match col {
            CollectionSuffix::Active | CollectionSuffix::Wait | CollectionSuffix::Paused => {
                let mut queue = self.fetch::<VecDeque<u64>>(col).unwrap_or_default();
                queue.contains(&item)
            }
            CollectionSuffix::Completed
            | CollectionSuffix::Delayed
            | CollectionSuffix::Failed
            | CollectionSuffix::Prioritized => {
                let mut map = self.fetch::<BTreeMap<u64, u64>>(col).unwrap_or_default();
                map.values().any(|val| *val == item)
            }
            CollectionSuffix::Stalled => {
                let mut map = self.fetch::<BTreeSet<u64>>(col).unwrap_or_default();
                map.contains(&item)
            }
            CollectionSuffix::Job(id) => self.job_exists(id).await,
            CollectionSuffix::Lock(_) | CollectionSuffix::StalledCheck => {
                self.locks.inner.contains_key(&col.tag())
            }

            _ => false,
        };
        Ok(result)
    }
    async fn metadata_field_exists(&self, field: &str) -> KioResult<bool> {
        let key = CollectionSuffix::Meta.to_bytes();
        let cf = self
            .db
            .cf_handle(&self.main_tree)
            .expect("failed to get handle");
        let exists = self.db.key_may_exist_cf(&cf, key);
        Ok(exists)
    }
    async fn set_event_mode(&self, event_mode: QueueEventMode) -> KioResult<()> {
        // this store only supports Stream mode, so we can't change it,
        Ok(())
    }
    async fn listen_to_events(
        &self,
        event_mode: QueueEventMode,
        block_interval: Option<u64>,
        emitter: &EventEmitter<R, P>,
        metrics: &QueueMetrics,
    ) -> KioResult<()> {
        // we do nothing  here as  this method isn't called for this store
        // we can directly use the emitter to emit events without need for a channel
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
        self.events.store(Some(emitter.clone()));
        self.notifier.store(Some(notifier.clone()));
        self.pause_workers.store(Some(pause_workers.clone()));
        let task = tokio::spawn(async move { Ok(()) });
        Ok(task)
    }
    async fn add_bulk_only(
        &self,
        iter: Box<dyn Iterator<Item = (String, Option<JobOptions>, D)> + Send>,
        queue_opts: QueueOpts,
        event_mode: QueueEventMode,
        is_paused: bool,
    ) -> KioResult<()> {
        let (mut list, mut priorized, mut delayed) = Default::default();
        for (ref name, opts, data) in iter {
            let mut opts = opts.unwrap_or_default();
            update_job_opts(&queue_opts, &mut opts);
            let mut pc = 0;
            if opts.priority > 0 {
                pc = self
                    .incr(CollectionSuffix::PriorityCounter, 1, None)
                    .await?;
            }
            let queue_name = format!("{}:{}", &self.prefix, &self.name);
            let id = self.incr(CollectionSuffix::Id, 1, None).await?;
            let mut job = Job::<D, R, P>::new(name, Some(data), opts.id, Some(&queue_name));
            self.prepare_for_insert(
                id,
                pc,
                is_paused,
                &mut job,
                (opts, &mut list, &mut priorized, &mut delayed),
                name,
            )
            .await?;
        }
        self.submit_changes(is_paused, (list, priorized, delayed))
            .await
    }
    async fn add_bulk(
        &self,
        iter: Box<dyn Iterator<Item = (String, Option<JobOptions>, D)> + Send>,
        queue_opts: QueueOpts,
        event_mode: QueueEventMode,
        is_paused: bool,
    ) -> KioResult<Vec<Job<D, R, P>>> {
        let mut jobs = vec![];
        let (mut list, mut priorized, mut delayed) = Default::default();
        for (ref name, opts, data) in iter {
            let mut opts = opts.unwrap_or_default();
            update_job_opts(&queue_opts, &mut opts);
            let mut pc = 0;
            if opts.priority > 0 {
                pc = self
                    .incr(CollectionSuffix::PriorityCounter, 1, None)
                    .await?;
            }
            let queue_name = format!("{}:{}", &self.prefix, &self.name);
            let id = self.incr(CollectionSuffix::Id, 1, None).await?;
            let mut job = Job::<D, R, P>::new(name, Some(data), opts.id, Some(&queue_name));
            self.prepare_for_insert(
                id,
                pc,
                is_paused,
                &mut job,
                (opts, &mut list, &mut priorized, &mut delayed),
                name,
            )
            .await?;
            jobs.push(job);
        }
        self.submit_changes(is_paused, (list, priorized, delayed))
            .await?;
        Ok(jobs)
    }
    async fn get_delayed_at(&self, start: i64, stop: i64) -> KioResult<(Vec<u64>, Vec<u64>)> {
        let delayed_key = CollectionSuffix::Delayed.to_bytes();
        let delayed_col: Option<BTreeMap<u64, u64>> = self.fetch(CollectionSuffix::Delayed);
        if let Some(mut delayed) = delayed_col {
            let before = (start - 1) as u64;
            let end = stop as u64;
            let start = start as u64;

            let missed_iter = delayed.range(..before);
            let jobs_iter = delayed.range(start..end);
            let mut to_remove = vec![];

            let jobs = jobs_iter
                .map(|(key, val)| {
                    to_remove.push(key);
                    *val
                })
                .collect();
            let missed = missed_iter
                .map(|(key, value)| {
                    to_remove.push(key);
                    *value
                })
                .collect();
            let mut delayed = delayed.clone();
            for key in to_remove {
                delayed.remove(key);
            }

            self.put(CollectionSuffix::Delayed, simd_json::to_vec(&delayed)?)
                .await?;

            return Ok((jobs, missed));
        }
        Ok((vec![], vec![]))
    }
    async fn pop_set(&self, col: CollectionSuffix, min: bool) -> KioResult<Vec<(u64, u64)>> {
        if matches!(
            col,
            CollectionSuffix::Completed
                | CollectionSuffix::Delayed
                | CollectionSuffix::Failed
                | CollectionSuffix::Prioritized
        ) {
            if let Some(mut map) = self.fetch::<BTreeMap<u64, u64>>(col) {
                if let Some(pairs) = if min { map.pop_first() } else { map.pop_last() } {
                    self.put(col, simd_json::to_vec(&map)?).await?;
                    return Ok(vec![(pairs.1, pairs.0)]);
                }
            }
        }
        Ok(vec![])
    }
    async fn expire(&self, col: CollectionSuffix, secs: i64) -> KioResult<()> {
        //todo!()
        Ok(())
    }
    async fn get_metrics(&self) -> KioResult<QueueMetrics> {
        use std::sync::atomic::Ordering;
        let mut metrics = QueueMetrics::default();

        let cf = self
            .db
            .cf_handle(&self.main_tree)
            .expect("failed to get tree");
        let mut result = [
            CollectionSuffix::Id,
            CollectionSuffix::Stalled,
            CollectionSuffix::Active,
            CollectionSuffix::Completed,
            CollectionSuffix::Delayed,
            CollectionSuffix::Wait,
            CollectionSuffix::Meta,
            CollectionSuffix::Prioritized,
            CollectionSuffix::Paused,
            CollectionSuffix::Failed,
        ]
        .map(|key| self.db.get_cf(&cf, key.to_bytes()).ok().flatten());

        if let Some(last_id) = &mut result[0] {
            let length = ivec_to_number(last_id) as u64;
            metrics.last_id.store(length, Ordering::Relaxed);
        }
        if let Some(ref mut stalled) = &mut result[1] {
            let col: BTreeSet<u64> = simd_json::from_slice(stalled)?;
            metrics.stalled.store(col.len() as u64, Ordering::Relaxed);
        }

        if let Some(ref mut active) = &mut result[2] {
            let col: VecDeque<u64> = simd_json::from_slice(active)?;
            metrics.active.store(col.len() as u64, Ordering::Relaxed);
        }
        if let Some(ref mut completed) = &mut result[3] {
            let col: BTreeMap<u64, u64> = simd_json::from_slice(completed)?;
            metrics.completed.store(col.len() as u64, Ordering::Relaxed);
        }
        if let Some(ref mut delayed) = &mut result[4] {
            let col: BTreeMap<u64, u64> = simd_json::from_slice(delayed)?;
            metrics.delayed.store(col.len() as u64, Ordering::Relaxed);
        }
        if let Some(ref mut waiting) = &mut result[5] {
            let col: VecDeque<u64> = simd_json::from_slice(waiting)?;
            metrics.waiting.store(col.len() as u64, Ordering::Relaxed);
        }
        if let Some(ref mut meta) = &mut result[6] {
            let col: Meta = simd_json::from_slice(meta)?;

            metrics.processing.store(col.processing, Ordering::Relaxed);

            metrics.is_paused.store(col.paused, Ordering::Relaxed);
        }
        if let Some(ref mut priorized) = &mut result[7] {
            let col: BTreeMap<u64, u64> = simd_json::from_slice(priorized)?;
            metrics
                .prioritized
                .store(col.len() as u64, Ordering::Relaxed);
        }
        if let Some(ref mut failed) = &mut result[9] {
            let col: BTreeMap<u64, u64> = simd_json::from_slice(failed)?;
            metrics.failed.store(col.len() as u64, Ordering::Relaxed);
        }

        if let Some(ref mut paused) = &mut result[8] {
            let col: VecDeque<u64> = simd_json::from_slice(paused)?;
            metrics.paused.store(col.len() as u64, Ordering::Relaxed);
        }
        Ok(metrics)
    }
    async fn get_job(&self, id: u64) -> Option<Job<D, R, P>> {
        let key = CollectionSuffix::Job(id);
        self.fetch(key)
    }
    async fn get_token(&self, id: u64) -> Option<JobToken> {
        let key = CollectionSuffix::Lock(id);
        self.fetch(key)
    }
    async fn get_state(&self, id: u64) -> Option<JobState> {
        self.get_job(id).await.map(|job: Job<D, R, P>| job.state)
    }
    fn update_job_progress(&self, job: &mut Job<D, R, P>, value: P) -> KioResult<()> {
        let cf = self
            .db
            .cf_handle(&self.jobs)
            .ok_or(std::io::Error::other("failed to get handle"))?;
        let id = job.id.unwrap_or_default();
        let key = CollectionSuffix::Job(id).to_bytes();
        if let Some(ref mut bytes) = self.db.get_cf(&cf, key)? {
            let mut old: Job<D, R, P> = simd_json::from_slice(bytes)?;
            old.progress = Some(value.clone());
            let next = simd_json::to_vec(&old)?;
            self.db.put_cf(&cf, key, next)?;
        }
        job.progress = Some(value);
        Ok(())
    }
    async fn add_item(
        &self,
        col: CollectionSuffix,
        item: u64,
        score: Option<i64>,
        append: bool,
    ) -> KioResult<()> {
        let key = col;
        match col {
            CollectionSuffix::Active | CollectionSuffix::Wait | CollectionSuffix::Paused => {
                let mut queue = self.fetch::<VecDeque<u64>>(col).unwrap_or_default();
                if append {
                    queue.push_front(item);
                } else {
                    queue.push_back(item);
                }

                self.put(key, simd_json::to_vec(&queue)?).await?;
            }
            CollectionSuffix::Completed
            | CollectionSuffix::Delayed
            | CollectionSuffix::Failed
            | CollectionSuffix::Prioritized => {
                let mut map = self.fetch::<BTreeMap<u64, u64>>(col).unwrap_or_default();
                if let Some(score) = score {
                    map.insert(score as u64, item);
                }
                self.put(key, simd_json::to_vec(&map)?).await?;
            }
            CollectionSuffix::Stalled => {
                let mut map = self.fetch::<BTreeSet<u64>>(col).unwrap_or_default();
                if let Some(score) = score {
                    map.insert(item);
                }
                self.put(key, simd_json::to_vec(&map)?).await?;
            }

            _ => {}
        };
        Ok(())
    }
    async fn pop_back_push_front(
        &self,
        src: CollectionSuffix,
        dst: CollectionSuffix,
    ) -> Option<u64> {
        // only for lists;
        let cf = self.db.cf_handle(&self.main_tree)?;
        let is_list = |col| matches!(col, CollectionSuffix::Wait | CollectionSuffix::Active);
        if is_list(src) && is_list(dst) {
            let src_key = src;
            let dst_key = dst;
            let mut src: VecDeque<u64> = self.fetch(src).unwrap_or_default();
            let mut dst: VecDeque<u64> = self.fetch(dst).unwrap_or_default();
            if let Some(value) = src.pop_back() {
                dst.push_front(value);
                let next_src = simd_json::to_vec(&src).ok()?;
                let next_dst = simd_json::to_vec(&dst).ok()?;
                self.db.put_cf(&cf, src_key.to_bytes(), next_src).ok()?;
                self.db.put_cf(&cf, dst_key.to_bytes(), next_dst).ok()?;
                return Some(value);
            }
        }

        None
    }

    async fn set_lock(
        &self,
        col: CollectionSuffix,
        token: Option<JobToken>,
        lock_duration: u64,
    ) -> KioResult<()> {
        use std::time::Duration;
        let lock_key = col.tag();
        let duration = Duration::from_millis(lock_duration);
        let mut lock = Lock::StallCheck;
        if let Some(token) = token {
            lock = Lock::Token(token);
        }
        self.locks.insert_expirable(lock_key, lock, duration);

        Ok(())
    }
    async fn set_fields(&self, job_id: u64, fields: Vec<JobField<R>>) -> KioResult<()> {
        let key = CollectionSuffix::Job(job_id);
        if let Some(mut job) = self.fetch::<Job<D, R, P>>(key) {
            for field in fields {
                match field {
                    JobField::BackTrace(trace) => job.stack_trace.push(trace),
                    JobField::State(state) => job.state = state,
                    JobField::ProcessedOn(ts) => {
                        job.processed_on = Dt::from_timestamp_micros(ts as i64);
                    }
                    JobField::FinishedOn(ts) => {
                        job.finished_on = Dt::from_timestamp_micros(ts as i64);
                    }
                    JobField::Token(token) => job.token = Some(token),

                    JobField::Payload(processed_result) => match processed_result {
                        ProcessedResult::Failed(failed_details) => {
                            job.failed_reason = Some(failed_details)
                        }
                        ProcessedResult::Success(result, _) => job.returned_value = Some(result),
                    },
                }
            }
            self.put(key, simd_json::to_vec(&job)?).await?;
        }
        Ok(())
    }
    async fn incr(
        &self,
        key: CollectionSuffix,
        delta: i64,
        hash_key: Option<&str>,
    ) -> KioResult<u64> {
        let col = key;
        let key = key.to_bytes();
        if let Some(field) = hash_key {
            // get attempts
            let update_job = |job: &mut Job<D, R, P>| match field {
                "attempts_made" | "attemptsMade" => {
                    job.attempts_made += ((job.attempts_made) as i64 + delta) as u64;
                    job.attempts_made
                }
                "stalled_counter" | "stalledCounter" => {
                    job.stalled_counter += ((job.attempts_made) as i64 + delta) as u64;
                    job.stalled_counter
                }
                _ => 0,
            };
            if let CollectionSuffix::Job(id) = col {
                if let Some(mut job) = self.fetch::<Job<D, R, P>>(col) {
                    let next = update_job(&mut job);
                    let updated = simd_json::to_vec(&job)?;
                    self.put(col, updated).await?;
                    return Ok(next);
                }
            }
            if let CollectionSuffix::Meta = col {
                let mut meta = self.fetch::<Meta>(col).unwrap_or_default();
                if delta.is_negative() {
                    meta.processing = meta.processing.saturating_sub(delta.unsigned_abs());
                } else {
                    meta.processing += delta.unsigned_abs();
                }
                let next = meta.processing;
                let updated = simd_json::to_vec(&meta)?;
                self.put(col, updated).await?;
                return Ok(next);
            }
        }
        let mut current = self.get_counter(col, None).await.unwrap_or(1);
        // counter should start at one
        let mut next = current;
        if delta.is_negative() {
            next -= delta.unsigned_abs()
        } else {
            next += delta.unsigned_abs();
        }
        let updated = next.to_be_bytes();

        self.put(col, updated).await?;
        if current == 0 {
            return Ok(1);
        }
        Ok(next)
    }
    async fn get_counter(&self, col: CollectionSuffix, hash_key: Option<&str>) -> Option<u64> {
        let cf = self.db.cf_handle(&self.main_tree).unwrap();
        let key = col.to_bytes();
        if let Some(field) = hash_key {
            // get attempts
            let field = field.to_lowercase();
            if let CollectionSuffix::Job(id) = col {
                return self.get_job(id).await.map(|job: Job<D, R, P>| {
                    if field.contains("attempts") {
                        return job.attempts_made;
                    }
                    job.stalled_counter
                });
            }
            return self.get_meta().map(|meta| meta.processing);
        }
        self.db
            .get_cf(&cf, key)
            .ok()
            .flatten()
            .map(|bytes| ivec_to_number(bytes) as u64)
    }
    async fn publish_event(
        &self,
        event_mode: QueueEventMode,
        event: QueueStreamEvent<R, P>,
    ) -> KioResult<()> {
        let key = event.id;
        if let Some(emitter) = self.events.load().as_ref() {
            let metrics = self.get_metrics().await?;
            if let (Some(notifier), Some(pause_workers)) = (
                self.notifier.load().as_ref(),
                self.pause_workers.load().as_ref(),
            ) {
                process_each_event(event, emitter, self, &metrics).await?;
                pause_or_resume_workers(notifier, &metrics, pause_workers, &self.is_inital);
            }
        }
        Ok(())
    }

    fn get_job_ids_in_state(
        &self,
        state: JobState,
        start: Option<usize>,
        end: Option<usize>,
    ) -> KioResult<VecDeque<u64>> {
        let start = start.unwrap_or_default();
        match state {
            JobState::Prioritized | JobState::Completed | JobState::Failed | JobState::Delayed => {
                let col = state.into();
                if let Some(mut map) = self.fetch::<BTreeMap<u64, u64>>(col) {
                    let end = end.unwrap_or(map.len().saturating_sub(1));
                    let start = map.iter().nth(start);
                    let end = map.iter().nth(end);
                    if let (Some(start_element), Some(last_element)) = (start, end) {
                        let start = *start_element.0;
                        let end = *last_element.0;
                        return Ok(map.range(start..=end).map(|pairs| *pairs.1).collect());
                    }
                }
            }
            JobState::Stalled => {
                let col = state.into();
                if let Some(mut set) = self.fetch::<BTreeSet<u64>>(col) {
                    let end = end.unwrap_or(set.len().saturating_sub(1));
                    let start = set.iter().nth(start);
                    let end = set.iter().nth(end);
                    if let (Some(start_element), Some(last_element)) = (start, end) {
                        return Ok(set.range(*start_element..=*last_element).copied().collect());
                    }
                }
            }
            JobState::Active | JobState::Wait | JobState::Paused => {
                let col = state.into();
                if let Some(mut queue) = self.fetch::<VecDeque<u64>>(col) {
                    if !queue.is_empty() {
                        let end = end.unwrap_or(queue.len().saturating_sub(1));
                        let result = queue.range(start..=end).copied().collect();
                        return Ok(result);
                    }
                }
            }
            _ => {}
        }
        Ok(VecDeque::new())
    }
    async fn job_exists(&self, id: u64) -> bool {
        let key = CollectionSuffix::Job(id).to_bytes();
        let cf = self
            .db
            .cf_handle(&self.jobs)
            .expect("failed to get cf here");

        self.db.key_may_exist_cf(&cf, key)
    }
    async fn remove_item(&self, col: CollectionSuffix, item: u64) -> KioResult<()> {
        let cf = self
            .db
            .cf_handle(&self.main_tree)
            .ok_or(std::io::Error::other("failed to get cf-handle"))?;
        match col {
            CollectionSuffix::Active | CollectionSuffix::Wait | CollectionSuffix::Paused => {
                if let Some(mut queue) = self.fetch::<VecDeque<u64>>(col) {
                    queue.retain(|value| *value != item);
                    let updated = simd_json::to_vec(&queue)?;
                    self.put(col, updated).await?;
                }
            }
            CollectionSuffix::Completed
            | CollectionSuffix::Delayed
            | CollectionSuffix::Failed
            | CollectionSuffix::Prioritized => {
                if let Some(mut map) = self.fetch::<BTreeMap<u64, u64>>(col) {
                    map.retain(|_, value| *value != item);
                    let updated = simd_json::to_vec(&map)?;
                    self.put(col, updated).await?;
                }
            }
            CollectionSuffix::Stalled => {
                if let Some(mut set) = self.fetch::<BTreeSet<u64>>(col) {
                    set.remove(&item);
                    let updated = simd_json::to_vec(&set)?;
                    self.put(col, updated).await?;
                }
            }
            _ => {
                return self.remove(col);
            }
        };
        Ok(())
    }
    fn remove(&self, key: CollectionSuffix) -> KioResult<()> {
        let key = key.to_bytes();
        let cf = self
            .db
            .cf_handle(&self.main_tree)
            .expect("failed to get cf here");

        self.db.delete_cf(&cf, key)?;
        Ok(())
    }
    async fn clear_collections(&self) -> KioResult<()> {
        let cf = self
            .db
            .cf_handle(&self.main_tree)
            .expect("failed to get cf here");
        self.db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
        self.locks.clear();
        Ok(())
    }
    async fn clear_jobs(&self, last_id: u64) -> KioResult<()> {
        let cf = self
            .db
            .cf_handle(&self.jobs)
            .expect("failed to get cf here");
        self.db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
        Ok(())
    }
    async fn pause(&self, pause: bool, event_mode: QueueEventMode) -> KioResult<()> {
        let wait_key = CollectionSuffix::Wait;
        let paused_key = CollectionSuffix::Paused;
        let src = if pause { wait_key } else { paused_key };
        let dst = if pause { paused_key } else { wait_key };
        let cf = self
            .db
            .cf_handle(&self.main_tree)
            .expect("failed to get cf here");

        if self.db.key_may_exist_cf(&cf, paused_key.to_bytes()) {
            if let Some(removed) = self.db.get_cf(&cf, src.to_bytes())? {
                self.db.delete_cf(&cf, src.to_bytes())?;
                self.put(dst, removed).await?;
            }
        }
        if let Some(mut meta) = self.get_meta() {
            meta.paused = pause;
            let meta = simd_json::to_vec(&meta)?;
            self.db
                .put_cf(&cf, CollectionSuffix::Meta.to_bytes(), meta)?;
        }

        Ok(())
    }
    fn fetch_jobs(&self, ids: &[u64]) -> KioResult<VecDeque<Job<D, R, P>>> {
        if ids.is_empty() {
            return Ok(VecDeque::new());
        }
        let jobs_cf = self
            .db
            .cf_handle(&self.jobs)
            .ok_or(std::io::Error::other("failed to get cf"))?;
        let mut results = VecDeque::with_capacity(ids.len());
        for id in ids {
            let key = CollectionSuffix::Job(*id);
            if let Some(job) = self.fetch(key) {
                results.push_back(job);
            }
        }

        Ok(results)
    }
}
// util;
pub fn temporary_rocks_db() -> RocksDb {
    let temp_dir = tempfile::TempDir::new().expect("failed to create temp dir");
    let mut opts = Options::default();
    opts.create_if_missing(true);
    let db = RocksDb::open(&opts, temp_dir.path()).expect("failed to open temporary RocksDB");
    db
}
