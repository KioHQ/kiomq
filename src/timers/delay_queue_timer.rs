use crate::metrics::{
    TaskInfo, WorkerMetrics, HISTOGRAM_MAX_NS, P_METRICS_COLLECTOR, WORKER_STATE_TTL,
};
use crate::worker::{ProcessingQueue, WorkerState, MIN_DELAY_MS_LIMIT as EVICTION_INTERVAL_MS};

use crate::{KioError, KioResult, WorkerMetaData};
use arc_swap::ArcSwapOption;
use chrono::Utc;
use crossbeam::atomic::AtomicCell;
use crossbeam_skiplist::SkipMap;
use derive_more::{Debug, Display};
use futures::{FutureExt, StreamExt};
use futures_delay_queue::{delay_queue, DelayHandle, DelayQueue, Receiver};
use futures_intrusive::buffer::GrowingHeapBuf;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "tracing")]
use tracing::{debug, info, info_span, instrument, Span};
use uuid::Uuid;
// model the timers (stall_check_lock,  extend_lock and job_promotion)
#[derive(Debug, Clone, Copy, Display)]
pub enum TimerType {
    #[display("StalledCheck after {:#?} Worker ({_1})", _0.elapsed())]
    #[debug("StalledCheck")]
    StalledCheck(Instant, Uuid),
    #[display("ExtendLock after {:#?} for Worker({_1})", _0.elapsed())]
    #[debug("ExtendLock")]
    ExtendLock(Instant, Uuid),
    #[debug("PromoteJob")]
    #[display(
        "Promoted job {} after {:#?}",
        _0,
        Duration::from_millis(EVICTION_INTERVAL_MS)
    )]
    PromotedDelayed(u64),
    #[display("Collecting metrics for Worker ({_0})")]
    CollectMetrics(Uuid),
    ReregisterWorker,
}
use tokio::time::Instant;

use crate::{
    worker::{JobMap, Task},
    Queue, Store, WorkerOpts,
};
#[derive(Debug)]
struct SenderInner {
    tx: DelayQueue<TimerType, GrowingHeapBuf<TimerType>>,
    workers: WorkerMetaData,
}
impl SenderInner {
    fn new(tx: DelayQueue<TimerType, GrowingHeapBuf<TimerType>>, workers: WorkerMetaData) -> Self {
        Self { tx, workers }
    }
}

#[derive(Clone, Debug)]
pub struct TimerSender {
    inner: Arc<SenderInner>,
}
impl TimerSender {
    pub fn new(
        tx: DelayQueue<TimerType, GrowingHeapBuf<TimerType>>,
        workers: WorkerMetaData,
    ) -> Self {
        let inner = Arc::new(SenderInner::new(tx, workers));
        Self { inner }
    }
    pub fn send(&self, timer: TimerType) {
        if let Some(duration) = self.next_duration(timer) {
            let handle = self.inner.tx.insert(timer, duration);
            self.set_key(timer, handle);
        }
    }
    pub fn next_duration(&self, timer: TimerType) -> Option<Duration> {
        match timer {
            TimerType::StalledCheck(_, worker_id) | TimerType::ExtendLock(_, worker_id) => self
                .inner
                .workers
                .get(&worker_id)
                .map(|entry| Duration::from_millis(entry.value().0.stalled_interval)),
            TimerType::CollectMetrics(worker_id) => self
                .inner
                .workers
                .get(&worker_id)
                .map(|entry| Duration::from_millis(entry.value().0.metrics_update_interval)),
            TimerType::PromotedDelayed(_) => Some(Duration::from_millis(EVICTION_INTERVAL_MS)),
            TimerType::ReregisterWorker => Some(Duration::from_millis(WORKER_STATE_TTL as u64)),
        }
    }
    pub fn set_key(&self, timer: TimerType, key: DelayHandle) {
        match timer {
            TimerType::ExtendLock(_, worker_id) => {
                self.inner
                    .workers
                    .get(&worker_id)
                    .map(|entry| entry.value().3 .0.swap(Some(key.into())));
            }

            TimerType::StalledCheck(_, worker_id) => {
                self.inner
                    .workers
                    .get(&worker_id)
                    .map(|entry| entry.value().3 .1.swap(Some(key.into())));
            }
            TimerType::CollectMetrics(worker_id) => {
                self.inner
                    .workers
                    .get(&worker_id)
                    .map(|entry| entry.value().3 .2.swap(Some(key.into())));
            }
            TimerType::PromotedDelayed(_) | TimerType::ReregisterWorker => {} // do nothing here, these are temporary one-shot timers
        }
    }
}

/// A Runner for both  the `stalled_check` and `lock_extension` timer that requires polling
#[derive(Clone, Debug)]
pub struct DelayQueueTimer<D, R, P, S> {
    pub(crate) sender: TimerSender,
    reciever: Receiver<TimerType>,
    #[debug(skip)]
    task_handle: Arc<ArcSwapOption<Task>>,
    #[cfg(feature = "tracing")]
    resource_span: Span,
    #[debug(skip)]
    queue: Queue<D, R, P, S>,
    #[debug(skip)]
    jobs: JobMap<D, R, P>,
    workers: WorkerMetaData,
    token: CancellationToken,
}

impl<
        D: Clone + DeserializeOwned + 'static + Send + Serialize + Sync,
        R: Clone + DeserializeOwned + 'static + Serialize + Send + Sync,
        P: Clone + DeserializeOwned + 'static + Send + Sync + Serialize,
        S: Clone + Store<D, R, P> + Send + 'static + Sync,
    > DelayQueueTimer<D, R, P, S>
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        jobs: JobMap<D, R, P>,
        queue: Queue<D, R, P, S>,
        workers: WorkerMetaData,
        cancellation_token: CancellationToken,
    ) -> Self {
        #[cfg(feature = "tracing")]
        let resource_span = info_span!("Timers");
        let (tx, reciever) = delay_queue();
        let sender = TimerSender::new(tx, workers.clone());
        let timer = Self {
            workers,
            reciever,
            task_handle: Arc::default(),
            sender,
            #[cfg(feature = "tracing")]
            resource_span,
            queue,
            jobs,
            token: cancellation_token,
        };
        let task_handle = timer.create_timer_task();
        timer.task_handle.store(Some(Arc::new(task_handle)));
        timer
    }
    #[cfg_attr(feature = "tracing", instrument(parent = &self.resource_span, skip(self)))]
    pub(crate) fn insert(&self, timer: TimerType) {
        #[cfg(feature = "tracing")]
        {
            let duration = self.sender.next_duration(timer);
            info!("Started {timer:?} timer running every {duration:?}");
        }
        self.sender.send(timer);
    }
    //#[cfg_attr(feature="tracing", instrument(parent = &self.resource_span))]
    pub(crate) async fn clear(&self) {
        for entry in self.sender.inner.workers.iter() {
            let (key1, key2, key3) = &entry.value().3;
            for stored_key in [key1, key2, key3] {
                if let Some(handle) = stored_key.swap(None).and_then(Arc::into_inner) {
                    let _ = handle.cancel().await;
                }
            }
        }
    }

    //#[cfg_attr(feature="tracing", instrument(parent = &self.resource_span))]
    pub(crate) async fn close(&self) {
        self.clear().await;
        let task_handle = self.task_handle.swap(None);
        if let Some(task_handle) = task_handle {
            task_handle.abort();
        }
        self.reciever.close();
        self.token.cancel();
    }
    //#[cfg_attr(feature="tracing", instrument(parent = &self.resource_span))]
    fn timer_task(&self) -> impl std::future::Future<Output = KioResult<()>> {
        use tokio_util::time::FutureExt as OtherExt;
        let queue = self.queue.clone();
        let (workers, jobs, token, sender, rx) = (
            self.workers.clone(),
            self.jobs.clone(),
            self.token.clone(),
            self.sender.clone(),
            self.reciever.clone(),
        );
        async move {
            let interval_ms = EVICTION_INTERVAL_MS.cast_signed();
            #[cfg(feature = "tracing")]
            info!("starting ...");
            let timeout = Duration::from_millis(5);
            let interval = sysinfo::MINIMUM_CPU_UPDATE_INTERVAL + Duration::from_millis(100);
            let metrics_stream = P_METRICS_COLLECTOR
                .intervals(interval, token.clone())
                .fuse();
            tokio::pin!(metrics_stream);
            while !token.is_cancelled() {
                let date_time = Utc::now();
                let (promotion_error, timer_error, stream_error, ()) = tokio::join![
                    queue.promote_delayed_jobs(date_time, interval_ms, &sender),
                    async {
                        while let Ok(Some(expired)) = rx.receive().timeout(timeout).await {
                            process_timer(expired, &queue, &jobs, &workers, &sender).await?;
                        }
                        Ok::<(), KioError>(())
                    },
                    async {
                        while let Ok(Some(metrics)) = metrics_stream.next().timeout(timeout).await {
                            queue
                                .store
                                .store_process_metrics(metrics, interval.as_millis() as u64)
                                .await?;
                        }
                        Ok::<(), KioError>(())
                    },
                    queue.store.purge_expired(),
                ];
                promotion_error?;
                timer_error?;
                stream_error?;
                // if pause_schedular.load() && processing.is_empty() {
                //     #[cfg(feature = "tracing")]
                //     debug!("pausing ... ");
                //     worker_state.store(WorkerState::Idle);
                //     // wait for all running jobs to completed
                //     if token
                //         .run_until_cancelled(notifier.notified())
                //         .await
                //         .is_none()
                //     {
                //         // handle cancellation here too
                //         break;
                //     }
                //     #[cfg(feature = "tracing")]
                //     debug!("resumed");
                //     worker_state.store(WorkerState::Active);
                // }
                // yield for allow other tasks to continue
                tokio::task::yield_now().await;
            }
            #[cfg(feature = "tracing")]
            info!("cancelled");
            Ok(())
        }
    }

    //#[cfg_attr(feature="tracing", instrument(parent = &self.resource_span))]
    pub(crate) fn start_timers(&self, worker_id: Uuid) {
        let instant = Instant::now();
        self.insert(TimerType::ExtendLock(instant, worker_id));
        self.insert(TimerType::StalledCheck(instant, worker_id));
        self.insert(TimerType::CollectMetrics(worker_id));
        self.insert(TimerType::ReregisterWorker);
    }
    //#[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(rx, self)))]
    fn create_timer_task(&self) -> JoinHandle<KioResult<()>> {
        let t_task = self.timer_task();
        #[cfg(feature = "tracing")]
        let sub_span = info_span!(parent: &self.resource_span, "runner_task");
        #[cfg(feature = "tracing")]
        let timers_and_clean_up_task = {
            use tracing::Instrument;
            tokio::spawn(t_task.instrument(sub_span).boxed())
        };
        #[cfg(not(feature = "tracing"))]
        let timers_and_clean_up_task = tokio::spawn(t_task.boxed());
        timers_and_clean_up_task
    }
}
//#[cfg_attr(feature="tracing", instrument(skip(queue, jobs,sender)))]
async fn process_timer<D, R, P, S>(
    key: TimerType,
    queue: &Queue<D, R, P, S>,
    jobs: &JobMap<D, R, P>,
    workers: &SkipMap<
        Uuid,
        (
            WorkerOpts,
            ProcessingQueue,
            Arc<AtomicCell<WorkerState>>,
            (
                ArcSwapOption<DelayHandle>,
                ArcSwapOption<DelayHandle>,
                ArcSwapOption<DelayHandle>,
            ),
        ),
    >,
    sender: &TimerSender,
) -> KioResult<()>
where
    D: Clone + DeserializeOwned + 'static + Send + Serialize + Sync,
    R: Clone + DeserializeOwned + 'static + Serialize + Send + Sync,
    P: Clone + DeserializeOwned + 'static + Send + Sync + Serialize,
    S: Clone + Store<D, R, P> + Send + 'static + Sync,
{
    let mut next_timer = None;
    #[cfg(feature = "tracing")]
    info!("Running {key} ");
    match key {
        TimerType::StalledCheck(_, worker_id) => {
            if let Some(entry) = workers.get(&worker_id) {
                let (opts, _, _, _) = entry.value();
                let (_failed, _stalled) = queue.make_stalled_jobs_wait(opts).await?;
            };
            next_timer.replace(key);
        }
        TimerType::ExtendLock(_, worker_id) => {
            for pair in jobs.iter().filter(|entry| entry.value().1 .0 == worker_id) {
                let (job, token, _handle, _, _, opts) = pair.value();

                if let Some(id) = job.id {
                    queue.extend_lock(id, opts.lock_duration, *token).await?;
                }
            }
            next_timer.replace(key);
        }
        TimerType::CollectMetrics(_) => {
            let mut tasks_per_worker: HashMap<Uuid, (Vec<TaskInfo>, WorkerOpts)> =
                HashMap::with_capacity(workers.len());
            for mut entry in jobs.iter_mut() {
                let (id, (_, job_token, task_handle, monitor, histogram, opts)) =
                    &mut entry.pair_mut();
                let task_id: u64 = task_handle
                    .load()
                    .as_ref()
                    .and_then(|t_handle| t_handle.id().to_string().parse().ok())
                    .unwrap_or(**id);
                let metrics = monitor.cumulative();
                let mean_poll = if metrics.total_poll_count > 0 {
                    let total_nanos = metrics.total_poll_duration.as_nanos();
                    let polls = u128::from(metrics.total_poll_count);
                    Duration::from_nanos(u64::try_from(total_nanos / polls).unwrap_or_default())
                } else {
                    Duration::ZERO
                };

                // Record the current mean poll time into the HDR histogram.
                let mean_ns = u64::try_from(mean_poll.as_nanos()).unwrap_or_default();
                if mean_ns > 0 {
                    let _ = histogram.record(mean_ns.min(HISTOGRAM_MAX_NS));
                }

                let task_info = TaskInfo::new(task_id, **id, metrics, histogram.clone());
                let worker_id = job_token.0;
                match tasks_per_worker.entry(worker_id) {
                    std::collections::hash_map::Entry::Occupied(mut occupied) => {
                        occupied.get_mut().0.push(task_info);
                    }
                    std::collections::hash_map::Entry::Vacant(vacant) => {
                        vacant.insert((vec![task_info], *opts));
                    }
                }
            }
            for (worker_id, (tasks, opts)) in tasks_per_worker {
                let active_len = tasks.len();
                let ttls = opts.metrics_update_interval;

                let worker_metrics = WorkerMetrics::new(worker_id, active_len, tasks, ttls);
                queue
                    .store_worker_metrics(worker_metrics, opts.metrics_update_interval)
                    .await?;
            }
            next_timer.replace(key);
        }
        TimerType::PromotedDelayed(job_id) => {
            queue
                .store
                .add_item(crate::CollectionSuffix::Wait, job_id, None, true)
                .await?;
        }
        TimerType::ReregisterWorker => {
            for entry in workers.iter() {
                let worker_id = *entry.key();
                let (_, _, state, _) = entry.value();
                P_METRICS_COLLECTOR
                    .register_worker(worker_id, state.clone())
                    .await;
            }
            next_timer.replace(key);
        }
    }
    if let Some(timer) = next_timer {
        sender.send(timer);
    }
    Ok(())
}
