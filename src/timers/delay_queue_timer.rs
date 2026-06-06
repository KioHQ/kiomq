use crate::metrics::{
    TaskInfo, TimerCommand, WorkerMetrics, HISTOGRAM_MAX_NS, P_METRICS_COLLECTOR, WORKER_STATE_TTL,
};
use crate::worker::{ProcessingQueue, WorkerState, MIN_DELAY_MS_LIMIT as EVICTION_INTERVAL_MS};

use crate::{KioResult, ProcessMetrics, WorkerMetaData};
use arc_swap::ArcSwapOption;
use chrono::Utc;
use crossbeam::atomic::AtomicCell;
use crossbeam_skiplist::SkipMap;
use derive_more::{Debug, Display};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{BroadcastStream, WatchStream};
use tokio_util::sync::CancellationToken;
#[cfg(feature = "tracing")]
use tracing::{info, info_span, instrument, Span};
use uuid::Uuid;
// model the timers (stall_check_lock,  extend_lock and job_promotion)
#[derive(Debug, Clone, Copy, Display)]
pub enum TimerType {
    #[display("StalledCheck after {_0:?}")]
    #[debug("StalledCheck")]
    StalledCheck(Duration),
    #[display("ExtendLock after {_0:?}")]
    #[debug("ExtendLock")]
    ExtendLock(Duration),
    #[debug("PromoteJob")]
    #[display(
        "Promoted job {} after {:?} for queueId({_1})",
        _0,
        Duration::from_millis(EVICTION_INTERVAL_MS)
    )]
    PromotedDelayed(u64, Uuid),
    #[display("Collecting metrics in  ({_0:?})")]
    CollectMetrics(Duration),
    ReregisterWorker,
    TriggerJobPromotion,
}
impl TimerType {
    #[must_use]
    pub const fn next_duration(&self) -> Duration {
        match self {
            Self::StalledCheck(duration)
            | Self::ExtendLock(duration)
            | Self::CollectMetrics(duration) => *duration,
            Self::PromotedDelayed(_, _) => Duration::from_millis(EVICTION_INTERVAL_MS),
            Self::ReregisterWorker => Duration::from_millis(WORKER_STATE_TTL as u64),
            Self::TriggerJobPromotion => Duration::from_millis(EVICTION_INTERVAL_MS / 3),
        }
    }
}

use crate::{
    worker::{JobMap, Task},
    Queue, Store, WorkerOpts,
};
#[derive(Debug)]
struct SenderInner {
    tx: mpsc::Sender<(Uuid, TimerType, oneshot::Sender<()>)>,
    workers: WorkerMetaData,
}
impl SenderInner {
    const fn new(
        tx: mpsc::Sender<(Uuid, TimerType, oneshot::Sender<()>)>,
        workers: WorkerMetaData,
    ) -> Self {
        Self { tx, workers }
    }
}

#[derive(Clone, Debug)]
pub struct TimerSender {
    queue_id: Uuid,
    inner: Arc<SenderInner>,
}
impl TimerSender {
    pub fn new(
        tx: mpsc::Sender<(Uuid, TimerType, oneshot::Sender<()>)>,
        workers: WorkerMetaData,
        queue_id: Uuid,
    ) -> Self {
        let inner = Arc::new(SenderInner::new(tx, workers));
        Self { queue_id, inner }
    }
    pub async fn send(&self, timer: TimerType) {
        let (sender, ack) = oneshot::channel();
        if self.timer_exists(&timer) {
            return;
        }
        let _ = self.inner.tx.send((self.queue_id, timer, sender)).await;
        ack.await.ok();
    }
    pub fn timer_exists(&self, timer: &TimerType) -> bool {
        P_METRICS_COLLECTOR.timer_exists(timer, &self.queue_id)
    }
}

/// A Runner for both  the `stalled_check` and `lock_extension` timer that requires polling
#[derive(Clone, Debug)]
pub struct DelayQueueTimer<D, R, P, S> {
    pub(crate) sender: TimerSender,
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
    pub(crate) fn new(
        jobs: JobMap<D, R, P>,
        queue: Queue<D, R, P, S>,
        workers: WorkerMetaData,
        tx: mpsc::Sender<(Uuid, TimerType, oneshot::Sender<()>)>,
        rx: Receiver<TimerCommand>,
        process_metrics_rx: watch::Receiver<Option<ProcessMetrics>>,
        cancellation_token: CancellationToken,
        stream_listener_task: BoxFuture<'static, KioResult<()>>,
    ) -> Self {
        #[cfg(feature = "tracing")]
        let resource_span = info_span!("Timers");
        let sender = TimerSender::new(tx, workers.clone(), queue.id);
        let timer = Self {
            workers,
            task_handle: Arc::default(),
            sender,
            #[cfg(feature = "tracing")]
            resource_span,
            queue,
            jobs,
            token: cancellation_token,
        };
        let task_handle = timer.create_timer_task(rx, process_metrics_rx, stream_listener_task);
        timer.task_handle.store(Some(Arc::new(task_handle)));
        timer
    }
    #[cfg_attr(feature = "tracing", instrument(parent = &self.resource_span, skip(self)))]
    pub(crate) async fn insert(&self, timer: TimerType) {
        #[cfg(feature = "tracing")]
        {
            let duration = timer.next_duration();
            info!("Started {timer:?} timer running every {duration:?}");
        }
        self.sender.send(timer).await;
    }
    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span))]
    pub(crate) fn clear(&self) {
        self.sender.inner.workers.clear();
    }

    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span))]
    pub(crate) fn close(&self) {
        self.clear();
        let task_handle = self.task_handle.swap(None);
        if let Some(task_handle) = task_handle {
            task_handle.abort();
        }
        self.token.cancel();
    }
    fn timer_task(
        &self,
        rx: Receiver<TimerCommand>,
        process_metrics_rx: watch::Receiver<Option<ProcessMetrics>>,
    ) -> impl std::future::Future<Output = KioResult<()>> {
        let queue = self.queue.clone();
        let (workers, jobs, token, sender, _) = (
            self.workers.clone(),
            self.jobs.clone(),
            self.token.clone(),
            self.sender.clone(),
            queue.timer_sender.clone(),
        );
        async move {
            #[cfg(feature = "tracing")]
            info!("starting ...");
            let interval = crate::PROCESS_METRIC_UPDATE_INTERVAL;
            let mut incoming_timer_stream = BroadcastStream::new(rx);
            let mut process_metrics_stream = WatchStream::from_changes(process_metrics_rx);
            let throttle_duration = Duration::from_millis(500);
            loop {
                tokio::select! {
                    () = token.cancelled() => {
                          break;
                     },
                    Some(Ok(timer_cmd)) = incoming_timer_stream.next() => {
                     match timer_cmd {
                         TimerCommand::RespondToTimer(timer) => {
                             process_timer(timer, &queue, &jobs, &workers, &sender).await?;
                         }
                     }
                 }
                     Some(process_metrics) = process_metrics_stream.next() =>{
                         if let Some(metrics) = process_metrics {
                          #[cfg(feature = "tracing")]
                           info!("Collecting Process Metrics");
                             queue
                                 .store
                                 .store_process_metrics(metrics, interval as u64)
                                 .await?;
                         }

                     }
                };
                if queue.current_metrics.is_idle() {
                    tokio::time::sleep(throttle_duration).await
                }
            }
            #[cfg(feature = "tracing")]
            info!("cancelled");
            Ok(())
        }
    }

    //#[cfg_attr(feature="tracing", instrument(parent = &self.resource_span))]
    pub(crate) async fn register_worker_timers(&self, opts: WorkerOpts) {
        let stalled_interval = Duration::from_millis(opts.stalled_interval);
        let extend_lock = Duration::from_millis(opts.lock_duration);
        let worker_metrics_interval = Duration::from_millis(opts.metrics_update_interval);
        self.insert(TimerType::TriggerJobPromotion).await;
        self.insert(TimerType::ExtendLock(extend_lock)).await;
        self.insert(TimerType::StalledCheck(stalled_interval)).await;
        self.insert(TimerType::CollectMetrics(worker_metrics_interval))
            .await;
        self.insert(TimerType::ReregisterWorker).await;
    }
    //#[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(rx, self)))]
    fn create_timer_task(
        &self,
        rx: Receiver<TimerCommand>,
        process_metrics_rx: watch::Receiver<Option<ProcessMetrics>>,
        stream_listener_task: BoxFuture<'static, KioResult<()>>,
    ) -> JoinHandle<KioResult<()>> {
        let timer_task = self.timer_task(rx, process_metrics_rx);
        let t_task = async {
            let (_listener_err, _timer_task_error) = tokio::join!(stream_listener_task, timer_task);
            #[cfg(feature = "tracing")]
            {
                use tracing::error;
                if let Err(err) = _listener_err {
                    error!("listener_stream_err:{err:?}")
                }
                if let Err(err) = _timer_task_error {
                    error!("timer_task_error:{err:?}")
                }
            }
            Ok(())
        };
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
    workers: &SkipMap<Uuid, (WorkerOpts, ProcessingQueue, Arc<AtomicCell<WorkerState>>)>,
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
        TimerType::StalledCheck(duration) => {
            // run_once for all workers
            if let Some(entry) = workers
                .iter()
                .find(|entry| Duration::from_millis(entry.value().0.stalled_interval) == duration)
            {
                let (opts, _, _) = entry.value();
                let (_failed, _stalled) = queue.make_stalled_jobs_wait(opts).await?;
            }
            next_timer.replace(key);
        }
        TimerType::ExtendLock(duration) => {
            let workers: HashSet<Uuid> = workers
                .iter()
                .filter_map(|entry| {
                    if Duration::from_millis(entry.value().0.lock_duration) == duration {
                        return Some(*entry.key());
                    }
                    None
                })
                .collect();
            for pair in jobs
                .iter()
                .filter(|entry| workers.contains(&entry.value().1 .0))
            {
                let (job, token, _handle, _, _, opts) = pair.value();

                if let Some(id) = job.id {
                    queue.extend_lock(id, opts.lock_duration, *token).await?;
                }
            }
            next_timer.replace(key);
        }
        TimerType::CollectMetrics(duration) => {
            let mut tasks_per_worker: HashMap<Uuid, (Vec<TaskInfo>, WorkerOpts)> =
                HashMap::with_capacity(workers.len());
            for entry in jobs.iter().filter(|entry| {
                Duration::from_millis(entry.value().5.metrics_update_interval) == duration
            }) {
                let (_, job_token, task_handle, monitor, histogram, opts) = entry.value();
                let id = *entry.key();

                let task_id: u64 = task_handle
                    .load()
                    .as_ref()
                    .and_then(|t_handle| t_handle.id().to_string().parse().ok())
                    .unwrap_or(id);
                let metrics = monitor.cumulative();
                let mean_poll = if metrics.total_poll_count > 0 {
                    let total_nanos = metrics.total_poll_duration.as_nanos();
                    let polls = u128::from(metrics.total_poll_count);
                    Duration::from_nanos(u64::try_from(total_nanos / polls).unwrap_or_default())
                } else {
                    Duration::ZERO
                };
                let mut histogram = histogram.lock();
                // Record the current mean poll time into the HDR histogram.
                let mean_ns = u64::try_from(mean_poll.as_nanos()).unwrap_or_default();
                if mean_ns > 0 {
                    let _ = histogram.record(mean_ns.min(HISTOGRAM_MAX_NS));
                }
                let task_info = TaskInfo::new(task_id, id, metrics, histogram.clone());
                drop(histogram);
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
        TimerType::PromotedDelayed(job_id, _) => {
            queue
                .store
                .add_item(crate::CollectionSuffix::Wait, job_id, None, true)
                .await?;
        }
        TimerType::ReregisterWorker => {
            for entry in workers {
                let worker_id = *entry.key();
                let (_, _, state) = entry.value();
                if matches!(state.load(), WorkerState::Active | WorkerState::Idle) {
                    P_METRICS_COLLECTOR.register_worker(worker_id, state.clone());
                }
            }
            P_METRICS_COLLECTOR.register_queue(
                queue.id,
                queue.timer_sender.clone(),
                queue.current_metrics.clone(),
            );
            next_timer.replace(key);
        }
        TimerType::TriggerJobPromotion => {
            let date_time = Utc::now();
            if queue.current_metrics.has_delayed() {
                queue
                    .promote_delayed_jobs(date_time, EVICTION_INTERVAL_MS.cast_signed(), sender)
                    .await?;
                next_timer.replace(key);
            }
            queue.store.purge_expired().await;
        }
    }
    if let Some(timer) = next_timer {
        sender.send(timer).await;
    }
    Ok(())
}
