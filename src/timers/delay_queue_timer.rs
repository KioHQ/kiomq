use crate::metrics::{
    HISTOGRAM_MAX_NS, P_METRICS_COLLECTOR, TaskInfo, TimerCommand, WORKER_STATE_TTL, WorkerMetrics,
};
use crate::utils::pause_or_resume_workers;
use crate::worker::{MIN_DELAY_MS_LIMIT as EVICTION_INTERVAL_MS, ProcessingQueue, WorkerState};

use crate::Dt;
use crate::{KioResult, ProcessMetrics, WorkerMetaData};
use arc_swap::ArcSwapOption;
use chrono::Utc;
use crossbeam::atomic::AtomicCell;
use crossbeam_skiplist::SkipMap;
use derive_more::{Debug, Display};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use serde::{Serialize, de::DeserializeOwned};
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
use tracing::{Span, info, info_span, instrument};
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
}
impl TimerType {
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub const fn next_duration(&self) -> Duration {
        match self {
            Self::StalledCheck(duration)
            | Self::ExtendLock(duration)
            | Self::CollectMetrics(duration) => *duration,
            Self::PromotedDelayed(_, _) => Duration::from_millis(EVICTION_INTERVAL_MS),
            Self::ReregisterWorker => Duration::from_millis(WORKER_STATE_TTL as u64),
        }
    }
}

use crate::{
    Queue, Store, WorkerOpts,
    worker::{JobMap, Task},
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
    #[allow(clippy::too_many_arguments)]
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
    ) -> impl std::future::Future<Output = KioResult<()>> + use<D, R, P, S> {
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
                let date_time = Utc::now();
                if queue.current_metrics.has_delayed() && !queue.current_metrics.is_idle() {
                    queue
                        .promote_delayed_jobs(
                            date_time,
                            EVICTION_INTERVAL_MS.cast_signed(),
                            &sender,
                        )
                        .await?;
                }

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
                             #[allow(clippy::cast_possible_truncation)]
                             queue
                                 .store
                                 .store_process_metrics(metrics, interval as u64)
                                 .await?;
                         }

                     }
                };
                if queue.current_metrics.is_idle() {
                    tokio::time::sleep(throttle_duration).await;
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
            #[allow(unused_variables)]
            let (listener_err, timer_task_error) = tokio::join!(stream_listener_task, timer_task);
            #[cfg(feature = "tracing")]
            {
                use tracing::error;
                if let Err(err) = listener_err {
                    error!("listener_stream_err:{err:?}");
                }
                if let Err(err) = timer_task_error {
                    error!("timer_task_error:{err:?}");
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
type WorkerMap = SkipMap<
    Uuid,
    (
        Arc<AtomicCell<WorkerState>>,
        ProcessingQueue,
        WorkerOpts,
        Dt,
    ),
>;

//#[cfg_attr(feature="tracing", instrument(skip(queue, jobs,sender)))]
#[allow(clippy::too_many_lines)]
async fn process_timer<D, R, P, S>(
    key: TimerType,
    queue: &Queue<D, R, P, S>,
    jobs: &JobMap<D, R, P>,
    #[allow(clippy::type_complexity)] workers: &WorkerMap,
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
                .find(|entry| Duration::from_millis(entry.value().2.stalled_interval) == duration)
            {
                let (_, _, opts, _) = entry.value();
                let (_failed, _stalled) = queue.make_stalled_jobs_wait(opts).await?;
            }
            next_timer.replace(key);
        }
        TimerType::ExtendLock(duration) => {
            let workers: HashSet<Uuid> = workers
                .iter()
                .filter_map(|entry| {
                    if Duration::from_millis(entry.value().2.lock_duration) == duration {
                        return Some(*entry.key());
                    }
                    None
                })
                .collect();
            for pair in jobs
                .iter()
                .filter(|entry| workers.contains(&entry.value().1.0))
            {
                let (job, token, _handle, _, _, opts) = pair.value();

                if let Some(id) = job.id {
                    queue.extend_lock(id, opts.lock_duration, *token).await?;
                }
            }
            next_timer.replace(key);
        }
        TimerType::CollectMetrics(duration) => {
            queue.store.purge_expired().await;
            let is_initial = AtomicCell::new(false);
            let updated_metrics = queue.get_metrics().await?;
            queue.current_metrics.update(&updated_metrics);
            pause_or_resume_workers(
                &queue.worker_notifier,
                &updated_metrics,
                &queue.pause_workers,
                &is_initial,
            );

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
                let (state, _, _, _) = entry.value();
                if matches!(state.load(), WorkerState::Active | WorkerState::Idle) {
                    queue.add_worker_heartbeat(&worker_id);
                }
            }
            P_METRICS_COLLECTOR.register_queue(
                queue.id,
                queue.timer_sender.clone(),
                queue.current_metrics.clone(),
            );
            next_timer.replace(key);
        }
    }
    if let Some(timer) = next_timer {
        sender.send(timer).await;
    }
    Ok(())
}

#[cfg(test)]
mod timer_type_tests {
    //! Robustness tests for [`TimerType`] — the only part of `delay_queue_timer`
    //! exercisable without a full `Queue`/`Store`/`JobMap` harness. Cover
    //! `next_duration` plus the `Display`/`Debug` representations.
    use super::{EVICTION_INTERVAL_MS, TimerType, WORKER_STATE_TTL};
    use std::time::Duration;
    use uuid::Uuid;

    #[test]
    fn next_duration_echoes_the_configured_interval_for_polling_timers() {
        // These variants must echo their configured duration so the timer
        // reschedules at the same cadence.
        let cases = [
            Duration::from_millis(250),
            Duration::from_secs(30),
            Duration::from_millis(1),
        ];
        for duration in cases {
            assert_eq!(
                TimerType::StalledCheck(duration).next_duration(),
                duration,
                "StalledCheck must echo its interval"
            );
            assert_eq!(
                TimerType::ExtendLock(duration).next_duration(),
                duration,
                "ExtendLock must echo its interval"
            );
            assert_eq!(
                TimerType::CollectMetrics(duration).next_duration(),
                duration,
                "CollectMetrics must echo its interval"
            );
        }
    }

    #[test]
    fn next_duration_handles_zero_and_very_large_intervals_without_panicking() {
        // A zero interval must round-trip verbatim rather than being clamped.
        assert_eq!(
            TimerType::StalledCheck(Duration::ZERO).next_duration(),
            Duration::ZERO,
            "a zero interval must round-trip unchanged"
        );
        let very_large = Duration::from_hours(8760); // one year
        assert_eq!(
            TimerType::ExtendLock(very_large).next_duration(),
            very_large,
            "a very large interval must round-trip unchanged"
        );
    }

    #[test]
    fn promoted_delayed_always_uses_the_fixed_eviction_interval() {
        // PromotedDelayed ignores its job/queue id and always reschedules after
        // the fixed eviction interval.
        let expected = Duration::from_millis(EVICTION_INTERVAL_MS);
        for job_id in [0_u64, 1, u64::MAX] {
            let timer = TimerType::PromotedDelayed(job_id, Uuid::new_v4());
            assert_eq!(
                timer.next_duration(),
                expected,
                "PromotedDelayed must always reschedule after EVICTION_INTERVAL_MS"
            );
        }
    }

    #[test]
    fn reregister_worker_uses_the_worker_state_ttl() {
        // Reschedules after the worker-state TTL so heartbeats refresh before the
        // registry entry expires.
        let ttl_ms = u64::try_from(WORKER_STATE_TTL).expect("worker-state TTL must fit in u64");
        // Must be strictly positive, else the timer would busy-loop.
        assert!(
            ttl_ms > 0,
            "worker-state TTL must be positive to avoid a tight reschedule loop"
        );
        assert_eq!(
            TimerType::ReregisterWorker.next_duration(),
            Duration::from_millis(ttl_ms),
            "ReregisterWorker must reschedule after WORKER_STATE_TTL"
        );
    }

    #[test]
    fn timer_type_is_copy_so_rescheduling_never_moves_the_original() {
        // `next_timer.replace(key)` in production relies on `TimerType: Copy`.
        let original = TimerType::StalledCheck(Duration::from_millis(10));
        let copied = original;
        assert_eq!(original.next_duration(), copied.next_duration());
    }

    #[test]
    fn display_representation_includes_the_expected_context() {
        let stalled = format!("{}", TimerType::StalledCheck(Duration::from_millis(5)));
        assert!(
            stalled.contains("StalledCheck") && stalled.contains("5ms"),
            "unexpected StalledCheck Display: {stalled}"
        );
        let extend = format!("{}", TimerType::ExtendLock(Duration::from_millis(7)));
        assert!(
            extend.contains("ExtendLock") && extend.contains("7ms"),
            "unexpected ExtendLock Display: {extend}"
        );
        let queue_id = Uuid::new_v4();
        let promoted = format!("{}", TimerType::PromotedDelayed(42, queue_id));
        assert!(
            promoted.contains("42") && promoted.contains(&queue_id.to_string()),
            "unexpected PromotedDelayed Display: {promoted}"
        );
        let collect = format!("{}", TimerType::CollectMetrics(Duration::from_millis(9)));
        assert!(
            collect.contains("9ms"),
            "unexpected CollectMetrics Display: {collect}"
        );
    }

    #[test]
    fn debug_representation_uses_stable_short_labels() {
        assert_eq!(
            format!("{:?}", TimerType::StalledCheck(Duration::from_millis(1))),
            "StalledCheck"
        );
        assert_eq!(
            format!("{:?}", TimerType::ExtendLock(Duration::from_millis(1))),
            "ExtendLock"
        );
        assert_eq!(
            format!("{:?}", TimerType::PromotedDelayed(1, Uuid::nil())),
            "PromoteJob"
        );
    }
}
