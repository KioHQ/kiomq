use crate::worker::{ProcessingQueue, WorkerState, MIN_DELAY_MS_LIMIT as EVICTION_INTERVAL_MS};
use crate::worker::{TaskInfo, WorkerMetrics, HISTOGRAM_MAX_NS};
use crate::{KioError, KioResult};
use arc_swap::ArcSwapOption;
use atomig::Atomic;
use chrono::Utc;
use derive_more::{Debug, Display};
use futures::FutureExt;
use futures_delay_queue::{delay_queue, DelayHandle, DelayQueue, Receiver};
use futures_intrusive::buffer::GrowingHeapBuf;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicBool, Arc};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "tracing")]
use tracing::{debug, info, info_span, instrument, Span};
use uuid::Uuid;
// model the timers (stall_check_lock,  extend_lock and job_promotion)
#[derive(Debug, Clone, Copy, Display)]
pub enum TimerType {
    #[display("StalledCheck after {:#?}", _0.elapsed())]
    #[debug("StalledCheck")]
    StalledCheck(Instant),
    #[display("ExtendLock after {:#?}", _0.elapsed())]
    #[debug("ExtendLock")]
    ExtendLock(Instant),
    #[debug("PromoteJob")]
    #[display(
        "Promoted job {} after {:#?}",
        _0,
        Duration::from_millis(EVICTION_INTERVAL_MS)
    )]
    PromotedDelayed(u64),
    CollectMetrics,
}
use tokio::time::Instant;

use crate::{
    worker::{JobMap, Task},
    Queue, Store, WorkerOpts,
};
#[derive(Debug)]
struct SenderInner {
    tx: DelayQueue<TimerType, GrowingHeapBuf<TimerType>>,
    keys: (
        ArcSwapOption<DelayHandle>,
        ArcSwapOption<DelayHandle>,
        ArcSwapOption<DelayHandle>,
    ),
}
impl SenderInner {
    fn new(tx: DelayQueue<TimerType, GrowingHeapBuf<TimerType>>) -> Self {
        let keys = (
            ArcSwapOption::default(),
            ArcSwapOption::default(),
            ArcSwapOption::default(),
        );
        Self { tx, keys }
    }
}

use tokio::sync::Notify;
#[derive(Clone, Debug)]
pub struct TimerSender {
    inner: Arc<SenderInner>,
    opts: WorkerOpts,
}
impl TimerSender {
    pub fn new(tx: DelayQueue<TimerType, GrowingHeapBuf<TimerType>>, opts: WorkerOpts) -> Self {
        let inner = Arc::new(SenderInner::new(tx));
        Self { inner, opts }
    }
    pub fn send(&self, timer: TimerType) {
        let duration = self.next_duration(timer);
        let handle = self.inner.tx.insert(timer, duration);
        self.set_key(timer, handle);
    }
    pub const fn next_duration(&self, timer: TimerType) -> Duration {
        match timer {
            TimerType::StalledCheck(_) => Duration::from_millis(self.opts.stalled_interval),
            TimerType::ExtendLock(_) => Duration::from_millis(self.opts.lock_duration),
            TimerType::CollectMetrics => Duration::from_millis(self.opts.metrics_update_interval),
            TimerType::PromotedDelayed(_) => Duration::from_millis(EVICTION_INTERVAL_MS),
        }
    }
    pub fn set_key(&self, timer: TimerType, key: DelayHandle) {
        match timer {
            TimerType::StalledCheck(_) => self.inner.keys.1.store(Some(key.into())),
            TimerType::ExtendLock(_) => self.inner.keys.0.store(Some(key.into())),
            TimerType::CollectMetrics => self.inner.keys.2.store(Some(key.into())),
            TimerType::PromotedDelayed(_) => {} // do nothing here, these are temporary one-shot timers
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
    start_signal: Arc<Notify>,
    #[debug(skip)]
    queue: Arc<Queue<D, R, P, S>>,
    #[debug(skip)]
    jobs: JobMap<D, R, P>,
    opts: WorkerOpts,
    worker_id: Uuid,
    token: Arc<CancellationToken>,
    worker_state: Arc<Atomic<WorkerState>>,
    #[debug(skip)]
    notifier: Arc<Notify>,
    pause_schedular: Arc<AtomicBool>,
    processing: ProcessingQueue,
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
        worker_id: uuid::Uuid,
        opts: WorkerOpts,
        queue: Arc<Queue<D, R, P, S>>,
        cancellation_token: Arc<CancellationToken>,
        worker_state: Arc<Atomic<WorkerState>>,
        notifier: Arc<Notify>,
        pause_schedular: Arc<AtomicBool>,
        processing: ProcessingQueue,
    ) -> Self {
        #[cfg(feature = "tracing")]
        let resource_span = info_span!("Timers");
        let start_signal: Arc<Notify> = Arc::default();
        let (tx, reciever) = delay_queue();
        let sender = TimerSender::new(tx, opts);
        let timer = Self {
            reciever,
            start_signal,
            task_handle: Arc::default(),
            sender,
            #[cfg(feature = "tracing")]
            resource_span,
            queue,
            jobs,
            opts,
            worker_id,
            token: cancellation_token,
            worker_state,
            notifier,
            pause_schedular,
            processing,
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
        let (a, b, c) = &self.sender.inner.keys;
        let keys = [a, b, c];
        for stored_key in keys {
            if let Some(handle) = stored_key.swap(None).and_then(Arc::into_inner) {
                let _ = handle.cancel().await;
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
    }
    //#[cfg_attr(feature="tracing", instrument(parent = &self.resource_span))]
    fn timer_task(&self) -> impl std::future::Future<Output = KioResult<()>> {
        use tokio_util::time::FutureExt as OtherExt;
        let processing = self.processing.clone();
        let notifier = self.notifier.clone();
        let queue = self.queue.clone();
        let start_signal = self.start_signal.clone();
        let (worker_id, opts, pause_schedular, worker_state, jobs, token, sender, rx) = (
            self.worker_id,
            self.opts,
            self.pause_schedular.clone(),
            self.worker_state.clone(),
            self.jobs.clone(),
            self.token.clone(),
            self.sender.clone(),
            self.reciever.clone(),
        );
        async move {
            start_signal.notified().await;
            let interval_ms = EVICTION_INTERVAL_MS.cast_signed();
            #[cfg(feature = "tracing")]
            info!("starting ...");
            let timeout = Duration::from_millis(5);
            while !token.is_cancelled() {
                let date_time = Utc::now();
                tokio::try_join!(
                    queue.promote_delayed_jobs(date_time, interval_ms, &sender),
                    async {
                        while let Ok(Some(expired)) = rx.receive().timeout(timeout).await {
                            process_timer(expired, &queue, &jobs, opts, worker_id, &sender).await?;
                        }
                        Ok::<(), KioError>(())
                    },
                    async {
                        queue.store.purge_expired().await;
                        Ok::<(), KioError>(())
                    }
                )?;
                if pause_schedular.load(Ordering::Acquire) && processing.is_empty() {
                    #[cfg(feature = "tracing")]
                    debug!("pausing ... ");
                    worker_state.store(WorkerState::Idle, Ordering::Release);
                    // wait for all running jobs to completed
                    if token
                        .run_until_cancelled(notifier.notified())
                        .await
                        .is_none()
                    {
                        // handle cancellation here too
                        break;
                    }
                    #[cfg(feature = "tracing")]
                    debug!("resumed");
                    worker_state.store(WorkerState::Active, Ordering::Release);
                }
                // yield for allow other tasks to continue
                tokio::task::yield_now().await;
            }
            #[cfg(feature = "tracing")]
            info!("cancelled");
            Ok(())
        }
    }

    //#[cfg_attr(feature="tracing", instrument(parent = &self.resource_span))]
    pub(crate) fn start_timers(&self) {
        let instant = Instant::now();
        self.insert(TimerType::ExtendLock(instant));
        self.insert(TimerType::StalledCheck(instant));
        self.insert(TimerType::CollectMetrics);
        self.start_signal.notify_one();
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
    opts: WorkerOpts,
    worker_id: Uuid,
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
        TimerType::StalledCheck(_) => {
            let (_failed, _stalled) = queue.make_stalled_jobs_wait(&opts).await?;
            next_timer.replace(key);
        }
        TimerType::ExtendLock(_) => {
            for pair in jobs.iter() {
                let (job, token, _handle, _, _) = pair.value();

                if let Some(id) = job.id {
                    queue.extend_lock(id, opts.lock_duration, *token).await?;
                }
            }
            next_timer.replace(key);
        }
        TimerType::CollectMetrics => {
            let mut tasks = Vec::with_capacity(jobs.len());
            for entry in jobs.iter() {
                let id = entry.key();
                let (_, _, task_handle, monitor, hist) = entry.value();
                let task_id: u64 = task_handle
                    .load()
                    .as_ref()
                    .and_then(|t_handle| t_handle.id().to_string().parse().ok())
                    .unwrap_or(*id);
                let metrics = monitor.cumulative();
                let mean_poll = if metrics.total_poll_count > 0 {
                    let total_nanos = metrics.total_poll_duration.as_nanos();
                    let polls = u128::from(metrics.total_poll_count);
                    Duration::from_nanos(u64::try_from(total_nanos / polls).unwrap_or_default())
                } else {
                    Duration::ZERO
                };

                let mut histogram = hist.lock().await;
                // Record the current mean poll time into the HDR histogram.
                let mean_ns = u64::try_from(mean_poll.as_nanos()).unwrap_or_default();
                if mean_ns > 0 {
                    let _ = histogram.record(mean_ns.min(HISTOGRAM_MAX_NS));
                }

                let task_info = TaskInfo::new(task_id, *id, metrics, histogram.clone());
                drop(histogram);
                tasks.push(task_info);
            }
            let active_len = tasks.len();
            let ttls = opts.metrics_update_interval;

            let worker_metrics = WorkerMetrics::new(worker_id, active_len, tasks, ttls);
            queue
                .store_worker_metrics(worker_metrics, opts.metrics_update_interval)
                .await?;
            next_timer.replace(key);
        }
        TimerType::PromotedDelayed(job_id) => {
            queue
                .store
                .add_item(crate::CollectionSuffix::Wait, job_id, None, true)
                .await?;
        }
    }
    if let Some(timer) = next_timer {
        sender.send(timer);
    }
    Ok(())
}
