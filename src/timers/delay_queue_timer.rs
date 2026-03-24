use crate::worker::{ProcessingQueue, WorkerState, MIN_DELAY_MS_LIMIT as EVICTION_INTERVAL_MS};
use crate::worker::{TaskInfo, WorkerMetrics, HISTOGRAM_MAX_NS};
use crate::KioResult;
use arc_swap::ArcSwapOption;
use atomig::Atomic;
use derive_more::Debug;
use futures::FutureExt;
use futures_delay_queue::{delay_queue, DelayHandle, DelayQueue};
use futures_intrusive::buffer::GrowingHeapBuf;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::{
    marker::PhantomData,
    sync::{atomic::AtomicBool, Arc},
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "tracing")]
use tracing::{debug, info, info_span, instrument, Span};
use uuid::Uuid;
// model the timers (stall_check_lock,  extend_lock and job_promotion)
#[derive(Debug, Clone, Copy, derive_more::Display)]
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
    keys: [ArcSwapOption<DelayHandle>; 4],
}
impl SenderInner {
    fn new(tx: DelayQueue<TimerType, GrowingHeapBuf<TimerType>>) -> Self {
        let keys = [
            ArcSwapOption::default(),
            ArcSwapOption::default(),
            ArcSwapOption::default(),
            ArcSwapOption::default(),
        ];
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
            TimerType::StalledCheck(_) => self.inner.keys[1].swap(Some(key.into())),
            TimerType::ExtendLock(_) => self.inner.keys[0].swap(Some(key.into())),
            TimerType::CollectMetrics => self.inner.keys[2].swap(Some(key.into())),
            TimerType::PromotedDelayed(_) => self.inner.keys[3].swap(Some(key.into())),
        };
    }
}

/// A Runner for both  the `stalled_check` and `lock_extension` timer that requires polling
#[derive(Clone, derive_more::Debug)]
pub struct DelayQueueTimer<D, R, P, S> {
    pub(crate) sender: Arc<ArcSwapOption<TimerSender>>,
    #[debug(skip)]
    task_handle: Arc<ArcSwapOption<Task>>,
    #[cfg(feature = "tracing")]
    resource_span: Span,
    _data: PhantomData<(D, R, P, S)>,
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
        Self {
            start_signal,
            task_handle: Arc::default(),
            sender: Arc::default(),
            #[cfg(feature = "tracing")]
            resource_span,
            _data: PhantomData,
            queue,
            jobs,
            opts,
            worker_id,
            token: cancellation_token,
            worker_state,
            notifier,
            pause_schedular,
            processing,
        }
    }
    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(self)))]
    pub(crate) fn insert(&self, timer: TimerType) {
        if let Some(sender) = self.sender.load().as_ref() {
            #[cfg(feature = "tracing")]
            {
                let duration = sender.next_duration(timer);
                info!("Started {timer:?} timer running every {duration:?}");
            }
            sender.send(timer);
        }
    }
    //#[cfg_attr(feature="tracing", instrument(parent = &self.resource_span))]
    pub(crate) async fn clear(&self) {
        if let Some(sender) = self.sender.load().as_ref() {
            #[allow(clippy::explicit_iter_loop)]
            for stored_key in sender.inner.keys.iter() {
                if let Some(handle) = stored_key.swap(None).and_then(Arc::into_inner) {
                    let _ = handle.cancel().await;
                }
            }
        }
    }

    //#[cfg_attr(feature="tracing", instrument(parent = &self.resource_span))]
    pub(crate) fn close(&self) {
        let task_handle = self.task_handle.swap(None);
        if let Some(task_handle) = task_handle {
            task_handle.abort();
        }
    }
    //#[cfg_attr(feature="tracing", instrument(parent = &self.resource_span))]
    fn timer_task(&self) -> impl std::future::Future<Output = KioResult<()>> {
        use futures::StreamExt;
        let processing = self.processing.clone();
        let notifier = self.notifier.clone();
        let queue = self.queue.clone();
        let start_signal = self.start_signal.clone();
        let (worker_id, opts, pause_schedular, worker_state, jobs, token) = (
            self.worker_id,
            self.opts,
            self.pause_schedular.clone(),
            self.worker_state.clone(),
            self.jobs.clone(),
            self.token.clone(),
        );
        let (tx, rx) = delay_queue();
        let sender = TimerSender::new(tx, self.opts);
        self.sender.store(Some(sender.clone().into()));
        async move {
            let rx_stream = rx.into_stream();
            tokio::pin!(rx_stream);
            start_signal.notified().await;
            #[cfg(feature = "tracing")]
            info!("starting ...");
            while !token.is_cancelled() {
                tokio::select! {
                    Some(expired) = rx_stream.next() => {
                        process_timer(expired, &queue, &jobs, opts, worker_id, &sender)
                            .await?;
                    }
                     ()= queue.store.purge_expired() => {},
                }
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
                tokio::task::yield_now().await;
            }
            #[cfg(feature = "tracing")]
            info!("cancelled");
            Ok(())
        }
    }

    //#[cfg_attr(feature="tracing", instrument(parent = &self.resource_span))]
    pub(crate) fn start_timers(&self) {
        let task_handle = self.create_timer_task();
        self.task_handle.store(Some(Arc::new(task_handle)));
        self.start_signal.notify_one();
        let instant = Instant::now();
        self.insert(TimerType::ExtendLock(instant));
        self.insert(TimerType::StalledCheck(instant));
        self.insert(TimerType::CollectMetrics);
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

            let worker_metrics = WorkerMetrics::new(worker_id, active_len, tasks);
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
