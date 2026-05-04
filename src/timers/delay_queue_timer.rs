use crate::worker::{ProcessingQueue, WorkerState, MIN_DELAY_MS_LIMIT as EVICTION_INTERVAL_MS};
use crate::worker::{TaskInfo, WorkerMetrics, HISTOGRAM_MAX_NS};
use crate::{KioError, KioResult};
use arc_swap::ArcSwapOption;
use chrono::Utc;
use crossbeam::atomic::AtomicCell;
use derive_more::{Debug, Display};
use futures::FutureExt;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{
    mpsc::{Receiver, Sender},
    oneshot,
};
use tokio::task::JoinHandle;
use tokio_util::time::FutureExt as OtherExt;
use tokio_util::{
    sync::CancellationToken,
    time::{delay_queue::Key, DelayQueue},
};
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
use tokio::time::{Instant, Timeout};

use crate::{
    worker::{JobMap, Task},
    Queue, Store, WorkerOpts,
};

#[derive(Debug)]
pub enum Cmd {
    Insert {
        timer: TimerType,
        ack: oneshot::Sender<Key>,
        duration: Duration,
    },

    Clear {
        ack: oneshot::Sender<()>,
    },
}

#[derive(Debug)]
struct SenderInner {
    tx: Sender<Cmd>,
    keys: (
        AtomicCell<Option<Key>>,
        AtomicCell<Option<Key>>,
        AtomicCell<Option<Key>>,
    ),
}
impl SenderInner {
    fn new(tx: Sender<Cmd>) -> Self {
        let keys = (
            AtomicCell::default(),
            AtomicCell::default(),
            AtomicCell::default(),
        );
        Self { tx, keys }
    }
}

use tokio::sync::{mpsc, Notify};
#[derive(Clone, Debug)]
pub struct TimerSender {
    inner: Arc<SenderInner>,
    opts: WorkerOpts,
}
impl TimerSender {
    pub fn new(tx: Sender<Cmd>, opts: WorkerOpts) -> Self {
        let inner = Arc::new(SenderInner::new(tx));
        Self { inner, opts }
    }
    pub async fn send(&self, timer: TimerType) -> KioResult<()> {
        let duration = self.next_duration(timer);
        let (ack, rx) = oneshot::channel();
        let cmd = Cmd::Insert {
            timer,
            ack,
            duration,
        };
        self.inner
            .tx
            .send(cmd)
            .await
            .map_err(std::io::Error::other)?;
        let timeout = Duration::from_millis(1);
        if let Ok(Ok(key)) = rx.timeout(timeout).await {
            self.set_key(timer, key);
        }
        Ok(())
    }
    pub async fn forward_clear(&self) -> KioResult<()> {
        let (ack, rx) = oneshot::channel();
        let clear_cmd = Cmd::Clear { ack };
        self.inner
            .tx
            .send(clear_cmd)
            .await
            .map_err(std::io::Error::other)?;
        rx.await.map_err(std::io::Error::other)?;
        Ok(())
    }
    pub const fn next_duration(&self, timer: TimerType) -> Duration {
        match timer {
            TimerType::StalledCheck(_) => Duration::from_millis(self.opts.stalled_interval),
            TimerType::ExtendLock(_) => Duration::from_millis(self.opts.lock_duration),
            TimerType::CollectMetrics => Duration::from_millis(self.opts.metrics_update_interval),
            TimerType::PromotedDelayed(_) => Duration::from_millis(EVICTION_INTERVAL_MS),
        }
    }
    pub fn set_key(&self, timer: TimerType, key: Key) {
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
    worker_state: Arc<AtomicCell<WorkerState>>,
    #[debug(skip)]
    notifier: Arc<Notify>,
    pause_schedular: Arc<AtomicCell<bool>>,
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
        worker_state: Arc<AtomicCell<WorkerState>>,
        notifier: Arc<Notify>,
        pause_schedular: Arc<AtomicCell<bool>>,
        processing: ProcessingQueue,
    ) -> Self {
        #[cfg(feature = "tracing")]
        let resource_span = info_span!("Timers");
        let start_signal: Arc<Notify> = Arc::default();
        let (tx, rx) = mpsc::channel(100000);
        let sender = TimerSender::new(tx, opts);
        let timer = Self {
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
        let task_handle = timer.create_timer_task(rx);
        timer.task_handle.store(Some(Arc::new(task_handle)));
        timer
    }
    #[cfg_attr(feature = "tracing", instrument(parent = &self.resource_span, skip(self)))]
    pub(crate) async fn insert(&self, timer: TimerType) -> KioResult<()> {
        #[cfg(feature = "tracing")]
        {
            let duration = self.sender.next_duration(timer);
            info!("Started {timer:?} timer running every {duration:?}");
        }
        self.sender.send(timer).await
    }
    //#[cfg_attr(feature="tracing", instrument(parent = &self.resource_span))]
    pub(crate) async fn clear(&self) {
        _ = self.sender.forward_clear().await;
    }

    //#[cfg_attr(feature="tracing", instrument(parent = &self.resource_span))]
    pub(crate) async fn close(&self) {
        self.clear().await;
        let task_handle = self.task_handle.swap(None);
        if let Some(task_handle) = task_handle {
            task_handle.abort();
        }
    }
    //#[cfg_attr(feature="tracing", instrument(parent = &self.resource_span))]
    fn timer_task(
        &self,
        mut rx: Receiver<Cmd>,
    ) -> impl std::future::Future<Output = KioResult<()>> {
        use futures::StreamExt;
        let processing = self.processing.clone();
        let notifier = self.notifier.clone();
        let queue = self.queue.clone();
        let start_signal = self.start_signal.clone();
        let (worker_id, opts, pause_schedular, worker_state, jobs, token, sender) = (
            self.worker_id,
            self.opts,
            self.pause_schedular.clone(),
            self.worker_state.clone(),
            self.jobs.clone(),
            self.token.clone(),
            self.sender.clone(),
        );
        async move {
            start_signal.notified().await;
            let interval_ms = EVICTION_INTERVAL_MS.cast_signed();
            #[cfg(feature = "tracing")]
            info!("starting ...");
            let mut delay_queue: DelayQueue<TimerType> = DelayQueue::new();
            while !token.is_cancelled() {
                let date_time = Utc::now();
                // tokio::try_join!(
                //     async {
                //         while let Ok(Some(expired)) = rx.receive().timeout(timeout).await {
                //         }
                //         Ok::<(), KioError>(())
                //     },
                //     async {
                //         queue.store.purge_expired().await;
                //         Ok::<(), KioError>(())
                //     }
                // )?;
                let timeout = Duration::from_millis(1);
                tokio::try_join!(
                    async {
                        tokio::select! {
                            Ok(Some(cmd))  = rx.recv().timeout(timeout) =>  {
                                match cmd {
                                    Cmd::Insert {
                                        timer,
                                        duration,
                                        ack,
                                    } => {
                                        let key = delay_queue.insert(timer, duration);
                                        let _= ack.send(key);
                                    }
                                    Cmd::Clear { ack  } => {
                                        delay_queue.clear();
                                        let _= ack.send(());
                                    }
                                }
                            },
                             Some(expired) = delay_queue.next() => {
                                let key = expired.into_inner();
                               process_timer(key, &queue, &jobs, opts, worker_id, &sender).await?;
                            },

                        }
                        Ok::<(), KioError>(())
                    },
                    queue.promote_delayed_jobs(date_time, interval_ms, &sender),
                    async {
                        queue.store.purge_expired().await;
                        Ok::<(), KioError>(())
                    }
                )?;
                if pause_schedular.load() && processing.is_empty() {
                    #[cfg(feature = "tracing")]
                    debug!("pausing ... ");
                    worker_state.store(WorkerState::Idle);
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
                    worker_state.store(WorkerState::Active);
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
    pub(crate) async fn start_timers(&self) -> KioResult<()> {
        let instant = Instant::now();
        self.insert(TimerType::ExtendLock(instant)).await?;
        self.insert(TimerType::StalledCheck(instant)).await?;
        self.insert(TimerType::CollectMetrics).await?;
        self.start_signal.notify_one();
        Ok(())
    }
    //#[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(rx, self)))]
    fn create_timer_task(&self, rx: Receiver<Cmd>) -> JoinHandle<KioResult<()>> {
        let t_task = self.timer_task(rx);
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
            for mut entry in jobs.iter_mut() {
                let (id, (_, _, task_handle, monitor, histogram)) = &mut entry.pair_mut();
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
        sender.send(timer).await?;
    }
    Ok(())
}
