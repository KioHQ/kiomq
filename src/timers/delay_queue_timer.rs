use crate::worker::WorkerState;
use crate::worker::{TaskInfo, WorkerMetrics, HISTOGRAM_MAX_NS, MIN_DELAY_MS_LIMIT};
use crate::{KioError, KioResult};
use atomig::Atomic;
use chrono::Utc;
use crossbeam::atomic::AtomicCell;
use derive_more::Debug;
#[cfg(not(feature = "tracing"))]
use futures::FutureExt;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::{
    marker::PhantomData,
    sync::{atomic::AtomicBool, Arc},
};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::time::DelayQueue;
use uuid::Uuid;

// model the timers (stall_check_locck  and extend_lock) as a tokio_util::DelayQueue

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
        Duration::from_millis(MIN_DELAY_MS_LIMIT)
    )]
    PromotedDelayed(u64),
    CollectMetrics,
}
use tokio::time::Instant;
use tokio_util::time::delay_queue::Key;

use crate::{
    worker::{JobMap, Task},
    Queue, Store, WorkerOpts,
};

use tokio::sync::Notify;
#[cfg(feature = "tracing")]
use tracing::{debug, info, info_span, instrument, Span};
pub enum Cmd {
    Insert {
        timer: TimerType,
        duration: Duration,
    },
    Remove {
        key: Key,
    },
}
#[derive(Clone, Debug)]
pub struct TimerSender {
    inner_tx: UnboundedSender<Cmd>,
    // (extendLock, Stalled)
    keys: Arc<[AtomicCell<Option<Key>>; 3]>,
    opts: WorkerOpts,
}
impl TimerSender {
    pub fn new(tx: UnboundedSender<Cmd>, opts: WorkerOpts) -> Self {
        let state = [
            AtomicCell::default(),
            AtomicCell::default(),
            AtomicCell::default(),
        ];
        let keys = Arc::new(state);
        Self {
            inner_tx: tx,
            keys,
            opts,
        }
    }
    pub fn send(&self, cmd: Cmd) {
        let _ = self.inner_tx.send(cmd);
    }
    pub const fn next_duration(&self, timer: TimerType) -> Duration {
        match timer {
            TimerType::StalledCheck(_) => Duration::from_millis(self.opts.stalled_interval),
            TimerType::ExtendLock(_) => Duration::from_millis(self.opts.lock_duration),
            TimerType::CollectMetrics => Duration::from_millis(self.opts.metrics_update_interval),
            TimerType::PromotedDelayed(_) => Duration::from_millis(MIN_DELAY_MS_LIMIT),
        }
    }
    pub fn set_key(&self, timer: TimerType, key: Key) {
        match timer {
            TimerType::StalledCheck(_) => self.keys[1].swap(Some(key)),
            TimerType::ExtendLock(_) => self.keys[0].swap(Some(key)),
            TimerType::CollectMetrics => self.keys[2].swap(Some(key)),
            TimerType::PromotedDelayed(_) => None,
        };
    }
}

/// A Runner for both  the `stalled_check` and `lock_extension` timer that requires polling
#[derive(Clone, derive_more::Debug)]
pub struct DelayQueueTimer<D, R, P, S> {
    sender: TimerSender,
    #[debug(skip)]
    task_handle: Arc<Task>,
    #[cfg(feature = "tracing")]
    resource_span: Span,
    _data: PhantomData<(D, R, P, S)>,
    start_signal: Arc<Notify>,
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
    ) -> Self {
        #[cfg(feature = "tracing")]
        let resource_span = info_span!("Timers");
        let (tx, rx) = mpsc::unbounded_channel();
        let sender = TimerSender::new(tx, opts);
        let start_signal: Arc<Notify> = Arc::default();
        let task_handle = Self::create_timer_task(
            sender.clone(),
            rx,
            queue,
            jobs,
            opts,
            worker_id,
            cancellation_token,
            worker_state,
            notifier,
            pause_schedular,
            start_signal.clone(),
        );

        Self {
            start_signal,
            task_handle: task_handle.into(),
            sender,
            #[cfg(feature = "tracing")]
            resource_span,
            _data: PhantomData,
        }
    }
    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(self)))]
    #[allow(clippy::future_not_send)]
    pub(crate) fn insert(&self, timer: TimerType) {
        let next_duration = self.sender.next_duration(timer);
        #[cfg(feature = "tracing")]
        {
            let duration = self.sender.next_duration(timer);
            info!("Started {timer:?} timer running every {duration:?}");
        }
        self.sender.send(Cmd::Insert {
            timer,
            duration: next_duration,
        });
    }
    #[allow(clippy::future_not_send)]
    pub(crate) fn clear(&self) {
        for stored_key in self.sender.keys.iter() {
            if let Some(key) = stored_key.load() {
                self.sender.send(Cmd::Remove { key });
            }
        }
    }

    pub(crate) fn close(&self) {
        self.task_handle.abort();
    }
    #[allow(clippy::future_not_send)]
    pub(crate) fn start_timers(&self) {
        self.start_signal.notify_one();
        let instant = Instant::now();
        self.insert(TimerType::ExtendLock(instant));
        self.insert(TimerType::StalledCheck(instant));
        self.insert(TimerType::CollectMetrics);
    }
    //#[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(rx, sender)))]
    #[allow(clippy::future_not_send)]
    #[allow(clippy::too_many_arguments)]
    fn create_timer_task(
        sender: TimerSender,
        mut rx: UnboundedReceiver<Cmd>,
        queue: Arc<Queue<D, R, P, S>>,
        jobs: JobMap<D, R, P>,
        opts: WorkerOpts,
        worker_id: Uuid,
        token: Arc<CancellationToken>,
        worker_state: Arc<Atomic<WorkerState>>,
        notifier: Arc<Notify>,
        pause_schedular: Arc<AtomicBool>,
        start_signal: Arc<Notify>,
    ) -> JoinHandle<KioResult<()>> {
        use futures::StreamExt;
        let t_task = async move {
            let mut delay_queue: DelayQueue<TimerType> = DelayQueue::new();

            let interval_ms = i64::try_from(MIN_DELAY_MS_LIMIT).unwrap_or(i64::MAX);
            // let mut tick_iternval =
            //     tokio::time::interval(Duration::from_millis(interval_ms.cast_unsigned()));
            //let timeout = Duration::from_millis(1);
            start_signal.notified().await;
            #[cfg(feature = "tracing")]
            info!("starting ...");
            while !token.is_cancelled() {
                let date_time = Utc::now();
                tokio::try_join!(
                    //tick_iternval.tick().await;
                    queue.promote_delayed_jobs(date_time, interval_ms, sender.clone()),
                    async {
                        tokio::select! {
                            Some(cmd)  = rx.recv() =>  {
                                match cmd {
                                    Cmd::Insert {
                                        timer,
                                        duration,
                                    } => {
                                        let key = delay_queue.insert(timer, duration);
                                        sender.set_key(timer, key);
                                    }
                                    Cmd::Remove { key } => {
                                          let _ = delay_queue.try_remove(&key);
                                    }
                                }
                            },
                             Some(expired) = delay_queue.next()=> {
                                let key = expired.into_inner();
                                process_timer(
                                    key,
                                    queue.clone(),
                                    jobs.clone(),
                                    opts,
                                    worker_id,
                                    sender.clone(),
                                )
                                .await?;
                            },
                            ()  =  queue.store.purge_expired() => {},

                        }
                        Ok::<(), KioError>(())
                    }
                )?;
                if pause_schedular.load(Ordering::Acquire) && jobs.is_empty() {
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
        };
        #[cfg(feature = "tracing")]
        let sub_span = info_span!(parent: None, "timer_and_clean_up_task");
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
#[allow(clippy::future_not_send)]
async fn process_timer<D, R, P, S>(
    key: TimerType,
    queue: Arc<Queue<D, R, P, S>>,
    jobs: JobMap<D, R, P>,
    opts: WorkerOpts,
    worker_id: Uuid,
    sender: TimerSender,
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
                    next_timer.replace(key);
                }
            }
        }
        TimerType::PromotedDelayed(job_id) => {
            queue
                .store
                .add_item(crate::CollectionSuffix::Wait, job_id, None, true)
                .await?;
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
    }
    if let Some(timer) = next_timer {
        let duration = sender.next_duration(timer);

        sender.send(Cmd::Insert { timer, duration });
    }
    Ok(())
}
