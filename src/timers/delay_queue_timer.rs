use crate::worker::{TaskInfo, WorkerMetrics, MIN_DELAY_MS_LIMIT};
use crate::KioResult;
use crossbeam::atomic::AtomicCell;
use crossbeam::queue::{ArrayQueue, SegQueue};
use derive_more::Debug;
use serde::{de::DeserializeOwned, Serialize};
use std::time::Duration;
use std::{
    marker::PhantomData,
    sync::{atomic::AtomicBool, Arc},
};
use tokio_util::time::DelayQueue;

// model the timers (stall_check_locck  and extend_lock) as a tokio_util::DelayQueue

#[derive(Debug, Clone, Copy, derive_more::Display)]
pub(crate) enum TimerType {
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
#[derive(Debug, Clone, Copy)]
struct PausedTimerState {
    timer: TimerType,
    deadline: Instant,
}

use tokio_util::time::delay_queue::Key;

use crate::{worker::JobMap, Queue, Store, WorkerOpts};
#[cfg(feature = "tracing")]
use tracing::{info, info_span, instrument, Span};
use xutex::AsyncMutex;
/// A Runner for both  the `stalled_check` and `lock_extension` timer that requires polling
#[derive(Clone, derive_more::Debug)]
pub struct DelayQueueTimer<D, R, P, S> {
    worker_id: uuid::Uuid,
    queue: Arc<Queue<D, R, P, S>>,
    #[debug(skip)]
    delay_queue: Arc<AsyncMutex<DelayQueue<TimerType>>>,
    jobs: JobMap<D, R, P>,
    opts: WorkerOpts,
    pause_state: Arc<ArrayQueue<PausedTimerState>>,
    // (extendLock, Stalled)
    keys: Arc<[AtomicCell<Option<Key>>; 3]>,
    close_now: Arc<AtomicBool>,
    #[cfg(feature = "tracing")]
    resource_span: Span,

    _data: PhantomData<S>,
}

impl<
        D: Clone + DeserializeOwned + 'static + Send + Serialize,
        R: Clone + DeserializeOwned + 'static + Serialize + Send + Sync,
        P: Clone + DeserializeOwned + 'static + Send + Sync + Serialize,
        S: Clone + Store<D, R, P> + Send + 'static,
    > DelayQueueTimer<D, R, P, S>
{
    pub(crate) fn new(
        jobs: JobMap<D, R, P>,
        worker_id: uuid::Uuid,
        opts: WorkerOpts,
        queue: Arc<Queue<D, R, P, S>>,
    ) -> Self {
        let state = [
            AtomicCell::default(),
            AtomicCell::default(),
            AtomicCell::default(),
        ];
        let keys = Arc::new(state);
        let pause_state = Arc::new(ArrayQueue::new(3));
        #[cfg(feature = "tracing")]
        let resource_span = info_span!("Timers");

        Self {
            #[cfg(feature = "tracing")]
            resource_span,
            worker_id,
            pause_state,
            keys,
            queue,
            delay_queue: Arc::new(AsyncMutex::new(DelayQueue::default())),
            close_now: Arc::default(),
            jobs,
            opts,
            _data: PhantomData,
        }
    }
    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(self)))]
    pub(crate) async fn insert(&self, timer: TimerType) {
        let next_duration = self.next_duration(timer);
        let key = self.delay_queue.lock().await.insert(timer, next_duration);
        self.set_key(timer, key);
        #[cfg(feature = "tracing")]
        {
            let duration = self.next_duration(timer);
            info!("Started {timer:?} timer running every {duration:?}");
        }
    }
    fn set_key(&self, timer: TimerType, key: Key) {
        match timer {
            TimerType::StalledCheck(_) => self.keys[1].swap(Some(key)),
            TimerType::ExtendLock(_) => self.keys[0].swap(Some(key)),
            TimerType::PromotedDelayed(_) => self.keys[0].swap(Some(key)),
            TimerType::CollectMetrics => self.keys[1].swap(Some(key)),
        };
    }
    pub(crate) async fn pause(&self) {
        if let Some(key) = self.keys[0].load().as_ref() {
            if let Some(expired) = self.delay_queue.lock().await.try_remove(key) {
                let deadline = expired.deadline();
                let state = PausedTimerState {
                    deadline,
                    timer: TimerType::ExtendLock(deadline),
                };
                let _ = self.pause_state.push(state);
            }
        }
        if let Some(key) = self.keys[1].load().as_ref() {
            if let Some(expired) = self.delay_queue.lock().await.try_remove(key) {
                let deadline = expired.deadline();
                let state = PausedTimerState {
                    deadline,
                    timer: TimerType::StalledCheck(deadline),
                };
                let _ = self.pause_state.push(state);
            }
        }
        if let Some(key) = self.keys[2].load().as_ref() {
            if let Some(expired) = self.delay_queue.lock().await.try_remove(key) {
                let deadline = expired.deadline();
                let state = PausedTimerState {
                    deadline,
                    timer: TimerType::CollectMetrics,
                };
                let _ = self.pause_state.push(state);
            }
        }
    }
    pub(crate) async fn resume(&self) {
        while let Some(PausedTimerState { timer, deadline }) = self.pause_state.pop() {
            self.delay_queue.lock().await.insert_at(timer, deadline);
        }
    }

    pub(crate) fn close(&self) {
        self.close_now
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
    pub(crate) async fn start_timers(&self) {
        let instant = Instant::now();
        self.insert(TimerType::ExtendLock(instant)).await;
        self.insert(TimerType::StalledCheck(instant)).await;
        self.insert(TimerType::CollectMetrics).await;
    }
    pub(crate) async fn clear(&self) {
        self.delay_queue.lock().await.clear();
    }

    const fn next_duration(&self, timer: TimerType) -> Duration {
        match timer {
            TimerType::StalledCheck(_) => Duration::from_millis(self.opts.stalled_interval),
            TimerType::ExtendLock(_) => Duration::from_millis(self.opts.lock_duration),
            TimerType::CollectMetrics => Duration::from_millis(self.opts.metrics_update_interval),
            _ => Duration::from_millis(MIN_DELAY_MS_LIMIT),
        }
    }
    #[cfg_attr(feature="tracing", instrument(parent = &self.resource_span, skip(self, _job_queue)))]
    pub(crate) async fn run(&self, _job_queue: &SegQueue<u64>) -> KioResult<()> {
        use futures::StreamExt;
        use tokio_util::time::FutureExt;
        if self.close_now.load(std::sync::atomic::Ordering::SeqCst) {
            self.clear().await;
            return Ok(());
        }
        let mut next_key = None;
        let timeout = Duration::from_millis(1);
        if let Ok(Some(expired)) = self.delay_queue.lock().await.next().timeout(timeout).await {
            let key = expired.into_inner();
            #[cfg(feature = "tracing")]
            info!("Running {key} ");
            match key {
                TimerType::StalledCheck(_) => {
                    let (_failed, _stalled) = self.queue.make_stalled_jobs_wait(&self.opts).await?;
                    next_key.replace(key);
                }
                TimerType::ExtendLock(_) => {
                    for pair in self.jobs.iter() {
                        let (job, token, _handle, _) = pair.value();

                        if let Some(id) = job.id {
                            self.queue
                                .extend_lock(id, self.opts.lock_duration, *token)
                                .await?;
                            next_key.replace(key);
                        }
                    }
                }
                TimerType::PromotedDelayed(job_id) => {
                    self.queue
                        .store
                        .add_item(crate::CollectionSuffix::Wait, job_id, None, true)
                        .await?;
                }
                TimerType::CollectMetrics => {
                    let tasks: Vec<_> = self
                        .jobs
                        .iter()
                        .map(|entry| {
                            let id = entry.key();
                            let (_, _, task_handle, monitor) = entry.value();
                            let task_id: u64 = task_handle
                                .load()
                                .as_ref()
                                .and_then(|t_handle| t_handle.id().to_string().parse().ok())
                                .unwrap_or(*id);
                            let metrics = monitor.cumulative();
                            TaskInfo::new(task_id, *id, metrics)
                        })
                        .collect();
                    let active_len = tasks.len();

                    let worker_id = self.worker_id;
                    let worker_metrics = WorkerMetrics::new(worker_id, active_len, tasks);
                    self.queue
                        .store_worker_metrics(worker_metrics, self.opts.metrics_update_interval)
                        .await?;
                    next_key.replace(key);
                }
            }
        }
        if let Some(key) = next_key {
            self.insert(key).await;
        }

        Ok(())
    }
}
