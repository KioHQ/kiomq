use crate::worker::MIN_DELAY_MS_LIMIT;
use crate::KioResult;
use crossbeam_queue::{ArrayQueue, SegQueue};
use crossbeam_utils::atomic::AtomicCell;
use derive_more::Debug;
use serde::{de::DeserializeOwned, Serialize};
use std::time::Duration;
use std::{
    marker::PhantomData,
    sync::{atomic::AtomicBool, Arc},
};
use tokio::sync::Mutex;
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
}
use tokio::time::Instant;
#[derive(Debug, Clone, Copy)]
struct PausedTimerState {
    timer: TimerType,
    deadline: Instant,
}

use arc_swap::ArcSwapOption;
use tokio_util::time::delay_queue::Key;

use crate::{worker::JobMap, Queue, Store, WorkerOpts};
use tracing::{info, info_span, instrument, Span};
use xutex::AsyncMutex;
/// A Runner for both  the stalled_check and lock_extension timer that requires polling
#[derive(Clone, derive_more::Debug)]
pub(crate) struct DelayQueueTimer<D, R, P, S> {
    queue: Arc<Queue<D, R, P, S>>,
    #[debug(skip)]
    delay_queue: Arc<AsyncMutex<DelayQueue<TimerType>>>,
    jobs: JobMap<D, R, P>,
    opts: WorkerOpts,
    pause_state: Arc<ArrayQueue<PausedTimerState>>,
    // (extendLock, Stalled)
    keys: Arc<[AtomicCell<Option<Key>>; 2]>,
    close_now: Arc<AtomicBool>,
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
    pub fn new(jobs: JobMap<D, R, P>, opts: WorkerOpts, queue: Arc<Queue<D, R, P, S>>) -> Self {
        let state = [AtomicCell::default(), AtomicCell::default()];
        let keys = Arc::new(state);
        let pause_state = Arc::new(ArrayQueue::new(2));
        let resource_span = info_span!("Timers");

        Self {
            resource_span,
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
    #[instrument(parent = &self.resource_span, skip(self))]
    pub async fn insert(&self, timer: TimerType) {
        let next_duration = self.next_duration(timer);
        let key = self.delay_queue.lock().await.insert(timer, next_duration);
        self.set_key(timer, key);
        let duration = self.next_duration(timer);
        info!("Started {timer:?} timer running every {duration:?}");
    }
    fn set_key(&self, timer: TimerType, key: Key) {
        match timer {
            TimerType::StalledCheck(_) => self.keys[1].swap(Some(key)),
            TimerType::ExtendLock(_) => self.keys[0].swap(Some(key)),
            TimerType::PromotedDelayed(_) => self.keys[0].swap(Some(key)),
        };
    }
    pub async fn pause(&self) {
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
    }
    pub async fn resume(&self) {
        while let Some(PausedTimerState { timer, deadline }) = self.pause_state.pop() {
            self.delay_queue.lock().await.insert_at(timer, deadline);
        }
    }

    pub fn close(&self) {
        self.close_now
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
    pub async fn start_timers(&self) {
        let instant = Instant::now();
        self.insert(TimerType::ExtendLock(instant)).await;
        self.insert(TimerType::StalledCheck(instant)).await;
    }
    pub async fn clear(&self) {
        self.delay_queue.lock().await.clear()
    }

    fn next_duration(&self, timer: TimerType) -> Duration {
        match timer {
            TimerType::StalledCheck(_) => Duration::from_millis(self.opts.stalled_interval),
            TimerType::ExtendLock(_) => Duration::from_millis(self.opts.lock_duration),
            _ => Duration::from_millis(MIN_DELAY_MS_LIMIT),
        }
    }
    #[instrument(parent = &self.resource_span, skip(self, job_queue))]
    pub async fn run(&self, job_queue: &SegQueue<u64>) -> KioResult<()> {
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
            info!("Running {key} ");
            match key {
                TimerType::StalledCheck(_) => {
                    let (_failed, _stalled) = self.queue.make_stalled_jobs_wait(&self.opts).await?;
                    next_key.replace(key);
                }
                TimerType::ExtendLock(_) => {
                    for pair in self.jobs.iter() {
                        let (job, token, handle) = pair.value();

                        if let Some(id) = job.id {
                            let done = self
                                .queue
                                .extend_lock(id, self.opts.lock_duration, *token)
                                .await;
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
            }
        }
        if let Some(key) = next_key {
            self.insert(key).await;
        }

        Ok(())
    }
}
