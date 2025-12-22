use derive_more::Debug;
use futures::future::{BoxFuture, Future, FutureExt};
use std::sync::Arc;
use std::time::Duration;
use std::{cell::RefCell, sync::atomic::AtomicBool};
use tokio::sync::Notify;
use tokio::{
    task::{self, JoinHandle},
    time::{sleep, Instant},
};
mod delay_queue_map;
mod delay_queue_timer;
pub(crate) use delay_queue_map::TimedMap;
pub(crate) use delay_queue_timer::{DelayQueueTimer, TimerType};
pub type EmptyCb = dyn Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static;
use tokio_util::sync::CancellationToken;
#[derive(Clone, Debug)]
pub struct Timer {
    interval: Duration,
    #[debug(skip)]
    callback: Arc<EmptyCb>,
    is_active: Arc<AtomicBool>,
    pub paused: Arc<AtomicBool>,
    pub skip_first_tick: Arc<AtomicBool>,
    pub notifier: Arc<Notify>,
    cancel: CancellationToken,
}

impl Timer {
    pub fn new<C, F>(delay_ms: u64, cb: C) -> Self
    where
        C: Fn() -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        let interval = Duration::from_millis(delay_ms);
        #[allow(clippy::redundant_closure)]
        let parsed_cb = move || cb().boxed();
        let is_active = Arc::default();
        let paused = Arc::default();
        let notifier = Arc::default();
        Self {
            notifier,
            paused,
            is_active,
            interval,
            callback: Arc::new(parsed_cb),
            cancel: Default::default(),
            skip_first_tick: Arc::default(),
        }
    }
    pub fn should_skip_first_tick(&self) -> bool {
        self.skip_first_tick
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            )
            .unwrap_or_default()
    }

    pub fn pause(&self) {
        self.paused.compare_exchange(
            false,
            true,
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
        );
        // set running to false
        self.is_active.compare_exchange(
            true,
            false,
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    pub fn run(&self) -> Option<JoinHandle<()>> {
        if self.is_running() {
            return None;
        }
        let mut interval = tokio::time::interval(self.interval);
        let callback = Arc::clone(&self.callback);
        let token = self.cancel.clone();
        let skip_first_tick = self
            .skip_first_tick
            .load(std::sync::atomic::Ordering::Relaxed);
        let is_active = self.is_active.clone();
        let is_paused = self.paused.clone();
        let notifier = self.notifier.clone();
        let mut task = task::spawn(async move {
            // wait for the first tick to ensure the initial delay;
            if !skip_first_tick {
                interval.tick().await;
            }
            while !token.is_cancelled() {
                if is_paused.load(std::sync::atomic::Ordering::Relaxed) {
                    notifier.notified().await;
                }
                callback().await;
                interval.tick().await;
            }
        });
        self.is_active
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Some(task)
    }

    pub fn stop(&self) {
        self.cancel.cancel();
        if self.cancel.is_cancelled() {
            self.is_active
                .store(false, std::sync::atomic::Ordering::Relaxed);
        }
    }
    pub fn is_running(&self) -> bool {
        self.is_active.load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn resume(&self) {
        if self.is_running() || !self.paused.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }

        self.notifier.notify_waiters();
        self.is_active
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.paused
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI8, AtomicUsize};

    use super::*;
    #[tokio::test]
    async fn runs_and_stops() {
        let now = tokio::time::Instant::now();
        let mut timer = Timer::new(100, || async { println!("hello") });
        timer.run();
        assert!(timer.is_running());

        tokio::time::sleep(Duration::from_millis(300)).await;
        timer.stop();
        assert!(!timer.is_running());
        println!("{:?}", now.elapsed());
    }
    #[tokio::test]
    async fn skips_first_ticks() {
        // without the first_tick, timer runs immediately and wait for n ms, so our counter is always going to be one a head
        let now = tokio::time::Instant::now();
        let counter: Arc<AtomicUsize> = Arc::default();
        let counter_clone = counter.clone();
        let mut timer = Timer::new(100, move || {
            let counter_clone = counter_clone.clone();
            async move {
                println!("hello");
                counter_clone.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            }
        });
        timer.should_skip_first_tick();
        timer.run();
        assert!(timer
            .skip_first_tick
            .load(std::sync::atomic::Ordering::Acquire));

        tokio::time::sleep(Duration::from_millis(100)).await;
        timer.stop();
        assert!(!timer.is_running());
        assert_eq!(counter.load(std::sync::atomic::Ordering::Acquire), 2);
    }
    #[tokio::test]
    async fn can_pause_and_resume() {
        let now = tokio::time::Instant::now();
        let counter: Arc<AtomicUsize> = Arc::default();
        let counter_clone = counter.clone();
        let mut timer = Timer::new(100, move || {
            let counter_clone = counter_clone.clone();
            async move {
                println!("hello");
                counter_clone.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            }
        });
        timer.should_skip_first_tick();
        timer.run();
        assert!(timer
            .skip_first_tick
            .load(std::sync::atomic::Ordering::Acquire));

        tokio::time::sleep(Duration::from_millis(100)).await;
        timer.pause();
        assert!(!timer.is_running());
        tokio::time::sleep(Duration::from_millis(100)).await;
        timer.resume();
        assert!(timer.is_running());
        assert_eq!(counter.load(std::sync::atomic::Ordering::Acquire), 2);
    }
}
