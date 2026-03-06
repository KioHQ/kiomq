use derive_more::Debug;
use futures::future::{BoxFuture, Future, FutureExt};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::{self, JoinHandle};
mod delay_queue_map;
mod delay_queue_timer;
pub use delay_queue_map::TimedMap;
pub use delay_queue_timer::{DelayQueueTimer, TimerType};
pub type EmptyCb = dyn Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static;
use tokio_util::sync::CancellationToken;
/// A repeating async timer that fires a callback at a fixed interval.
///
/// Create one with [`Timer::new`], then call [`Timer::run`] to start it.
/// While running the callback is invoked after each interval tick. Use
/// [`Timer::pause`] / [`Timer::resume`] to suspend and resume without
/// stopping the timer entirely, and [`Timer::stop`] to cancel it permanently.
#[derive(Clone, Debug)]
pub struct Timer {
    interval: Duration,
    #[debug(skip)]
    callback: Arc<EmptyCb>,
    is_active: Arc<AtomicBool>,
    /// `true` when the timer has been suspended via [`Timer::pause`].
    pub paused: Arc<AtomicBool>,
    /// `true` after [`Timer::should_skip_first_tick`] has been called; causes
    /// the callback to fire before the first interval tick.
    pub skip_first_tick: Arc<AtomicBool>,
    /// Notifier used to wake the timer loop when [`Timer::resume`] is called.
    pub notifier: Arc<Notify>,
    cancel: CancellationToken,
}

impl Timer {
    /// Creates a new `Timer` that will call `cb` every `delay_ms` milliseconds.
    ///
    /// The timer is not started until [`Timer::run`] is called.
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
            cancel: CancellationToken::default(),
            skip_first_tick: Arc::default(),
        }
    }
    /// Marks the timer so that the very first interval tick is skipped,
    /// causing the callback to fire immediately on the first poll cycle.
    ///
    /// Returns `true` the first time it is called, then `false` for all
    /// subsequent calls (the flag is set atomically).
    #[must_use]
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

    /// Suspends the timer until [`Timer::resume`] is called.
    ///
    /// If the timer is already paused or not running, this is a no-op.
    pub fn pause(&self) {
        let _ = self.paused.compare_exchange(
            false,
            true,
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
        );
        // set running to false
        let _ = self.is_active.compare_exchange(
            true,
            false,
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// Starts the timer loop in a background Tokio task.
    ///
    /// Returns `Some(JoinHandle)` on success, or `None` if the timer is
    /// already running (idempotent).
    #[must_use]
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
        let is_paused = self.paused.clone();
        let notifier = self.notifier.clone();
        let task = task::spawn(async move {
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

    /// Permanently cancels the timer.
    ///
    /// Once stopped the timer cannot be restarted; create a new `Timer`
    /// if you need to run again.
    pub fn stop(&self) {
        self.cancel.cancel();
        if self.cancel.is_cancelled() {
            self.is_active
                .store(false, std::sync::atomic::Ordering::Relaxed);
        }
    }
    /// Returns `true` if the timer is currently ticking (not paused or stopped).
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.is_active.load(std::sync::atomic::Ordering::Relaxed)
    }
    /// Resumes a previously paused timer.
    ///
    /// If the timer is already running or has been stopped, this is a no-op.
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
    use std::sync::atomic::AtomicUsize;

    use super::*;
    #[tokio::test]
    async fn runs_and_stops() {
        let timer = Timer::new(100, || async { println!("hello") });
        timer.run();
        assert!(timer.is_running());

        tokio::time::sleep(Duration::from_millis(300)).await;
        timer.stop();
        assert!(!timer.is_running());
    }
    #[tokio::test]
    async fn skips_first_ticks() {
        // without the first_tick, timer runs immediately and wait for n ms, so our counter is always going to be one a head
        let counter: Arc<AtomicUsize> = Arc::default();
        let counter_clone = counter.clone();
        let timer = Timer::new(100, move || {
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
        let counter: Arc<AtomicUsize> = Arc::default();
        let counter_clone = counter.clone();
        let timer = Timer::new(100, move || {
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
