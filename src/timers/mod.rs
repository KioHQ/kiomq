use crossbeam::atomic::AtomicCell;
use derive_more::{Debug, IsVariant};
use futures::future::{BoxFuture, Future, FutureExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::{self, JoinHandle};
mod delay_queue_map;
mod delay_queue_timer;
pub use delay_queue_map::TimedMap;
pub use delay_queue_timer::DelayQueueTimer;
pub use delay_queue_timer::{TimerSender, TimerType};
pub type EmptyCb = dyn Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static;
#[derive(Debug, Copy, Clone, IsVariant, Default)]
pub enum TimerState {
    #[default]
    Stopped,
    Active,
    Paused,
}
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
    pub(crate) state: Arc<AtomicCell<TimerState>>,
    /// `true` after [`Timer::should_skip_first_tick`] has been called; causes
    /// the callback to fire before the first interval tick.
    pub skip_first_tick: Arc<AtomicCell<bool>>,
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
        let state = Arc::default();
        let notifier = Arc::default();
        Self {
            notifier,
            state,
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
            .compare_exchange(false, true)
            .unwrap_or_default()
    }

    /// Suspends the timer until [`Timer::resume`] is called.
    ///
    /// If the timer is already paused or not running, this is a no-op.
    pub fn pause(&self) {
        if self.state.load().is_paused() {
            return;
        }
        self.state.store(TimerState::Paused);
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
        let skip_first_tick = self.skip_first_tick.load();
        let notifier = self.notifier.clone();
        let state = self.state.clone();
        let task = task::spawn(async move {
            // wait for the first tick to ensure the initial delay;
            if !skip_first_tick {
                interval.tick().await;
            }
            while !token.is_cancelled() {
                if state.load().is_paused() {
                    if token
                        .run_until_cancelled(notifier.notified())
                        .await
                        .is_none()
                    {
                        state.store(TimerState::Stopped);
                        break;
                    }

                    state.store(TimerState::Active);
                }
                callback().await;
                interval.tick().await;
            }
        });
        self.state.store(TimerState::Active);
        Some(task)
    }

    /// Permanently cancels the timer.
    ///
    /// Once stopped the timer cannot be restarted; create a new `Timer`
    /// if you need to run again.
    pub fn stop(&self) {
        self.cancel.cancel();
        if self.cancel.is_cancelled() {
            self.state.store(TimerState::Stopped);
        }
    }
    /// Returns `true` if the timer is currently ticking (not paused or stopped).
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.state.load().is_active()
    }
    /// Resumes a previously paused timer.
    ///
    /// If the timer is already running or has been stopped, this is a no-op.
    pub fn resume(&self) {
        if matches!(self.state.load(), TimerState::Active | TimerState::Stopped) {
            return;
        }
        self.notifier.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;
    #[tokio::test]
    async fn runs_and_stops() {
        let timer = Timer::new(100, || async { println!("hello") });
        let _ = timer.run();
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
        let _ = timer.should_skip_first_tick();
        let _ = timer.run();
        assert!(timer.skip_first_tick.load());

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
        let _ = timer.should_skip_first_tick();
        let _ = timer.run();
        assert!(timer.skip_first_tick.load());

        tokio::time::sleep(Duration::from_millis(100)).await;
        timer.pause();
        assert!(timer.state.load().is_paused());
        tokio::time::sleep(Duration::from_millis(100)).await;
        timer.resume();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(timer.is_running());
        assert_eq!(counter.load(std::sync::atomic::Ordering::Acquire), 4);
    }
    #[tokio::test]
    async fn stops_when_paused() {
        let timer = Timer::new(100, || async {
            println!("hello");
        });
        let state = timer.state.clone();
        let _ = timer.run();
        dbg!(state.load());
        tokio::time::sleep(Duration::from_millis(100)).await;
        timer.pause();
        dbg!(state.load());
        tokio::time::sleep(Duration::from_millis(100)).await;
        timer.stop();
        dbg!(state.load());
        assert!(timer.state.load().is_stopped());
    }
}
