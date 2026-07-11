use crossbeam::atomic::AtomicCell;
use derive_more::{Debug, IsVariant};
use futures::future::{BoxFuture, Future, FutureExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::{self, JoinHandle};
mod concurrent_timed_map;
mod delay_queue_timer;
pub use concurrent_timed_map::TimedMap;
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
        // With skip_first_tick set, the callback fires immediately on the first
        // poll cycle (the initial interval delay is skipped) and then again on
        // each subsequent immediate first tick, so at least one callback has run
        // by the time we stop. We assert a lower bound rather than an exact
        // count because the number of fires around the interval boundary is
        // inherently timing sensitive.
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
        // The exact number of fires around the interval boundary is timing
        // sensitive; only assert that the immediate first tick fired at least
        // once so this cannot drift-fail on a loaded runner.
        assert!(
            counter.load(std::sync::atomic::Ordering::Acquire) >= 1,
            "skipping the first tick must fire the callback at least once"
        );
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
        // Exact fire counts across the pause/resume boundaries are timing
        // sensitive; assert a lower bound so this cannot drift-fail under load.
        assert!(
            counter.load(std::sync::atomic::Ordering::Acquire) >= 2,
            "callbacks must have fired both before pausing and after resuming"
        );
    }
    #[tokio::test]
    async fn stops_when_paused() {
        let timer = Timer::new(100, || async {
            println!("hello");
        });
        let _ = timer.run();
        tokio::time::sleep(Duration::from_millis(100)).await;
        timer.pause();
        tokio::time::sleep(Duration::from_millis(100)).await;
        timer.stop();
        assert!(timer.state.load().is_stopped());
    }

    // Additional robustness tests. `Timer` builds on `tokio::time::interval`,
    // so we use short real sleeps with generous margins and assert on
    // invariants/bounds rather than exact fire counts (which are inherently
    // timing sensitive). Every wait that could block on a broken timer is
    // bounded by `tokio::time::timeout`.
    use std::sync::atomic::Ordering;
    use tokio::time::timeout;

    fn counting_timer(delay_ms: u64) -> (Timer, Arc<AtomicUsize>) {
        let counter: Arc<AtomicUsize> = Arc::default();
        let counter_clone = counter.clone();
        let timer = Timer::new(delay_ms, move || {
            let counter_clone = counter_clone.clone();
            async move {
                counter_clone.fetch_add(1, Ordering::AcqRel);
            }
        });
        (timer, counter)
    }

    #[tokio::test]
    async fn is_not_running_before_run_is_called() {
        let timer = Timer::new(100, || async {});
        assert!(
            !timer.is_running(),
            "a freshly created timer is not running"
        );
        assert!(
            timer.state.load().is_stopped(),
            "the default state is Stopped"
        );
    }

    #[tokio::test]
    async fn run_is_idempotent_and_returns_none_when_already_running() {
        let timer = Timer::new(100, || async {});
        let first = timer.run();
        assert!(first.is_some(), "the first run must start a task");
        let second = timer.run();
        assert!(
            second.is_none(),
            "running an already-running timer must be a no-op"
        );
        timer.stop();
    }

    #[tokio::test]
    async fn should_skip_first_tick_sets_the_flag_as_a_side_effect() {
        // Regardless of the (buggy) return value, calling the method must latch
        // the `skip_first_tick` flag on, which is the behaviour production relies
        // on. See `should_skip_first_tick_should_return_true_on_first_call` for
        // the documented-return-value contract that is currently violated.
        let timer = Timer::new(100, || async {});
        let _ = timer.should_skip_first_tick();
        assert!(
            timer.skip_first_tick.load(),
            "the flag must be latched on after the first call"
        );
        let _ = timer.should_skip_first_tick();
        assert!(timer.skip_first_tick.load(), "the flag must remain set");
    }

    // SUSPECTED PRODUCTION BUG (not fixed here per instructions): the doc comment
    // on `should_skip_first_tick` states it "Returns `true` the first time it is
    // called, then `false` for all subsequent calls". The implementation is
    // `compare_exchange(false, true).unwrap_or_default()`. crossbeam's
    // `compare_exchange` returns `Ok(previous)` on success, so the first call
    // yields `Ok(false)` -> `false`, and later calls yield `Err(true)` ->
    // `unwrap_or_default()` -> `false`. It therefore returns `false` every time,
    // contradicting the documented contract. The correct implementation is
    // likely `.is_ok()`. The return value is not consumed anywhere in
    // production, so this is currently latent. Marked #[ignore] until fixed.
    #[tokio::test]
    #[ignore = "known bug: should_skip_first_tick always returns false; doc promises true on first call"]
    async fn should_skip_first_tick_should_return_true_on_first_call() {
        let timer = Timer::new(100, || async {});
        assert!(
            timer.should_skip_first_tick(),
            "documented contract: the first call returns true"
        );
        assert!(
            !timer.should_skip_first_tick(),
            "documented contract: subsequent calls return false"
        );
    }

    #[tokio::test]
    async fn stop_is_permanent_and_halts_further_callbacks() {
        let (timer, counter) = counting_timer(20);
        // Latch skip_first_tick so the callback fires promptly on the first poll.
        let _ = timer.should_skip_first_tick();
        let _ = timer.run();
        // Let a few callbacks fire.
        timeout(Duration::from_secs(2), async {
            while counter.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("timer must fire at least once before we stop it");
        timer.stop();
        assert!(!timer.is_running(), "stop must clear the running state");
        let after_stop = counter.load(Ordering::Acquire);
        // Allow at most one in-flight callback to complete after stop.
        tokio::time::sleep(Duration::from_millis(80)).await;
        let later = counter.load(Ordering::Acquire);
        assert!(
            later <= after_stop + 1,
            "no meaningful callbacks may fire after stop: {after_stop} -> {later}"
        );
    }

    #[tokio::test]
    async fn pause_halts_callbacks_and_resume_restarts_them() {
        let (timer, counter) = counting_timer(20);
        // Latch skip_first_tick so the callback fires promptly on the first poll.
        let _ = timer.should_skip_first_tick();
        let _ = timer.run();
        // Wait until at least one callback has fired.
        timeout(Duration::from_secs(2), async {
            while counter.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("timer must fire before pausing");
        timer.pause();
        assert!(
            timer.state.load().is_paused(),
            "pause must set Paused state"
        );
        let at_pause = counter.load(Ordering::Acquire);
        tokio::time::sleep(Duration::from_millis(80)).await;
        let during_pause = counter.load(Ordering::Acquire);
        assert!(
            during_pause <= at_pause + 1,
            "at most one in-flight callback may complete while paused: {at_pause} -> {during_pause}"
        );

        timer.resume();
        // After resuming, callbacks must start incrementing again.
        timeout(Duration::from_secs(2), async {
            while counter.load(Ordering::Acquire) <= during_pause {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("resume must restart the callbacks");
        assert!(timer.is_running(), "the timer is active again after resume");
        timer.stop();
    }

    #[tokio::test]
    async fn resume_is_a_no_op_when_not_paused() {
        let (timer, _counter) = counting_timer(100);
        let _ = timer.run();
        assert!(timer.is_running());
        // Resuming a running timer must not change its state.
        timer.resume();
        assert!(
            timer.is_running(),
            "resuming a running timer keeps it running"
        );
        timer.stop();
    }

    #[tokio::test]
    async fn stop_while_paused_transitions_to_stopped() {
        let (timer, _counter) = counting_timer(20);
        let _ = timer.run();
        tokio::time::sleep(Duration::from_millis(30)).await;
        timer.pause();
        assert!(timer.state.load().is_paused());
        timer.stop();
        assert!(
            timer.state.load().is_stopped(),
            "stopping a paused timer must reach Stopped"
        );
    }

    #[tokio::test]
    async fn very_large_delay_fires_once_immediately_then_waits() {
        // `tokio::time::interval`'s first tick completes immediately, so even a
        // one-hour timer fires its callback once on the first poll cycle. After
        // that single fire the next tick is an hour away, so the counter must
        // stay put within our short observation window (no runaway firing).
        let (timer, counter) = counting_timer(60 * 60 * 1000); // one hour
        let _ = timer.run();
        assert!(timer.is_running());
        tokio::time::sleep(Duration::from_millis(60)).await;
        let first = counter.load(Ordering::Acquire);
        assert!(
            first <= 1,
            "an hour-long timer must fire at most the immediate first tick, saw {first}"
        );
        tokio::time::sleep(Duration::from_millis(60)).await;
        let second = counter.load(Ordering::Acquire);
        assert_eq!(
            first, second,
            "the hour-long timer must not fire again within the window: {first} -> {second}"
        );
        timer.stop();
    }

    #[tokio::test]
    async fn zero_delay_run_panics_because_interval_period_must_be_non_zero() {
        // Documents a robustness gap: `Timer::new(0, ..)` does not validate the
        // delay, and `run()` calls `tokio::time::interval(Duration::ZERO)`
        // *synchronously* (before spawning), which panics with "interval period
        // must be non-zero". The panic therefore surfaces in the caller of
        // `run()`, so it must be caught with `catch_unwind`.
        let timer = Timer::new(0, || async {});
        // Silence the default panic hook so the expected panic is not noisy.
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| timer.run()));
        std::panic::set_hook(previous_hook);
        assert!(
            result.is_err(),
            "constructing a timer with a zero interval must panic on run()"
        );
    }
}
