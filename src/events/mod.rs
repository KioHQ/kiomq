use crate::{FailedDetails, JobMetrics, JobState};
use compact_str::CompactString;
use derive_more::Debug;
use uuid::Uuid;
/// The payload delivered to event listeners registered on a [`Queue`](crate::Queue).
///
/// Each variant corresponds to a [`JobState`](crate::JobState) transition or
/// observability event.  Subscribe via [`Queue::on`](crate::Queue::on) or
/// [`Queue::on_all_events`](crate::Queue::on_all_events).
///
/// # Examples
///
/// ```rust
/// # #[tokio::main]
/// # async fn main() -> kiomq::KioResult<()> {
/// use kiomq::{EventParameters, InMemoryStore, JobState, Queue};
///
/// let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "evt-demo");
/// let queue = Queue::new(store, None).await?;
///
/// queue.on_all_events(|evt: EventParameters<u64, ()>| async move {
///     match evt {
///         EventParameters::Completed { job_id, .. } => {
///             println!("job {job_id} completed");
///         }
///         EventParameters::Failed { job_id, reason, .. } => {
///             println!("job {job_id} failed: {}", reason.reason);
///         }
///         _ => {}
///     }
/// });
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub enum EventParameters<R, P> {
    /// A job was moved into the priority sorted-set.
    Prioritized {
        /// Numeric job ID.
        job_id: u64,
        /// Job name.
        name: Option<CompactString>,
        /// Assigned priority score.
        priority: u64,
    },
    /// A new job was added to the queue.
    Added {
        /// Numeric job ID.
        job_id: u64,
        /// Job name.
        name: Option<CompactString>,
    },
    /// A delayed or stalled job is now waiting to be processed.
    WaitingToRun {
        /// Numeric job ID.
        job_id: u64,
        /// The state the job was in before transitioning to wait.
        prev_state: Option<JobState>,
    },
    /// A job has been moved to the delayed state.
    Delayed {
        /// Numeric job ID.
        job_id: u64,
        /// How long the job will wait before becoming eligible.
        delay: Duration,
    },
    /// A job has been picked up by a worker and is now active.
    Active {
        /// Numeric job ID.
        job_id: u64,
        /// The state the job was in before becoming active.
        prev_state: Option<JobState>,
    },
    /// A job finished successfully.
    Completed {
        /// Numeric job ID.
        job_id: u64,
        /// Timing and attempt statistics.
        job_metrics: JobMetrics,
        /// How far in advance this run was scheduled (0 for non-delayed jobs).
        expected_delay: Duration,
        /// The state the job was in before completing.
        prev_state: Option<JobState>,
        /// The value returned by the processor.
        #[debug(skip)]
        result: R,
    },
    /// A placeholder event with no meaningful payload (e.g. queue drained).
    Void,
    /// A progress update reported by the processor.
    Progress {
        /// Numeric job ID.
        job_id: u64,
        /// The progress value emitted by the processor.
        #[debug(skip)]
        data: P,
    },
    /// A job was detected as stalled and moved for recovery.
    Stalled {
        /// Numeric job ID.
        job_id: u64,
        /// The state the job was in before stalling.
        prev_state: JobState,
    },
    /// A job permanently failed.
    Failed {
        /// Failure details (reason and run count).
        reason: FailedDetails,
        /// Numeric job ID.
        job_id: u64,
        /// The state the job was in before failing.
        prev_state: JobState,
    },
    /// A worker started or finished processing a job.
    Processing {
        /// The worker that picked up the job.
        worker_id: Uuid,
        /// Numeric job ID.
        job_id: u64,
        /// The job's new state.
        status: JobState,
    },
}
use serde::de::DeserializeOwned;
use std::{sync::Arc, time::Duration};
use typed_emitter::TypedEmitter;
pub type Emitter<R, P> = TypedEmitter<JobState, EventParameters<R, P>>;
pub type EventEmitter<R, P> = Arc<Emitter<R, P>>;
mod redis_events;
pub use redis_events::QueueStreamEvent;

use crate::KioResult;
impl<R: DeserializeOwned, P: DeserializeOwned> EventParameters<R, P> {
    /// Converts a raw store event into the corresponding typed [`EventParameters`] variant.
    ///
    /// # Errors
    ///
    /// Returns [`KioError`](crate::KioError) if the event payload cannot be decoded.
    ///
    /// # Panics
    ///
    /// Panics if a `Completed` event has no returned value, or a `Progress` event has no data.
    pub fn from_queue_event(event: QueueStreamEvent<R, P>) -> KioResult<Self> {
        let job_state = event.event;
        let job_id = event.job_id;
        let parameter = match job_state {
            JobState::Prioritized => Self::Prioritized {
                job_id: event.job_id,
                name: event.name,
                priority: event.priority.unwrap_or_default(),
            },
            JobState::Wait if event.prev.is_none() => Self::Added {
                job_id: event.job_id,
                name: event.name,
            },
            JobState::Wait => Self::WaitingToRun {
                job_id: event.job_id,
                prev_state: event.prev,
            },
            JobState::Stalled => Self::Stalled {
                job_id: event.job_id,
                prev_state: event.prev.unwrap_or_default(),
            },
            JobState::Active => Self::Active {
                job_id,
                prev_state: event.prev,
            },
            JobState::Paused | JobState::Resumed | JobState::Obliterated => Self::Void,
            JobState::Completed => {
                let job_metrics = event.metrics.unwrap_or_default();
                Self::Completed {
                    job_metrics,
                    job_id,
                    prev_state: event.prev,
                    expected_delay: Duration::from_millis(job_metrics.delay),
                    result: event.returned_value.expect("there is no result"),
                }
            }
            JobState::Failed => Self::Failed {
                reason: event.failed_reason.unwrap_or_default(),
                job_id: event.job_id,
                prev_state: event.prev.unwrap_or_default(),
            },
            JobState::Delayed => Self::Delayed {
                job_id: event.job_id,
                delay: Duration::from_millis(event.delay.unwrap_or_default()),
            },
            JobState::Progress => Self::Progress {
                job_id: event.job_id,
                data: event.progress_data.expect("expecting a value"),
            },
            JobState::Processing => Self::Processing {
                worker_id: event.worker_id.unwrap_or_default(),
                job_id: event.job_id,
                status: event.prev.unwrap_or_default(),
            },
        };

        Ok(parameter)
    }
}

#[cfg(test)]
mod events_tests {
    #![allow(clippy::unused_async)]
    use super::{Emitter, EventParameters, QueueStreamEvent};
    use crate::{FailedDetails, JobMetrics, JobState};
    use compact_str::ToCompactString;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use uuid::Uuid;

    // A short, generous bound for any await so a mis-behaving emitter cannot
    // hang the suite (TigerStyle: never block unboundedly).
    const AWAIT_BOUND: Duration = Duration::from_secs(5);

    fn event_with(state: JobState, job_id: u64) -> QueueStreamEvent<u64, u64> {
        QueueStreamEvent {
            event: state,
            job_id,
            ..Default::default()
        }
    }

    #[test]
    fn every_variant_can_be_constructed_and_matched() {
        let metrics = JobMetrics::default();
        let variants: Vec<EventParameters<u64, u64>> = vec![
            EventParameters::Prioritized {
                job_id: 1,
                name: Some("alpha".to_compact_string()),
                priority: 7,
            },
            EventParameters::Added {
                job_id: 2,
                name: None,
            },
            EventParameters::WaitingToRun {
                job_id: 3,
                prev_state: Some(JobState::Delayed),
            },
            EventParameters::Delayed {
                job_id: 4,
                delay: Duration::from_millis(250),
            },
            EventParameters::Active {
                job_id: 5,
                prev_state: None,
            },
            EventParameters::Completed {
                job_id: 6,
                job_metrics: metrics,
                expected_delay: Duration::ZERO,
                prev_state: Some(JobState::Active),
                result: 42,
            },
            EventParameters::Void,
            EventParameters::Progress {
                job_id: 7,
                data: 99,
            },
            EventParameters::Stalled {
                job_id: 8,
                prev_state: JobState::Active,
            },
            EventParameters::Failed {
                reason: FailedDetails::default(),
                job_id: 9,
                prev_state: JobState::Active,
            },
            EventParameters::Processing {
                worker_id: Uuid::nil(),
                job_id: 10,
                status: JobState::Active,
            },
        ];
        assert_eq!(variants.len(), 11, "expected one of every variant");

        for variant in &variants {
            // Exhaustive match (no wildcard) forces every variant to be covered.
            let rendered = format!("{variant:?}");
            assert!(!rendered.is_empty(), "Debug output must not be empty");
            match variant {
                EventParameters::Prioritized { priority, .. } => assert_eq!(*priority, 7),
                EventParameters::Added { job_id, name } => {
                    assert_eq!(*job_id, 2);
                    assert!(name.is_none());
                }
                EventParameters::WaitingToRun { prev_state, .. } => {
                    assert_eq!(*prev_state, Some(JobState::Delayed));
                }
                EventParameters::Delayed { delay, .. } => {
                    assert_eq!(*delay, Duration::from_millis(250));
                }
                EventParameters::Active { prev_state, .. } => assert!(prev_state.is_none()),
                EventParameters::Completed { result, .. } => assert_eq!(*result, 42),
                EventParameters::Void => {}
                EventParameters::Progress { data, .. } => assert_eq!(*data, 99),
                EventParameters::Stalled { prev_state, .. } => {
                    assert_eq!(*prev_state, JobState::Active);
                }
                EventParameters::Failed { job_id, .. } => assert_eq!(*job_id, 9),
                EventParameters::Processing { worker_id, .. } => assert!(worker_id.is_nil()),
            }
        }
    }

    #[test]
    fn clone_preserves_payload() {
        let original = EventParameters::<u64, u64>::Completed {
            job_id: 11,
            job_metrics: JobMetrics::default(),
            expected_delay: Duration::from_millis(5),
            prev_state: Some(JobState::Active),
            result: 1234,
        };
        let cloned = original.clone();
        match (&original, &cloned) {
            (
                EventParameters::Completed {
                    job_id: orig_id,
                    result: orig_result,
                    expected_delay: orig_delay,
                    ..
                },
                EventParameters::Completed {
                    job_id,
                    result,
                    expected_delay,
                    ..
                },
            ) => {
                assert_eq!(*job_id, 11);
                assert_eq!(*result, 1234);
                assert_eq!(*expected_delay, Duration::from_millis(5));
                assert_eq!(orig_id, job_id);
                assert_eq!(orig_result, result);
                assert_eq!(orig_delay, expected_delay);
            }
            other => panic!("clone changed the variant: {other:?}"),
        }
    }

    #[test]
    fn debug_skips_opaque_result_and_progress_payloads() {
        // `result` and `data` carry `#[debug(skip)]`, so a distinctive sentinel
        // value must never leak into the Debug rendering.
        let completed = EventParameters::<u64, u64>::Completed {
            job_id: 1,
            job_metrics: JobMetrics::default(),
            expected_delay: Duration::ZERO,
            prev_state: None,
            result: 987_654_321,
        };
        let rendered = format!("{completed:?}");
        assert!(rendered.contains("Completed"));
        assert!(
            !rendered.contains("987654321"),
            "opaque result payload must be skipped in Debug, got: {rendered}"
        );

        let progress = EventParameters::<u64, u64>::Progress {
            job_id: 2,
            data: 123_456_789,
        };
        let rendered = format!("{progress:?}");
        assert!(rendered.contains("Progress"));
        assert!(
            !rendered.contains("123456789"),
            "opaque progress payload must be skipped in Debug, got: {rendered}"
        );
    }

    #[test]
    fn from_queue_event_maps_prioritized_with_priority_default() {
        // priority `None` falls back to 0.
        let none =
            EventParameters::<u64, u64>::from_queue_event(event_with(JobState::Prioritized, 1))
                .expect("prioritized conversion must succeed");
        assert!(matches!(
            none,
            EventParameters::Prioritized {
                priority: 0,
                job_id: 1,
                ..
            }
        ));

        let mut with_priority = event_with(JobState::Prioritized, 2);
        with_priority.priority = Some(55);
        with_priority.name = Some("named".to_compact_string());
        let some = EventParameters::<u64, u64>::from_queue_event(with_priority).unwrap();
        match some {
            EventParameters::Prioritized {
                priority,
                job_id,
                name,
            } => {
                assert_eq!(priority, 55);
                assert_eq!(job_id, 2);
                assert_eq!(name.as_deref(), Some("named"));
            }
            other => panic!("expected Prioritized, got {other:?}"),
        }
    }

    #[test]
    fn from_queue_event_distinguishes_added_from_waiting_to_run() {
        // Wait with no previous state => a brand new Added job.
        let added =
            EventParameters::<u64, u64>::from_queue_event(event_with(JobState::Wait, 10)).unwrap();
        assert!(matches!(
            added,
            EventParameters::Added {
                job_id: 10,
                name: None
            }
        ));

        // Wait with a previous state => a job returning to the queue.
        let mut waiting = event_with(JobState::Wait, 11);
        waiting.prev = Some(JobState::Delayed);
        let waiting = EventParameters::<u64, u64>::from_queue_event(waiting).unwrap();
        assert!(matches!(
            waiting,
            EventParameters::WaitingToRun {
                job_id: 11,
                prev_state: Some(JobState::Delayed)
            }
        ));
    }

    #[test]
    fn from_queue_event_maps_stalled_with_default_prev() {
        let stalled =
            EventParameters::<u64, u64>::from_queue_event(event_with(JobState::Stalled, 12))
                .unwrap();
        // Missing prev defaults to `JobState::Wait` (the enum default).
        assert!(matches!(
            stalled,
            EventParameters::Stalled {
                job_id: 12,
                prev_state: JobState::Wait
            }
        ));
    }

    #[test]
    fn from_queue_event_maps_active_preserving_prev() {
        let mut active = event_with(JobState::Active, 13);
        active.prev = Some(JobState::Wait);
        let active = EventParameters::<u64, u64>::from_queue_event(active).unwrap();
        assert!(matches!(
            active,
            EventParameters::Active {
                job_id: 13,
                prev_state: Some(JobState::Wait)
            }
        ));
    }

    #[test]
    fn from_queue_event_collapses_paused_resumed_obliterated_to_void() {
        for state in [JobState::Paused, JobState::Resumed, JobState::Obliterated] {
            let void =
                EventParameters::<u64, u64>::from_queue_event(event_with(state, 14)).unwrap();
            assert!(
                matches!(void, EventParameters::Void),
                "{state:?} should map to Void"
            );
        }
    }

    #[test]
    fn from_queue_event_maps_completed_and_derives_expected_delay_from_metrics() {
        let mut completed = event_with(JobState::Completed, 15);
        completed.returned_value = Some(777);
        completed.prev = Some(JobState::Active);
        completed.metrics = Some(JobMetrics {
            delay: 1_500,
            ..Default::default()
        });

        let completed = EventParameters::<u64, u64>::from_queue_event(completed).unwrap();
        match completed {
            EventParameters::Completed {
                job_id,
                expected_delay,
                result,
                prev_state,
                ..
            } => {
                assert_eq!(job_id, 15);
                assert_eq!(result, 777);
                assert_eq!(prev_state, Some(JobState::Active));
                assert_eq!(
                    expected_delay,
                    Duration::from_millis(1_500),
                    "expected_delay must be derived from metrics.delay"
                );
            }
            other => panic!("expected Completed, got {other:?}"),
        }
    }

    #[test]
    #[should_panic(expected = "there is no result")]
    fn from_queue_event_completed_without_result_panics() {
        // Documented panic: a Completed event with no returned value.
        let event = event_with(JobState::Completed, 16);
        let _ = EventParameters::<u64, u64>::from_queue_event(event);
    }

    #[test]
    fn from_queue_event_maps_failed_with_defaults() {
        let mut failed = event_with(JobState::Failed, 17);
        failed.failed_reason = Some(FailedDetails {
            run: 3,
            reason: "boom".to_compact_string(),
        });
        failed.prev = Some(JobState::Active);
        let failed = EventParameters::<u64, u64>::from_queue_event(failed).unwrap();
        match failed {
            EventParameters::Failed {
                reason,
                job_id,
                prev_state,
            } => {
                assert_eq!(job_id, 17);
                assert_eq!(reason.run, 3);
                assert_eq!(reason.reason.as_str(), "boom");
                assert_eq!(prev_state, JobState::Active);
            }
            other => panic!("expected Failed, got {other:?}"),
        }

        // Missing reason and prev fall back to their defaults.
        let defaulted =
            EventParameters::<u64, u64>::from_queue_event(event_with(JobState::Failed, 18))
                .unwrap();
        assert!(matches!(
            defaulted,
            EventParameters::Failed {
                job_id: 18,
                prev_state: JobState::Wait,
                ..
            }
        ));
    }

    #[test]
    fn from_queue_event_maps_delayed_from_millis() {
        let mut delayed = event_with(JobState::Delayed, 19);
        delayed.delay = Some(2_000);
        let delayed = EventParameters::<u64, u64>::from_queue_event(delayed).unwrap();
        assert!(matches!(
            delayed,
            EventParameters::Delayed {
                job_id: 19,
                delay
            } if delay == Duration::from_secs(2)
        ));

        // Absent delay defaults to zero.
        let zero = EventParameters::<u64, u64>::from_queue_event(event_with(JobState::Delayed, 20))
            .unwrap();
        assert!(matches!(
            zero,
            EventParameters::Delayed { delay, .. } if delay == Duration::ZERO
        ));
    }

    #[test]
    fn from_queue_event_maps_progress_with_data() {
        let mut progress = event_with(JobState::Progress, 21);
        progress.progress_data = Some(314);
        let progress = EventParameters::<u64, u64>::from_queue_event(progress).unwrap();
        assert!(matches!(
            progress,
            EventParameters::Progress {
                job_id: 21,
                data: 314
            }
        ));
    }

    #[test]
    #[should_panic(expected = "expecting a value")]
    fn from_queue_event_progress_without_data_panics() {
        // Documented panic: a Progress event with no data payload.
        let event = event_with(JobState::Progress, 22);
        let _ = EventParameters::<u64, u64>::from_queue_event(event);
    }

    #[test]
    fn from_queue_event_maps_processing_with_defaults() {
        let worker = Uuid::from_u128(0x1234);
        let mut processing = event_with(JobState::Processing, 23);
        processing.worker_id = Some(worker);
        processing.prev = Some(JobState::Active);
        let processing = EventParameters::<u64, u64>::from_queue_event(processing).unwrap();
        match processing {
            EventParameters::Processing {
                worker_id,
                job_id,
                status,
            } => {
                assert_eq!(worker_id, worker);
                assert_eq!(job_id, 23);
                assert_eq!(status, JobState::Active);
            }
            other => panic!("expected Processing, got {other:?}"),
        }

        // Missing worker_id => nil UUID; missing prev => default status.
        let defaulted =
            EventParameters::<u64, u64>::from_queue_event(event_with(JobState::Processing, 24))
                .unwrap();
        assert!(matches!(
            defaulted,
            EventParameters::Processing {
                worker_id,
                job_id: 24,
                status: JobState::Wait
            } if worker_id.is_nil()
        ));
    }

    fn new_emitter() -> Emitter<u64, u64> {
        Emitter::<u64, u64>::new()
    }

    #[tokio::test]
    async fn emit_invokes_only_listeners_for_the_matching_event() {
        let emitter = new_emitter();
        let completed_hits = Arc::new(AtomicUsize::new(0));
        let failed_hits = Arc::new(AtomicUsize::new(0));

        let c = Arc::clone(&completed_hits);
        emitter.on(
            JobState::Completed,
            move |_evt: EventParameters<u64, u64>| {
                let c = Arc::clone(&c);
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                }
            },
        );
        let f = Arc::clone(&failed_hits);
        emitter.on(JobState::Failed, move |_evt: EventParameters<u64, u64>| {
            let f = Arc::clone(&f);
            async move {
                f.fetch_add(1, Ordering::SeqCst);
            }
        });

        tokio::time::timeout(
            AWAIT_BOUND,
            emitter.emit(JobState::Completed, EventParameters::Void),
        )
        .await
        .expect("emit must not hang");

        assert_eq!(completed_hits.load(Ordering::SeqCst), 1);
        assert_eq!(
            failed_hits.load(Ordering::SeqCst),
            0,
            "a non-matching listener must not fire"
        );
    }

    #[tokio::test]
    async fn many_subscribers_on_the_same_event_all_fire() {
        const SUBSCRIBERS: usize = 64;
        let emitter = new_emitter();
        let hits = Arc::new(AtomicUsize::new(0));

        for _ in 0..SUBSCRIBERS {
            let hits = Arc::clone(&hits);
            emitter.on(JobState::Active, move |_evt: EventParameters<u64, u64>| {
                let hits = Arc::clone(&hits);
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                }
            });
        }
        assert_eq!(
            emitter.listener_count_by_event(&JobState::Active),
            SUBSCRIBERS
        );

        tokio::time::timeout(
            AWAIT_BOUND,
            emitter.emit(JobState::Active, EventParameters::Void),
        )
        .await
        .expect("emit must not hang");

        assert_eq!(hits.load(Ordering::SeqCst), SUBSCRIBERS);
    }

    #[tokio::test]
    async fn once_listener_fires_exactly_once() {
        let emitter = new_emitter();
        let hits = Arc::new(AtomicUsize::new(0));
        let h = Arc::clone(&hits);
        emitter.once(JobState::Delayed, move |_evt: EventParameters<u64, u64>| {
            let h = Arc::clone(&h);
            async move {
                h.fetch_add(1, Ordering::SeqCst);
            }
        });

        for _ in 0..3 {
            tokio::time::timeout(
                AWAIT_BOUND,
                emitter.emit(JobState::Delayed, EventParameters::Void),
            )
            .await
            .expect("emit must not hang");
        }
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "once must fire a single time"
        );
    }

    #[tokio::test]
    async fn removed_listener_no_longer_fires() {
        let emitter = new_emitter();
        let hits = Arc::new(AtomicUsize::new(0));
        let h = Arc::clone(&hits);
        let id = emitter.on(JobState::Stalled, move |_evt: EventParameters<u64, u64>| {
            let h = Arc::clone(&h);
            async move {
                h.fetch_add(1, Ordering::SeqCst);
            }
        });

        let removed = emitter.remove_listener(id);
        assert_eq!(removed, Some(id), "removing a real listener returns its id");

        tokio::time::timeout(
            AWAIT_BOUND,
            emitter.emit(JobState::Stalled, EventParameters::Void),
        )
        .await
        .expect("emit must not hang");
        assert_eq!(hits.load(Ordering::SeqCst), 0);

        // Removing an unknown id is a no-op returning None.
        assert_eq!(emitter.remove_listener(Uuid::new_v4()), None);
    }

    #[tokio::test]
    async fn global_listener_receives_every_event_alongside_specific_listeners() {
        let emitter = new_emitter();
        let global_hits = Arc::new(AtomicUsize::new(0));
        let specific_hits = Arc::new(AtomicUsize::new(0));

        let g = Arc::clone(&global_hits);
        emitter.on_all(move |_evt: EventParameters<u64, u64>| {
            let g = Arc::clone(&g);
            async move {
                g.fetch_add(1, Ordering::SeqCst);
            }
        });
        let s = Arc::clone(&specific_hits);
        emitter.on(
            JobState::Completed,
            move |_evt: EventParameters<u64, u64>| {
                let s = Arc::clone(&s);
                async move {
                    s.fetch_add(1, Ordering::SeqCst);
                }
            },
        );

        for state in [JobState::Completed, JobState::Failed, JobState::Active] {
            tokio::time::timeout(AWAIT_BOUND, emitter.emit(state, EventParameters::Void))
                .await
                .expect("emit must not hang");
        }

        assert_eq!(
            global_hits.load(Ordering::SeqCst),
            3,
            "global listener fires for every event"
        );
        assert_eq!(
            specific_hits.load(Ordering::SeqCst),
            1,
            "specific listener fires only for its event"
        );
    }

    #[tokio::test]
    #[should_panic(expected = "only one global listener is allowed")]
    async fn registering_two_global_listeners_panics() {
        let emitter = new_emitter();
        emitter.on_all(|_evt: EventParameters<u64, u64>| async {});
        // A second global listener is rejected by the underlying emitter.
        emitter.on_all(|_evt: EventParameters<u64, u64>| async {});
    }

    #[tokio::test]
    async fn emit_with_no_listeners_is_a_harmless_noop() {
        let emitter = new_emitter();
        tokio::time::timeout(
            AWAIT_BOUND,
            emitter.emit(JobState::Wait, EventParameters::Void),
        )
        .await
        .expect("emit on an empty emitter must not hang");
        assert_eq!(emitter.event_count(), 0);
        assert_eq!(emitter.listener_count_by_event(&JobState::Wait), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_emits_across_threads_all_dispatch() {
        const EMITS: usize = 50;
        let emitter = Arc::new(new_emitter());
        let hits = Arc::new(AtomicUsize::new(0));
        let h = Arc::clone(&hits);
        emitter.on(
            JobState::Progress,
            move |_evt: EventParameters<u64, u64>| {
                let h = Arc::clone(&h);
                async move {
                    h.fetch_add(1, Ordering::SeqCst);
                }
            },
        );

        let mut handles = Vec::with_capacity(EMITS);
        for _ in 0..EMITS {
            let emitter = Arc::clone(&emitter);
            handles.push(tokio::spawn(async move {
                emitter
                    .emit(JobState::Progress, EventParameters::Void)
                    .await;
            }));
        }
        for handle in handles {
            tokio::time::timeout(AWAIT_BOUND, handle)
                .await
                .expect("emit task must not hang")
                .expect("emit task must not panic");
        }
        assert_eq!(hits.load(Ordering::SeqCst), EMITS);
    }

    #[tokio::test]
    async fn listener_receives_the_exact_emitted_payload() {
        let emitter = new_emitter();
        let seen = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
        let s = Arc::clone(&seen);
        emitter.on(
            JobState::Completed,
            move |evt: EventParameters<u64, u64>| {
                let s = Arc::clone(&s);
                async move {
                    if let EventParameters::Completed { result, .. } = evt {
                        s.lock().unwrap().push(result);
                    }
                }
            },
        );

        let payload = EventParameters::Completed {
            job_id: 1,
            job_metrics: JobMetrics::default(),
            expected_delay: Duration::ZERO,
            prev_state: None,
            result: 555,
        };
        tokio::time::timeout(AWAIT_BOUND, emitter.emit(JobState::Completed, payload))
            .await
            .expect("emit must not hang");

        assert_eq!(seen.lock().unwrap().as_slice(), &[555]);
    }
}
