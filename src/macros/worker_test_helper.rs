/// Macro that generates the worker integration test suite for any Store implementation.
///
/// The suite is written for `D=R=P=i32`
///
/// # Parameters
/// - `$mod_name`: a unique module name (prevents test name collisions)
/// - `$make_store`: an async expression that returns `kiomq::KioResult<S>`
///   where `S: kiomq::Store<i32,i32,i32> + Clone + Send + 'static`.
///
/// # Example (`InMemory`)
/// ```ignore
/// use kiomq::macros::worker_store_suite;
/// use kiomq::InMemoryStore;
/// use uuid::Uuid;
///
/// worker_store_suite!(inmemory, async {
///     let name = Uuid::new_v4().to_string();
///     Ok::<_, kiomq::KioError>(InMemoryStore::<i32,i32,i32>::new(None, &name))
/// });
/// ```
#[macro_export]
macro_rules! worker_store_suite {
    ($mod_name:ident, $make_store:expr) => {
        mod $mod_name {
            use super::*;
            use crossbeam::queue::ArrayQueue;
            use kiomq::{
                CollectionSuffix, EventParameters, JobOptions, KioError, KioResult, Queue,
                QueueEventMode, QueueOpts, Store, Worker, WorkerOpts, WorkerState,
            };
            use std::collections::VecDeque;
            use std::sync::Arc;
            use std::time::Duration;

            type D = i32;
            type R = i32;
            type P = i32;

            static SUITE_RT: std::sync::LazyLock<tokio::runtime::Runtime> =
                std::sync::LazyLock::new(|| {
                    tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .build()
                        .expect("failed to build shared suite runtime")
                });

            async fn make_store() -> KioResult<impl Store<D, R, P> + Clone + Send + 'static> {
                ($make_store).await
            }

            #[test]
            fn runs_jobs_to_completion() -> KioResult<()> {
                SUITE_RT.block_on(async move {
                    let queue_opts = QueueOpts::default();
                    let store = make_store().await?;
                    let queue = Queue::<D, R, P, _>::new(store, Some(queue_opts)).await?;

                    let count = 4;
                    let completed: Arc<ArrayQueue<u64>> =
                        Arc::new(ArrayQueue::new((count + 2) as usize));
                    let jobs = completed.clone();

                    queue.on(
                        kiomq::JobState::Completed,
                        move |state: kiomq::EventParameters<D, R>| {
                            let completed = jobs.clone();
                            async move {
                                if let EventParameters::Completed { job_id, .. } = state {
                                    let _ = completed.push(job_id);
                                }
                            }
                        },
                    );

                    let job_iterator = (0..count).map(|i| (i.to_string(), None, i));
                    let processor = move |_conn, job: kiomq::Job<D, R, P>| async move {
                        Ok::<R, KioError>(job.data.unwrap())
                    };

                    let worker = Worker::new_async(&queue, processor, None)?;
                    worker.run()?;
                    assert!(worker.is_running());

                    let jobs = queue.bulk_add(job_iterator).await?;
                    while !queue.current_metrics.all_jobs_completed() {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }

                    worker.close();
                    assert!(!worker.is_running());

                    let metrics = queue.get_metrics().await?;
                    assert_eq!(metrics.waiting.load(), 0);
                    assert_eq!(metrics.completed.load(), jobs.len() as u64);

                    queue.obliterate().await?;
                    Ok(())
                })
            }

            #[test]
            fn errors_with_multiple_run_calls() -> KioResult<()> {
                SUITE_RT.block_on(async move {
                    use kiomq::WorkerError;

                    let queue_opts = QueueOpts::default();
                    let store = make_store().await?;
                    let queue = Queue::<D, R, P, _>::new(store, Some(queue_opts)).await?;

                    let processor = move |_conn, job: kiomq::Job<D, R, P>| async move {
                        Ok::<R, KioError>(job.data.unwrap())
                    };

                    let worker = Worker::new_async(&queue, processor, None)?;
                    let id = worker.id;

                    worker.run()?;
                    let next_call = worker.run();
                    assert!(next_call.is_err());

                    if let Err(err) = next_call {
                        let expected =
                            KioError::WorkerError(WorkerError::WorkerAlreadyRunningWithId(id));
                        assert!(matches!(err, expected))
                    }

                    queue.obliterate().await?;
                    Ok(())
                })
            }

            #[test]
            fn runs_delayed_jobs() -> KioResult<()> {
                SUITE_RT.block_on(async move {
                    let queue_opts = QueueOpts {
                        ..Default::default()
                    };
                    let store = make_store().await?;
                    let queue = Queue::<D, R, P, _>::new(store, Some(queue_opts)).await?;

                    let count: D = 4;
                    let job_iterator = (0..count).map(|i| {
                        let job_opts = JobOptions {
                            delay: ((i * 100) as i64).into(),
                            ..Default::default()
                        };
                        (i.to_string(), Some(job_opts), i)
                    });

                    let completed: Arc<ArrayQueue<u64>> = Arc::new(ArrayQueue::new(count as usize));
                    let jobs = completed.clone();

                    queue.on_all_events(move |state: kiomq::EventParameters<D, R>| {
                        let completed = jobs.clone();
                        async move {
                            match state {
                                EventParameters::Active { job_id, .. }
                                | EventParameters::Failed { job_id, .. } => {
                                    let _ = completed.push(job_id);
                                }
                                _ => {}
                            }
                        }
                    });

                    let processor = move |_conn, job: kiomq::Job<D, R, P>| async move {
                        Ok::<R, KioError>(job.data.unwrap())
                    };

                    let worker_opts = WorkerOpts {
                        ..Default::default()
                    };
                    let worker = Worker::new_async(&queue, processor, Some(worker_opts))?;
                    worker.run()?;

                    let _jobs = queue.bulk_add(job_iterator).await?;
                    assert!(worker.is_running());

                    let mut waited_ms = 0u64;
                    while completed.len() != count as usize {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        waited_ms += 5;
                        assert!(
                            waited_ms < 20_000,
                            "delayed jobs were not all promoted within 20s (promoted {}/{})",
                            completed.len(),
                            count
                        );
                    }
                    worker.close();
                    assert!(!worker.is_running());
                    assert_eq!(completed.len(), count as usize);

                    let metrics = queue.current_metrics.as_ref();
                    assert_eq!(metrics.delayed.load(), 0);

                    while let Some(id) = completed.pop() {
                        if let Some(job) = queue.get_job(id).await {
                            if let Some(processed_on) = job.processed_on {
                                let diff = processed_on
                                    .signed_duration_since(job.ts)
                                    .num_milliseconds();
                                let delay = job.delay as i64;
                                if delay > 0 {
                                    assert!(diff + 25 >= delay);
                                }
                            }
                        }
                    }

                    queue.obliterate().await?;
                    Ok(())
                })
            }

            #[test]
            fn pauses_and_resumes_when_queue_is_idle() -> KioResult<()> {
                SUITE_RT.block_on(async move {
                    let queue_opts = QueueOpts {
                        ..Default::default()
                    };
                    let store = make_store().await?;
                    let queue = Queue::<D, R, P, _>::new(store, Some(queue_opts)).await?;

                    let count = 4;
                    let active: Arc<ArrayQueue<u64>> = Arc::new(ArrayQueue::new(5 as usize));
                    let completed: Arc<ArrayQueue<u64>> = Arc::new(ArrayQueue::new(5 as usize));
                    let jobs = active.clone();
                    let completed_clone = completed.clone();

                    queue.on_all_events(move |state: kiomq::EventParameters<D, R>| {
                        let active = jobs.clone();
                        let completed = completed_clone.clone();
                        async move {
                            match state {
                                EventParameters::Active { job_id, .. } => {
                                    active.push(job_id);
                                }
                                EventParameters::Completed { job_id, .. } => {
                                    completed.push(job_id);
                                }
                                _ => {}
                            }
                        }
                    });

                    let job_iterator = (0..count).map(|i| (i.to_string(), None, i));
                    let processor = move |_conn, job: kiomq::Job<D, R, P>| async move {
                        Ok::<R, KioError>(job.data.unwrap())
                    };

                    let worker = Worker::new_async(&queue, processor, None)?;
                    worker.run()?;
                    assert!(worker.is_running());

                    queue.bulk_add(job_iterator).await?;
                    while completed.len() < 4 {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }

                    queue.pause_active_workers();
                    let mut became_idle = false;
                    for _ in 0..300 {
                        if worker.is_idle() {
                            became_idle = true;
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    assert!(became_idle);

                    queue.add_job("test", 1, None).await?;
                    while active.len() < 5 {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }

                    let metrics = queue.get_metrics().await?;
                    assert_eq!(metrics.waiting.load(), 0);

                    while completed.len() < 5 {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                    queue.obliterate().await?;
                    Ok(())
                })
            }

            #[test]
            fn runs_prioritized_jobs_correctly() -> KioResult<()> {
                SUITE_RT.block_on(async move {
                    let queue_opts = QueueOpts::default();
                    let store = make_store().await?;
                    let queue = Queue::<D, R, P, _>::new(store, Some(queue_opts)).await?;

                    let count: D = 4;
                    let job_iterator = (0..count).map(move |i| {
                        let job_opts = JobOptions {
                            priority: (count - i) as u64,
                            ..Default::default()
                        };
                        (i.to_string(), Some(job_opts), i)
                    });

                    let moved_to_active: Arc<ArrayQueue<u64>> =
                        Arc::new(ArrayQueue::new(count as usize));
                    let jobs = moved_to_active.clone();

                    queue.on(
                        kiomq::JobState::Active,
                        move |state: kiomq::EventParameters<D, R>| {
                            let completed = jobs.clone();
                            async move {
                                if let EventParameters::Active { job_id, .. } = state {
                                    let _ = completed.push(job_id);
                                }
                            }
                        },
                    );

                    let processor = move |_conn, job: kiomq::Job<D, R, P>| async move {
                        Ok::<R, KioError>(job.data.unwrap())
                    };

                    let worker = Worker::new_async(&queue, processor, None)?;
                    worker.run()?;

                    let jobs = queue.bulk_add(job_iterator).await?;
                    while moved_to_active.len() < count as usize {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                    assert_eq!(moved_to_active.len(), count as usize);

                    let metrics = queue.current_metrics.as_ref();
                    assert_eq!(metrics.waiting.load(), 0);

                    while !queue.current_metrics.all_jobs_completed() {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }

                    let mut expected_ordered: VecDeque<u64> = jobs
                        .into_iter()
                        .map(|job| job.id.unwrap_or_default())
                        .collect();

                    while let (Some(expected), Some(received)) =
                        (expected_ordered.pop_back(), moved_to_active.pop())
                    {
                        assert_eq!(expected, received);
                    }

                    queue.obliterate().await?;
                    Ok(())
                })
            }

            #[test]
            fn runs_jobs_and_respects_clean_up() -> KioResult<()> {
                SUITE_RT.block_on(async move {
                    use kiomq::RemoveOnCompletionOrFailure;

                    let removal_opts = RemoveOnCompletionOrFailure::Bool(true);
                    let queue_opts = QueueOpts {
                        remove_on_fail: Some(removal_opts),
                        remove_on_complete: Some(removal_opts),
                        event_mode: Some(QueueEventMode::PubSub),
                        ..Default::default()
                    };

                    let store = make_store().await?;
                    let queue = Queue::<D, R, P, _>::new(store.clone(), Some(queue_opts)).await?;

                    let count = 4;
                    let completed: Arc<ArrayQueue<u64>> = Arc::new(ArrayQueue::new(count as usize));
                    let jobs = completed.clone();

                    queue.on_all_events(move |state: kiomq::EventParameters<D, R>| {
                        let completed = jobs.clone();
                        async move {
                            if let EventParameters::Completed { job_id, .. } = state {
                                let _ = completed.push(job_id);
                            }
                            if let EventParameters::Failed { job_id, .. } = state {
                                let _ = completed.push(job_id);
                            }
                        }
                    });

                    let job_iterator = (0..count).map(|i| (i.to_string(), None, i));
                    let processor = move |_conn, job: kiomq::Job<D, R, P>| async move {
                        if job.id.unwrap_or_default() == 3 {
                            panic!("failed here")
                        }
                        Ok::<R, KioError>(job.data.unwrap())
                    };

                    let worker = Worker::new_async(&queue, processor, None)?;
                    worker.run()?;
                    assert!(worker.is_running());

                    let jobs = queue.bulk_add(job_iterator).await?;
                    while completed.len() < count as usize {
                        tokio::time::sleep(Duration::from_millis(2)).await
                    }
                    worker.close();
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

                    // Store-generic cleanup assertion
                    let mut fetched: Vec<bool> = vec![];
                    for i in 1..count {
                        let exists = store
                            .exists_in(CollectionSuffix::Job(i as u64), i as u64)
                            .await?;
                        fetched.push(exists);
                    }
                    assert!(fetched.iter().all(|v| !v));

                    queue.obliterate().await?;
                    Ok(())
                })
            }

            #[test]
            fn sync_worker_catches_panics_and_errors() -> KioResult<()> {
                SUITE_RT.block_on(async move {
                    let store = make_store().await?;
                    let queue = Queue::<D, R, P, _>::new(store, Some(QueueOpts::default())).await?;

                    let failed: Arc<ArrayQueue<u64>> = Arc::new(ArrayQueue::new(2));
                    let failed_clone = failed.clone();

                    queue.on(
                        kiomq::JobState::Failed,
                        move |state: kiomq::EventParameters<D, R>| {
                            let failed = failed_clone.clone();
                            async move {
                                if let EventParameters::Failed { job_id, .. } = state {
                                    let _ = failed.push(job_id);
                                }
                            }
                        },
                    );

                    let count = 2;
                    let job_iterator = (0..count).map(|i| (i.to_string(), None, i));
                    queue.bulk_add_only(job_iterator).await?;

                    let processor = move |_conn, job: kiomq::Job<D, R, P>| {
                        if job.id.unwrap_or_default() == 1 {
                            return Err(std::io::Error::other("failed here").into());
                        }
                        panic!("panicked here");
                        #[allow(unreachable_code)]
                        Ok::<R, KioError>(job.data.unwrap())
                    };

                    let worker = Worker::new_sync(&queue, processor, None)?;
                    worker.run()?;

                    while failed.len() < 2 {
                        tokio::time::sleep(Duration::from_millis(2)).await
                    }

                    queue.obliterate().await?;
                    Ok(())
                })
            }

            #[test]
            fn async_worker_catches_panics_and_errors() -> KioResult<()> {
                SUITE_RT.block_on(async move {
                    let store = make_store().await?;
                    let queue = Queue::<D, R, P, _>::new(store, Some(QueueOpts::default())).await?;

                    let failed: Arc<ArrayQueue<u64>> = Arc::new(ArrayQueue::new(2));
                    let failed_clone = failed.clone();

                    queue.on(
                        kiomq::JobState::Failed,
                        move |state: kiomq::EventParameters<D, R>| {
                            let failed = failed_clone.clone();
                            async move {
                                if let EventParameters::Failed { job_id, .. } = state {
                                    let _ = failed.push(job_id);
                                }
                            }
                        },
                    );

                    let count = 2;
                    let job_iterator = (0..count).map(|i| (i.to_string(), None, i));
                    queue.bulk_add_only(job_iterator).await?;

                    let processor = move |_conn, job: kiomq::Job<D, R, P>| async move {
                        if job.id.unwrap_or_default() == 1 {
                            return Err(std::io::Error::other("failed here").into());
                        }
                        panic!("panicked here");
                        #[allow(unreachable_code)]
                        Ok::<R, KioError>(job.data.unwrap())
                    };

                    let worker = Worker::new_async(&queue, processor, None)?;
                    worker.run()?;

                    while failed.len() < 2 {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                    assert_eq!(failed.len(), 2);
                    queue.obliterate().await?;
                    Ok(())
                })
            }

            #[test]
            #[cfg(any(feature = "default", not(feature = "redis-store")))]
            fn task_metrics_update_over_time() -> KioResult<()> {
                SUITE_RT.block_on(async move {
                    let store = make_store().await?;
                    let queue = Queue::<D, R, P, _>::new(store, None).await?;

                    let processor = move |_conn, _job: kiomq::Job<D, R, P>| async move {
                        for _ in 0..10 {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                        Ok::<R, KioError>(42)
                    };

                    let worker_opts = WorkerOpts {
                        metrics_update_interval: 17,
                        ..Default::default()
                    };
                    let worker = Worker::new_async(&queue, processor, Some(worker_opts))?;
                    worker.run()?;

                    queue.add_job("test", 1, None).await?;

                    let mut found_non_zero = false;
                    for _ in 0..150 {
                        tokio::time::sleep(Duration::from_millis(30)).await;
                        let metrics_map = queue.fetch_worker_metrics().await?;
                        if let Some(worker_metrics) = metrics_map.values().next() {
                            if let Some(task_info) = worker_metrics.tasks.first() {
                                if task_info.metrics.total_poll_count >= 2 {
                                    assert!(task_info.metrics.total_idle_duration > Duration::ZERO);
                                    found_non_zero = true;
                                    break;
                                }
                            }
                        }
                    }

                    assert!(found_non_zero);

                    while !queue.current_metrics.all_jobs_completed() {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                    worker.close();
                    Ok(())
                })
            }

            #[test]
            fn empty_queue_worker_keeps_running_without_spinning_out() -> KioResult<()> {
                SUITE_RT.block_on(async move {
                    let store = make_store().await?;
                    let queue = Queue::<D, R, P, _>::new(store, None).await?;
                    let processor = move |_conn, job: kiomq::Job<D, R, P>| async move {
                        Ok::<R, KioError>(job.data.unwrap())
                    };
                    let worker = Worker::new_async(&queue, processor, None)?;
                    worker.run()?;

                    // An idle worker parks in WorkerState::Idle; it must not close itself.
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    assert!(!worker.closed(), "an idle worker must not close itself");
                    assert_ne!(worker.state.load(), WorkerState::Closed);

                    queue.add_job("job", 21, None).await?;
                    while !queue.current_metrics.all_jobs_completed() {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                    worker.close();
                    queue.obliterate().await?;
                    Ok(())
                })
            }

            #[test]
            fn concurrency_of_one_still_completes_every_job() -> KioResult<()> {
                SUITE_RT.block_on(async move {
                    let store = make_store().await?;
                    let queue = Queue::<D, R, P, _>::new(store, None).await?;
                    let opts = WorkerOpts {
                        concurrency: 1,
                        ..Default::default()
                    };
                    let processor = move |_conn, job: kiomq::Job<D, R, P>| async move {
                        Ok::<R, KioError>(job.data.unwrap())
                    };
                    let worker = Worker::new_async(&queue, processor, Some(opts))?;
                    worker.run()?;

                    let count = 6;
                    queue
                        .bulk_add((0..count).map(|i| (i.to_string(), None, i)))
                        .await?;
                    while !queue.current_metrics.all_jobs_completed() {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                    worker.close();
                    assert_eq!(queue.get_metrics().await?.completed.load(), count as u64);
                    queue.obliterate().await?;
                    Ok(())
                })
            }

            #[test]
            fn high_concurrency_completes_every_job() -> KioResult<()> {
                SUITE_RT.block_on(async move {
                    let store = make_store().await?;
                    let queue = Queue::<D, R, P, _>::new(store, None).await?;
                    let opts = WorkerOpts {
                        concurrency: 64,
                        ..Default::default()
                    };
                    let processor = move |_conn, job: kiomq::Job<D, R, P>| async move {
                        Ok::<R, KioError>(job.data.unwrap())
                    };
                    let worker = Worker::new_async(&queue, processor, Some(opts))?;
                    worker.run()?;

                    let count = 100;
                    queue
                        .bulk_add((0..count).map(|i| (i.to_string(), None, i)))
                        .await?;
                    while !queue.current_metrics.all_jobs_completed() {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                    worker.close();
                    let metrics = queue.get_metrics().await?;
                    assert_eq!(metrics.completed.load(), count as u64);
                    assert_eq!(metrics.waiting.load(), 0);
                    queue.obliterate().await?;
                    Ok(())
                })
            }

            #[test]
            fn concurrency_of_zero_processes_no_jobs() -> KioResult<()> {
                SUITE_RT.block_on(async move {
                    // Concurrency 0 yields a semaphore with no permits: the worker stalls. No
                    // validation guards against this, so this pins the stalling behaviour.
                    let store = make_store().await?;
                    let queue = Queue::<D, R, P, _>::new(store, None).await?;
                    let opts = WorkerOpts {
                        concurrency: 0,
                        ..Default::default()
                    };
                    let processor = move |_conn, job: kiomq::Job<D, R, P>| async move {
                        Ok::<R, KioError>(job.data.unwrap())
                    };
                    let worker = Worker::new_async(&queue, processor, Some(opts))?;
                    worker.run()?;

                    queue.add_job("stalled", 1, None).await?;
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    assert!(!queue.current_metrics.all_jobs_completed());
                    let metrics = queue.get_metrics().await?;
                    assert_eq!(metrics.completed.load(), 0);
                    assert_eq!(metrics.waiting.load(), 1);

                    worker.close();
                    queue.obliterate().await?;
                    Ok(())
                })
            }

            #[test]
            fn worker_keeps_processing_after_a_handler_panics() -> KioResult<()> {
                SUITE_RT.block_on(async move {
                    let store = make_store().await?;
                    let queue = Queue::<D, R, P, _>::new(store, None).await?;

                    let count = 5;
                    let terminal: Arc<ArrayQueue<u64>> = Arc::new(ArrayQueue::new(count as usize));
                    let terminal_sink = terminal.clone();
                    queue.on_all_events(move |state: EventParameters<D, R>| {
                        let terminal = terminal_sink.clone();
                        async move {
                            if let EventParameters::Completed { job_id, .. }
                            | EventParameters::Failed { job_id, .. } = state
                            {
                                let _ = terminal.push(job_id);
                            }
                        }
                    });

                    let processor = move |_conn, job: kiomq::Job<D, R, P>| async move {
                        assert!(job.data.unwrap_or_default() != 0, "first job panics");
                        Ok::<R, KioError>(job.data.unwrap())
                    };
                    let worker = Worker::new_async(&queue, processor, None)?;
                    worker.run()?;

                    queue
                        .bulk_add((0..count).map(|i| (i.to_string(), None, i)))
                        .await?;
                    while terminal.len() < count as usize {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                    worker.close();

                    let metrics = queue.get_metrics().await?;
                    assert_eq!(metrics.waiting.load(), 0);
                    assert_eq!(metrics.failed.load(), 1);
                    assert_eq!(metrics.completed.load(), (count - 1) as u64);
                    queue.obliterate().await?;
                    Ok(())
                })
            }
        }
    };
}

pub use crate::worker_store_suite;
