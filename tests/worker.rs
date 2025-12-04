#[cfg(test)]
mod worker {
    use crossbeam_queue::ArrayQueue;
    use kio_mq::{
        fetch_redis_pass, EventParameters, JobOptions, KioError, QueueEventMode, RedisStore,
    };
    use kio_mq::{Config, KioResult, Queue, QueueOpts, Worker};
    use std::collections::VecDeque;
    use std::sync::{Arc, LazyLock};
    use uuid::Uuid;
    static CONFIG: LazyLock<Config> = LazyLock::new(|| {
        let password = fetch_redis_pass();
        let mut config = Config::default();
        if let Some(cfg) = config.connection.as_mut() {
            cfg.redis.password = password;
        }
        config
    });

    #[tokio::test(flavor = "multi_thread")]
    async fn runs_jobs_to_completion() -> KioResult<()> {
        let config = &CONFIG;
        let queue_opts = QueueOpts::default();
        let name = Uuid::new_v4().to_string();
        let store = RedisStore::new(None, &name, config).await?;
        let queue = Queue::<i32, i32, i32, _>::new(store, Some(queue_opts)).await?;
        let count = 4;
        let completed: Arc<ArrayQueue<u64>> = Arc::new(ArrayQueue::new(count as usize));
        let jobs = completed.clone();
        queue.on(
            kio_mq::JobState::Completed,
            move |state: kio_mq::EventParameters<i32, i32, i32>| {
                let completed = jobs.clone();
                async move {
                    if let EventParameters::Completed {
                        job_metrics: _,
                        result: _,
                        expected_delay: _,
                        prev_state: _,
                        job_id,
                        _dt,
                    } = state
                    {
                        let _ = completed.push(job_id);
                    }
                }
            },
        );
        let job_iterator = (0..count).map(|i| (i.to_string(), None, i));
        let processor = move |_conn, job: kio_mq::Job<i32, i32, i32>| async move {
            Ok::<i32, KioError>(job.data.unwrap())
        };
        let worker = Worker::new_async(&queue, processor, None)?;
        worker.run()?;
        assert!(worker.is_running());
        let jobs = queue.bulk_add(job_iterator).await?;
        // wait for metrics to update
        while completed.len() != count as usize {}
        worker.close();
        assert!(!worker.is_running());
        let metrics = queue.get_metrics().await?;
        assert_eq!(
            metrics.waiting.load(std::sync::atomic::Ordering::Acquire),
            0,
        );
        assert_eq!(
            metrics.completed.load(std::sync::atomic::Ordering::Acquire),
            jobs.len() as u64,
        );
        queue.obliterate().await?;
        Ok(())
    }
    #[tokio::test]
    async fn errors_with_multiple_run_calls() -> KioResult<()> {
        use kio_mq::WorkerError;
        let config = &CONFIG;
        let queue_opts = QueueOpts::default();
        let name = Uuid::new_v4().to_string();
        let store = RedisStore::new(None, &name, config).await?;
        let queue = Queue::<i32, i32, i32, _>::new(store, Some(queue_opts)).await?;
        let processor = move |_conn, job: kio_mq::Job<i32, i32, i32>| async move {
            Ok::<i32, KioError>(job.data.unwrap())
        };
        let worker = Worker::new_async(&queue, processor, None)?;
        let id = worker.id;
        worker.run()?;
        let next_call = worker.run();
        assert!(next_call.is_err());
        if let Err(err) = next_call {
            let _expected = KioError::WorkerError(WorkerError::WorkerAlreadyRunningWithId(id));
            assert!(matches!(err, _expected))
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn runs_delayed_jobs() -> KioResult<()> {
        use crossbeam_queue::ArrayQueue;
        let config = &CONFIG;
        let queue_opts = QueueOpts {
            event_mode: Some(QueueEventMode::PubSub),
            ..Default::default()
        };
        let name = Uuid::new_v4().to_string();
        let store = RedisStore::new(None, &name, config).await?;
        let queue = Queue::<i32, i32, i32, _>::new(store, Some(queue_opts)).await?;
        let count: i32 = 4;
        let job_iterator = (0..count).map(|i| {
            let job_opts = JobOptions {
                delay: ((i * 100) as i64).into(),
                ..Default::default()
            };
            (i.to_string(), Some(job_opts), i)
        });
        let completed: Arc<ArrayQueue<_>> = Arc::new(ArrayQueue::new(count as usize));
        let jobs = completed.clone();
        queue.on(
            kio_mq::JobState::Active,
            move |state: kio_mq::EventParameters<i32, i32, i32>| {
                let completed = jobs.clone();
                async move {
                    if let EventParameters::Completed {
                        job_metrics,
                        result: _,
                        expected_delay,
                        prev_state: _,
                        job_id: _,
                        _dt,
                    } = state
                    {
                        let _ = completed.push((job_metrics, expected_delay));
                    }
                }
            },
        );
        let processor = move |_conn, job: kio_mq::Job<i32, i32, i32>| async move {
            Ok::<i32, KioError>(job.data.unwrap())
        };
        let worker = Worker::new_async(&queue, processor, None)?;
        worker.run()?;
        let _jobs = queue.bulk_add(job_iterator).await?;
        assert!(worker.is_running());
        while completed.len() != count as usize {}
        worker.close();
        assert!(!worker.is_running());
        assert_eq!(completed.len(), count as usize);
        let metrics = queue.current_metrics.as_ref();
        assert_eq!(
            metrics.delayed.load(std::sync::atomic::Ordering::Acquire),
            0,
        );
        queue.obliterate().await?;
        while let Some((metrics, expected)) = completed.pop() {
            use std::time::Duration;
            // check delay is with accepted range
            let allowable_delay_diff = Duration::from_millis(90);

            if metrics.delayed_for > Duration::ZERO {
                assert!(metrics.delayed_for - expected <= allowable_delay_diff);
            }
        }
        Ok(())
    }
    #[tokio::test]
    async fn pauses_and_resumes_when_queue_is_idle() -> KioResult<()> {
        use std::time::Duration;
        let config = &CONFIG;
        let queue_opts = QueueOpts {
            event_mode: Some(QueueEventMode::PubSub),
            ..Default::default()
        };
        let name = Uuid::new_v4().to_string();
        let store = RedisStore::new(None, &name, config).await?;
        let queue = Queue::<i32, i32, i32, _>::new(store, Some(queue_opts)).await?;
        let count = 4;
        let completed: Arc<ArrayQueue<_>> = Arc::new(ArrayQueue::new(count as usize));
        let jobs = completed.clone();
        queue.on(
            kio_mq::JobState::Completed,
            move |state: kio_mq::EventParameters<i32, i32, i32>| {
                let completed = jobs.clone();
                async move {
                    if let EventParameters::Completed {
                        job_metrics: _,
                        result: _,
                        expected_delay: _,
                        prev_state: _,
                        job_id,
                        _dt,
                    } = state
                    {
                        let _ = completed.push(job_id);
                    }
                }
            },
        );
        let job_iterator = (0..count).map(|i| (i.to_string(), None, i));
        let processor = move |_conn, job: kio_mq::Job<i32, i32, i32>| async move {
            Ok::<i32, KioError>(job.data.unwrap())
        };
        let worker = Worker::new_async(&queue, processor, None)?;
        worker.run()?;
        assert!(worker.is_running());
        queue.bulk_add(job_iterator).await?;
        // wait for all previous to complete
        while completed.len() != count as usize {}
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(worker.is_idle());
        queue.add_job("test", 1, None).await?;
        // wait for new job to get picked up
        while queue.get_metrics().await?.queue_has_work() {}
        assert!(!worker.is_idle());
        assert!(worker.is_running());
        let metrics = queue.get_metrics().await?;
        assert_eq!(
            metrics.waiting.load(std::sync::atomic::Ordering::Acquire),
            0,
        );
        assert_eq!(metrics.active.load(std::sync::atomic::Ordering::Acquire), 1);
        queue.obliterate().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn runs_prioritized_jobs_correctly() -> KioResult<()> {
        use crossbeam_queue::ArrayQueue;
        let config = &CONFIG;
        let queue_opts = QueueOpts::default();
        let name = Uuid::new_v4().to_string();
        let store = RedisStore::new(None, &name, config).await?;
        let queue = Queue::<i32, i32, i32, _>::new(store, Some(queue_opts)).await?;
        let count: i32 = 4;
        let job_iterator = (0..count).map(move |i| {
            let job_opts = JobOptions {
                priority: (count - i) as u64,
                ..Default::default()
            };
            (i.to_string(), Some(job_opts), i)
        });
        let moved_to_active: Arc<ArrayQueue<u64>> = Arc::new(ArrayQueue::new(count as usize));
        let jobs = moved_to_active.clone();
        queue.on(
            kio_mq::JobState::Active,
            move |state: kio_mq::EventParameters<i32, i32, i32>| {
                let completed = jobs.clone();
                async move {
                    if let EventParameters::Completed {
                        job_metrics: _,
                        result: _,
                        expected_delay: _,
                        prev_state: _,
                        job_id,
                        _dt,
                    } = state
                    {
                        let _ = completed.push(job_id);
                    }
                }
            },
        );
        let processor = move |_conn, job: kio_mq::Job<i32, i32, i32>| async move {
            Ok::<i32, KioError>(job.data.unwrap())
        };
        let worker = Worker::new_async(&queue, processor, None)?;
        worker.run()?;
        let jobs = queue.bulk_add(job_iterator).await?;
        // wait for metrics to update
        //tokio::time::slee
        while moved_to_active.len() < count as usize {}
        assert_eq!(moved_to_active.len(), count as usize);
        let metrics = queue.current_metrics.as_ref();
        assert_eq!(
            metrics.waiting.load(std::sync::atomic::Ordering::Acquire),
            0,
        );
        let mut expected_ordered: VecDeque<u64> = jobs
            .into_iter()
            .map(|job| job.id.unwrap_or_default())
            .collect();
        while let (Some(expected), Some(recieved)) =
            (expected_ordered.pop_back(), moved_to_active.pop())
        {
            assert_eq!(expected, recieved);
        }
        queue.obliterate().await?;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "reason"]
    async fn runs_jobs_and_respects_clean_up() -> KioResult<()> {
        use kio_mq::{CollectionSuffix, RemoveOnCompletionOrFailure};
        let removal_opts = RemoveOnCompletionOrFailure::Bool(true);
        let config = &CONFIG;
        let queue_opts = QueueOpts {
            remove_on_fail: Some(removal_opts),
            remove_on_complete: Some(removal_opts),
            event_mode: Some(QueueEventMode::PubSub),

            ..Default::default()
        };
        let name = Uuid::new_v4().to_string();
        let count = 4;
        let store = RedisStore::new(None, &name, config).await?;
        let queue = Queue::<i32, i32, i32, _>::new(store.clone(), Some(queue_opts)).await?;
        let completed: Arc<ArrayQueue<u64>> = Arc::new(ArrayQueue::new(count as usize));
        let jobs = completed.clone();
        queue.on_all_events(move |state: kio_mq::EventParameters<i32, i32, i32>| {
            let completed = jobs.clone();
            async move {
                if let EventParameters::Completed {
                    job_metrics: _,
                    result: _,
                    expected_delay: _,
                    prev_state: _,
                    job_id,
                    _dt,
                } = state
                {
                    let _ = completed.push(job_id);
                }
                if let EventParameters::Failed {
                    reason: _,
                    job_id,
                    prev_state: _,
                } = state
                {
                    let _ = completed.push(job_id);
                }
            }
        });
        let job_iterator = (0..count).map(|i| (i.to_string(), None, i));
        let processor = move |_conn, job: kio_mq::Job<i32, i32, i32>| async move {
            // fail the 3 third job
            if job.id.unwrap_or_default() == 3 {
                panic!("failed here")
            }
            Ok::<i32, KioError>(job.data.unwrap())
        };
        let worker = Worker::new_async(&queue, processor, None)?;
        worker.run()?;
        assert!(worker.is_running());
        let _jobs = queue.bulk_add(job_iterator).await?;
        // wait for metrics to update
        while completed.len() != count as usize {}
        worker.close();
        let last_id = queue
            .current_metrics
            .last_id
            .load(std::sync::atomic::Ordering::Acquire);
        let mut pipeline = redis::pipe();
        pipeline.atomic();
        let mut conn = store.get_connection().await?;
        for id in 1..last_id {
            let job_key = CollectionSuffix::Job(id).to_collection_name(&store.prefix, &store.name);
            pipeline.exists(job_key);
        }
        let all_exist: Vec<bool> = pipeline.query_async(&mut conn).await?;
        assert!(!all_exist.is_empty());
        assert!(all_exist.iter().all(|v| !v));

        queue.obliterate().await?;
        Ok(())
    }
}
