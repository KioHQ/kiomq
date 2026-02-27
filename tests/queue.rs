#[cfg(test)]
mod queue {
    use kiomq::{fetch_redis_pass, JobOptions, JobState, RedisStore};
    use kiomq::{Config, KioResult, Queue, QueueOpts};
    use std::sync::LazyLock;
    use uuid::Uuid;
    static CONFIG: LazyLock<Config> = LazyLock::new(|| {
        let password = fetch_redis_pass();
        let mut config = Config::default();
        if let Some(cfg) = config.connection.as_mut() {
            cfg.redis.password = password;
        }
        config
    });
    #[tokio::test]
    async fn add_and_fetch_job_single() -> KioResult<()> {
        let config = &CONFIG;
        let queue_opts = QueueOpts::default();
        let name = Uuid::new_v4().to_string();
        let store = RedisStore::new(None, &name, config).await?;
        let queue = Queue::<i32, i32, i32, _>::new(store, Some(queue_opts)).await?;

        let job = queue.add_job("test", 1, None).await?;
        // wait for metrics to update
        let metrics = queue.get_metrics().await?;
        let waiting = metrics.waiting.load(std::sync::atomic::Ordering::Acquire);
        assert_eq!(waiting, 1);
        let expected_id = metrics.last_id.load(std::sync::atomic::Ordering::Acquire);
        let fetched_job = queue.get_job(expected_id).await;

        if let Some(fetched) = fetched_job {
            assert_eq!(job.id, fetched.id);
        }
        queue.obliterate().await?;
        Ok(())
    }
    #[tokio::test]
    async fn add_bulk_jobs() -> KioResult<()> {
        let config = &CONFIG;
        let queue_opts = QueueOpts::default();
        let name = Uuid::new_v4().to_string();
        let store = RedisStore::new(None, &name, config).await?;
        let queue = Queue::<i32, i32, i32, _>::new(store, Some(queue_opts)).await?;
        let job_iterator = (0..4).map(|i| (i.to_string(), None, i));
        let jobs = queue.bulk_add(job_iterator).await?;
        // wait for metrics to update
        let metrics = queue.get_metrics().await?;
        dbg!(&metrics);
        assert_eq!(
            metrics.waiting.load(std::sync::atomic::Ordering::Acquire),
            jobs.len() as u64,
        );
        queue.obliterate().await?;
        Ok(())
    }
    #[tokio::test]
    async fn obliterate() -> KioResult<()> {
        let config = &CONFIG;
        let queue_opts = QueueOpts::default();
        let name = Uuid::new_v4().to_string();
        let store = RedisStore::new(None, &name, config).await?;
        let queue = Queue::<i32, i32, i32, _>::new(store, Some(queue_opts)).await?;
        let job_iterator = (0..4).map(|i| (i.to_string(), None, i));
        let jobs = queue.bulk_add(job_iterator).await?;
        let metrics = queue.get_metrics().await?;
        assert_eq!(
            metrics.waiting.load(std::sync::atomic::Ordering::Acquire),
            jobs.len() as u64,
        );
        queue.obliterate().await?;
        assert_eq!(
            queue
                .current_metrics
                .waiting
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );
        Ok(())
    }
    #[tokio::test]
    async fn add_delayed_jobs() -> KioResult<()> {
        let config = &CONFIG;
        let queue_opts = QueueOpts::default();
        let job_opts = JobOptions {
            delay: 200.into(),
            ..Default::default()
        };
        let name = Uuid::new_v4().to_string();
        let store = RedisStore::new(None, &name, config).await?;
        let queue = Queue::<i32, i32, i32, _>::new(store, Some(queue_opts)).await?;
        let job = queue.add_job("delay", 1, Some(job_opts)).await?;
        // wait for metrics to update
        let metrics = queue.get_metrics().await?;
        let delayed = metrics.delayed.load(std::sync::atomic::Ordering::Acquire);
        let expected_id = metrics.last_id.load(std::sync::atomic::Ordering::Acquire);
        let fetched_job = queue.get_job(expected_id).await;
        assert!(metrics.has_delayed());
        assert_eq!(delayed, 1);
        if let Some(fetched) = fetched_job {
            assert_eq!(job.id, fetched.id);
            assert_eq!(fetched.delay, 200);
            assert_eq!(fetched.state, JobState::Delayed);
            assert_eq!(job.opts.delay, fetched.opts.delay)
        }
        queue.obliterate().await?;
        Ok(())
    }
    #[tokio::test]
    async fn add_prioritized() -> KioResult<()> {
        let config = &CONFIG;
        let queue_opts = QueueOpts::default();
        let job_opts = JobOptions {
            priority: 2,
            ..Default::default()
        };
        let name = Uuid::new_v4().to_string();
        let store = RedisStore::new(None, &name, config).await?;
        let queue = Queue::<i32, i32, i32, _>::new(store, Some(queue_opts)).await?;

        let job = queue.add_job("Priorized", 1, Some(job_opts)).await?;
        // wait for metrics to update
        let metrics = queue.get_metrics().await?;
        let expected_id = metrics.last_id.load(std::sync::atomic::Ordering::Acquire);
        let fetched_job = queue.get_job(expected_id).await;
        if let Some(fetched) = fetched_job {
            assert_eq!(job.id, fetched.id);
            assert_eq!(fetched.priority, 2);
            assert_eq!(fetched.state, JobState::Prioritized);
            assert_eq!(job.opts.delay, fetched.opts.delay)
        }
        queue.obliterate().await?;
        Ok(())
    }
    #[tokio::test]
    async fn pause_and_resume() -> KioResult<()> {
        let config = &CONFIG;
        let name = Uuid::new_v4().to_string();
        let store = RedisStore::new(None, &name, config).await?;
        let queue_opts = QueueOpts {
            event_mode: Some(kiomq::QueueEventMode::PubSub),
            ..Default::default()
        };
        let queue = Queue::<i32, i32, i32, _>::new(store, Some(queue_opts)).await?;
        let _job = queue.add_job(&name, 1, None).await?;
        let metrics = queue.get_metrics().await?;
        assert_eq!(
            metrics.waiting.load(std::sync::atomic::Ordering::Acquire),
            1
        );
        // when the queue is paused, the waiting list is renamed to paused;
        queue.pause_or_resume().await?;
        let metrics = queue.get_metrics().await?;
        assert!(metrics.is_paused.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(
            metrics.waiting.load(std::sync::atomic::Ordering::Acquire),
            0
        );
        // resume here
        queue.pause_or_resume().await?;
        assert!(!queue.is_paused());
        queue.obliterate().await?;
        Ok(())
    }
}
