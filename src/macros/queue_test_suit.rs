/// Macro that generates the queue integration test suite for any Store implementation.
///
/// The suite is written for `D=R=P=i32` (matching the existing tests).
///
/// # Parameters
/// - `$mod_name`: unique module name (prevents test name collisions)
/// - `$make_store`: async expression returning `kiomq::KioResult<S>`
///   where `S: kiomq::Store<i32,i32,i32> + Clone + Send + 'static`.
#[macro_export]
macro_rules! queue_store_suite {
    ($mod_name:ident, $make_store:expr) => {
        mod $mod_name {
            use super::*;
            use kiomq::{JobOptions, JobState, KioResult, Queue, QueueOpts, Store};

            use uuid::Uuid;

            type D = i32;
            type R = i32;
            type P = i32;

            async fn make_store() -> KioResult<impl Store<D, R, P> + Clone + Send + 'static> {
                ($make_store).await
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn add_and_fetch_job_single() -> KioResult<()> {
                let queue_opts = QueueOpts::default();
                let store = make_store().await?;
                let queue = Queue::<D, R, P, _>::new(store, Some(queue_opts)).await?;

                let job = queue.add_job("test", 1, None).await?;
                let metrics = queue.get_metrics().await?;
                let waiting = metrics.waiting.load();
                assert_eq!(waiting, 1);

                let expected_id = metrics.last_id.load();
                let fetched_job = queue.get_job(expected_id).await;

                if let Some(fetched) = fetched_job {
                    assert_eq!(job.id, fetched.id);
                }

                queue.obliterate().await?;
                Ok(())
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn add_bulk_jobs() -> KioResult<()> {
                let queue_opts = QueueOpts::default();
                let store = make_store().await?;
                let queue = Queue::<D, R, P, _>::new(store, Some(queue_opts)).await?;

                let job_iterator = (0..4).map(|i| (i.to_string(), None, i));
                let jobs = queue.bulk_add(job_iterator).await?;

                let metrics = queue.get_metrics().await?;
                assert_eq!(metrics.waiting.load(), jobs.len() as u64,);

                queue.obliterate().await?;
                Ok(())
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn obliterate() -> KioResult<()> {
                let queue_opts = QueueOpts::default();
                let store = make_store().await?;
                let queue = Queue::<D, R, P, _>::new(store, Some(queue_opts)).await?;

                let job_iterator = (0..4).map(|i| (i.to_string(), None, i));
                let jobs = queue.bulk_add(job_iterator).await?;

                let metrics = queue.get_metrics().await?;
                assert_eq!(metrics.waiting.load(), jobs.len() as u64,);

                queue.obliterate().await?;

                assert_eq!(queue.current_metrics.waiting.load(), 0);

                Ok(())
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn add_delayed_jobs() -> KioResult<()> {
                let queue_opts = QueueOpts::default();
                let job_opts = JobOptions {
                    delay: 200.into(),
                    ..Default::default()
                };

                let store = make_store().await?;
                let queue = Queue::<D, R, P, _>::new(store, Some(queue_opts)).await?;

                let job = queue.add_job("delay", 1, Some(job_opts)).await?;

                let metrics = queue.get_metrics().await?;
                let delayed = metrics.delayed.load();
                let expected_id = metrics.last_id.load();
                let fetched_job = queue.get_job(expected_id).await;

                assert!(metrics.has_delayed());
                assert_eq!(delayed, 1);

                if let Some(fetched) = fetched_job {
                    assert_eq!(job.id, fetched.id);
                    assert_eq!(fetched.delay, 200);
                    assert_eq!(fetched.state, JobState::Delayed);
                    assert_eq!(job.opts.delay, fetched.opts.delay);
                }

                queue.obliterate().await?;
                Ok(())
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn add_prioritized() -> KioResult<()> {
                let queue_opts = QueueOpts::default();
                let job_opts = JobOptions {
                    priority: 2,
                    ..Default::default()
                };

                let store = make_store().await?;
                let queue = Queue::<D, R, P, _>::new(store, Some(queue_opts)).await?;

                let job = queue.add_job("Priorized", 1, Some(job_opts)).await?;

                let metrics = queue.get_metrics().await?;
                let expected_id = metrics.last_id.load();
                let fetched_job = queue.get_job(expected_id).await;

                if let Some(fetched) = fetched_job {
                    assert_eq!(job.id, fetched.id);
                    assert_eq!(fetched.priority, 2);
                    assert_eq!(fetched.state, JobState::Prioritized);
                    assert_eq!(job.opts.delay, fetched.opts.delay);
                }

                queue.obliterate().await?;
                Ok(())
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn pause_and_resume() -> KioResult<()> {
                let queue_opts = QueueOpts {
                    event_mode: Some(kiomq::QueueEventMode::PubSub),
                    ..Default::default()
                };

                let name = Uuid::new_v4().to_string();

                let store = make_store().await?;
                let queue = Queue::<D, R, P, _>::new(store, Some(queue_opts)).await?;

                let _job = queue.add_job(&name, 1, None).await?;
                let metrics = queue.get_metrics().await?;
                assert_eq!(metrics.waiting.load(), 1);

                queue.pause_or_resume().await?;
                let metrics = queue.get_metrics().await?;
                assert!(metrics.is_paused.load());
                assert_eq!(metrics.waiting.load(), 0);
                assert_eq!(metrics.paused.load(), 1, "paused is empty");

                queue.pause_or_resume().await?;
                assert!(!queue.is_paused());

                let metrics = queue.get_metrics().await?;
                assert_eq!(metrics.waiting.load(), 1, "waiting is empty");

                queue.obliterate().await?;
                Ok(())
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn add_job_accepts_empty_name() -> KioResult<()> {
                let store = make_store().await?;
                let queue = Queue::<D, R, P, _>::new(store, None).await?;
                let job = queue.add_job("", 1, None).await?;
                assert!(job.id.is_some());
                assert_eq!(queue.get_metrics().await?.waiting.load(), 1);
                queue.obliterate().await?;
                Ok(())
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn add_job_accepts_very_long_name() -> KioResult<()> {
                let store = make_store().await?;
                let queue = Queue::<D, R, P, _>::new(store, None).await?;
                let job = queue.add_job(&"n".repeat(10_000), 1, None).await?;
                assert!(job.id.is_some());
                queue.obliterate().await?;
                Ok(())
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn duplicate_named_jobs_receive_distinct_ids() -> KioResult<()> {
                let store = make_store().await?;
                let queue = Queue::<D, R, P, _>::new(store, None).await?;
                let first = queue.add_job("dup", 1, None).await?;
                let second = queue.add_job("dup", 1, None).await?;
                assert_ne!(first.id, second.id);
                assert_eq!(queue.get_metrics().await?.waiting.load(), 2);
                queue.obliterate().await?;
                Ok(())
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn zero_delay_job_goes_straight_to_waiting() -> KioResult<()> {
                let store = make_store().await?;
                let queue = Queue::<D, R, P, _>::new(store, None).await?;
                let opts = JobOptions {
                    delay: 0i64.into(),
                    ..Default::default()
                };
                queue.add_job("immediate", 1, Some(opts)).await?;
                let metrics = queue.get_metrics().await?;
                assert_eq!(metrics.waiting.load(), 1);
                assert_eq!(metrics.delayed.load(), 0);
                queue.obliterate().await?;
                Ok(())
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn bulk_add_empty_iterator_adds_nothing() -> KioResult<()> {
                let store = make_store().await?;
                let queue = Queue::<D, R, P, _>::new(store, None).await?;
                let empty: Vec<(String, Option<JobOptions>, P)> = Vec::new();
                let jobs = queue.bulk_add(empty.into_iter()).await?;
                assert!(jobs.is_empty());
                assert_eq!(queue.get_metrics().await?.waiting.load(), 0);
                queue.obliterate().await?;
                Ok(())
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn bulk_add_large_batch_counts_all_jobs() -> KioResult<()> {
                let store = make_store().await?;
                let queue = Queue::<D, R, P, _>::new(store, None).await?;
                let jobs = queue
                    .bulk_add((0..250).map(|i| (format!("job-{i}"), None, i)))
                    .await?;
                assert_eq!(jobs.len(), 250);
                assert_eq!(queue.get_metrics().await?.waiting.load(), 250);
                queue.obliterate().await?;
                Ok(())
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn bulk_add_only_still_enqueues_jobs() -> KioResult<()> {
                let store = make_store().await?;
                let queue = Queue::<D, R, P, _>::new(store, None).await?;
                queue
                    .bulk_add_only((0..8).map(|i| (format!("j{i}"), None, i)))
                    .await?;
                assert_eq!(queue.get_metrics().await?.waiting.load(), 8);
                queue.obliterate().await?;
                Ok(())
            }

            #[tokio::test(flavor = "multi_thread")]
            async fn get_job_ids_in_state_lists_waiting_jobs() -> KioResult<()> {
                let store = make_store().await?;
                let queue = Queue::<D, R, P, _>::new(store, None).await?;
                queue
                    .bulk_add((0..3).map(|i| (format!("w{i}"), None, i)))
                    .await?;
                let ids = queue
                    .get_job_ids_in_state(JobState::Wait, None, None)
                    .await?;
                assert_eq!(ids.len(), 3);
                queue.obliterate().await?;
                Ok(())
            }
        }
    };
}

pub use crate::queue_store_suite;
