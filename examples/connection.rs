use deadpool_redis::{Config, Connection};
use kio_mq::{fetch_redis_pass, Job, KioResult, Queue, Worker};
#[tokio::main]
async fn main() -> KioResult<()> {
    let now = tokio::time::Instant::now();
    let password = fetch_redis_pass();
    let mut config = Config::default();
    if let Some(cfg) = config.connection.as_mut() {
        cfg.redis.password = password;
    }
    let queue = Queue::<String, (), i32>::new(None, "trial", &config).await?;

    for _ in 0..3 {
        let job = queue.add_job("testd", "data".to_lowercase(), None).await?;
        queue
            .move_job_to_state(
                &job.id.unwrap_or_default(),
                kio_mq::JobState::Wait,
                kio_mq::JobState::Active,
                None,
            )
            .await?;
    }
    let processor = |con: Connection, mut job: Job<String, (), i32>| async move {
        if let Some(progess) = job.progress {
            let _ = job.update_progress(progess + 1, con).await;
        }
        Ok(())
    };
    let worker = Worker::new(&queue, processor, None);
    worker.run().await?;
    let result = queue.make_stalled_jobs_wait(&worker.opts).await?;
    dbg!(result);
    //let next_job = queue.wait_for_job(100).await?;
    //let done = queue.extend_lock(2, 5000, "test").await?;
    dbg!(worker);
    println!("{:?}", now.elapsed());
    Ok(())
}
