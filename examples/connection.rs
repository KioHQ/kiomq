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
    let queue = Queue::<String, String, i32>::new(None, "trial", &config).await?;

    for _ in 0..3 {
        let _ = queue.add_job("testd", "data".to_lowercase(), None).await?;
    }
    let processor = |con: _, mut job: Job<_, _, _>| async move {
        let progress = job.progress.unwrap_or_default();
        let _ = job.update_progress(progress + 1, con).await;
        if job.id.unwrap_or_default().contains("3") {
            panic!("paniced here, don't save");
        }
        Ok("done".to_lowercase())
    };
    let worker = Worker::new(&queue, processor, None);
    worker.run().await?;
    let result = queue.make_stalled_jobs_wait(&worker.opts).await?;
    //let next_job = queue.wait_for_job(100).await?;
    //let done = queue.extend_lock(2, 5000, "test").await?;
    //dbg!(worker);
    println!("{:?}", now.elapsed());
    Ok(())
}
