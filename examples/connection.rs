use deadpool_redis::{Config, Connection};
use kio_mq::{fetch_redis_pass, framed, Job, KioResult, Queue, Worker};
#[tokio::main]
#[framed]
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
    let event_listener = |_state: _| async {
        // do something with return state
    };
    queue.on_all_events(event_listener).await;
    let processor = |con: _, job: Job<_, _, _>| process_callback(con, job);
    let worker = Worker::new(&queue, processor, None);
    worker.run().await?;
    //let result = queue.make_stalled_jobs_wait(&worker.opts).await?;
    //let next_job = queue.wait_for_job(100).await?;
    //let done = queue.extend_lock(2, 5000, "test").await?;
    //dbg!(worker);
    println!("{:?}", now.elapsed());
    Ok(())
}
#[framed]
async fn process_callback(
    con: Connection,
    mut job: Job<String, String, i32>,
) -> Result<String, std::io::Error> {
    let progress = job.progress.unwrap_or_default();
    let _ = job.update_progress(progress + 1, con).await;
    if job.id.unwrap_or_default().contains("3") {
        return Err(std::io::Error::other("error here"));
    }
    Ok("done".to_string())
}
