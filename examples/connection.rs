use deadpool_redis::Config;
use kio_mq::{fetch_redis_pass, JobState, KioError, Queue};
#[tokio::main]
async fn main() -> Result<(), KioError> {
    let now = tokio::time::Instant::now();
    let password = fetch_redis_pass();
    let mut config = Config::default();
    if let Some(cfg) = config.connection.as_mut() {
        cfg.redis.password = Some(password);
    }
    let url = &config.url;
    dbg!(url);
    let queue = Queue::new(None, "trial", &config).await?;
    let job = queue
        .add_job::<_, (), i32>("test_job", "data".to_lowercase(), None)
        .await?;

    queue
        .move_job_to_state(job.id.unwrap(), JobState::Wait, JobState::Active, None)
        .await?;

    let mut stored_job = queue.get_job::<String, (), i32>(job.id.unwrap()).await?;
    let con = queue.conn_pool.get().await?;
    stored_job.update_progress(100, con).await?;
    let previous_state = queue.is_paused();
    queue.pause_or_resume().await?;
    assert_ne!(queue.is_paused(), previous_state);
    assert_eq!(stored_job.progress, Some(100));
    println!("{:?}", now.elapsed());
    Ok(())
}
