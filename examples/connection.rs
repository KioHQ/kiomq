use deadpool_redis::{Config, Connection};
use kio_mq::{
    fetch_redis_pass, framed, EventParameters, Job, KioResult, Queue, Worker, WorkerOpts,
};
use uuid::Uuid;
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

    let count = 3;
    for _ in 0..count {
        let name = Uuid::new_v4().to_string();
        let _ = queue.add_job(&name, "data".to_lowercase(), None).await?;
    }
    let opts = WorkerOpts {
        concurrency: 1,
        ..Default::default()
    };
    let last_job_id = queue.current_jobs();
    let processor = |con: _, job: Job<_, _, _>| process_callback(con, job);
    let worker = Worker::new(&queue, processor, Some(opts));
    let cancel = worker.cancellation_token.clone();
    let event_listener = move |state: _| {
        let cancel = cancel.clone();
        async move {
            // do something with return state
            if let EventParameters::Completed {
                job,
                result: _,
                prev_state: _,
            } = dbg!(state)
            {
                let id = dbg!(last_job_id.to_string());
                if job.id.unwrap().contains(&id) {
                    println!("finished in {:#?}", now.elapsed());
                    cancel.cancel();
                }
            }
        }
    };
    queue.on_all_events(event_listener).await;
    worker.run().await?;
    //let result = queue.make_stalled_jobs_wait(&worker.opts).await?;
    //let next_job = queue.wait_for_job(100).await?;
    //let done = queue.extend_lock(2, 5000, "test").await?;
    //dbg!(worker);
    Ok(())
}
#[framed]
async fn process_callback(
    con: Connection,
    mut job: Job<String, String, i32>,
) -> Result<String, std::io::Error> {
    let progress = job.progress.unwrap_or_default();
    let _ = job.update_progress(progress + 1, con).await;
    if job.id.unwrap_or_default() == "3" {
        panic!("panicked here");
    }
    Ok("done".to_string())
}
