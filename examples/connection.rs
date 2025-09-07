use deadpool_redis::{Config, Connection};
use kio_mq::{
    fetch_redis_pass, framed, EventParameters, Job, JobOptions, KioResult, Queue, Worker,
    WorkerOpts,
};
use uuid::Uuid;
#[tokio::main]
#[framed]
async fn main() -> KioResult<()> {
    let password = fetch_redis_pass();
    let mut config = Config::default();
    if let Some(cfg) = config.connection.as_mut() {
        cfg.redis.password = password;
    }
    let queue = Queue::<String, String, i32>::new(None, "trial", &config).await?;

    let count = 10;
    for i in 0..count {
        let job_opts = JobOptions {
            delay: 100 * i as u64,
            ..Default::default()
        };
        let name = Uuid::new_v4().to_string();
        let _ = queue
            .add_job(&name, "data".to_lowercase(), Some(job_opts))
            .await?;
    }
    let opts = WorkerOpts {
        //concurrency: 1,
        ..Default::default()
    };
    dbg!(&opts);
    let last_job_id = queue.current_jobs();
    let processor = |con: _, job: Job<_, _, _>| process_callback(con, job);
    let worker = Worker::new(&queue, processor, Some(opts))?;
    let cancel = worker.cancellation_token.clone();
    let event_listener = move |state: _| {
        let cancel = cancel.clone();
        async move {
            // do something with return state
            if let EventParameters::Completed {
                job,
                result: _,
                prev_state: _,
            } = state
            {
                let diff = (job.processed_on.unwrap_or_default() - job.ts).num_milliseconds();
                let id = last_job_id.to_string();
                if job.id.as_ref().unwrap().contains(&id) {
                    //println!("finished in {:#?}: delay_ms: {diff}", now.elapsed());
                    cancel.cancel();
                }
                println!(
                    "finished job {} delayed for {} ms, expected_delay: {}",
                    job.id.unwrap_or_default(),
                    diff,
                    job.opts.delay,
                );
            }
        }
    };
    queue.on_all_events(event_listener).await;
    worker.run()?;

    while worker.is_running() {} // do nothing
    if worker.closed() {
        queue.obliterate().await?;
    }
    Ok(())
}
#[framed]
async fn process_callback(
    mut con: Connection,
    mut job: Job<String, String, i32>,
) -> Result<String, std::io::Error> {
    let progress = job.progress.unwrap_or_default();
    let _ = job.update_progress(progress + 1, &mut con).await;
    if job.id.unwrap_or_default() == "3" {
        // uncomment the line below to test to catching panics
        //panic!("panicked here");
    }
    Ok("done".to_string())
}
