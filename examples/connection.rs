use deadpool_redis::{Config, Connection};
use kio_mq::{
    fetch_redis_pass, framed, EventParameters, Job, JobOptions, KioResult, Queue, Worker,
    WorkerOpts,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
#[tokio::main]
#[framed]
async fn main() -> KioResult<()> {
    let password = fetch_redis_pass();
    let mut config = Config::default();
    if let Some(cfg) = config.connection.as_mut() {
        cfg.redis.password = password;
    }
    let queue = Queue::<String, String, i32>::new(None, "trial", &config, None).await?;
    let token = CancellationToken::default();
    let cancel = token.clone();
    let metrics = queue.current_metrics.clone();
    let event_listener = move |state: _| {
        let cancel = cancel.clone();
        let metrics = metrics.clone();
        async move {
            // do something with return state
            if let EventParameters::Completed {
                job,
                result: _,
                prev_state: _,
            } = state
            {
                let diff = (job.processed_on.unwrap_or_default() - job.ts).num_milliseconds();
                let ran_time = (job.finished_on.unwrap_or_default()
                    - job.processed_on.unwrap_or_default())
                .num_milliseconds();
                if metrics.all_jobs_completed() {
                    cancel.cancel();
                }
                println!(
                    "finished job  {}  ran for {ran_time} ms with an actual delay of  {} ms and  expected_delay: {}",
                    job.id.unwrap_or_default(),
                    diff,
                    job.opts.delay,
                );
            }
        }
    };
    queue.on_all_events(event_listener).await;

    let count = 10;
    for _i in 0..count {
        //use rand::Rng;
        //let priority = rand::rng().random_range(1..count); // ucomment to use  random priority

        //let priority = count - _i; // ucomment to use a priority of count - index (job_id -1)
        let job_opts = JobOptions {
            //delay: 200 * _i as u64, // uncomment to add delay
            //priority, // uncomment to set priority
            ..Default::default()
        };
        let name = Uuid::new_v4().to_string();
        let _ = queue
            .add_job(&name, "data".to_lowercase(), Some(job_opts))
            .await?;
    }
    let opts = WorkerOpts {
        //concurrency: 1, // uncomment to use set concurrency
        ..Default::default()
    };
    let processor = |con: _, job: Job<_, _, _>| process_callback(con, job);
    let mut worker = Worker::new(&queue, processor, Some(opts))?;
    worker.cancellation_token = token;

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
