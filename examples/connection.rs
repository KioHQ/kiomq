use std::time::Duration;

use deadpool_redis::{Config, Connection};
use kio_mq::{
    fetch_redis_pass, framed, get_job_metrics, EventParameters, Job, JobOptions, KioResult, Queue,
    QueueOpts, RemoveOnCompletionOrFailure, Worker, WorkerOpts,
};
use uuid::Uuid;
#[tokio::main]
#[framed]
async fn main() -> KioResult<()> {
    console_subscriber::init();
    let password = fetch_redis_pass();
    let mut config = Config::default();
    if let Some(cfg) = config.connection.as_mut() {
        cfg.redis.password = password;
    }
    let remove_opts = RemoveOnCompletionOrFailure::Opts(kio_mq::KeepJobs {
        age: Some(60),
        count: None,
    });
    let queue_opts = QueueOpts {
        remove_on_fail: Some(remove_opts),
        remove_on_complete: Some(remove_opts),
    };

    let queue = Queue::<String, String, i32>::new(None, "trial", &config, Some(queue_opts)).await?;
    let event_listener = move |state: _| async move {
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
            println!(
                "finished job  {}  ran for {ran_time} ms with an actual delay of  {} ms and  expected_delay: {}",
                job.id.unwrap_or_default(),
                diff,
                job.opts.delay,
            );
        }
    };
    queue.on_all_events(event_listener).await;

    let count = 10;
    for _i in 0..count {
        //use rand::Rng;
        //let priority = rand::rng().random_range(1..count); // ucomment to use  random priority

        //let priority = count - _i; // ucomment to use a priority of count - index (job_id -1)
        let job_opts = JobOptions {
            //delay: 500 * _i as u64, // uncomment to add delay
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
    let worker = Worker::new(&queue, processor, Some(opts))?;
    worker.run()?;

    let mut conn = queue.get_connection().await?;
    while !get_job_metrics(&queue.prefix, &queue.name, &mut conn)
        .await?
        .all_jobs_completed()
    {
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    worker.close(true);
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
