use std::{
    sync::{atomic::AtomicUsize, Arc},
    time::{Duration, Instant},
};

use deadpool_redis::{Config, Connection};
use kio_mq::{
    fetch_redis_pass, framed, BackOffJobOptions, EventParameters, Job, JobOptions, KioResult,
    Queue, QueueEventMode, QueueOpts, RemoveOnCompletionOrFailure, Repeat, Worker, WorkerOpts,
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
        age: Some(60 * 60),
        count: None,
    });
    let backoff_opts = BackOffJobOptions::Opts(kio_mq::BackOffOptions {
        type_: Some("exponential".to_owned()),
        delay: Some(200),
    });
    let queue_opts = QueueOpts {
        remove_on_fail: Some(remove_opts),
        remove_on_complete: Some(remove_opts),
        attempts: 2,
        default_backoff: Some(backoff_opts.clone()),
        event_mode: Some(QueueEventMode::PubSub),
        ..Default::default()
    };
    let counter = Arc::new(AtomicUsize::default());
    let events = counter.clone();
    let queue = Queue::<i32, i32, i32>::new(None, "trial", &config, Some(queue_opts)).await?;
    let event_listener = move |state: EventParameters<_, _, _>| {
        let completed = events.clone();
        async move {
            // do something with return state
            if let EventParameters::Completed {
                job,
                result: _,
                prev_state: _,
            } = state
            {
                completed.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
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
        }
    };
    queue.on_all_events(event_listener).await;

    let count = 10;
    let repeats = 2;
    use croner::Cron;
    let cron_schedule: Cron = "1/2 * * * * *".parse()?;
    let iterator = (0..count).map(move |_i| {
        //use rand::Rng;
        //let priority = rand::rng().random_range(1..count); // ucomment to use  random priority

        //let priority = (count - _i) as u64; // ucomment to use a priority of count - index (job_id -1)
        let mut job_opts = JobOptions {
            //delay: 100 * _i as u64, // uncomment to add delay
            //priority, // uncomment to set priority
            ..Default::default()
        };
        if _i == 2 {
            job_opts.attempts = repeats + 1;
            job_opts.repeat = Some(Repeat::WithCron(Box::new(cron_schedule.clone())));
            job_opts.delay = cron_schedule.clone().into();
        }
        let name = Uuid::new_v4().to_string();
        (name, Some(job_opts), _i as i32)
    });

    let opts = WorkerOpts {
        autorun: true,
        //concurrency: 1, // uncomment to use set concurrency
        ..Default::default()
    };
    let processor = |con: _, job: Job<_, _, _>| process_callback(con, job);
    //let _worker = Worker::new(&queue, processor, Some(opts.clone()))?;
    //let _worker = Worker::new(&queue, processor, Some(opts.clone()))?;
    let worker = Worker::new(&queue, processor, Some(opts))?;
    let adding = Instant::now();
    queue.bulk_add_only(iterator).await?;
    println!("adding items took {:?}", adding.elapsed());
    //worker.run()?;
    let now = Instant::now();
    while counter.load(std::sync::atomic::Ordering::Acquire) < count + (repeats as usize) {
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    dbg!(now.elapsed());
    worker.close(true);
    if worker.closed() {
        queue.obliterate().await?;
    }
    Ok(())
}
#[framed]
async fn process_callback(
    mut con: Connection,
    mut job: Job<i32, i32, i32>,
) -> Result<i32, std::io::Error> {
    let progress = job.progress.unwrap_or_default();
    let _ = job.update_progress(progress + 1, &mut con).await;
    //let id: u64 = job.id.unwrap_or_default().parse().unwrap_or_default();
    //if id % 2 == 0 && job.attempts_made < job.opts.attempts - 1 {
    //    //uncomment the line below to test to catching panics
    //    panic!("panicked here");
    //}
    Ok(job.id.unwrap_or_default() as i32)
}
