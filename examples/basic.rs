use std::{
    sync::{atomic::AtomicUsize, Arc},
    time::Instant,
};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use kio_mq::{
    framed, BackOffJobOptions, EventParameters, InMemoryStore, Job, JobOptions, KioResult, Queue,
    QueueEventMode, QueueOpts, RemoveOnCompletionOrFailure, Store, Worker, WorkerOpts,
};

#[cfg(all(feature = "redis-store", not(feature = "default")))]
use kio_mq::{fetch_redis_pass, Config, RedisStore};
#[cfg(feature = "rocksdb-store")]
use kio_mq::{temporary_rocks_db, RocksDbStore};
use uuid::Uuid;
#[tokio::main]
#[framed]
async fn main() -> KioResult<()> {
    setup_tracing();

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
    let _store: InMemoryStore<i32, i32, i32> = InMemoryStore::new(None, "trial");
    #[cfg(all(feature = "redis-store", not(feature = "default")))]
    let password = fetch_redis_pass();
    #[cfg(all(feature = "redis-store", not(feature = "default")))]
    let mut config = Config::default();
    #[cfg(all(feature = "redis-store", not(feature = "default")))]
    if let Some(cfg) = config.connection.as_mut() {
        cfg.redis.password = password;
    }
    #[cfg(all(feature = "redis-store", not(feature = "default")))]
    let _store = RedisStore::new(None, "trial", &config).await?;
    #[cfg(feature = "rocksdb-store")]
    let db = Arc::new(temporary_rocks_db());
    #[cfg(feature = "rocksdb-store")]
    let _store = RocksDbStore::new(None, "test", db.clone())?;
    let events = counter.clone();
    let queue = Queue::new(_store, Some(queue_opts)).await?;
    let event_listener = move |state: EventParameters<_, _>| {
        let completed = events.clone();
        async move {
            // do something with return state
            if let EventParameters::Completed {
                job_metrics,
                result: _,
                expected_delay,
                prev_state: _,
                job_id: _,
            } = state
            {
                completed.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                info!("{job_metrics}  expected_delay: {expected_delay:?}",);
            }
        }
    };
    queue.on_all_events(event_listener);

    let count = 1000;
    let repeats = 2;
    use croner::Cron;
    let _cron_schedule: Cron = "1/2 * * * * *".parse()?;
    let iterator = (0..count).map(move |_i| {
        //use rand::Rng;
        //let priority = rand::rng().random_range(1..count); // ucomment to use  random priority

        //let priority = (count - _i) as u64; // ucomment to use a priority of count - index (job_id -1)
        let mut job_opts = JobOptions {
            //delay: (100 * _i as i64).into(), // uncomment to add delay
            //priority, // uncomment to set priority
            ..Default::default()
        };
        if _i == 2 {
            job_opts.attempts = repeats + 1;
            //job_opts.repeat = _cron_schedule.as_str().try_into().ok();
            //job_opts.delay = _cron_schedule.clone().into();
        }
        let name = Uuid::new_v4().to_string();
        (name, Some(job_opts), _i)
    });

    let opts = WorkerOpts {
        //concurrency: 100, // uncomment to use set concurrency
        ..Default::default()
    };
    let processor = |con: _, job: Job<_, _, _>| process_callback(con, job);
    let worker = Worker::new_async(&queue, processor, Some(opts))?;
    let adding = Instant::now();
    queue.bulk_add_only(iterator).await?;
    println!("adding items took {:#?}", adding.elapsed());
    worker.run()?;
    let now = Instant::now();
    while counter.load(std::sync::atomic::Ordering::Acquire) < count as usize {
        //tokio::time::sleep(Duration::from_millis(300)).await;
    }
    dbg!(now.elapsed());
    worker.close();
    if worker.closed() {
        queue.obliterate().await?;
    }
    Ok(())
}
#[framed]
async fn process_callback<S: Store<i32, i32, i32>>(
    store: Arc<S>,
    mut job: Job<i32, i32, i32>,
) -> Result<i32, std::io::Error> {
    let progress = job.progress.unwrap_or_default();
    let _ = store.update_job_progress(&mut job, progress + 1);
    //let id: u64 = job.id.unwrap_or_default().parse().unwrap_or_default();
    //if id % 2 == 0 && job.attempts_made < job.opts.attempts - 1 {
    //    //uncomment the line below to test to catching panics
    //    panic!("panicked here");
    //}
    Ok(job.id.unwrap_or_default() as i32)
}

fn setup_tracing() {
    let console_layer = console_subscriber::spawn();
    let fmt_layer = tracing_subscriber::fmt::layer().with_target(true);
    let filter_layer = tracing_subscriber::EnvFilter::from_default_env()
        .add_directive("tokio=trace".parse().unwrap()) // Required for console
        .add_directive("runtime=trace".parse().unwrap()) // Required for console
        .add_directive("info".parse().unwrap()); // Required for console
    tracing_subscriber::registry()
        .with(console_layer)
        .with(filter_layer)
        .with(fmt_layer)
        .init();
}
