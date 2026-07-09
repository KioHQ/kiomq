#![allow(unused_must_use)]
use kiomq::macros::worker_store_suite;
use uuid::Uuid;

#[cfg(any(feature = "default", not(feature = "redis-store")))]
worker_store_suite!(worker_inmemory_store, async {
    use kiomq::InMemoryStore;
    let name = Uuid::new_v4().to_string();
    Ok::<_, kiomq::KioError>(InMemoryStore::<i32, i32, i32>::new(None, &name))
});

// `Worker::close` busy-waits on the main-loop task with `while !handle.is_finished() {}`.
// On a current-thread runtime that spin blocks the single worker thread, so the
// main loop can never be polled to observe the cancellation → deadlock.
// We drive it from a helper OS thread and fail (rather than hang the suite) via a
// timeout on the completion signal.
#[test]
fn close_does_not_deadlock_on_current_thread_runtime() {
    use kiomq::{InMemoryStore, Job, KioError, Queue, Worker};
    use std::sync::mpsc;
    use std::time::Duration;

    let (done_tx, done_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build current-thread runtime");
        rt.block_on(async {
            let name = Uuid::new_v4().to_string();
            let store = InMemoryStore::<u64, u64, ()>::new(None, &name);
            let queue = Queue::new(store, None).await.expect("queue");
            let worker = Worker::new_async(
                &queue,
                |_store, _job: Job<u64, u64, ()>| async move { Ok::<u64, KioError>(0) },
                None,
            )
            .expect("worker");
            worker.run().expect("run");
            worker.close();
        });
        let _ = done_tx.send(());
    });

    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("worker.close() deadlocked on a current-thread runtime (busy-wait spin)");
}

#[cfg(all(feature = "redis-store", not(feature = "default")))]
mod worker_redis {
    use super::*;
    use kiomq::{Config, RedisStore, SharedRedis, fetch_redis_pass};
    use std::sync::LazyLock;

    pub static SHARED_REDIS: LazyLock<SharedRedis> = LazyLock::new(|| {
        let password = fetch_redis_pass();
        let mut config = Config::default();
        if let Some(cfg) = config.connection.as_mut() {
            cfg.redis.password = password;
        }
        SharedRedis::create(&config).expect("failed to create connection")
    });

    worker_store_suite!(redis_store, async {
        let name = Uuid::new_v4().to_string();
        RedisStore::new(None, &name, &SHARED_REDIS).await
    });
}
