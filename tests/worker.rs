#![allow(unused_must_use)]
use kiomq::macros::worker_store_suite;
use uuid::Uuid;

#[cfg(any(feature = "default", not(feature = "redis-store")))]
worker_store_suite!(worker_inmemory_store, async {
    use kiomq::InMemoryStore;
    let name = Uuid::new_v4().to_string();
    Ok::<_, kiomq::KioError>(InMemoryStore::<i32, i32, i32>::new(None, &name))
});

// `Worker::close()` is a synchronous "stop and drain" barrier and is meant to be
// callable from ordinary sync code (workers are typically long-lived singletons).
// Here we start the worker inside a multi-threaded runtime, then call `close()`
// from a plain thread that is *not* on the runtime — exercising the
// `futures::executor::block_on` path. The runtime stays alive on its own worker
// threads to poll the main loop to completion while `close()` blocks. We drive it
// from a helper OS thread and fail (rather than hang the suite) via a timeout on
// the completion signal.
#[test]
fn close_from_sync_code_blocks_until_drained() {
    use kiomq::{InMemoryStore, Job, KioError, Queue, Worker};
    use std::sync::mpsc;
    use std::time::Duration;

    let (done_tx, done_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("failed to build multi-thread runtime");
        // Create and start the worker inside the runtime.
        let worker = rt.block_on(async {
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
            worker
        });
        // Close from sync code (no runtime on this thread).
        worker.close();
        assert!(
            !worker.is_running(),
            "worker should be stopped after close()"
        );
        drop(rt);
        let _ = done_tx.send(());
    });

    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("worker.close() from sync code hung instead of draining");
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
