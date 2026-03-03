#![allow(unused)]
use kiomq::macros::worker_store_suite;
use uuid::Uuid;

#[cfg(any(feature = "default", not(feature = "redis-store")))]
worker_store_suite!(worker_inmemory_store, async {
    use kiomq::InMemoryStore;
    let name = Uuid::new_v4().to_string();
    Ok::<_, kiomq::KioError>(InMemoryStore::<i32, i32, i32>::new(None, &name))
});

#[cfg(all(feature = "redis-store", not(feature = "default")))]
mod worker_redis {
    use super::*;
    use kiomq::{fetch_redis_pass, Config, RedisStore};
    use std::sync::LazyLock;

    pub static CONFIG: LazyLock<Config> = LazyLock::new(|| {
        let password = fetch_redis_pass();
        let mut config = Config::default();
        if let Some(cfg) = config.connection.as_mut() {
            cfg.redis.password = password;
        }
        config
    });

    worker_store_suite!(redis_store, async {
        let name = Uuid::new_v4().to_string();
        RedisStore::new(None, &name, &CONFIG).await
    });
}
