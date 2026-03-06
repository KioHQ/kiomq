use kiomq::macros::queue_store_suite;

#[cfg(any(feature = "default", not(feature = "redis-store")))]
queue_store_suite!(queue_inmemory_store, async {
    use kiomq::InMemoryStore;
    use uuid::Uuid;
    let name = Uuid::new_v4().to_string();
    Ok::<_, kiomq::KioError>(InMemoryStore::<i32, i32, i32>::new(None, &name))
});

#[cfg(all(feature = "redis-store", not(feature = "default")))]
mod queue_redis {
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

    queue_store_suite!(redis_store, async {
        use uuid::Uuid;
        let name = Uuid::new_v4().to_string();
        RedisStore::new(None, &name, &CONFIG).await
    });
}
