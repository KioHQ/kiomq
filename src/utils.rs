use serde::Serialize;

use std::fmt::Write;

use crate::KioResult;

// ---------------- REDIS FUNCTION here
pub fn fetch_redis_pass() -> Option<String> {
    use dotenv;
    if let Err(err) = dotenv::dotenv() {
        // dothing; continue
    }
    std::env::var("REDIS_PASSWORD").ok()
}

pub fn serialize_into_pairs<V: Serialize>(item: &V) -> Vec<(String, String)> {
    if let Ok(value) = serde_json::to_value(item) {
        if let Some(obj) = value.as_object() {
            return obj
                .into_iter()
                .map(|(key, val)| (key.to_owned(), val.to_string()))
                .collect();
        }
    }
    vec![]
}
pub struct Batches {
    n: usize,
    batch_size: usize,
    i: usize,
}

impl Batches {
    pub fn new(n: usize, batch_size: usize) -> Self {
        Batches {
            n,
            batch_size,
            i: 0,
        }
    }
}

impl Iterator for Batches {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let from = self.i * self.batch_size + 1;
        if from <= self.n {
            let to = std::cmp::min(from + self.batch_size - 1, self.n);
            self.i += 1;
            Some((from, to))
        } else {
            None
        }
    }
}
use crate::{CollectionSuffix, JobMetrics};
pub async fn get_job_metrics<C: redis::aio::ConnectionLike>(
    prefix: &str,
    name: &str,
    conn: &mut C,
) -> KioResult<JobMetrics> {
    let [job_id_key, stalled_key, active_key, completed_key] = [
        CollectionSuffix::Id,
        CollectionSuffix::Stalled,
        CollectionSuffix::Active,
        CollectionSuffix::Completed,
    ]
    .map(|key| key.to_collection_name(prefix, name));
    let mut pipeline = redis::pipe();
    pipeline.atomic();
    pipeline.zcard(completed_key);
    pipeline.llen(active_key);

    pipeline.scard(stalled_key);
    pipeline.get(job_id_key);
    let (completed, active, stalled, last_id) = pipeline.query_async(conn).await?;

    Ok(JobMetrics {
        active,
        last_id,
        stalled,
        completed,
    })
}
