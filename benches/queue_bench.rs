use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use tokio::runtime::Runtime;

use kiomq::{InMemoryStore, Job, JobState, KioError, Queue, Worker, WorkerOpts};

fn bench_bulk_add(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("queue_bulk_add");

    for &size in &[1_000usize, 5_000, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    let store: InMemoryStore<i32, i32, i32> =
                        InMemoryStore::new(None, "bench-bulk");
                    let queue = Queue::new(store, None).await.unwrap();
                    let jobs = (0..n).map(|i| (i.to_string(), None, i as i32));
                    queue.bulk_add_only(jobs).await.unwrap();
                    let _ = queue.obliterate().await;
                });
            })
        });
    }

    group.finish();
}

fn bench_end_to_end_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("queue_end_to_end_throughput");

    for &size in &[1_000usize, 5_000, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    let store: InMemoryStore<u64, u64, ()> = InMemoryStore::new(None, "bench-e2e");
                    let queue = Queue::new(store, None).await.unwrap();
                    let completed: Arc<AtomicUsize> = Arc::default();
                    let c_clone = completed.clone();
                    queue.on(JobState::Completed, move |_state| {
                        let counter = c_clone.clone();
                        async move {
                            counter.fetch_add(1, Ordering::AcqRel);
                        }
                    });

                    let processor = |_store: Arc<_>, job: Job<u64, u64, ()>| async move {
                        Ok::<u64, KioError>(job.data.unwrap_or_default())
                    };

                    let opts = WorkerOpts::default();
                    let worker = Worker::new_async(&queue, processor, Some(opts)).unwrap();
                    worker.run().unwrap();

                    let jobs = (0..n as u64).map(|i| (i.to_string(), None, i));
                    queue.bulk_add_only(jobs).await.unwrap();

                    let start = Instant::now();
                    while !completed.load(Ordering::Acquire) < n {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    let elapsed = start.elapsed();
                    worker.close();
                    if worker.closed() {
                        let _ = queue.obliterate().await;
                    }
                    let _ = elapsed;
                });
            })
        });
    }

    group.finish();
}

fn bench_single_job_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("queue_single_job_latency");

    group.bench_function("single_job_roundtrip_1000", |b| {
        b.iter(|| {
            rt.block_on(async {
                let store: InMemoryStore<String, (), ()> =
                    InMemoryStore::new(None, "bench-latency");
                let queue = Queue::new(store, None).await.unwrap();
                let counter: Arc<AtomicU8> = Arc::default();
                let c_clone = counter.clone();
                queue.on(JobState::Completed, move |_state| {
                    let counter = c_clone.clone();
                    async move {
                        counter.fetch_add(1, Ordering::AcqRel);
                    }
                });

                let processor =
                    |_store: Arc<_>, _job: Job<String, (), ()>| async { Ok::<(), KioError>(()) };
                let worker = Worker::new_async(&queue, processor, None).unwrap();
                worker.run().unwrap();

                let start = Instant::now();
                let job_name = "latency-job".to_string();
                queue
                    .bulk_add_only(std::iter::once((job_name.clone(), None, job_name)))
                    .await
                    .unwrap();

                while !counter.load(Ordering::Acquire) < 1 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }

                let _lat = start.elapsed();
                worker.close();
                if worker.closed() {
                    let _ = queue.obliterate().await;
                }
            })
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_bulk_add,
    bench_end_to_end_throughput,
    bench_single_job_latency
);
criterion_main!(benches);
