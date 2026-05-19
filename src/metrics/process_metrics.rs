use crate::timers::TimerType;
#[cfg(feature = "redis-store")]
use crate::utils::to_redis_parsing_error;
use crate::worker::WorkerState;
use crate::{Dt, TimedMap};
use chrono::Utc;
use crossbeam::atomic::AtomicCell;
use crossbeam_skiplist::{SkipMap, SkipSet};
use derive_more::Debug;
use flume::{self, Sender};
use futures::{FutureExt, Stream, StreamExt};
use heapster::{Heapster, Stats};
#[cfg(feature = "redis-store")]
use redis::{self, FromRedisValue, ParsingError};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::Arc;
use std::{alloc::System as SystemAlloc, sync::LazyLock, time::Duration};
use sysinfo::{get_current_pid, Pid, Process, ProcessRefreshKind, System};
use tokio::runtime::Handle;
use tokio::sync::{oneshot, RwLock};
use tokio_metrics::{RuntimeMetrics, RuntimeMonitor};
use tokio_util::sync::CancellationToken;
use tokio_util::time::{delay_queue::Key, DelayQueue};
use uuid::Uuid;
/// The TTL for the Worker State stored in [`ProcessMoniterCollector`].
pub const WORKER_STATE_TTL: u128 =
    sysinfo::MINIMUM_CPU_UPDATE_INTERVAL.as_millis() + Duration::from_secs(1000).as_millis();
/// Global allocator instrumented by [`Heapster`].
///
/// `Heapster` wraps the system allocator and exposes allocation statistics via
/// `stats()`. Setting this as the global allocator enables the process to
/// report heap metrics through `GLOBAL.stats()`.
#[global_allocator]
pub static GLOBAL: Heapster<SystemAlloc> = Heapster::new(SystemAlloc);

/// Aggregated process- and runtime-level metrics.
///
/// Holds the monitored process id, a `tokio_metrics::RuntimeMonitor` for
/// collecting Tokio runtime statistics, a `sysinfo::System` instance used to
/// refresh process-specific CPU/memory figures, and a `TimedMap` that tracks
/// active worker UUIDs. The collector is deliberately lightweight and is
/// intended to live for the lifetime of the process.
#[derive(Clone)]
pub struct ProcessMetricsCollector {
    /// PID of the process being monitored.
    pub pid: Pid,
    /// Shared wrapper for [`System`], workers and `last_updated`
    pub inner: Arc<CollectorInner>,
    cancel_token: CancellationToken,
    /// Timer Sender
    pub(crate) tx: Sender<(Uuid, TimerType, oneshot::Sender<()>)>,
}

pub struct CollectorInner {
    /// Runtime monitor used to obtain `RuntimeMetrics` for the current runtime.
    pub rt_monitor: RuntimeMonitor,
    /// `sysinfo::System` used to refresh process-specific data.
    pub process_monitor: RwLock<System>,
    /// Timestamp of the last successful metric ffs refresh.
    pub last_updated: AtomicCell<Dt>,
    /// Registry of active worker IDs mapped to their state [`WorkerState`].
    pub workers: TimedMap<Uuid, Arc<AtomicCell<WorkerState>>>,
    /// Registry of queues and their `TimerSender`.
    pub queues: TimedMap<Uuid, Sender<TimerCommand>>,
    /// all timer by Duration
    pub global_timers: SkipMap<Duration, TimerData>,
}
#[derive(Debug, Default)]
pub struct TimerData {
    pub queues: SkipSet<Uuid>,
    pub key: AtomicCell<Option<Key>>,
}
#[derive(Debug, Clone)]
pub enum TimerCommand {
    PostMetrics(Box<ProcessMetrics>),
    RespondToTimer(TimerType),
}
/// Lazily-initialised global [`ProcessMetricsCollector`].
///
/// Use `P_METRICS_COLLECTOR` to register/unregister workers and to access the
/// runtime/process monitoring primitives provided by the collector.
pub static P_METRICS_COLLECTOR: LazyLock<ProcessMetricsCollector> = LazyLock::new(|| {
    let rt_monitor = RuntimeMonitor::new(&Handle::current());
    let sys = System::new();
    let pid = get_current_pid().unwrap_or_else(|_| Pid::from_u32(0));
    let last_updated = AtomicCell::new(Utc::now());
    let workers = TimedMap::default();
    let queues = TimedMap::default();
    let global_timers = SkipMap::default();
    let cancel_token = CancellationToken::new();
    let process_monitor = RwLock::new(sys);
    let (tx, rx) = flume::bounded(100000);
    let inner = Arc::new(CollectorInner {
        rt_monitor,
        process_monitor,
        last_updated,
        workers,
        queues,
        global_timers,
    });
    let collector = ProcessMetricsCollector {
        pid,
        inner,
        cancel_token,
        tx,
    };
    collector.create_global_timer_task(rx);
    collector
});

impl ProcessMetricsCollector {
    /// adds or refresh
    pub async fn register_queue(&self, queue_id: Uuid, sender: Sender<TimerCommand>) {
        let queues = &self.inner.queues;
        if queues.inner.contains_key(&queue_id) {
            return;
        }
        let timeout = Duration::from_millis(WORKER_STATE_TTL as u64);
        queues.insert_expirable(queue_id, sender, timeout).await;
    }
    /// Insert or refresh an active worker id.
    pub async fn register_worker(&self, worker_id: Uuid, state: Arc<AtomicCell<WorkerState>>) {
        let workers = &self.inner.workers;
        if workers.inner.contains_key(&worker_id) {
            return;
        }
        let timeout = Duration::from_millis(WORKER_STATE_TTL as u64);
        workers.insert_expirable(worker_id, state, timeout).await;
    }

    /// Remove a previously-registered worker id.
    pub fn unregister_worker(&self, uuid: Uuid) {
        self.inner.workers.remove(&uuid);
    }
    pub fn unregister_queue(&self, queue_id: Uuid) {
        self.inner.queues.remove(&queue_id);
        self.inner.global_timers.iter().for_each(|entry| {
            entry.value().queues.remove(&queue_id);
        });
    }

    fn create_global_timer_task(
        &self,
        rx: flume::Receiver<(Uuid, TimerType, oneshot::Sender<()>)>,
    ) -> tokio::task::JoinHandle<()> {
        let processor = self.clone();
        let token = processor.cancel_token.clone();
        let interval = sysinfo::MINIMUM_CPU_UPDATE_INTERVAL + Duration::from_millis(100);

        tokio::spawn(async move {
            let metrics_stream = processor.intervals(interval, token.clone()).fuse();
            let  incoming_cmd_stream = rx.stream().fuse();
            tokio::pin!(metrics_stream);
            tokio::pin!(incoming_cmd_stream);

            let inner = processor.inner.clone();
            let mut delayed_queue: DelayQueue<(Duration, TimerType)> = DelayQueue::new();

            while !token.is_cancelled() {
                tokio::select! {
                    biased;

                    () = token.cancelled() => {
                        break;
                    }

                   Some((queue_id, timer,  ack)) = incoming_cmd_stream.next() => {
                                let duration = timer.next_duration();
                                let entry = inner.global_timers.get_or_insert_with(duration,TimerData::default);
                                let value = entry.value();
                                value.queues.insert(queue_id);
                                let key = delayed_queue.insert((duration, timer), duration);
                                value.key.swap(Some(key));
                                ack.send(()).ok();

                        }
                    Some(expired) = delayed_queue.next() => {
                        let (duration, timer) = expired.into_inner();
                        let cmd = TimerCommand::RespondToTimer(timer);

                        if let Some(entry) = inner.global_timers.remove(&duration) {

                            let data = entry.value();
                            let  targets = &data.queues;

                            if let TimerType::PromotedDelayed(_, queue_id) = timer {
                                targets.insert(queue_id);

                            }

                            for queue_id in targets {
                                if let Some(entry) = inner.queues.inner.get(queue_id.value()) {
                                    let _ = entry.value().value.send(cmd.clone());
                                }
                            }

                        }
                    }

                    Some(metrics) = metrics_stream.next() => {
                        let cmd = TimerCommand::PostMetrics(Box::new(metrics));
                        for entry in &inner.queues.inner {
                            let _ = entry.value().value.send(cmd.clone());
                        }
                    }
                }
            }
        }.boxed())
    }
    pub fn intervals(
        &self,
        duration: Duration,
        cancel_token: CancellationToken,
    ) -> impl Stream<Item = ProcessMetrics> + use<'_> {
        let intervals = self.inner.rt_monitor.intervals();
        let inner = self.inner.clone();
        #[allow(unused)]
        enum State<S> {
            Active(S),
            Done,
        }

        futures::stream::unfold(State::Active(intervals), move |state| {
            let cancel = cancel_token.clone();
            let pid = self.pid;
            let inner = inner.clone();
            async move {
                let intervals = match state {
                    State::Done => return None,
                    State::Active(s) => s,
                };

                tokio::select! {
                    biased;
                    () = cancel.cancelled() => None,
                    () = tokio::time::sleep(duration) => {
                        let refresh_kind = ProcessRefreshKind::nothing()
                            .with_cpu()
                            .with_memory();
                        {
                             inner.process_monitor.write().await.refresh_processes_specifics(
                                sysinfo::ProcessesToUpdate::Some(&[pid]),
                                true,
                                refresh_kind,
                            );
                            inner.last_updated.store(Utc::now());
                        }

                        let mut intervals = intervals;
                        let rt_metrics = intervals.next()?;
                        inner.workers.purge_expired().await;
                        inner.queues.purge_expired().await;
                        let workers: Vec<Uuid> = inner.workers
                            .inner
                            .iter()
                            .map(|e| *e.key())
                            .collect();

                        let sys = inner.process_monitor.read().await;
                        let process = sys.process(pid)?;
                        let metrics = ProcessMetrics::new(pid, rt_metrics, process, workers);
                        drop(sys);
                        Some((metrics, State::Active(intervals)))
                    }
                }
            }
        })
    }
}

/// Snapshot of node-level metrics.
///
/// [`ProcessMetrics`] is a compact, serialisable snapshot that includes hostname,
/// PID, allocator statistics from the global `Heapster` allocator, observed
/// CPU usage for the monitored process, Tokio runtime metrics, and a list of
/// active worker UUIDs. It is intended for monitoring endpoints and health
/// checks.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProcessMetrics {
    /// Hostname of the machine running the process.
    pub hostname: String,
    #[serde(serialize_with = "serialize_pid", deserialize_with = "deserialize_pid")]
    /// PID for the process that produced this snapshot.
    pub pid: Pid,
    /// Memory allocator statistics from the global [`Heapster`] allocator.
    pub memory_stats: Stats,
    /// Observed CPU usage for the process (percentage).
    pub cpu_usage: f32,
    /// Tokio runtime metrics captured for the runtime that produced the snapshot.
    pub rt_metrics: RawRuntimeMetrics,
    /// Known active worker UUIDs at the time the snapshot was taken.
    pub workers: Vec<Uuid>,
    /// Timestamp of the last successful metric ffs refresh.
    pub last_updated: Dt,
}

impl ProcessMetrics {
    /// Create a new [`ProcessMetrics`] snapshot.
    ///
    /// Collects the hostname, allocator stats from the global [`Heapster`], the
    /// CPU usage read from `process`, and the supplied `rt_metrics` and
    /// `workers` list.
    ///
    /// # Arguments
    ///
    /// * `pid` - the process id being monitored.
    /// * `rt_metrics` - Tokio runtime metrics obtained from a [`RuntimeMonitor`].
    /// * `process` - `sysinfo::Process` corresponding to `pid`; used to read CPU usage.
    /// * `workers` - list of active worker UUIDs to embed in the snapshot.
    #[must_use]
    pub fn new(
        pid: Pid,
        rt_metrics: RuntimeMetrics,
        process: &Process,
        workers: Vec<Uuid>,
    ) -> Self {
        let hostname = System::host_name().unwrap_or_else(|| "<Unknown>".to_string());
        let memory_stats = GLOBAL.stats();
        let cpu_usage = process.cpu_usage();

        Self {
            hostname,
            pid,
            memory_stats,
            cpu_usage,
            rt_metrics: rt_metrics.into(),
            workers,
            last_updated: Utc::now(),
        }
    }
}
/// A mininal mirror of [`RuntimeMetrics`] with serde traits implemented.
///
/// For full documents, check [`RuntimeMetrics`] .
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RawRuntimeMetrics {
    /// Number of worker threads.
    pub workers_count: usize,
    /// Number of currently alive tasks.
    pub live_tasks_count: usize,
    /// Total times worker threads parked.
    pub total_park_count: u64,
    /// Maximum times any single worker parked.
    pub max_park_count: u64,
    /// Minimum times any single worker parked.
    pub min_park_count: u64,
    /// Total duration workers spent executing tasks.
    pub total_busy_duration: Duration,
    /// Maximum continuous duration a worker was busy.
    pub max_busy_duration: Duration,
    /// Minimum continuous duration a worker was busy.
    pub min_busy_duration: Duration,
    /// Current depth of the global injection queue.
    pub global_queue_depth: usize,
    /// Elapsed time for this metrics interval.
    pub elapsed: Duration,
}
impl From<RuntimeMetrics> for RawRuntimeMetrics {
    fn from(value: RuntimeMetrics) -> Self {
        Self {
            workers_count: value.workers_count,
            live_tasks_count: value.live_tasks_count,
            total_park_count: value.total_park_count,
            max_park_count: value.max_park_count,
            min_park_count: value.min_park_count,
            total_busy_duration: value.total_busy_duration,
            max_busy_duration: value.max_busy_duration,
            min_busy_duration: value.min_busy_duration,
            global_queue_depth: value.global_queue_depth,
            elapsed: value.elapsed,
        }
    }
}

#[allow(clippy::trivially_copy_pass_by_ref)] // a reference is required for serde(serialize_with)
fn serialize_pid<S>(pid: &Pid, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    (pid.as_u32()).serialize(serializer)
}

fn deserialize_pid<'de, D>(deserializer: D) -> Result<Pid, D::Error>
where
    D: Deserializer<'de>,
{
    let value = u32::deserialize(deserializer)?;
    Ok(Pid::from_u32(value))
}

#[cfg(feature = "redis-store")]
impl FromRedisValue for ProcessMetrics {
    fn from_redis_value(v: redis::Value) -> Result<Self, ParsingError> {
        let mut bytes: Vec<u8> = redis::from_redis_value(v)?;
        let metrics = simd_json::from_slice(&mut bytes).map_err(to_redis_parsing_error)?;
        Ok(metrics)
    }
}
