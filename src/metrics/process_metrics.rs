use crate::timers::TimerType;
#[cfg(feature = "redis-store")]
use crate::utils::to_redis_parsing_error;
use crate::worker::WorkerState;
use crate::{Dt, TimedMap};
use chrono::Utc;
use crossbeam::atomic::AtomicCell;
use crossbeam_skiplist::{SkipMap, SkipSet};
use derive_more::Debug;
use futures::{FutureExt, Stream, StreamExt};
use heapster::{Heapster, Stats};
#[cfg(feature = "redis-store")]
use redis::{self, FromRedisValue, ParsingError};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::Arc;
use std::{alloc::System as SystemAlloc, sync::LazyLock, time::Duration};
use sysinfo::{get_current_pid, Pid, Process, ProcessRefreshKind, System};
use tokio::runtime::Handle;
use tokio::sync::broadcast::Sender;
use tokio::sync::{mpsc, watch};
use tokio::sync::{oneshot, RwLock};
use tokio_metrics::{RuntimeMetrics, RuntimeMonitor};
use tokio_util::sync::CancellationToken;
use tokio_util::time::{delay_queue::Key, DelayQueue};
use uuid::Uuid;
/// The TTL for the Worker State stored in [`ProcessMoniterCollector`].
pub const WORKER_STATE_TTL: u128 =
    sysinfo::MINIMUM_CPU_UPDATE_INTERVAL.as_millis() + Duration::from_secs(100).as_millis();
/// How often process metrics are updated by [`ProcessMoniterCollector`] in milliseconds.
pub const PROCESS_METRIC_UPDATE_INTERVAL: u128 =
    sysinfo::MINIMUM_CPU_UPDATE_INTERVAL.as_millis() + Duration::from_millis(200).as_millis();
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
    /// Number of logical processors from the num_cpus crate.
    pub cpu_count: usize,
    /// PID of the process being monitored.
    pub pid: Pid,
    /// Shared wrapper for [`System`], workers and `last_updated`
    pub inner: Arc<CollectorInner>,
    cancel_token: CancellationToken,
    /// Timer Sender
    pub(crate) tx: mpsc::Sender<(Uuid, TimerType, oneshot::Sender<()>)>,
}

pub struct CollectorInner {
    /// a [`tokio::sync::watch::Sender`] Sender  for updating [`ProcessMetrics`]
    updating_metrics_sender: watch::Sender<Option<ProcessMetrics>>,
    /// a [`tokio::sync::watch::Receiver`] Receiver  for updating [`ProcessMetrics`]
    pub updating_metrics_receiver: watch::Receiver<Option<ProcessMetrics>>,
    /// Runtime monitor used to obtain `RuntimeMetrics` for the current runtime.
    pub rt_monitor: RuntimeMonitor,
    /// `sysinfo::System` used to refresh process-specific data.
    pub process_monitor: RwLock<System>,
    /// Timestamp of the last successful metric ffs refresh.
    pub last_updated: AtomicCell<Dt>,
    /// Registry of active worker IDs mapped to their state [`WorkerState`].
    pub workers: TimedMap<Uuid, Arc<AtomicCell<WorkerState>>>,
    /// Registry of queues and their `TimerSender`.
    pub queues: SkipMap<Uuid, Sender<TimerCommand>>,
    /// all timer by Duration
    pub global_timers: SkipMap<Duration, TimerData>,
}
#[derive(Debug, Default)]
pub struct TimerData {
    pub queues: SkipSet<Uuid>,
    pub key: AtomicCell<Option<Key>>,
}
#[derive(Debug, Clone, Copy)]
pub enum TimerCommand {
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
    let queues = SkipMap::default();
    let global_timers = SkipMap::default();
    let cancel_token = CancellationToken::new();
    let process_monitor = RwLock::new(sys);
    let cpu_count = num_cpus::get();
    let (tx, rx) = mpsc::channel(100_000);
    let (updating_metrics_sender, updating_metrics_receiver) = watch::channel(None);
    let inner = Arc::new(CollectorInner {
        updating_metrics_sender,
        updating_metrics_receiver,
        rt_monitor,
        process_monitor,
        last_updated,
        workers,
        queues,
        global_timers,
    });
    let collector = ProcessMetricsCollector {
        cpu_count,
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
    pub fn register_queue(&self, queue_id: Uuid, sender: Sender<TimerCommand>) {
        let queues = &self.inner.queues;
        if queues.contains_key(&queue_id) {
            return;
        }
        queues.insert(queue_id, sender);
    }
    /// Insert or refresh an active worker id.
    pub async fn register_worker(&self, worker_id: Uuid, state: Arc<AtomicCell<WorkerState>>) {
        let workers = &self.inner.workers;
        if workers.inner.contains_key(&worker_id) {
            return;
        }
        let timeout = Duration::from_secs((WORKER_STATE_TTL) as u64);
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
        rx: tokio::sync::mpsc::Receiver<(Uuid, TimerType, oneshot::Sender<()>)>,
    ) -> tokio::task::JoinHandle<()> {
        let processor = self.clone();
        let token = processor.cancel_token.clone();
        let interval = Duration::from_millis(PROCESS_METRIC_UPDATE_INTERVAL as u64);

        tokio::spawn(async move {
            let metrics_stream = processor.create_process_metrics_stream(interval, token.clone()).fuse();
            let  incoming_cmd_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
            tokio::pin!(metrics_stream);
            tokio::pin!(incoming_cmd_stream);

            let inner = processor.inner.clone();
            let mut delayed_queue: DelayQueue<(Duration, TimerType)> = DelayQueue::new();
            let updating_metrics_sender = &inner.updating_metrics_sender;

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
                                if let Some(entry) = inner.queues.get(queue_id.value()) {
                                    let _ = entry.value().send(cmd.clone());
                                }
                            }

                        }
                    }

                    Some(metrics) = metrics_stream.next() => {
                        let _= updating_metrics_sender.send(Some(metrics));
                    }
                }
            }
        }.boxed())
    }
    pub fn create_process_metrics_stream(
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
            let cpu_count = self.cpu_count;
            async move {
                let intervals = match state {
                    State::Done => return None,
                    State::Active(s) => s,
                };

                tokio::select! {
                    biased;
                    () = cancel.cancelled() => None,
                    () = tokio::time::sleep(duration) => {
                        let process_refresh_kind = ProcessRefreshKind::nothing()
                            .with_cpu()
                            .with_memory();
                        {
                            let mut sys = inner.process_monitor.write().await;
                             sys.refresh_processes_specifics(
                                sysinfo::ProcessesToUpdate::Some(&[pid]),
                                true,
                                process_refresh_kind,
                            );
                              sys.refresh_memory();
                              drop(sys);
                            inner.last_updated.store(Utc::now());
                        }

                        let mut intervals = intervals;
                        let rt_metrics = intervals.next()?;
                        inner.workers.purge_expired().await;
                        let workers: Vec<_> = inner.workers
                            .inner
                            .iter()
                            .map(|e| (*e.key(), e.value().value.load()))
                            .collect();

                        let sys = inner.process_monitor.read().await;
                        let process = sys.process(pid)?;
                        let  metrics = ProcessMetrics::new(pid,cpu_count, &sys, rt_metrics, process, workers);
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
    pub process_cpu_usage: f32,
    /// Overall CPU usage of the current Machine(percentage)
    pub cpu_usage: f32,
    ///  Memory Usage  in bytes of the  current Process
    pub memory_usage: u64,
    /// Tokio runtime metrics captured for the runtime that produced the snapshot.
    pub rt_metrics: RawRuntimeMetrics,
    /// Known active workers and their state at the time the snapshot was taken.
    pub workers: Vec<(Uuid, WorkerState)>,
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
        cpu_thread_count: usize,
        sys: &System,
        rt_metrics: RuntimeMetrics,
        process: &Process,
        workers: Vec<(Uuid, WorkerState)>,
    ) -> Self {
        let cpu_usage = sys.global_cpu_usage();
        let memory_usage = process.memory();
        let hostname = System::host_name().unwrap_or_else(|| "<Unknown>".to_string());
        let memory_stats = GLOBAL.stats();
        let mut process_cpu_usage = process.cpu_usage();
        process_cpu_usage /= cpu_thread_count as f32;

        Self {
            memory_usage,
            process_cpu_usage,
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
