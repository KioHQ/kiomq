//! Process-level metrics and a global collector
//!
//! This module provides a lightweight, process-global metrics collector
//! (`P_METRICS_COLLECTOR`) which gathers CPU/memory and Tokio runtime
//! metrics and maintains a registry of active workers and queue timers.

use super::process_tree_tracker::{ProcessTreeStats, ProcessTreeTracker};
use crate::timers::TimerType;
#[cfg(feature = "redis-store")]
use crate::utils::to_redis_parsing_error;
use crate::worker::{ProcessingQueue, WorkerState};
use crate::{Dt, QueueMetrics, TimedMap, WorkerOpts};
use chrono::Utc;
use compact_str::{CompactString, ToCompactString};
use crossbeam::atomic::AtomicCell;
use crossbeam_skiplist::{SkipMap, SkipSet};
use derive_more::Debug;
use futures::{FutureExt, Stream, StreamExt};
use heapster::{Heapster, Stats};
use num_traits::AsPrimitive;
use parking_lot::RwLock;
#[cfg(feature = "redis-store")]
use redis::{self, FromRedisValue, ParsingError};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::{alloc::System as SystemAlloc, sync::LazyLock, time::Duration};
use tokio::runtime::Handle;
use tokio::sync::broadcast::Sender;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_metrics::{RuntimeMetrics, RuntimeMonitor};
use tokio_util::sync::CancellationToken;
use tokio_util::time::{delay_queue::Key, DelayQueue};
use uuid::Uuid;
/// Worker state TTL (milliseconds)
///
/// How long a worker's state is retained in the collector's registry
/// before being considered expired.
#[cfg(not(target_os = "linux"))]
pub const WORKER_STATE_TTL: u128 =
    sysinfo::MINIMUM_CPU_UPDATE_INTERVAL.as_millis() + Duration::from_secs(100).as_millis();
/// Worker state TTL (milliseconds)
///
/// How long a worker's state is retained in the collector's registry
/// before being considered expired.
#[cfg(target_os = "linux")]
pub const WORKER_STATE_TTL: u128 = Duration::from_secs(100).as_millis();

/// Process metrics collection interval (milliseconds)
///
/// How often the global collector refreshes system and runtime metrics.
#[cfg(not(target_os = "linux"))]
pub const PROCESS_METRIC_UPDATE_INTERVAL: u128 =
    sysinfo::MINIMUM_CPU_UPDATE_INTERVAL.as_millis() + Duration::from_millis(200).as_millis();
/// Process metrics collection interval (milliseconds)
///
/// How often the global collector refreshes system and runtime metrics.
#[cfg(target_os = "linux")]
pub const PROCESS_METRIC_UPDATE_INTERVAL: u128 = 300;
/// Global allocator instrumented by [`Heapster`].
///
/// `Heapster` wraps the system allocator and exposes allocation statistics via
/// `stats()`. Setting this as the global allocator enables the process to
/// report heap metrics through `GLOBAL.stats()`.
#[global_allocator]
pub static GLOBAL: Heapster<SystemAlloc> = Heapster::new(SystemAlloc);

/// Global process and runtime metrics collector
///
/// A small, clonable collector that exposes runtime and system-level
/// snapshots (CPU, memory, and Tokio runtime metrics). Use the
/// [`P_METRICS_COLLECTOR`] singleton to register queues/workers and to
/// receive periodic [`ProcessMetrics`] updates.
#[derive(Clone)]
pub struct ProcessMetricsCollector {
    /// Hostname of the machine running the process.
    pub hostname: String,
    /// PID of the process being monitored.
    pub pid: u32,
    /// Shared wrapper for [`System`], workers and `last_updated`
    pub inner: Arc<CollectorInner>,
    cancel_token: CancellationToken,
    /// Sender used by timers to register global timer requests
    pub tx: mpsc::Sender<(Uuid, TimerType, oneshot::Sender<()>)>,
}

type WorkerRegistry = TimedMap<
    Uuid,
    (
        Arc<AtomicCell<WorkerState>>,
        ProcessingQueue,
        WorkerOpts,
        Dt,
    ),
>;

/// Internal shared collector state
pub struct CollectorInner {
    /// a [`tokio::sync::watch::Sender`] Sender  for updating [`ProcessMetrics`]
    updating_metrics_sender: watch::Sender<Option<ProcessMetrics>>,
    /// a [`tokio::sync::watch::Receiver`] Receiver  for updating [`ProcessMetrics`]
    pub updating_metrics_receiver: watch::Receiver<Option<ProcessMetrics>>,
    /// Runtime monitor used to obtain `RuntimeMetrics` for the current runtime.
    pub rt_monitor: RuntimeMonitor,
    /// a structure used to refresh process-specific data.
    pub process_monitor: RwLock<ProcessTreeTracker>,
    /// Timestamp of the last successful metric ffs refresh.
    pub last_updated: AtomicCell<Dt>,
    /// Registry of active worker IDs mapped to their state [`WorkerState`].
    pub workers: WorkerRegistry,
    /// Registry of queues and their `TimerSender`.
    pub queues: SkipMap<Uuid, (Sender<TimerCommand>, Arc<QueueMetrics>)>,
    /// all timer by Duration
    pub global_timers: SkipMap<Duration, TimerData>,
}
#[derive(Debug, Default)]
/// Metadata for a global timer entry.
///
/// Tracks which queues are subscribed to a particular timer duration and
/// stores the `DelayQueue` key for the scheduled entry.
pub struct TimerData {
    /// Queues subscribed to this timer duration.
    pub queues: SkipSet<Uuid>,
    /// `DelayQueue` key for the scheduled timer entry.
    pub key: AtomicCell<Option<Key>>,
}
#[derive(Debug, Clone, Copy)]
/// Commands delivered to per-queue timer tasks when a global timer fires.
pub enum TimerCommand {
    /// Ask the timer task to process the supplied `TimerType`.
    RespondToTimer(TimerType),
}
/// Lazily-initialised global [`ProcessMetricsCollector`].
///
/// Use `P_METRICS_COLLECTOR` to register/unregister workers and to access the
/// runtime/process monitoring primitives provided by the collector.
pub static P_METRICS_COLLECTOR: LazyLock<ProcessMetricsCollector> = LazyLock::new(|| {
    let rt_monitor = RuntimeMonitor::new(&Handle::current());
    let pid = std::process::id();
    let last_updated = AtomicCell::new(Utc::now());
    let workers = TimedMap::default();
    let queues = SkipMap::default();
    let global_timers = SkipMap::default();
    let cancel_token = CancellationToken::new();
    let process_tracker = ProcessTreeTracker::new();
    let process_monitor = RwLock::new(process_tracker);
    let hostname = hostname::get()
        .and_then(|name| {
            name.into_string()
                .map_err(|_| std::io::Error::other("failed to convert from ostring"))
        })
        .unwrap_or_else(|_| "<Unknown>".to_owned());
    let (tx, rx) = mpsc::channel(10_000);
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
        hostname,
        pid,
        inner,
        cancel_token,
        tx,
    };
    collector.create_global_timer_task(rx);
    collector
});

impl ProcessMetricsCollector {
    /// Register a queue so it can receive global timer events.
    pub fn register_queue(
        &self,
        queue_id: Uuid,
        sender: Sender<TimerCommand>,
        metrics: Arc<QueueMetrics>,
    ) {
        let queues = &self.inner.queues;
        if queues.contains_key(&queue_id) {
            return;
        }
        queues.insert(queue_id, (sender, metrics));
    }
    /// Insert or refresh an active worker id.
    pub fn register_worker(
        &self,
        worker_id: Uuid,
        state: (
            Arc<AtomicCell<WorkerState>>,
            ProcessingQueue,
            WorkerOpts,
            Dt,
        ),
    ) {
        let workers = &self.inner.workers;
        if workers.contains_key(&worker_id) {
            return;
        }
        let timeout = Duration::from_secs(WORKER_STATE_TTL.as_());
        workers.insert_expirable(worker_id, state, timeout);
    }

    /// Returns if a timer exists for a specific queue
    pub fn timer_exists(&self, timer: &TimerType, queue_id: &Uuid) -> bool {
        let duration = timer.next_duration();
        if let Some(existing_timer) = self.inner.global_timers.get(&duration) {
            return existing_timer.value().queues.contains(queue_id);
        }

        false
    }
    /// Remove a previously-registered worker id.
    pub fn unregister_worker(&self, uuid: Uuid) {
        self.inner.workers.remove(&uuid);
    }
    /// Remove a previously-registered queue id.
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
        let interval = Duration::from_millis(PROCESS_METRIC_UPDATE_INTERVAL.as_());

        tokio::spawn(async move {
            let  mut metrics_stream = processor.create_process_metrics_stream(interval, token.clone()).boxed();
            let  mut incoming_cmd_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
            let inner = processor.inner.clone();
            let mut delayed_queue: DelayQueue<(Duration, TimerType)> = DelayQueue::new();
            let updating_metrics_sender = &inner.updating_metrics_sender;
            loop{
                tokio::select! {
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
                                    let _ = entry.value().0.send(cmd);
                                }
                            }

                        }
                    }
                    Some(recieved_metrics) = metrics_stream.next() => {
                        if let Some(metrics) = recieved_metrics {
                        let _= updating_metrics_sender.send(Some(metrics));
                        }
                    }
                }
                tokio::task::yield_now().await;
            }
        }.boxed())
    }
    /// Return a stream that yields `ProcessMetrics` snapshots every `duration`.
    ///
    /// The stream terminates when `cancel_token` is cancelled.
    pub fn create_process_metrics_stream(
        &self,
        duration: Duration,
        cancel_token: CancellationToken,
    ) -> impl Stream<Item = Option<ProcessMetrics>> + use<'_> {
        #[allow(unused)]
        enum State<S> {
            Active(S),
            Done,
        }
        let intervals = self.inner.rt_monitor.intervals();
        let inner = self.inner.clone();

        futures::stream::unfold(State::Active(intervals), move |state| {
            let cancel = cancel_token.clone();
            let pid = self.pid;
            let hostname = &self.hostname;
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

                        let mut intervals = intervals;
                        let rt_metrics = intervals.next()?;
                        let mut process_monitor = inner.process_monitor.write();
                         let stats = process_monitor.sample();
                        drop(process_monitor);
                        inner.workers.purge_expired();
                        let workers: Vec<WorkerMeta> = inner.workers
                            .iter()
                            .map(|entry| {
                                let worker_id =  *entry.key();
                                let data = entry.value().get();
                                let data = data.lock();
                                WorkerMeta::new(worker_id, data.3, data.0.load(), data.2, data.1.len())

                            }  )

                            .collect();
                        let  metrics = ProcessMetrics::new(hostname.to_compact_string(), pid, rt_metrics,stats, workers);

                       inner.last_updated.store(Utc::now());
                         Some((Some(metrics), State::Active(intervals)))

                    }
                }
            }
        })
    }
}

/// Process-level metrics snapshot
///
/// Compact, serialisable snapshot of system and Tokio runtime metrics for the
/// current process.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProcessMetrics {
    /// Hostname of the machine running the process.
    pub hostname: CompactString,
    /// PID for the process that produced this snapshot.
    pub pid: u32,
    /// Memory allocator statistics from the global [`Heapster`] allocator.
    pub memory_stats: Stats,
    /// Observed CPU usage for the process (percentage).
    pub process_cpu_usage: f32,
    ///  Memory Usage  in bytes of the  current Process
    pub memory_usage: u64,
    /// Tokio runtime metrics captured for the runtime that produced the snapshot.
    pub rt_metrics: RawRuntimeMetrics,
    /// Known active workers and their state at the time the snapshot was taken.
    pub workers: Vec<WorkerMeta>,
    /// Timestamp of the last successful metric ffs refresh.
    pub last_updated: Dt,
}
/// Sample metadata from  a worker
#[derive(Clone, Debug, Serialize, Deserialize, Copy)]
pub struct WorkerMeta {
    /// The id of the worker
    pub worker_id: Uuid,
    /// The date and time this worker was created and registered.
    pub started_at: Dt,
    /// The current state of the worker. check [`WorkerState`]
    pub state: WorkerState,
    /// (worker options)[`crate::worker::WorkerOpts`].
    pub worker_opts: WorkerOpts,
    /// The creation datetime of [`WorkerMeta`].
    pub last_updated: Dt,
    /// The current number of jobs the worker is processing.
    pub processing: usize,
}
impl WorkerMeta {
    /// Construct a [`WorkerMeta`] snapshot.
    #[must_use]
    pub fn new(
        worker_id: Uuid,
        started_at: Dt,
        state: WorkerState,
        worker_opts: WorkerOpts,
        processing: usize,
    ) -> Self {
        Self {
            worker_id,
            started_at,
            state,
            worker_opts,
            processing,
            last_updated: Utc::now(),
        }
    }
}

impl ProcessMetrics {
    /// Construct a `ProcessMetrics` snapshot from system and runtime values.
    #[must_use]
    pub fn new(
        hostname: CompactString,
        pid: u32,
        rt_metrics: RuntimeMetrics,
        stats: ProcessTreeStats,
        workers: Vec<WorkerMeta>,
    ) -> Self {
        let memory_usage = stats.rss_bytes;
        let memory_stats = GLOBAL.stats();
        let process_cpu_usage = stats.cpu_usage;

        Self {
            memory_usage,
            process_cpu_usage,
            hostname,
            pid,
            memory_stats,
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

#[cfg(feature = "redis-store")]
impl FromRedisValue for ProcessMetrics {
    fn from_redis_value(v: redis::Value) -> Result<Self, ParsingError> {
        let mut bytes: Vec<u8> = redis::from_redis_value(v)?;
        let metrics = simd_json::from_slice(&mut bytes).map_err(to_redis_parsing_error)?;
        Ok(metrics)
    }
}
