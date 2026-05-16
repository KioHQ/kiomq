use crate::{Dt, TimedMap};
use chrono::Utc;
use crossbeam::atomic::AtomicCell;
use derive_more::Debug;
use heapster::{Heapster, Stats};
use std::sync::RwLock;
use std::{alloc::System as SystemAlloc, sync::LazyLock, time::Duration};
use sysinfo::{get_current_pid, Pid, Process, ProcessRefreshKind, System};
use tokio::runtime::{Handle, RuntimeMetrics};
use tokio_metrics::RuntimeMonitor;
use uuid::Uuid;

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
pub struct ProcessMetricsCollector {
    /// PID of the process being monitored.
    pub pid: Pid,
    /// Runtime monitor used to obtain `RuntimeMetrics` for the current runtime.
    pub rt_monitor: RuntimeMonitor,
    /// `sysinfo::System` used to refresh process-specific data.
    pub process_monitor: RwLock<System>,
    /// Timestamp of the last successful metrics refresh.
    pub last_updated: AtomicCell<Dt>,
    /// Registry of active worker IDs mapped to their last-seen timestamp.
    pub workers: TimedMap<Uuid, Dt>,
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
    let process_monitor = RwLock::new(sys);
    ProcessMetricsCollector {
        pid,
        rt_monitor,
        process_monitor,
        last_updated,
        workers,
    }
});

impl ProcessMetricsCollector {
    /// Insert or refresh an active worker id.
    ///
    /// Records the provided `uuid` with the current timestamp. The entry is
    /// inserted with a short expiry so that workers which stop reporting are
    /// pruned automatically.
    pub async fn register_worker(&self, uuid: Uuid) {
        let dt = Utc::now();
        let timeout =
            Duration::from_nanos_u128(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL.as_millis() * 1000 * 10);
        self.workers.insert_expirable(uuid, dt, timeout).await;
    }

    /// Remove a previously-registered worker id.
    pub fn unregister_worker(&self, uuid: Uuid) {
        self.workers.remove(&uuid);
    }

    /// Refresh CPU and memory information for the monitored PID.
    /// This updates the internal [`sysinfo::System`] so subsequent reads of the
    /// monitored `Process` reflect recent CPU/memory values.
    pub fn refresh_process_metrics(&self) {
        let refresh_kind = ProcessRefreshKind::nothing().with_cpu().with_memory();
        self.process_monitor
            .write()
            .unwrap()
            .refresh_processes_specifics(
                sysinfo::ProcessesToUpdate::Some(&[self.pid]),
                true,
                refresh_kind,
            );
    }
}

/// Snapshot of node-level metrics.
///
/// [`ProcessMetrics`] is a compact, serialisable snapshot that includes hostname,
/// PID, allocator statistics from the global `Heapster` allocator, observed
/// CPU usage for the monitored process, Tokio runtime metrics, and a list of
/// active worker UUIDs. It is intended for monitoring endpoints and health
/// checks.
#[derive(Clone, Debug)]
pub struct ProcessMetrics {
    /// Hostname of the machine running the process.
    pub hostname: String,
    /// PID for the process that produced this snapshot.
    pub pid: Pid,
    /// Memory allocator statistics from the global [`Heapster`] allocator.
    pub memory_stats: Stats,
    /// Observed CPU usage for the process (percentage).
    pub cpu_usage: f32,
    /// Tokio runtime metrics captured for the runtime that produced the snapshot.
    pub rt_metrics: RuntimeMetrics,
    /// Known active worker UUIDs at the time the snapshot was taken.
    pub workers: Vec<Uuid>,
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
            rt_metrics,
            workers,
        }
    }
}
