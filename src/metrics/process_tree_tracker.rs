/// Number of logical CPUs, falling back to 1 when the platform can't report it.
fn get() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZero::get)
}
#[cfg(not(target_os = "linux"))]
use std::collections::HashSet;
#[cfg(target_os = "linux")]
use std::time::Instant;
#[cfg(not(target_os = "linux"))]
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate};

#[derive(Debug, Clone, Copy)]
pub struct ProcessTreeStats {
    pub cpu_usage: f32,
    pub rss_bytes: u64,
    pub virt_bytes: u64,
}

#[cfg(target_os = "linux")]
pub struct ProcessTreeTracker {
    me: procfs::process::Process,
    prev_total_ticks: f32,
    prev_time: Instant,
    ticks_per_second: f32,
    page_size: u64,
    cpu_count: usize,
}

#[cfg(target_os = "linux")]
impl ProcessTreeTracker {
    pub fn new() -> Self {
        let me = procfs::process::Process::myself().expect("Failed to access /proc/self");
        let ticks_per_second = procfs::ticks_per_second() as f32;
        let page_size = procfs::page_size();
        let cpu_count = get();

        let mut tracker = Self {
            me,
            prev_total_ticks: 0.0,
            prev_time: Instant::now(),
            ticks_per_second,
            page_size,
            cpu_count,
        };

        let (ticks, _, _) = tracker.sample_tree_metrics();
        tracker.prev_total_ticks = ticks;
        tracker
    }

    fn sample_tree_metrics(&self) -> (f32, u64, u64) {
        let mut total_ticks = 0.0;
        let mut total_rss_pages = 0;
        let mut total_virt_bytes = 0;

        if let Ok(stat) = self.me.stat() {
            total_ticks += (stat.utime
                + stat.stime
                + stat.cutime.cast_unsigned()
                + stat.cstime.cast_unsigned()) as f32;
            total_rss_pages += stat.rss;
            total_virt_bytes += stat.vsize;
        }

        if let Ok(processes) = procfs::process::all_processes() {
            for child_proc in processes.filter_map(|process_result| {
                if let Ok(process) = process_result
                    && let Ok(stat) = process.stat()
                    && stat.ppid == self.me.pid
                {
                    return Some(process);
                }
                None
            }) {
                if let Ok(child_stat) = child_proc.stat() {
                    total_ticks += (child_stat.utime + child_stat.stime) as f32;
                    total_rss_pages += child_stat.rss;
                    total_virt_bytes += child_stat.vsize;
                }
            }
        }

        (
            total_ticks,
            total_rss_pages * self.page_size,
            total_virt_bytes,
        )
    }

    pub fn sample(&mut self) -> ProcessTreeStats {
        let now = Instant::now();
        let elapsed_secs = now.duration_since(self.prev_time).as_secs_f32();

        let (current_total_ticks, rss_bytes, virt_bytes) = self.sample_tree_metrics();
        let delta_ticks = current_total_ticks - self.prev_total_ticks;
        let cpu_time_spent = delta_ticks / self.ticks_per_second;
        let mut cpu_usage = (cpu_time_spent / elapsed_secs) * 100.0;

        // Normalise across CPU cores to match non-Linux implementation
        cpu_usage /= self.cpu_count as f32;

        self.prev_total_ticks = current_total_ticks;
        self.prev_time = now;

        ProcessTreeStats {
            cpu_usage,
            rss_bytes,
            virt_bytes,
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub struct ProcessTreeTracker {
    system: sysinfo::System,
    pid: sysinfo::Pid,
    cpu_count: usize,
    child_processes: HashSet<sysinfo::Pid>,
}

#[cfg(not(target_os = "linux"))]
impl ProcessTreeTracker {
    pub fn new() -> Self {
        let mut sys = sysinfo::System::new();
        let pid = sysinfo::Pid::from(std::process::id() as usize);
        let process_refresh_kind = ProcessRefreshKind::nothing().with_memory().with_cpu();

        sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            true,
            process_refresh_kind,
        );
        let child_processes = HashSet::new();

        Self {
            system: sys,
            pid,
            cpu_count: get(),
            child_processes,
        }
    }

    pub fn sample(&mut self) -> ProcessTreeStats {
        let mut processes: Vec<_> = self.child_processes.iter().copied().collect();
        processes.push(self.pid);
        let process_to_update = if processes.len() > 1 {
            ProcessesToUpdate::All
        } else {
            ProcessesToUpdate::Some(&processes)
        };

        let process_refresh_kind = ProcessRefreshKind::nothing().with_memory().with_cpu();
        self.system
            .refresh_processes_specifics(process_to_update, true, process_refresh_kind);

        let mut cpu_usage = 0.0;
        let mut rss_bytes = 0;
        let mut virt_bytes = 0;

        if let Some(parent_proc) = self.system.process(self.pid) {
            cpu_usage += parent_proc.cpu_usage();
            rss_bytes += parent_proc.memory();
            virt_bytes += parent_proc.virtual_memory();
        }

        for (_, process) in self
            .system
            .processes()
            .iter()
            .filter(|(pid, _)| **pid != self.pid)
        {
            if let Some(parent_pid) = process.parent()
                && parent_pid == self.pid
            {
                cpu_usage += process.cpu_usage();
                rss_bytes += process.memory();
                virt_bytes += process.virtual_memory();
                self.child_processes.insert(process.pid());
            }
        }
        cpu_usage /= self.cpu_count as f32;
        ProcessTreeStats {
            cpu_usage,
            rss_bytes,
            virt_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Assert the machine-independent invariants every sample must uphold, so the
    /// suite stays portable across CI hosts, PIDs and core counts.
    fn assert_sane(stats: &ProcessTreeStats) {
        assert!(
            stats.cpu_usage.is_finite(),
            "CPU usage must never be NaN or infinite (got {})",
            stats.cpu_usage
        );
        assert!(
            stats.cpu_usage >= 0.0,
            "CPU usage must never be negative (got {})",
            stats.cpu_usage
        );
    }

    #[test]
    fn new_tracker_constructs_without_panicking() {
        // Constructing the tracker touches live OS state (/proc or sysinfo); it
        // must succeed on any supported host.
        let _tracker = ProcessTreeTracker::new();
    }

    #[test]
    fn first_sample_upholds_invariants() {
        let mut tracker = ProcessTreeTracker::new();
        let stats = tracker.sample();
        assert_sane(&stats);
    }

    #[test]
    fn repeated_sampling_stays_sane_and_never_panics() {
        let mut tracker = ProcessTreeTracker::new();
        // Rapid back-to-back samples exercise the elapsed-time delta maths, which
        // could divide by a near-zero interval — the result must stay finite.
        for _ in 0..8 {
            let stats = tracker.sample();
            assert_sane(&stats);
        }
    }

    #[test]
    fn independent_trackers_can_sample_concurrently_without_panicking() {
        // Each tracker owns its own OS handle; sampling from several threads at
        // once must not panic or produce insane readings.
        let handles: Vec<_> = (0..4)
            .map(|_| {
                std::thread::spawn(|| {
                    let mut tracker = ProcessTreeTracker::new();
                    let stats = tracker.sample();
                    assert_sane(&stats);
                })
            })
            .collect();
        for handle in handles {
            handle.join().expect("sampling thread must not panic");
        }
    }

    #[test]
    fn stats_are_copy_so_snapshots_are_value_independent() {
        // Compile-time guard: `ProcessTreeStats` must be `Copy`, so a captured
        // snapshot can never be mutated by a later `sample()`.
        const fn assert_copy<T: Copy>() {}
        assert_copy::<ProcessTreeStats>();
    }
}
