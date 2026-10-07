//! The standard `process_*` metrics etcd exports (Go's process collector),
//! read from `/proc/self` on every scrape (fastetcd#84): how much memory
//! the member holds is what "RSS flat over a long run" is measured with,
//! and etcd dashboards graph these names.
//!
//! - `process_resident_memory_bytes`, `process_virtual_memory_bytes`
//!   (`/proc/self/status` `VmRSS` / `VmSize`);
//! - `process_cpu_seconds_total` (`/proc/self/stat` utime + stime);
//! - `process_start_time_seconds` (start in ticks after boot, plus
//!   `btime` from `/proc/stat`);
//! - `process_open_fds`, `process_max_fds` (`/proc/self/fd`, the soft
//!   `Max open files` limit).
//!
//! Clock ticks are taken as 100 per second (Linux's `USER_HZ` on every
//! architecture fastetcd builds for). Off Linux nothing is reported.

use std::sync::atomic::AtomicU64;
use std::sync::Mutex;

use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;

const USER_HZ: f64 = 100.0;

/// One reading of the process's figures.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Reading {
    pub resident_bytes: u64,
    pub virtual_bytes: u64,
    pub cpu_seconds: f64,
    pub start_time_seconds: f64,
    pub open_fds: u64,
    pub max_fds: u64,
}

/// `VmRSS` / `VmSize` from `/proc/<pid>/status`, in bytes.
pub fn parse_status(status: &str) -> (u64, u64) {
    let kb = |name: &str| {
        status
            .lines()
            .find_map(|l| l.strip_prefix(name))
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|n| n.parse::<u64>().ok())
            .map_or(0, |n| n * 1024)
    };
    (kb("VmRSS:"), kb("VmSize:"))
}

/// (utime + stime in seconds, start time in seconds after boot) from
/// `/proc/<pid>/stat`. The command name (field 2) can hold spaces and
/// parentheses, so fields are counted after its closing parenthesis.
pub fn parse_stat(stat: &str) -> Option<(f64, f64)> {
    let rest = &stat[stat.rfind(')')? + 1..];
    let f: Vec<&str> = rest.split_whitespace().collect();
    // After the name: state is field 3, so field n is f[n - 3].
    let ticks = |n: usize| f.get(n - 3)?.parse::<f64>().ok();
    Some(((ticks(14)? + ticks(15)?) / USER_HZ, ticks(22)? / USER_HZ))
}

/// `btime` (boot time, Unix seconds) from `/proc/stat`.
pub fn parse_btime(stat: &str) -> Option<f64> {
    stat.lines().find_map(|l| l.strip_prefix("btime ")).and_then(|v| v.trim().parse().ok())
}

/// The soft `Max open files` limit from `/proc/<pid>/limits`.
pub fn parse_max_fds(limits: &str) -> Option<u64> {
    let line = limits.lines().find(|l| l.starts_with("Max open files"))?;
    line["Max open files".len()..].split_whitespace().next()?.parse().ok()
}

/// Read this process's figures, or `None` where `/proc` is missing.
pub fn read() -> Option<Reading> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let (resident_bytes, virtual_bytes) = parse_status(&status);
    let (cpu_seconds, started_after_boot) =
        parse_stat(&std::fs::read_to_string("/proc/self/stat").ok()?)?;
    let btime = parse_btime(&std::fs::read_to_string("/proc/stat").ok()?).unwrap_or(0.0);
    let open_fds = std::fs::read_dir("/proc/self/fd").map(|d| d.count() as u64).unwrap_or(0);
    let max_fds = std::fs::read_to_string("/proc/self/limits")
        .ok()
        .and_then(|l| parse_max_fds(&l))
        .unwrap_or(0);
    Some(Reading {
        resident_bytes,
        virtual_bytes,
        cpu_seconds,
        start_time_seconds: btime + started_after_boot,
        open_fds,
        max_fds,
    })
}

/// The registered metrics; [`ProcessMetrics::refresh`] on every scrape.
#[derive(Default)]
pub struct ProcessMetrics {
    resident: Gauge,
    virtual_: Gauge,
    cpu: Counter<f64, AtomicU64>,
    cpu_last: Mutex<f64>,
    start: Gauge<f64, AtomicU64>,
    open_fds: Gauge,
    max_fds: Gauge,
}

impl ProcessMetrics {
    pub fn register(&self, reg: &mut Registry) {
        reg.register("process_resident_memory_bytes", "Resident memory size in bytes", self.resident.clone());
        reg.register("process_virtual_memory_bytes", "Virtual memory size in bytes", self.virtual_.clone());
        reg.register("process_cpu_seconds", "Total user and system CPU time spent in seconds", self.cpu.clone());
        reg.register(
            "process_start_time_seconds",
            "Start time of the process since unix epoch in seconds",
            self.start.clone(),
        );
        reg.register("process_open_fds", "Number of open file descriptors", self.open_fds.clone());
        reg.register("process_max_fds", "Maximum number of open file descriptors", self.max_fds.clone());
    }

    pub fn refresh(&self) {
        let Some(r) = read() else { return };
        self.resident.set(r.resident_bytes as i64);
        self.virtual_.set(r.virtual_bytes as i64);
        {
            let mut last = self.cpu_last.lock().unwrap();
            if r.cpu_seconds > *last {
                self.cpu.inc_by(r.cpu_seconds - *last);
                *last = r.cpu_seconds;
            }
        }
        self.start.set(r.start_time_seconds);
        self.open_fds.set(r.open_fds as i64);
        self.max_fds.set(r.max_fds as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_proc_files() {
        let status = "Name:\tfastetcd\nVmSize:\t  204800 kB\nVmRSS:\t   51200 kB\n";
        assert_eq!(parse_status(status), (51200 * 1024, 204800 * 1024));
        // A name with a space and a parenthesis; utime 250, stime 50,
        // starttime 12345 ticks.
        let stat = "42 (fast etcd) S 1 42 42 0 -1 4194560 100 0 0 0 250 50 0 0 20 0 8 0 12345 1000 200";
        assert_eq!(parse_stat(stat), Some((3.0, 123.45)));
        assert_eq!(parse_btime("cpu 1 2 3\nbtime 1791300000\nprocesses 9\n"), Some(1791300000.0));
        let limits = "Limit                     Soft Limit           Hard Limit           Units\n\
                      Max open files            65536                524288               files\n";
        assert_eq!(parse_max_fds(limits), Some(65536));
    }

    #[test]
    fn reads_this_process() {
        let r = read().expect("/proc on linux");
        assert!(r.resident_bytes > 0 && r.virtual_bytes >= r.resident_bytes, "{r:?}");
        assert!(r.open_fds > 0 && r.max_fds >= r.open_fds, "{r:?}");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        assert!(r.start_time_seconds > now - 3600.0 && r.start_time_seconds <= now + 1.0, "{r:?}");
    }
}
