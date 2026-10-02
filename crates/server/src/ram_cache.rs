//! Sizing the MVCC layer's RAM cache (fastetcd#82).
//!
//! The value cache's default budget is the smaller of 128 MiB and 5% of
//! the memory this process may use: the cgroup limit when there is one
//! (a container), else the machine's total memory.

use fastetcd_storage::mvcc::cache::{CacheConfig, DEFAULT_VALUE_CACHE_BYTES};

/// Share of usable memory the default value cache may take, in percent.
pub const DEFAULT_MEMORY_PERCENT: u64 = 5;

/// The memory this process may use: the tightest of the cgroup v2
/// `memory.max`, the cgroup v1 `memory.limit_in_bytes` and `MemTotal`.
/// `None` when none of them can be read.
pub fn usable_memory_bytes() -> Option<u64> {
    let cgroup_v2 = read_limit("/sys/fs/cgroup/memory.max");
    let cgroup_v1 = read_limit("/sys/fs/cgroup/memory/memory.limit_in_bytes");
    let total = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| parse_meminfo_total(&s));
    [cgroup_v2, cgroup_v1, total].into_iter().flatten().min()
}

fn read_limit(path: &str) -> Option<u64> {
    let s = std::fs::read_to_string(path).ok()?;
    let v: u64 = s.trim().parse().ok()?; // "max" → None
    // cgroup v1 reports "no limit" as a huge page-rounded number.
    (v < (1u64 << 60)).then_some(v)
}

fn parse_meminfo_total(meminfo: &str) -> Option<u64> {
    let line = meminfo.lines().find(|l| l.starts_with("MemTotal:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib * 1024)
}

/// The default value-cache budget for `usable` bytes of memory.
pub fn default_value_cache_bytes(usable: Option<u64>) -> u64 {
    match usable {
        Some(m) => (m / 100 * DEFAULT_MEMORY_PERCENT).min(DEFAULT_VALUE_CACHE_BYTES),
        None => DEFAULT_VALUE_CACHE_BYTES,
    }
}

/// The cache configuration from the flags: an explicit budget wins.
pub fn config(value_cache_bytes: Option<u64>, max_entry_bytes: u64) -> CacheConfig {
    CacheConfig {
        value_cache_bytes: value_cache_bytes
            .unwrap_or_else(|| default_value_cache_bytes(usable_memory_bytes())),
        max_entry_bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_five_percent_capped_at_128_mib() {
        let mib = 1024 * 1024;
        assert_eq!(default_value_cache_bytes(Some(1000 * mib)), 50 * mib);
        assert_eq!(default_value_cache_bytes(Some(64 * 1024 * mib)), 128 * mib);
        assert_eq!(default_value_cache_bytes(None), 128 * mib);
    }

    #[test]
    fn meminfo_total_parses() {
        let s = "MemTotal:       16314368 kB\nMemFree:  1 kB\n";
        assert_eq!(parse_meminfo_total(s), Some(16314368 * 1024));
    }

    #[test]
    fn explicit_budget_wins() {
        assert_eq!(config(Some(0), 10).value_cache_bytes, 0);
        assert_eq!(config(Some(12345), 10).max_entry_bytes, 10);
    }
}
