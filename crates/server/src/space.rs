//! Disk-space accounting, pressure reclaim, and the NOSPACE alarm.
//!
//! fastetcd normally runs on a fixed-size volume. Left alone, the store
//! only grows: every write keeps the previous revision of the key, the
//! raft log accumulates until a snapshot purges it, the snapshot itself
//! is a second full copy of the database, and a copy-on-write B-tree
//! never returns freed pages to the filesystem on its own. On a bounded
//! volume that turns "grows slowly" into "stops working on a schedule"
//! (fastetcd#14).
//!
//! Running out is not a graceful degradation, it is a deadlock: the
//! snapshot write fails with ENOSPC, openraft surfaces that storage
//! error on the read barrier *and* on every proposal, and the one
//! recovery a client could attempt — deleting keys to shrink the next
//! snapshot — is refused for the same reason. Every process is healthy
//! and the cluster is down.
//!
//! So the wall must never be reached. This module:
//!
//! 1. **Measures** occupancy continuously — database file, snapshot
//!    directory, and the free space actually left on the device (not
//!    just a configured quota, which says nothing about a 64 MB volume).
//! 2. **Reclaims** at a high-water mark, well before the wall: compact
//!    MVCC history, trigger a raft snapshot so the log can be purged,
//!    then defragment so the freed pages go back to the filesystem.
//! 3. **Alarms** at a higher mark: raises etcd's NOSPACE alarm and
//!    rejects writes with `ResourceExhausted`, while reads, deletes,
//!    compaction and defragment keep working — so an operator (or the
//!    reclaim path) can still dig the store out. This is etcd's
//!    precedent, and the whole point is that it fires while the store
//!    still functions.
//!
//! The alarm clears itself once occupancy drops back below the low-water
//! mark; `etcdctl alarm disarm` clears it manually.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tonic::Status;

use crate::state::ServerState;

/// How stale a live-bytes measurement may be before it is recomputed.
/// The measurement walks the whole database, so it runs on the monitor's
/// own schedule rather than on any request path; `Maintenance.Status`
/// and `/metrics` read whatever the monitor last recorded.
pub const IN_USE_MAX_AGE: Duration = Duration::from_secs(300);

/// Message etcd returns when a write is refused for lack of space.
/// Client libraries match on this text, so keep it verbatim.
pub const ERR_NO_SPACE: &str = "etcdserver: mvcc: database space exceeded";

/// Tunables for the space monitor. Percentages are of the effective
/// capacity (see [`SpaceGuard::capacity_bytes`]).
#[derive(Debug, Clone)]
pub struct SpaceConfig {
    /// Hard ceiling on the store's footprint, in bytes. `0` means "no
    /// configured quota" — the filesystem is the only limit.
    pub quota_backend_bytes: u64,
    /// Start reclaiming above this percentage of capacity.
    pub high_water_percent: u8,
    /// Raise the NOSPACE alarm above this percentage.
    pub alarm_percent: u8,
    /// Clear the NOSPACE alarm once back below this percentage.
    pub clear_percent: u8,
    /// How often occupancy is sampled.
    pub interval: Duration,
    /// Revisions of MVCC history to keep when compacting under
    /// pressure. Applies even when auto-compaction is off: an unbounded
    /// history is the usual reason a bounded volume fills.
    pub reclaim_retention: i64,
    /// Whether the reclaim path may defragment the engine. Defragment
    /// blocks reads and writes for its duration, but it is the only way
    /// to hand freed pages back to the filesystem.
    pub auto_defrag: bool,
    /// Don't defragment unless at least this many bytes would come back.
    pub defrag_min_reclaim_bytes: u64,
    /// Minimum gap between reclaim attempts.
    pub reclaim_backoff: Duration,
}

impl Default for SpaceConfig {
    fn default() -> Self {
        Self {
            quota_backend_bytes: 0,
            high_water_percent: 80,
            alarm_percent: 95,
            clear_percent: 70,
            interval: Duration::from_secs(30),
            reclaim_retention: 1000,
            auto_defrag: true,
            defrag_min_reclaim_bytes: 4 * 1024 * 1024,
            reclaim_backoff: Duration::from_secs(60),
        }
    }
}

/// A point-in-time reading of the store's footprint.
#[derive(Debug, Clone, Copy, Default)]
pub struct SpaceStats {
    /// Size of the engine's database file.
    pub db_bytes: u64,
    /// Bytes in the database holding live data (`0` until a usage walk
    /// has run — it is too expensive to do on every sample).
    pub db_in_use_bytes: u64,
    /// Size of the retained raft snapshots.
    pub snapshot_bytes: u64,
    /// Everything in the data directory: the database, the snapshots,
    /// and anything else living beside them (upgrade safety backups,
    /// replaced files from a restore). All of it competes for the same
    /// volume, so all of it counts.
    pub data_dir_bytes: u64,
    /// Size of the filesystem holding the data directory, and what is
    /// still available on it. Both `0` if the platform has no probe.
    pub fs_total_bytes: u64,
    pub fs_available_bytes: u64,
    /// The ceiling occupancy is measured against.
    pub capacity_bytes: u64,
    /// Occupancy in parts per million of capacity.
    pub used_ppm: u64,
    /// Whether the NOSPACE alarm is currently raised.
    pub nospace: bool,
}

impl SpaceStats {
    /// Everything the store occupies on the data volume.
    ///
    /// The data directory's total is the honest number — a stale
    /// upgrade backup fills a volume exactly as well as live data does
    /// — with the database plus snapshots as a floor for the case where
    /// the directory scan came up short (a data dir elsewhere, an
    /// unreadable subdirectory).
    pub fn used_bytes(&self) -> u64 {
        self.data_dir_bytes
            .max(self.db_bytes.saturating_add(self.snapshot_bytes))
    }

    /// Occupancy as a fraction of capacity, 0.0–1.0+.
    pub fn used_ratio(&self) -> f64 {
        self.used_ppm as f64 / 1_000_000.0
    }
}

/// Live space state, shared by the monitor task, the gRPC services and
/// the metrics endpoint. All fields are atomics: readers on the request
/// path never block behind the monitor.
pub struct SpaceGuard {
    cfg: SpaceConfig,
    data_dir: PathBuf,
    enabled: bool,
    db_bytes: AtomicU64,
    db_in_use_bytes: AtomicU64,
    snapshot_bytes: AtomicU64,
    data_dir_bytes: AtomicU64,
    fs_total_bytes: AtomicU64,
    fs_available_bytes: AtomicU64,
    capacity_bytes: AtomicU64,
    used_ppm: AtomicU64,
    nospace: AtomicBool,
    /// Unix seconds at which `db_in_use_bytes` was last measured. The
    /// measurement walks the whole database, so it is rate-limited
    /// rather than run on every caller's request.
    in_use_measured_at: AtomicU64,
}

impl SpaceGuard {
    pub fn new(data_dir: PathBuf, cfg: SpaceConfig) -> Self {
        Self {
            cfg,
            data_dir,
            enabled: true,
            db_bytes: AtomicU64::new(0),
            db_in_use_bytes: AtomicU64::new(0),
            snapshot_bytes: AtomicU64::new(0),
            data_dir_bytes: AtomicU64::new(0),
            fs_total_bytes: AtomicU64::new(0),
            fs_available_bytes: AtomicU64::new(0),
            capacity_bytes: AtomicU64::new(0),
            used_ppm: AtomicU64::new(0),
            nospace: AtomicBool::new(false),
            in_use_measured_at: AtomicU64::new(0),
        }
    }

    /// A guard that never measures anything and never alarms. Used by
    /// tests and any embedding that manages space itself.
    pub fn disabled() -> Self {
        let mut g = Self::new(PathBuf::new(), SpaceConfig::default());
        g.enabled = false;
        g
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn config(&self) -> &SpaceConfig {
        &self.cfg
    }

    /// True while the NOSPACE alarm is raised.
    pub fn nospace(&self) -> bool {
        self.enabled && self.nospace.load(Ordering::Relaxed)
    }

    /// Gate for requests that consume space. Reads, deletes, compaction
    /// and defragment must *not* call this — refusing them is what turns
    /// a full volume into an unrecoverable one.
    pub fn check_write(&self) -> Result<(), Status> {
        if self.nospace() {
            return Err(Status::resource_exhausted(ERR_NO_SPACE));
        }
        Ok(())
    }

    /// Clear the alarm by operator request (`etcdctl alarm disarm`). If
    /// the store is still over the mark the next sample raises it again.
    pub fn disarm_nospace(&self) {
        if self.nospace.swap(false, Ordering::Relaxed) {
            tracing::warn!(target: "fastetcd::space", "NOSPACE alarm disarmed by request");
        }
    }

    pub fn stats(&self) -> SpaceStats {
        SpaceStats {
            db_bytes: self.db_bytes.load(Ordering::Relaxed),
            db_in_use_bytes: {
                let in_use = self.db_in_use_bytes.load(Ordering::Relaxed);
                let db = self.db_bytes.load(Ordering::Relaxed);
                if db == 0 {
                    in_use
                } else {
                    in_use.min(db)
                }
            },
            snapshot_bytes: self.snapshot_bytes.load(Ordering::Relaxed),
            data_dir_bytes: self.data_dir_bytes.load(Ordering::Relaxed),
            fs_total_bytes: self.fs_total_bytes.load(Ordering::Relaxed),
            fs_available_bytes: self.fs_available_bytes.load(Ordering::Relaxed),
            capacity_bytes: self.capacity_bytes.load(Ordering::Relaxed),
            used_ppm: self.used_ppm.load(Ordering::Relaxed),
            nospace: self.nospace(),
        }
    }

    /// The ceiling occupancy is measured against: the smaller of the
    /// configured quota and what the store could actually reach on this
    /// filesystem (what it already occupies plus what is still free).
    ///
    /// Using the device's free space — not the quota alone — is the
    /// point. A quota of 2 GiB on a 64 MB volume is not a limit, it is a
    /// fiction, and it is the fiction that let the store run into ENOSPC
    /// with every threshold reporting healthy.
    fn capacity_bytes(&self, used: u64, fs_available: Option<u64>) -> u64 {
        let reachable = fs_available.map(|avail| used.saturating_add(avail));
        match (self.cfg.quota_backend_bytes, reachable) {
            (0, Some(r)) => r,
            (0, None) => 0, // nothing to measure against
            (q, Some(r)) => q.min(r),
            (q, None) => q,
        }
    }

    /// Live bytes in the database, measured at most once per
    /// `max_age`.
    ///
    /// The measurement is a full walk of the engine's B-tree, so it is
    /// deliberately not on any hot path: callers that want a number for
    /// a human (`Maintenance.Status`, `/metrics`) get the cached one and
    /// pay for a fresh walk only when it has gone stale.
    /// Invalidate the cached live-bytes measurement, so the next reader
    /// takes a fresh one. Called after a defragment, which changes the
    /// answer completely — without this, `Status` could report more
    /// bytes in use than the file now contains.
    pub fn invalidate_in_use(&self) {
        self.in_use_measured_at.store(0, Ordering::Relaxed);
    }

    pub async fn in_use_bytes(
        &self,
        engine: &Arc<dyn fastetcd_storage::KvStore>,
        max_age: Duration,
    ) -> u64 {
        let now = unix_secs();
        let measured_at = self.in_use_measured_at.load(Ordering::Relaxed);
        if now.saturating_sub(measured_at) >= max_age.as_secs() {
            match engine.usage().await {
                Ok(usage) => {
                    self.db_in_use_bytes
                        .store(usage.in_use_bytes, Ordering::Relaxed);
                    self.in_use_measured_at.store(now, Ordering::Relaxed);
                }
                Err(e) => tracing::warn!(target: "fastetcd::space", "usage: {e}"),
            }
        }
        // Never claim more is in use than the file holds: the cached
        // measurement can predate a defragment that shrank it.
        let db_bytes = self.db_bytes.load(Ordering::Relaxed);
        let in_use = self.db_in_use_bytes.load(Ordering::Relaxed);
        if db_bytes == 0 {
            in_use
        } else {
            in_use.min(db_bytes)
        }
    }

    /// Sample the store's footprint and update the alarm. Returns the
    /// new stats. Cheap — a file size, a directory scan and a `statvfs`
    /// — so it is safe to call from a request path.
    pub async fn refresh(&self, state: &ServerState) -> SpaceStats {
        if !self.enabled {
            return self.stats();
        }
        let db_bytes = state
            .sm
            .mvcc()
            .engine()
            .size_on_disk()
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(target: "fastetcd::space", "size_on_disk: {e}");
                self.db_bytes.load(Ordering::Relaxed)
            });
        let snapshot_bytes = state.sm.snapshot_bytes();
        let data_dir_bytes = fastetcd_storage::fs_space::dir_size(&self.data_dir);
        let fs = fastetcd_storage::fs_space::probe(&self.data_dir);

        let used = data_dir_bytes.max(db_bytes.saturating_add(snapshot_bytes));
        let capacity = self.capacity_bytes(used, fs.map(|f| f.available));
        let used_ppm = if capacity == 0 {
            0
        } else {
            ((used as u128 * 1_000_000) / capacity as u128).min(u64::MAX as u128) as u64
        };

        self.db_bytes.store(db_bytes, Ordering::Relaxed);
        self.snapshot_bytes.store(snapshot_bytes, Ordering::Relaxed);
        self.data_dir_bytes.store(data_dir_bytes, Ordering::Relaxed);
        self.fs_total_bytes
            .store(fs.map(|f| f.total).unwrap_or(0), Ordering::Relaxed);
        self.fs_available_bytes
            .store(fs.map(|f| f.available).unwrap_or(0), Ordering::Relaxed);
        self.capacity_bytes.store(capacity, Ordering::Relaxed);
        self.used_ppm.store(used_ppm, Ordering::Relaxed);

        self.update_alarm(used_ppm, used, capacity);
        self.stats()
    }

    /// Record an in-use measurement taken elsewhere (the reclaim path
    /// and the Defragment RPC already pay for the walk).
    pub fn record_in_use(&self, bytes: u64) {
        self.db_in_use_bytes.store(bytes, Ordering::Relaxed);
        self.in_use_measured_at.store(unix_secs(), Ordering::Relaxed);
    }

    fn update_alarm(&self, used_ppm: u64, used: u64, capacity: u64) {
        if capacity == 0 {
            return; // nothing measurable — don't invent an alarm
        }
        let alarm_ppm = pct_ppm(self.cfg.alarm_percent);
        let clear_ppm = pct_ppm(self.cfg.clear_percent);
        let raised = self.nospace.load(Ordering::Relaxed);
        if !raised && used_ppm >= alarm_ppm {
            self.nospace.store(true, Ordering::Relaxed);
            tracing::error!(
                target: "fastetcd::space",
                used_bytes = used,
                capacity_bytes = capacity,
                percent = used_ppm / 10_000,
                "NOSPACE alarm raised — writes are rejected until space is reclaimed. \
                 Delete keys, compact (`etcdctl compact <rev>`) and defragment \
                 (`etcdctl defrag`); reads, deletes and compaction still work."
            );
        } else if raised && used_ppm <= clear_ppm {
            self.nospace.store(false, Ordering::Relaxed);
            tracing::warn!(
                target: "fastetcd::space",
                used_bytes = used,
                capacity_bytes = capacity,
                "NOSPACE alarm cleared — occupancy back below the low-water mark"
            );
        }
    }
}

fn pct_ppm(percent: u8) -> u64 {
    percent as u64 * 10_000
}

fn unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Spawn the space monitor. Returns `None` when the guard is disabled.
pub fn spawn(state: Arc<ServerState>) -> Option<tokio::task::JoinHandle<()>> {
    if !state.space.is_enabled() {
        return None;
    }
    Some(tokio::spawn(async move {
        let guard = state.space.clone();
        let cfg = guard.config().clone();
        let mut tick = tokio::time::interval(cfg.interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_reclaim: Option<tokio::time::Instant> = None;
        loop {
            tick.tick().await;
            let stats = guard.refresh(&state).await;
            // Keep the live-bytes number fresh enough to report, on the
            // monitor's schedule — the walk is O(database), so no
            // request path should ever trigger it.
            guard
                .in_use_bytes(state.sm.mvcc().engine(), IN_USE_MAX_AGE)
                .await;
            let high_ppm = pct_ppm(cfg.high_water_percent);
            if stats.capacity_bytes == 0 || stats.used_ppm < high_ppm {
                continue;
            }
            let now = tokio::time::Instant::now();
            if last_reclaim.is_some_and(|t| now.duration_since(t) < cfg.reclaim_backoff) {
                continue;
            }
            last_reclaim = Some(now);
            tracing::warn!(
                target: "fastetcd::space",
                used_bytes = stats.used_bytes(),
                capacity_bytes = stats.capacity_bytes,
                percent = stats.used_ppm / 10_000,
                "store is above the high-water mark — reclaiming space"
            );
            if let Err(e) = reclaim(&state, &cfg).await {
                tracing::error!(target: "fastetcd::space", "space reclaim failed: {e}");
            }
            // Re-sample immediately so the alarm reflects the reclaim
            // rather than waiting a whole interval.
            guard.refresh(&state).await;
        }
    }))
}

/// One reclaim pass: compact MVCC history, snapshot so the raft log can
/// be purged, then defragment to hand the freed pages back.
///
/// The order matters. Compaction drops old key revisions but leaves the
/// pages on the engine's free list; a snapshot lets openraft purge the
/// log it has already applied; only the defragment at the end actually
/// shrinks the file on the volume.
pub async fn reclaim(state: &ServerState, cfg: &SpaceConfig) -> anyhow::Result<()> {
    // 1. Compact MVCC history. Leader-only — it goes through Raft so
    //    every member compacts identically.
    match crate::compaction::compact_to_retention(
        state,
        cfg.reclaim_retention,
        "compacting MVCC history under disk pressure",
    )
    .await
    {
        Ok(Some(rev)) => {
            tracing::info!(target: "fastetcd::space", compact_rev = rev, "compacted")
        }
        Ok(None) => {}
        Err(e) => tracing::warn!(target: "fastetcd::space", "pressure compaction failed: {e}"),
    }

    // 2. Ask openraft for a snapshot now rather than at the next
    //    `--snapshot-count` boundary. Purging the log is gated on having
    //    snapshotted the entries first, so this is what lets the log
    //    shrink between thresholds.
    if let Err(e) = state.raft.trigger().snapshot().await {
        tracing::debug!(target: "fastetcd::space", "snapshot trigger declined: {e}");
    }
    // Give the snapshot + purge a moment to land before measuring what
    // a defragment could return.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 3. Defragment. This is the only step that returns space to the
    //    filesystem, and it blocks the engine while it runs, so only do
    //    it when there is a worthwhile amount to get back.
    if !cfg.auto_defrag {
        return Ok(());
    }
    let engine = state.sm.mvcc().engine().clone();
    let usage = engine.usage().await?;
    state.space.record_in_use(usage.in_use_bytes);
    if usage.reclaimable_bytes() < cfg.defrag_min_reclaim_bytes {
        tracing::info!(
            target: "fastetcd::space",
            file_bytes = usage.file_bytes,
            in_use_bytes = usage.in_use_bytes,
            "nothing worth defragmenting — the data itself is what fills the volume"
        );
        return Ok(());
    }
    tracing::warn!(
        target: "fastetcd::space",
        file_bytes = usage.file_bytes,
        in_use_bytes = usage.in_use_bytes,
        reclaimable_bytes = usage.reclaimable_bytes(),
        "defragmenting the store (reads and writes pause until it completes)"
    );
    engine.defragment().await?;
    // The measurement above describes a file that no longer exists.
    state.space.invalidate_in_use();
    let after = engine.size_on_disk().await.unwrap_or(usage.file_bytes);
    tracing::warn!(
        target: "fastetcd::space",
        before_bytes = usage.file_bytes,
        after_bytes = after,
        freed_bytes = usage.file_bytes.saturating_sub(after),
        "defragment complete"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn guard(quota: u64) -> SpaceGuard {
        SpaceGuard::new(
            PathBuf::from("/tmp"),
            SpaceConfig {
                quota_backend_bytes: quota,
                ..SpaceConfig::default()
            },
        )
    }

    #[test]
    fn capacity_prefers_the_filesystem_over_an_oversized_quota() {
        // A 2 GiB quota on a volume with 10 MB free is not a 2 GiB
        // limit. Capacity must be what the store can actually reach.
        let g = guard(2 * 1024 * 1024 * 1024);
        let used = 54 * 1024 * 1024;
        let avail = 10 * 1024 * 1024;
        assert_eq!(g.capacity_bytes(used, Some(avail)), used + avail);
    }

    #[test]
    fn a_quota_below_the_filesystem_still_binds() {
        let g = guard(100 * 1024 * 1024);
        assert_eq!(
            g.capacity_bytes(10 * 1024 * 1024, Some(500 * 1024 * 1024)),
            100 * 1024 * 1024
        );
    }

    #[test]
    fn no_quota_and_no_probe_means_nothing_to_measure() {
        assert_eq!(guard(0).capacity_bytes(1024, None), 0);
    }

    #[test]
    fn alarm_raises_at_the_alarm_mark_and_clears_at_the_low_mark() {
        let g = guard(1000);
        g.update_alarm(pct_ppm(90), 900, 1000);
        assert!(!g.nospace(), "90% is below the 95% alarm mark");
        g.update_alarm(pct_ppm(96), 960, 1000);
        assert!(g.nospace(), "96% must raise the alarm");
        // Hysteresis: still alarmed between the clear and alarm marks.
        g.update_alarm(pct_ppm(80), 800, 1000);
        assert!(g.nospace(), "80% is above the 70% clear mark");
        g.update_alarm(pct_ppm(65), 650, 1000);
        assert!(!g.nospace(), "65% must clear the alarm");
    }

    #[test]
    fn writes_are_refused_under_the_alarm_and_allowed_otherwise() {
        let g = guard(1000);
        assert!(g.check_write().is_ok());
        g.update_alarm(pct_ppm(99), 990, 1000);
        let err = g.check_write().expect_err("write must be refused");
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
        assert_eq!(err.message(), ERR_NO_SPACE);
        g.disarm_nospace();
        assert!(g.check_write().is_ok(), "disarm must let writes through");
    }

    #[test]
    fn a_disabled_guard_never_alarms() {
        let g = SpaceGuard::disabled();
        g.update_alarm(pct_ppm(100), 1000, 1000);
        assert!(!g.nospace());
        assert!(g.check_write().is_ok());
    }
}
