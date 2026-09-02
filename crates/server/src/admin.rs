//! Offline data-directory operations and the shared upgrade-recovery
//! path: pre-version safety backup, `backup` / `restore`, `fsck`, and
//! `recover_data_dir` (used by both server startup and `fsck --repair`).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use openraft::storage::{RaftLogReader, RaftLogStorage};
use openraft::{BasicNode, EntryPayload, LogId, Membership, StoredMembership};

use fastetcd_raft::kv_log_store::KvLogStore;
use fastetcd_raft::types::NodeId;
use fastetcd_raft::FastetcdStateMachine;
use fastetcd_storage::mvcc::store::FORMAT_VERSION;
use fastetcd_storage::mvcc::MvccStore;
use fastetcd_storage::redb_engine::RedbEngine;
use fastetcd_storage::KvStore;

/// The single file that holds a fastetcd data directory.
pub const DATA_FILE: &str = "fastetcd.redb";

pub fn data_file(data_dir: &Path) -> PathBuf {
    data_dir.join(DATA_FILE)
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// What `recover_data_dir` did, for logging / fsck reporting.
#[derive(Debug, Default)]
pub struct RecoveryReport {
    pub adopted_applied: Option<LogId<NodeId>>,
    pub membership_source: Option<&'static str>,
    pub membership_voters: Vec<NodeId>,
    pub stamped_format: bool,
}

impl RecoveryReport {
    pub fn did_something(&self) -> bool {
        self.adopted_applied.is_some()
            || self.membership_source.is_some()
            || self.stamped_format
    }
}

/// Repair a pre-v1.0.1 data directory in place: adopt the log's purge
/// floor as `last_applied` if it was never persisted (#9), recover the
/// raft membership — preferring the retained log, falling back to the
/// configured cluster — if it is empty (#11), and stamp the format
/// version. Shared by server startup and `fsck --repair`.
pub async fn recover_data_dir(
    sm: &FastetcdStateMachine,
    log: &mut KvLogStore,
    all_members: &BTreeMap<NodeId, BasicNode>,
    node_id: NodeId,
    force_new_cluster: bool,
) -> anyhow::Result<RecoveryReport> {
    let mut report = RecoveryReport::default();
    let format = sm.mvcc().read_format_version().await?;
    let log_state = log.get_log_state().await?;
    let has_data = sm.mvcc().current_revision().await > 0;

    if let Some(adopted) = sm.recover_applied_floor(log_state.last_purged_log_id).await? {
        report.adopted_applied = Some(adopted);
        tracing::warn!(
            last_applied = ?adopted,
            "recovered a data directory with no persisted applied position \
             (pre-0.8.3 format); adopting the log's last purged id. Log entries \
             after this point are re-applied, which may advance the revision."
        );
    }

    let recovery_log_id = log_state.last_purged_log_id.or(log_state.last_log_id);

    // Recover the real voter set from the retained log — but ONLY when we
    // actually need it (membership is empty), and bounded to a window near
    // the tip so we never materialize a huge log. A healthy cluster has a
    // non-empty membership and must never pay this scan on startup; doing
    // it unconditionally over a large un-purged log was part of the #13
    // startup hang. Membership entries are near cluster events, so the
    // newest one is in the recent tail; older ones (bootstrap) live in the
    // purged prefix and are covered by the --initial-cluster fallback.
    let need_membership_recovery =
        !force_new_cluster && has_data && sm.membership_is_empty().await;
    let membership_from_log: Option<StoredMembership<NodeId, BasicNode>> =
        if need_membership_recovery {
            const WINDOW: u64 = 20_000;
            let hi = log_state.last_log_id.map(|l| l.index + 1).unwrap_or(0);
            let lo_floor = log_state.last_purged_log_id.map(|l| l.index + 1).unwrap_or(0);
            let lo = lo_floor.max(hi.saturating_sub(WINDOW));
            let mut latest = None;
            if hi > lo {
                for entry in log.try_get_log_entries(lo..hi).await? {
                    if let EntryPayload::Membership(m) = entry.payload {
                        latest = Some(StoredMembership::new(Some(entry.log_id), m));
                    }
                }
            }
            latest
        } else {
            None
        };

    if force_new_cluster {
        let node = all_members.get(&node_id).cloned().unwrap_or_default();
        let members: std::collections::BTreeSet<NodeId> = std::iter::once(node_id).collect();
        let nodes: BTreeMap<NodeId, BasicNode> = std::iter::once((node_id, node)).collect();
        let stored = StoredMembership::new(recovery_log_id, Membership::new(vec![members], nodes));
        report.membership_voters = stored.membership().voter_ids().collect();
        report.membership_source = Some("--force-new-cluster (single node)");
        sm.recover_membership(stored).await?;
        tracing::warn!(
            %node_id,
            "--force-new-cluster: rebuilt single-node membership; MVCC data \
             preserved. Re-add the other members with `member add` once up."
        );
    } else if need_membership_recovery {
        let (stored, source) = match membership_from_log {
            Some(m) => (m, "retained raft log (exact)"),
            None => {
                let members: std::collections::BTreeSet<NodeId> =
                    all_members.keys().copied().collect();
                (
                    StoredMembership::new(
                        recovery_log_id,
                        Membership::new(vec![members], all_members.clone()),
                    ),
                    "--initial-cluster (log purged clean of membership; best effort)",
                )
            }
        };
        report.membership_voters = stored.membership().voter_ids().collect();
        report.membership_source = Some(source);
        sm.recover_membership(stored).await?;
        tracing::warn!(
            source,
            voters = ?report.membership_voters,
            "upgraded a pre-v1.0.1 data directory in place: recovered raft membership \
             and persisted it durably (fastetcd#11). MVCC data preserved."
        );
    }

    if format != Some(FORMAT_VERSION) {
        sm.mvcc().write_format_version(FORMAT_VERSION).await?;
        report.stamped_format = true;
    }
    Ok(report)
}

/// Take a safety backup of the data file before a newer fastetcd version
/// touches it. Called at startup while the engine is already open (and
/// thus exclusively locked) but before any write, so a plain file copy
/// captures the exact prior-version state. No-op unless the stored
/// version differs from `current` and there is data to protect.
///
/// Returns the backup path if one was written.
pub async fn backup_before_version(
    mvcc: &MvccStore,
    data_dir: &Path,
    backup_dir: &Path,
    current: &str,
    retain: usize,
) -> anyhow::Result<Option<PathBuf>> {
    let stored = mvcc.read_open_version().await?;
    let has_data = mvcc.current_revision().await > 0;
    if !has_data || stored.as_deref() == Some(current) {
        return Ok(None);
    }
    let from = stored.as_deref().unwrap_or("pre-1.0.3");
    std::fs::create_dir_all(backup_dir)?;
    // Each backup is a full copy of the database, and it lands on the
    // same volume the database has to keep running on. Roll the old
    // ones off oldest-first *before* the copy, so the directory never
    // holds more than `retain` of them even momentarily (fastetcd#14).
    prune_backups(backup_dir, retain.saturating_sub(1));
    let dst = backup_dir.join(format!(
        "fastetcd-{}-to-{}-{}.redb",
        sanitize(from),
        sanitize(current),
        unix_secs()
    ));
    let bytes = std::fs::copy(data_file(data_dir), &dst)?;
    tracing::warn!(
        from_version = from,
        to_version = current,
        backup = %dst.display(),
        bytes,
        "took a safety backup of the data directory before starting a new fastetcd \
         version. Restore with `fastetcd restore <path>` if the upgrade misbehaves."
    );
    Ok(Some(dst))
}

/// Keep at most `keep` upgrade safety backups, deleting the oldest
/// first. Only files this code wrote are considered — anything an
/// operator dropped in the directory by hand is left alone.
pub fn prune_backups(backup_dir: &Path, keep: usize) {
    let Ok(entries) = std::fs::read_dir(backup_dir) else {
        return;
    };
    let mut backups: Vec<(std::time::SystemTime, PathBuf, u64)> = entries
        .flatten()
        .filter_map(|e| {
            let path = e.path();
            let name = path.file_name()?.to_str()?;
            if !name.starts_with("fastetcd-") || !name.ends_with(".redb") {
                return None;
            }
            let meta = e.metadata().ok()?;
            Some((meta.modified().ok()?, path, meta.len()))
        })
        .collect();
    if backups.len() <= keep {
        return;
    }
    backups.sort_by_key(|(mtime, _, _)| *mtime);
    let drop_count = backups.len() - keep;
    for (_, path, bytes) in backups.into_iter().take(drop_count) {
        match std::fs::remove_file(&path) {
            Ok(()) => tracing::info!(
                backup = %path.display(),
                freed_bytes = bytes,
                "rolled off an old upgrade safety backup"
            ),
            Err(e) => tracing::warn!(
                backup = %path.display(),
                error = %e,
                "could not remove an old upgrade safety backup"
            ),
        }
    }
}

fn sanitize(v: &str) -> String {
    v.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '.' { c } else { '_' })
        .collect()
}

// ---------------- CLI subcommands ----------------

/// `fastetcd backup --out <path>`: consistent single-file copy. Opens
/// the data file exclusively first, so it refuses while the server holds
/// it (a raw copy of a live db would be inconsistent — use
/// `fastetcd-ctl snapshot-save` for a hot backup).
pub async fn cmd_backup(data_dir: &Path, out: &Path) -> anyhow::Result<()> {
    let src = data_file(data_dir);
    if !src.exists() {
        anyhow::bail!("no data directory at {}", src.display());
    }
    // Opening validates the file and proves no other process holds it.
    let engine = open_or_hint(&src)?;
    let bytes = std::fs::copy(&src, out)?;
    drop(engine);
    println!(
        "backup: copied {} ({} bytes) -> {}",
        src.display(),
        bytes,
        out.display()
    );
    Ok(())
}

/// `fastetcd defrag` — offline space reclaim.
///
/// This is the escape hatch for a volume that is already full
/// (fastetcd#14). Every other route out needs something the wedged
/// server cannot do: a linearizable read needs a read barrier, a delete
/// needs a raft proposal, and both fail behind the same pending
/// snapshot write. Defragmenting offline needs none of that — no
/// quorum, no snapshot, no log append — it rewrites the data file
/// compactly and truncates it.
///
/// It also does not need free space to work: redb's compaction moves
/// live pages toward the front of the existing file and shortens it.
///
/// The server must be stopped; opening the file exclusively is what
/// proves that.
pub async fn cmd_defrag(data_dir: &Path) -> anyhow::Result<()> {
    let src = data_file(data_dir);
    if !src.exists() {
        anyhow::bail!("no data directory at {}", src.display());
    }
    let before = std::fs::metadata(&src)?.len();
    let engine = open_or_hint(&src)?;
    let usage = engine
        .usage()
        .await
        .map_err(|e| anyhow::anyhow!("reading store usage: {e}"))?;
    println!(
        "defrag: {} is {} bytes, {} bytes live (up to {} bytes may be recoverable)",
        src.display(),
        usage.file_bytes,
        usage.in_use_bytes,
        usage.reclaimable_bytes()
    );
    engine
        .defragment()
        .await
        .map_err(|e| anyhow::anyhow!("defragment: {e}"))?;
    drop(engine);
    let after = std::fs::metadata(&src)?.len();
    println!(
        "defrag: {} -> {} bytes ({} bytes returned to the filesystem)",
        before,
        after,
        before.saturating_sub(after)
    );
    if after >= before {
        println!(
            "defrag: nothing came back. The figure above is an upper bound — it is \
             the space not currently held by live pages, and the allocator can only \
             return whole regions at the end of the file. Compact MVCC history \
             (`etcdctl compact <rev>`, or run with --auto-compaction-retention) to \
             turn live pages into free ones, or give the volume more room."
        );
    }
    Ok(())
}

/// `fastetcd restore <backup> [--force]`.
pub async fn cmd_restore(data_dir: &Path, backup: &Path, force: bool) -> anyhow::Result<()> {
    if !backup.exists() {
        anyhow::bail!("backup file {} does not exist", backup.display());
    }
    let backup_rev = revision_of(backup).await.map_err(|e| {
        anyhow::anyhow!("{} is not a readable fastetcd backup: {e}", backup.display())
    })?;

    let dst = data_file(data_dir);
    if dst.exists() {
        let cur_rev = revision_of(&dst).await?;
        if cur_rev > backup_rev && !force {
            anyhow::bail!(
                "refusing to restore: current data dir is at revision {cur_rev}, newer than \
                 the backup at revision {backup_rev}. Re-run with --force to overwrite \
                 (the current file is kept as fastetcd.redb.replaced-*)."
            );
        }
        // Keep the pre-restore file so restore itself is reversible.
        let saved = data_dir.join(format!("{DATA_FILE}.replaced-{}", unix_secs()));
        std::fs::copy(&dst, &saved)?;
        println!("restore: saved current data as {}", saved.display());
    } else {
        std::fs::create_dir_all(data_dir)?;
    }
    let bytes = std::fs::copy(backup, &dst)?;
    println!(
        "restore: {} (rev {}) -> {} ({} bytes)",
        backup.display(),
        backup_rev,
        dst.display(),
        bytes
    );
    Ok(())
}

/// `fastetcd fsck [--repair]`. Returns the process exit code: 0 clean,
/// 1 problems found (or found and repaired), 2 unreadable.
pub async fn cmd_fsck(
    data_dir: &Path,
    all_members: &BTreeMap<NodeId, BasicNode>,
    node_id: NodeId,
    repair: bool,
) -> anyhow::Result<i32> {
    let src = data_file(data_dir);
    if !src.exists() {
        println!("fsck: no data directory at {} (nothing to check)", src.display());
        return Ok(0);
    }
    let engine: Arc<dyn KvStore> = match RedbEngine::open(&src) {
        Ok(e) => Arc::new(e),
        Err(e) => {
            println!("FAIL  structural: cannot open {}: {e}", src.display());
            println!("      the redb file is unreadable; restore from a backup.");
            return Ok(2);
        }
    };
    println!("ok    structural: {} opens", src.display());

    let mvcc = MvccStore::open(engine.clone()).await?;
    let sm = FastetcdStateMachine::open(mvcc.clone(), data_dir.join("snapshots")).await?;
    let mut log = KvLogStore::new(engine);

    let mut problems = 0u32;
    let current_rev = mvcc.current_revision().await;
    let compact_rev = mvcc.compact_revision().await;
    let has_data = current_rev > 0;

    // Format version.
    match mvcc.read_format_version().await? {
        Some(v) if v == FORMAT_VERSION => println!("ok    format_version: {v}"),
        Some(v) => {
            problems += 1;
            println!("WARN  format_version: {v} (this binary expects {FORMAT_VERSION})");
        }
        None if has_data => {
            problems += 1;
            println!("WARN  format_version: absent — pre-v1.0.1 directory, needs upgrade");
        }
        None => println!("ok    format_version: absent (empty directory)"),
    }

    // Raft membership.
    if has_data && sm.membership_is_empty().await {
        problems += 1;
        println!("FAIL  raft membership: empty voter set with data present — would not elect a leader (#11)");
    } else if has_data {
        println!("ok    raft membership: non-empty voter set");
    }

    // Log bounds (needed to judge last_applied).
    let ls = log.get_log_state().await?;
    println!(
        "info  raft log: purged={:?} last={:?}",
        ls.last_purged_log_id, ls.last_log_id
    );

    // last_applied. Its absence only matters if the log has been purged:
    // then openraft would try to replay purged entries and crash-loop
    // (#9). With an intact log, openraft simply replays from the start.
    let (applied, _m) = {
        use openraft::storage::RaftStateMachine;
        let mut sm2 = sm.clone();
        sm2.applied_state().await?
    };
    if has_data && applied.is_none() && ls.last_purged_log_id.is_some() {
        problems += 1;
        println!("FAIL  raft last_applied: absent but log is purged — would crash-loop on restart (#9)");
    } else {
        println!("ok    raft last_applied: {applied:?}");
    }

    // Counter sanity.
    if compact_rev < 0 || compact_rev > current_rev {
        problems += 1;
        println!("FAIL  mvcc counters: compact_rev {compact_rev} out of range [0, {current_rev}]");
    } else {
        println!("ok    mvcc counters: current_rev={current_rev} compact_rev={compact_rev}");
    }

    if problems == 0 {
        println!("\nfsck: clean.");
        return Ok(0);
    }

    if repair {
        println!("\nfsck --repair: applying metadata repairs...");
        let report = recover_data_dir(&sm, &mut log, all_members, node_id, false).await?;
        if report.did_something() {
            if let Some(src) = report.membership_source {
                println!(
                    "  repaired raft membership from {src}: voters={:?}",
                    report.membership_voters
                );
            }
            if report.adopted_applied.is_some() {
                println!("  set last_applied to {:?}", report.adopted_applied);
            }
            if report.stamped_format {
                println!("  stamped format_version={FORMAT_VERSION}");
            }
            println!("fsck: repaired {problems} problem(s). Re-run fsck to confirm.");
        } else {
            println!("fsck: nothing was auto-repairable ({problems} problem(s) remain).");
            println!("      deep MVCC key-index damage is not auto-repaired; restore from a backup.");
        }
        Ok(1)
    } else {
        println!("\nfsck: {problems} problem(s) found. Re-run with --repair to fix repairable ones.");
        Ok(1)
    }
}

// ---------------- helpers ----------------

fn open_or_hint(src: &Path) -> anyhow::Result<RedbEngine> {
    RedbEngine::open(src).map_err(|e| {
        anyhow::anyhow!(
            "cannot open {} exclusively ({e}); is the fastetcd server still running? \
             Stop it first, or use `fastetcd-ctl snapshot-save` for a live backup.",
            src.display()
        )
    })
}

async fn revision_of(file: &Path) -> anyhow::Result<i64> {
    let engine: Arc<dyn KvStore> = Arc::new(open_or_hint(file)?);
    let mvcc = MvccStore::open(engine).await?;
    Ok(mvcc.current_revision().await)
}
