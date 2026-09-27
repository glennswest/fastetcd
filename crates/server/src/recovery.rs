//! Start from a backup instead of crash-looping on a corrupt data file
//! (fastetcd#37).
//!
//! After a power cut on a device that loses fsync'd writes, redb can
//! refuse to open the store ("All roots are corrupted"). Without this,
//! the node crash-looped with nothing to recover from.
//!
//! What happens when the data file is corrupt:
//!
//! - **A lone member restores locally** (`--on-corruption=restore`, the
//!   default). The newest backup in `--backup-dir` whose checksum
//!   verifies is written into a new data file; only then is the corrupt
//!   file moved aside as `fastetcd.redb.corrupt.<unix-ms>` (never
//!   deleted), with the retained raft snapshots beside it, and the
//!   restored file put in its place. The recovery is recorded in the
//!   store, so the CORRUPT alarm and the metrics report it until an
//!   operator disarms it. Everything written after that backup is lost,
//!   and the alarm says how much.
//! - **A member of a multi-node cluster refuses to start.** Its raft
//!   vote was in the corrupt file. Restarting without it (empty, or from
//!   a backup holding an older vote) could let it vote twice in one term
//!   and elect two leaders. The corrupt file is left in place and the
//!   error explains how to replace the member.
//! - **`--on-corruption=refuse`**, no `--backup-dir`, or no usable backup:
//!   refuse, leaving the file untouched.
//!
//! "Lone" means `--initial-cluster` names at most one member *and* the
//! newest backup records no other raft member. Backups are taken on
//! every membership change, so the backup is as current as the
//! membership.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use fastetcd_raft::types::NodeId;
use fastetcd_storage::redb_engine::RedbEngine;
use fastetcd_storage::{KvStore, StorageError, WriteBatch, WriteOptions};

use crate::backup;
use crate::cluster_id::TABLE_NODE_META;

const KEY_RECOVERY: &[u8] = b"recovered_from_backup";
const KEY_RECOVERIES: &[u8] = b"recoveries_total";

/// What to do when the data file is corrupt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum OnCorruption {
    /// Restore a lone member from its newest good backup.
    Restore,
    /// Refuse to start, leaving the file untouched.
    Refuse,
}

/// How to open the data file.
#[derive(Debug, Clone)]
pub struct OpenOptions {
    pub backup_dir: Option<PathBuf>,
    pub on_corruption: OnCorruption,
    pub node_id: NodeId,
    /// Members named by `--initial-cluster` (1 for a single node).
    pub configured_members: usize,
}

/// A recovery from backup, persisted until the alarm is disarmed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryRecord {
    pub recovered_unix_ms: u64,
    /// The corruption redb reported.
    pub reason: String,
    pub corrupt_file: String,
    pub backup_file: String,
    pub backup_created_unix_ms: u64,
    /// The revision the store was restored to. Anything after it is lost.
    pub backup_revision: i64,
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Open the data file, restoring it from a backup if it is corrupt and
/// this node may. Returns the engine, and the recovery if one happened
/// on this start.
pub async fn open_or_recover(
    data_file: &Path,
    opts: &OpenOptions,
) -> anyhow::Result<(RedbEngine, Option<RecoveryRecord>)> {
    finish_interrupted_restore(data_file)?;
    let reason = match RedbEngine::open(data_file) {
        Ok(engine) => return Ok((engine, None)),
        Err(StorageError::Corrupted(reason)) => reason,
        Err(e) => return Err(e.into()),
    };
    let data_dir = data_file.parent().unwrap_or(Path::new("."));
    tracing::error!(
        target: "fastetcd::recovery",
        file = %data_file.display(),
        reason = %reason,
        "the data file is CORRUPT"
    );

    if opts.on_corruption == OnCorruption::Refuse {
        anyhow::bail!(
            "the data file {} is corrupt ({reason}) and --on-corruption=refuse is set. \
             The file is left untouched. To recover, stop fastetcd, move the file aside, and \
             restore a backup with `fastetcd restore <backup>`.",
            data_file.display()
        );
    }
    if opts.configured_members > 1 {
        anyhow::bail!(multi_member_error(data_file, &reason, opts.node_id));
    }
    let Some(backup_dir) = &opts.backup_dir else {
        anyhow::bail!(
            "the data file {} is corrupt ({reason}) and there is no --backup-dir to restore \
             from. The file is left untouched. Restore a backup with `fastetcd restore \
             <backup>`, or move the file aside to start this node empty (all data is lost).",
            data_file.display()
        );
    };

    // The newest backup that verifies, and belongs to this node.
    let mut chosen = None;
    for path in backup::list(backup_dir) {
        match backup::verify(&path) {
            Ok(h) if h.node_id != opts.node_id => tracing::warn!(
                target: "fastetcd::recovery",
                file = %path.display(),
                backup_node = h.node_id,
                "skipping a backup taken by a different node"
            ),
            Ok(h) => {
                chosen = Some((path, h));
                break;
            }
            Err(e) => tracing::warn!(
                target: "fastetcd::recovery",
                file = %path.display(),
                error = %e,
                "skipping a backup that does not verify"
            ),
        }
    }
    let Some((backup_path, header)) = chosen else {
        anyhow::bail!(
            "the data file {} is corrupt ({reason}) and {} holds no backup of this node that \
             verifies. The file is left untouched.",
            data_file.display(),
            backup_dir.display()
        );
    };
    if header.members.iter().any(|m| *m != opts.node_id) {
        anyhow::bail!(multi_member_error(data_file, &reason, opts.node_id));
    }

    // Build the restored store beside the corrupt one first, with the
    // recovery already recorded in it. Only once it is complete and
    // durable does anything move, so a failure here leaves everything as
    // it was, and a crash during the moves below is finished by
    // `finish_interrupted_restore` on the next start.
    let restored = restored_path(data_file);
    let _ = std::fs::remove_file(&restored);
    backup::restore_to(&backup_path, &restored).await?;

    let now = unix_ms();
    let corrupt_file = data_dir.join(format!(
        "{}.corrupt.{now}",
        data_file.file_name().and_then(|n| n.to_str()).unwrap_or("fastetcd.redb")
    ));
    let record = RecoveryRecord {
        recovered_unix_ms: now,
        reason,
        corrupt_file: corrupt_file.display().to_string(),
        backup_file: backup_path.display().to_string(),
        backup_created_unix_ms: header.created_unix_ms,
        backup_revision: header.revision,
    };
    {
        let engine = RedbEngine::open(&restored)?;
        let recoveries = read_count(&engine).await? + 1;
        let mut batch = WriteBatch::new();
        batch.put(TABLE_NODE_META, KEY_RECOVERY, &bincode::serialize(&record)?);
        batch.put(TABLE_NODE_META, KEY_RECOVERIES, &recoveries.to_be_bytes());
        engine.commit(batch, WriteOptions::default()).await?;
        engine.sync().await?;
    }

    // The order matters to `finish_interrupted_restore`: the data file
    // goes first, so "no data file, a restored file present" means the
    // restored file is complete.
    std::fs::rename(data_file, &corrupt_file)?;
    backup::sync_dir(data_dir);
    move_snapshots_aside(data_dir, now)?;
    std::fs::rename(&restored, data_file)?;
    backup::sync_dir(data_dir);
    let engine = RedbEngine::open(data_file)?;

    tracing::error!(
        target: "fastetcd::recovery",
        backup = %record.backup_file,
        backup_revision = record.backup_revision,
        backup_age_secs = now.saturating_sub(record.backup_created_unix_ms) / 1000,
        corrupt_file = %record.corrupt_file,
        "RECOVERED FROM BACKUP: the store is back at the backup's revision and everything \
         written after it is LOST. The CORRUPT alarm stays raised until `etcdctl alarm \
         disarm`. The corrupt file is kept for inspection."
    );
    Ok((engine, Some(record)))
}

/// Where a restore builds the new store before swapping it in.
fn restored_path(data_file: &Path) -> PathBuf {
    data_file.with_extension("redb.restored")
}

/// The retained raft snapshots may be newer than the backup. Left in
/// place, openraft would take the restored store to be behind a snapshot
/// it has; they belong with the corrupt file.
fn move_snapshots_aside(data_dir: &Path, now: u64) -> std::io::Result<()> {
    let snapshots = data_dir.join("snapshots");
    if snapshots.exists() {
        std::fs::rename(&snapshots, data_dir.join(format!("snapshots.corrupt.{now}")))?;
        backup::sync_dir(data_dir);
    }
    Ok(())
}

/// Finish a restore that a crash interrupted after the corrupt file was
/// moved aside. Without this, redb would create an empty store where the
/// data file was, and the node would start with nothing.
///
/// A restored file is only ever present without a data file if it was
/// complete, recovery record included, before the data file moved.
pub fn finish_interrupted_restore(data_file: &Path) -> anyhow::Result<()> {
    let restored = restored_path(data_file);
    if data_file.exists() || !restored.exists() {
        return Ok(());
    }
    let data_dir = data_file.parent().unwrap_or(Path::new("."));
    tracing::warn!(
        target: "fastetcd::recovery",
        file = %data_file.display(),
        restored = %restored.display(),
        "finishing a restore from backup that was interrupted"
    );
    move_snapshots_aside(data_dir, unix_ms())?;
    std::fs::rename(&restored, data_file)?;
    backup::sync_dir(data_dir);
    Ok(())
}

fn multi_member_error(data_file: &Path, reason: &str, node_id: NodeId) -> String {
    format!(
        "the data file {} is corrupt ({reason}) and this node is a member of a multi-node \
         cluster, so it will not start from a backup: its raft vote was in the corrupt file, \
         and a member that forgets its vote could help elect two leaders in one term. The \
         file is left in place. The other members keep serving. To replace this member: on \
         a healthy member run `etcdctl member remove {node_id:x}`; move this node's data \
         directory aside; `etcdctl member add` it again; and start it with \
         --initial-cluster-state=existing so it catches up from the leader.",
        data_file.display()
    )
}

async fn read_count(engine: &RedbEngine) -> anyhow::Result<u64> {
    let snap = engine.snapshot().await?;
    Ok(snap
        .get(TABLE_NODE_META, KEY_RECOVERIES)
        .await?
        .and_then(|v| <[u8; 8]>::try_from(v.as_slice()).ok())
        .map(u64::from_be_bytes)
        .unwrap_or(0))
}

/// The CORRUPT alarm: raised while a recovery is on record, until an
/// operator disarms it.
pub struct RecoveryAlarm {
    record: Mutex<Option<RecoveryRecord>>,
    recoveries: std::sync::atomic::AtomicU64,
}

impl Default for RecoveryAlarm {
    fn default() -> Self {
        Self {
            record: Mutex::new(None),
            recoveries: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl RecoveryAlarm {
    /// Load the recovery on record in the store, if any.
    pub async fn load(engine: &Arc<dyn KvStore>) -> anyhow::Result<Self> {
        let snap = engine.snapshot().await?;
        let record = match snap.get(TABLE_NODE_META, KEY_RECOVERY).await? {
            Some(bytes) => Some(bincode::deserialize(&bytes)?),
            None => None,
        };
        let recoveries = snap
            .get(TABLE_NODE_META, KEY_RECOVERIES)
            .await?
            .and_then(|v| <[u8; 8]>::try_from(v.as_slice()).ok())
            .map(u64::from_be_bytes)
            .unwrap_or(0);
        Ok(Self {
            record: Mutex::new(record),
            recoveries: std::sync::atomic::AtomicU64::new(recoveries),
        })
    }

    /// The recovery the alarm is raised for.
    pub fn active(&self) -> Option<RecoveryRecord> {
        self.record.lock().unwrap().clone()
    }

    /// Recoveries from backup over the life of this store.
    pub fn recoveries(&self) -> u64 {
        self.recoveries.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Clear the alarm (`etcdctl alarm disarm`) and forget the record.
    /// The recovery count is kept.
    pub async fn disarm(&self, engine: &Arc<dyn KvStore>) -> anyhow::Result<()> {
        if self.active().is_none() {
            return Ok(());
        }
        let mut batch = WriteBatch::new();
        batch.delete(TABLE_NODE_META, KEY_RECOVERY);
        engine.commit(batch, WriteOptions::default()).await?;
        *self.record.lock().unwrap() = None;
        tracing::info!(target: "fastetcd::recovery", "CORRUPT alarm disarmed");
        Ok(())
    }
}
