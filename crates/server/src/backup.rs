//! Online backups to a separate volume (fastetcd#37).
//!
//! A backup is every table of the store, read from one engine snapshot
//! (one redb read transaction), so it is a single point-in-time copy of
//! the whole node: the MVCC data, the raft log and vote, leases, auth,
//! and the node-local metadata such as the cluster id. It is the state
//! a crash at that instant would have left, which is a state fastetcd
//! must already start from. The raft snapshot that `Maintenance.Snapshot`
//! serves would not do: it carries only the MVCC tables, so restoring
//! from it would lose leases, users and the cluster id.
//!
//! File format (`fastetcd-backup-<unix-ms>-rev<revision>.fbak`):
//!
//! ```text
//! 8 bytes   magic "FEBACKUP"
//! 4 bytes   format version, u32 little-endian (1)
//! bincode   BackupHeader
//! bincode   Vec<TableDump>
//! 32 bytes  SHA-256 of everything above
//! ```
//!
//! Written to a temp file, fsynced, renamed into place, and the
//! directory fsynced, so a backup that is listed is whole. The newest
//! `retain` are kept. A backup whose checksum fails is skipped in
//! favour of the next older one.
//!
//! Taking a backup holds the table contents in memory while it is
//! written, as building a raft snapshot does.

use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use fastetcd_raft::types::NodeId;
use fastetcd_storage::mvcc::store::META_KEY_RAFT_MEMBERSHIP;
use fastetcd_storage::mvcc::MvccStore;
use fastetcd_storage::redb_engine::RedbEngine;
use fastetcd_storage::{KvStore, WriteBatch, WriteOptions};

use crate::cluster_id::TABLE_NODE_META;

const MAGIC: &[u8; 8] = b"FEBACKUP";
const FORMAT: u32 = 1;
const PREFIX: &str = "fastetcd-backup-";
const EXT: &str = "fbak";
const TMP_SUFFIX: &str = ".tmp";
/// Rows per write batch when restoring, to bound one commit's size.
const RESTORE_BATCH_ROWS: usize = 10_000;

/// Describes a backup; stored at the front of the file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupHeader {
    pub fastetcd_version: String,
    pub created_unix_ms: u64,
    /// The node that took it. A node only restores its own backups.
    pub node_id: NodeId,
    pub cluster_id: Option<u64>,
    /// MVCC revision the backup holds.
    pub revision: i64,
    /// Raft members (voters and learners) recorded in the backup. Empty
    /// if the store had no membership yet.
    pub members: Vec<NodeId>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TableDump {
    name: String,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
}

/// A backup on disk.
#[derive(Debug, Clone)]
pub struct BackupInfo {
    pub path: PathBuf,
    pub header: BackupHeader,
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn bincode_io(e: bincode::Error) -> io::Error {
    match *e {
        bincode::ErrorKind::Io(io) => io,
        other => io::Error::new(io::ErrorKind::InvalidData, other),
    }
}

/// A writer that hashes everything written through it.
struct HashingWriter<W: Write> {
    inner: W,
    hash: Sha256,
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hash.update(&buf[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Take a backup of every table in `engine` into `dir`, then roll off
/// all but the newest `retain`.
pub async fn take(
    engine: &Arc<dyn KvStore>,
    dir: &Path,
    node_id: NodeId,
    retain: usize,
) -> anyhow::Result<BackupInfo> {
    // One snapshot for every table: a single point-in-time view. It is
    // dropped as soon as the tables are read, before anything is
    // written, so it holds no read transaction during the disk write.
    let tables = {
        let snap = engine.snapshot().await?;
        let mut tables = Vec::new();
        for name in snap.table_names().await? {
            let rows = snap
                .range(&name, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded, 0)
                .await?;
            tables.push(TableDump { name, rows });
        }
        tables
    };
    let header = BackupHeader {
        fastetcd_version: env!("CARGO_PKG_VERSION").to_string(),
        created_unix_ms: unix_ms(),
        node_id,
        cluster_id: find(&tables, TABLE_NODE_META, b"cluster_id")
            .and_then(|v| <[u8; 8]>::try_from(v).ok())
            .map(u64::from_be_bytes),
        revision: find(&tables, "mvcc_meta", b"current_rev")
            .and_then(|v| <[u8; 8]>::try_from(v).ok())
            .map(i64::from_be_bytes)
            .unwrap_or(0),
        members: find(&tables, "mvcc_meta", META_KEY_RAFT_MEMBERSHIP)
            .and_then(|v| {
                bincode::deserialize::<openraft::StoredMembership<NodeId, openraft::BasicNode>>(v)
                    .ok()
            })
            .map(|m| m.membership().nodes().map(|(id, _)| *id).collect())
            .unwrap_or_default(),
    };
    let dir = dir.to_path_buf();
    tokio::task::spawn_blocking(move || write_backup(&dir, header, &tables, retain)).await?
}

fn find<'a>(tables: &'a [TableDump], table: &str, key: &[u8]) -> Option<&'a [u8]> {
    tables
        .iter()
        .find(|t| t.name == table)?
        .rows
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_slice())
}

fn write_backup(
    dir: &Path,
    header: BackupHeader,
    tables: &[TableDump],
    retain: usize,
) -> anyhow::Result<BackupInfo> {
    std::fs::create_dir_all(dir)?;
    remove_temp_files(dir);
    let name = format!(
        "{PREFIX}{}-rev{}.{EXT}",
        header.created_unix_ms, header.revision
    );
    let path = dir.join(&name);
    let tmp = dir.join(format!(".{name}{TMP_SUFFIX}"));
    let written = write_file(&tmp, &header, tables).or_else(|e| {
        // Out of room: give up the oldest backup and try once more, so
        // a full backup volume keeps a fresh backup rather than none.
        if fastetcd_raft::snapshot_store::is_out_of_space(&e) && list(dir).len() > 1 {
            let _ = std::fs::remove_file(&tmp);
            prune(dir, list(dir).len() - 1);
            write_file(&tmp, &header, tables)
        } else {
            Err(e)
        }
    });
    if let Err(e) = written.and_then(|()| std::fs::rename(&tmp, &path)) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e.into());
    }
    sync_dir(dir);
    prune(dir, retain.max(1));
    Ok(BackupInfo { path, header })
}

fn write_file(path: &Path, header: &BackupHeader, tables: &[TableDump]) -> io::Result<()> {
    let file = File::create(path)?;
    let mut w = HashingWriter {
        inner: BufWriter::with_capacity(1 << 20, file),
        hash: Sha256::new(),
    };
    w.write_all(MAGIC)?;
    w.write_all(&FORMAT.to_le_bytes())?;
    bincode::serialize_into(&mut w, header).map_err(bincode_io)?;
    bincode::serialize_into(&mut w, tables).map_err(bincode_io)?;
    let digest = w.hash.finalize();
    let mut inner = w.inner;
    inner.write_all(&digest)?;
    let file = inner.into_inner().map_err(|e| e.into_error())?;
    file.sync_all()
}

/// Remove temp files left by a backup that was interrupted by a crash.
fn remove_temp_files(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if name.starts_with(&format!(".{PREFIX}")) && name.ends_with(TMP_SUFFIX) {
            let _ = std::fs::remove_file(e.path());
        }
    }
}

/// Make a rename in `dir` durable. Best-effort: not every platform can
/// open a directory.
pub(crate) fn sync_dir(dir: &Path) {
    #[cfg(unix)]
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
    #[cfg(not(unix))]
    let _ = dir;
}

/// The backups in `dir`, newest first. Only files this module writes are
/// considered.
pub fn list(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut found: Vec<(u64, PathBuf)> = entries
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_str()?.to_string();
            let stem = name.strip_prefix(PREFIX)?.strip_suffix(&format!(".{EXT}"))?;
            let ms = stem.split('-').next()?.parse::<u64>().ok()?;
            Some((ms, e.path()))
        })
        .collect();
    found.sort_by(|a, b| b.0.cmp(&a.0));
    found.into_iter().map(|(_, p)| p).collect()
}

/// Keep the newest `keep` backups, deleting older ones.
fn prune(dir: &Path, keep: usize) {
    for old in list(dir).into_iter().skip(keep) {
        match std::fs::remove_file(&old) {
            Ok(()) => tracing::info!(
                target: "fastetcd::backup",
                file = %old.display(),
                "rolled off an old backup"
            ),
            Err(e) => tracing::warn!(
                target: "fastetcd::backup",
                file = %old.display(),
                error = %e,
                "could not roll off an old backup"
            ),
        }
    }
}

/// Check a backup's checksum and read its header.
pub fn verify(path: &Path) -> anyhow::Result<BackupHeader> {
    let (header, _) = read(path, false)?;
    Ok(header)
}

/// Verify the checksum over the whole file, then decode it. With
/// `tables = false` only the header is decoded.
fn read(path: &Path, tables: bool) -> anyhow::Result<(BackupHeader, Vec<TableDump>)> {
    let mut file = File::open(path)?;
    let len = file.metadata()?.len();
    if len < (MAGIC.len() + 4 + 32) as u64 {
        anyhow::bail!("{} is too short to be a backup", path.display());
    }
    let body_len = len - 32;

    // Checksum first: nothing is trusted until it matches.
    let mut hash = Sha256::new();
    {
        let mut body = BufReader::with_capacity(1 << 20, (&file).take(body_len));
        let mut buf = vec![0u8; 1 << 20];
        loop {
            let n = body.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hash.update(&buf[..n]);
        }
    }
    let mut stored = [0u8; 32];
    file.seek(SeekFrom::Start(body_len))?;
    file.read_exact(&mut stored)?;
    if hash.finalize().as_slice() != stored {
        anyhow::bail!("{} fails its checksum", path.display());
    }

    file.seek(SeekFrom::Start(0))?;
    let mut r = BufReader::with_capacity(1 << 20, (&file).take(body_len));
    let mut magic = [0u8; 8];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC {
        anyhow::bail!("{} is not a fastetcd backup", path.display());
    }
    let mut format = [0u8; 4];
    r.read_exact(&mut format)?;
    let format = u32::from_le_bytes(format);
    if format != FORMAT {
        anyhow::bail!(
            "{} is backup format {format}; this fastetcd reads format {FORMAT}",
            path.display()
        );
    }
    let header: BackupHeader = bincode::deserialize_from(&mut r)?;
    let tables = if tables {
        bincode::deserialize_from(&mut r)?
    } else {
        Vec::new()
    };
    Ok((header, tables))
}

/// True if `path` starts with the backup magic, so the offline restore
/// can tell a backup from a raw copy of the data file.
pub fn is_backup_file(path: &Path) -> bool {
    let mut magic = [0u8; 8];
    File::open(path)
        .and_then(|mut f| f.read_exact(&mut magic))
        .is_ok()
        && &magic == MAGIC
}

/// Rebuild a data file from a backup. The new store is written to a temp
/// file beside `data_file` and renamed over it only once it is complete
/// and durable, so a failed restore leaves `data_file` as it was.
pub async fn restore_to(backup: &Path, data_file: &Path) -> anyhow::Result<BackupHeader> {
    let backup_path = backup.to_path_buf();
    let (header, tables) = tokio::task::spawn_blocking(move || read(&backup_path, true)).await??;
    let tmp = data_file.with_extension("redb.restoring");
    let _ = std::fs::remove_file(&tmp);
    {
        let engine = RedbEngine::open(&tmp)?;
        for table in &tables {
            for chunk in table.rows.chunks(RESTORE_BATCH_ROWS) {
                let mut batch = WriteBatch::new();
                for (k, v) in chunk {
                    batch.put(&table.name, k, v);
                }
                engine.commit(batch, WriteOptions::default()).await?;
            }
        }
        engine.sync().await?;
    }
    std::fs::rename(&tmp, data_file)?;
    if let Some(dir) = data_file.parent() {
        sync_dir(dir);
    }
    Ok(header)
}

// ---------------- the periodic backup task ----------------

/// When to take backups.
#[derive(Debug, Clone)]
pub struct BackupConfig {
    pub dir: PathBuf,
    pub interval: Duration,
    /// Also back up once this many revisions have been written since the
    /// last backup. 0 disables the revision trigger.
    pub every_revisions: i64,
    pub retain: usize,
    pub node_id: NodeId,
}

/// What the last backup captured, for deciding when the next is due.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Last {
    pub revision: i64,
    pub members: u64,
}

/// Whether a backup is due. `since` is the time since the last backup
/// (`None` if there has not been one); `members` is a fingerprint of the
/// raft membership. Nothing changed means nothing to back up, however
/// long it has been.
pub fn due(
    cfg: &BackupConfig,
    last: Option<Last>,
    since: Option<Duration>,
    revision: i64,
    members: u64,
) -> bool {
    let Some(last) = last else {
        return true;
    };
    let changed = revision != last.revision || members != last.members;
    if !changed {
        return false;
    }
    // A membership change is backed up at once: whether a node may
    // restore itself depends on whether it was alone (fastetcd#37).
    members != last.members
        || since.map_or(true, |s| s >= cfg.interval)
        || (cfg.every_revisions > 0 && revision - last.revision >= cfg.every_revisions)
}

/// A fingerprint of the raft membership, to notice it change.
fn members_fingerprint(raft: &openraft::Raft<fastetcd_raft::TypeConfig>) -> u64 {
    let m = raft.metrics().borrow().membership_config.clone();
    let mut ids: Vec<NodeId> = m.membership().nodes().map(|(id, _)| *id).collect();
    ids.sort_unstable();
    ids.iter().fold(0xcbf2_9ce4_8422_2325u64, |h, id| {
        (h ^ id).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// Run periodic backups for the life of the process.
pub fn spawn(
    cfg: BackupConfig,
    engine: Arc<dyn KvStore>,
    mvcc: MvccStore,
    raft: openraft::Raft<fastetcd_raft::TypeConfig>,
) {
    tokio::spawn(async move {
        // Start from the newest backup already on the volume, so a
        // restart does not immediately take another.
        let newest = list(&cfg.dir).into_iter().find_map(|p| verify(&p).ok());
        let mut last: Option<Last> = newest.as_ref().map(|h| Last {
            revision: h.revision,
            members: 0,
        });
        let mut last_at: Option<Instant> = newest.as_ref().and_then(|h| {
            let age = unix_ms().saturating_sub(h.created_unix_ms);
            Instant::now().checked_sub(Duration::from_millis(age))
        });
        let tick = cfg.interval.min(Duration::from_secs(10)).max(Duration::from_secs(1));
        loop {
            tokio::time::sleep(tick).await;
            let revision = mvcc.current_revision().await;
            let members = members_fingerprint(&raft);
            // A backup read from disk has no fingerprint; adopt the
            // current one so it is not mistaken for a membership change.
            if let Some(l) = last.as_mut().filter(|l| l.members == 0) {
                l.members = members;
            }
            if !due(&cfg, last, last_at.map(|t| t.elapsed()), revision, members) {
                continue;
            }
            let started = Instant::now();
            match take(&engine, &cfg.dir, cfg.node_id, cfg.retain).await {
                Ok(info) => {
                    tracing::info!(
                        target: "fastetcd::backup",
                        file = %info.path.display(),
                        revision = info.header.revision,
                        took_ms = started.elapsed().as_millis() as u64,
                        "backup written"
                    );
                    last = Some(Last { revision: info.header.revision, members });
                    last_at = Some(Instant::now());
                }
                Err(e) => {
                    // Retried on the next tick. A failing backup volume
                    // must never affect serving.
                    tracing::error!(
                        target: "fastetcd::backup",
                        dir = %cfg.dir.display(),
                        error = %e,
                        "backup FAILED — the node keeps serving, but a corrupt data \
                         file could not be recovered past the last good backup"
                    );
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> BackupConfig {
        BackupConfig {
            dir: PathBuf::from("/nonexistent"),
            interval: Duration::from_secs(900),
            every_revisions: 100,
            retain: 4,
            node_id: 1,
        }
    }

    #[test]
    fn a_backup_is_due_on_time_revisions_or_membership() {
        let c = cfg();
        let last = Some(Last { revision: 10, members: 7 });
        let fresh = Some(Duration::from_secs(5));
        let old = Some(Duration::from_secs(901));

        assert!(due(&c, None, None, 0, 7), "no backup yet");
        assert!(!due(&c, last, old, 10, 7), "nothing changed, however long");
        assert!(!due(&c, last, fresh, 11, 7), "a change, but not yet due");
        assert!(due(&c, last, old, 11, 7), "interval elapsed");
        assert!(due(&c, last, fresh, 110, 7), "revision trigger");
        assert!(due(&c, last, fresh, 10, 8), "membership changed");

        let no_rev_trigger = BackupConfig { every_revisions: 0, ..cfg() };
        assert!(!due(&no_rev_trigger, last, fresh, 1_000_000, 7));
    }
}
