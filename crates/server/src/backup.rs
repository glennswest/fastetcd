//! Online backups to a separate volume (fastetcd#37).
//!
//! A backup is every table of the store, read from one engine snapshot
//! (one redb read transaction), so it is a single point-in-time copy of
//! the whole node: the MVCC data, the raft log and vote, leases, auth,
//! and the node-local metadata such as the cluster id. It is the state
//! a crash at that instant would have left, which is a state fastetcd
//! must already start from. `Maintenance.Snapshot` (`etcdctl snapshot
//! save`, `fastetcd-ctl snapshot-save`) streams the same format, so a
//! live snapshot restores with `fastetcd restore` (fastetcd#61). Before
//! that it streamed the raft snapshot body, which carries only the MVCC
//! tables and which nothing could restore.
//!
//! File format (`fastetcd-backup-<unix-ms>-rev<revision>.fbak`):
//!
//! ```text
//! 8 bytes   magic "FEBACKUP"
//! 4 bytes   format version, u32 little-endian (1)
//! bincode   BackupHeader
//! bincode   Vec<TableDump>
//! [roles trailer (#225): bincode MembershipRoles,
//!  4 bytes its length (u32 LE), 8 bytes magic "FEBKROL1"]
//! 32 bytes  SHA-256 of everything above
//! ```
//!
//! The roles trailer (fastetcd#225) records which members were voters
//! (per configuration, two during a joint change) and which learners,
//! so a restart can tell whether the membership changed since the
//! backup. bincode reads one value and ignores what follows, so an
//! older fastetcd verifies and restores a backup with the trailer as
//! before; a backup without it has unknown roles, and the next backup
//! tick takes a new one.
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
const ROLES_MAGIC: &[u8; 8] = b"FEBKROL1";
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
    /// Who of `members` votes and who learns, from the roles trailer.
    /// `None` if the store had no membership, or the backup predates
    /// the trailer. Not part of the bincode header, which older
    /// versions read.
    #[serde(skip)]
    pub roles: Option<MembershipRoles>,
}

/// The roles in a raft membership: what [`spawn`] compares to notice a
/// membership change, a learner's promotion included (fastetcd#225).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipRoles {
    /// The voters of each configuration, sorted: one set, or two while a
    /// joint-consensus change is in progress.
    pub voter_sets: Vec<Vec<NodeId>>,
    /// Members that do not vote, sorted.
    pub learners: Vec<NodeId>,
}

impl MembershipRoles {
    pub fn of(m: &openraft::Membership<NodeId, openraft::BasicNode>) -> Self {
        let mut learners: Vec<NodeId> = m.learner_ids().collect();
        learners.sort_unstable();
        MembershipRoles {
            voter_sets: m
                .get_joint_config()
                .iter()
                .map(|set| set.iter().copied().collect())
                .collect(),
            learners,
        }
    }
}

type StoredMembership = openraft::StoredMembership<NodeId, openraft::BasicNode>;

fn decode_membership(bytes: &[u8]) -> Option<StoredMembership> {
    bincode::deserialize::<StoredMembership>(bytes).ok()
}

/// The roles in the membership `engine` holds now: what a backup taken
/// now would record.
pub async fn current_roles(engine: &Arc<dyn KvStore>) -> anyhow::Result<Option<MembershipRoles>> {
    Ok(engine
        .get("mvcc_meta", META_KEY_RAFT_MEMBERSHIP)
        .await?
        .and_then(|v| decode_membership(&v))
        .map(|m| MembershipRoles::of(m.membership())))
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

// Takes `bincode::Error` (a `Box`) as is, to be passed to `map_err`.
#[allow(clippy::boxed_local)]
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

/// Every table in `engine`, read from one engine snapshot (a single
/// point-in-time view), and the header describing it.
pub struct Dump {
    pub header: BackupHeader,
    tables: Vec<TableDump>,
}

impl Dump {
    /// Read every table. The snapshot is dropped as soon as the tables
    /// are read, so it holds no read transaction while they are written.
    pub async fn read(engine: &Arc<dyn KvStore>, node_id: NodeId) -> anyhow::Result<Dump> {
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
        let membership =
            find(&tables, "mvcc_meta", META_KEY_RAFT_MEMBERSHIP).and_then(decode_membership);
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
            members: membership
                .as_ref()
                .map(|m| m.membership().nodes().map(|(id, _)| *id).collect())
                .unwrap_or_default(),
            roles: membership.as_ref().map(|m| MembershipRoles::of(m.membership())),
        };
        Ok(Dump { header, tables })
    }

    /// Bytes [`write_to`](Self::write_to) writes.
    pub fn encoded_len(&self) -> io::Result<u64> {
        let header = bincode::serialized_size(&self.header).map_err(bincode_io)?;
        let tables = bincode::serialized_size(&self.tables).map_err(bincode_io)?;
        let trailer = match &self.header.roles {
            Some(r) => bincode::serialized_size(r).map_err(bincode_io)? + 4 + 8,
            None => 0,
        };
        Ok(MAGIC.len() as u64 + 4 + header + tables + trailer + 32)
    }

    /// Write the backup file format to `w`: what `fastetcd restore`
    /// reads, from a file in `--backup-dir` or from `Maintenance.Snapshot`
    /// (fastetcd#61).
    pub fn write_to<W: Write>(&self, w: W) -> io::Result<W> {
        let mut w = HashingWriter { inner: w, hash: Sha256::new() };
        w.write_all(MAGIC)?;
        w.write_all(&FORMAT.to_le_bytes())?;
        bincode::serialize_into(&mut w, &self.header).map_err(bincode_io)?;
        bincode::serialize_into(&mut w, &self.tables).map_err(bincode_io)?;
        if let Some(roles) = &self.header.roles {
            let bytes = bincode::serialize(roles).map_err(bincode_io)?;
            w.write_all(&bytes)?;
            w.write_all(&(bytes.len() as u32).to_le_bytes())?;
            w.write_all(ROLES_MAGIC)?;
        }
        let digest = w.hash.finalize();
        let mut inner = w.inner;
        inner.write_all(&digest)?;
        Ok(inner)
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
    let dump = Dump::read(engine, node_id).await?;
    let dir = dir.to_path_buf();
    tokio::task::spawn_blocking(move || write_backup(&dir, &dump, retain)).await?
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

fn write_backup(dir: &Path, dump: &Dump, retain: usize) -> anyhow::Result<BackupInfo> {
    let header = dump.header.clone();
    std::fs::create_dir_all(dir)?;
    remove_temp_files(dir);
    let name = format!(
        "{PREFIX}{}-rev{}.{EXT}",
        header.created_unix_ms, header.revision
    );
    let path = dir.join(&name);
    let tmp = dir.join(format!(".{name}{TMP_SUFFIX}"));
    let written = write_file(&tmp, dump).or_else(|e| {
        // Out of room: give up the oldest backup and try once more, so
        // a full backup volume keeps a fresh backup rather than none.
        if fastetcd_raft::snapshot_store::is_out_of_space(&e) && list(dir).len() > 1 {
            let _ = std::fs::remove_file(&tmp);
            prune(dir, list(dir).len() - 1);
            write_file(&tmp, dump)
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

fn write_file(path: &Path, dump: &Dump) -> io::Result<()> {
    let file = File::create(path)?;
    let w = dump.write_to(BufWriter::with_capacity(1 << 20, file))?;
    let file = w.into_inner().map_err(|e| e.into_error())?;
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
    found.sort_by_key(|f| std::cmp::Reverse(f.0));
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
    let mut header: BackupHeader = bincode::deserialize_from(&mut r)?;
    let header_end = MAGIC.len() as u64 + 4 + bincode::serialized_size(&header)?;
    header.roles = read_roles(&mut file, header_end, body_len);
    file.seek(SeekFrom::Start(header_end))?;
    let mut r = BufReader::with_capacity(1 << 20, (&file).take(body_len - header_end));
    let tables = if tables {
        bincode::deserialize_from(&mut r)?
    } else {
        Vec::new()
    };
    Ok((header, tables))
}

/// The roles trailer at the end of a body of `body_len` bytes, if there
/// is one. Anything that does not decode is taken as no trailer: the
/// roles are then unknown, and the scheduler backs up again.
fn read_roles(file: &mut File, header_end: u64, body_len: u64) -> Option<MembershipRoles> {
    let footer_at = body_len.checked_sub(12).filter(|at| *at >= header_end)?;
    file.seek(SeekFrom::Start(footer_at)).ok()?;
    let mut footer = [0u8; 12];
    file.read_exact(&mut footer).ok()?;
    if &footer[4..] != ROLES_MAGIC {
        return None;
    }
    let len = u32::from_le_bytes(footer[..4].try_into().ok()?) as u64;
    let at = footer_at.checked_sub(len).filter(|at| *at >= header_end)?;
    file.seek(SeekFrom::Start(at)).ok()?;
    let mut bytes = vec![0u8; len as usize];
    file.read_exact(&mut bytes).ok()?;
    bincode::deserialize(&bytes).ok()
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Last {
    pub revision: i64,
    /// The membership's roles the backup holds (`None`: no membership,
    /// or unknown for a backup without the roles trailer).
    pub roles: Option<MembershipRoles>,
}

/// Whether a backup is due. `since` is the time since the last backup
/// (`None` if there has not been one); `roles` is the raft membership
/// now, voters and learners. Nothing changed means nothing to back up,
/// however long it has been.
pub fn due(
    cfg: &BackupConfig,
    last: Option<&Last>,
    since: Option<Duration>,
    revision: i64,
    roles: Option<&MembershipRoles>,
) -> bool {
    let Some(last) = last else {
        return true;
    };
    let members_changed = roles != last.roles.as_ref();
    if revision == last.revision && !members_changed {
        return false;
    }
    // A membership change is backed up at once: whether a node may
    // restore itself depends on whether it was alone (fastetcd#37), and
    // a restore must not undo a promotion (fastetcd#225).
    members_changed
        || since.is_none_or(|s| s >= cfg.interval)
        || (cfg.every_revisions > 0 && revision - last.revision >= cfg.every_revisions)
}

/// The backup task's state: what the last backup holds, and when it
/// was taken.
#[derive(Debug, Default)]
pub struct Scheduler {
    last: Option<Last>,
    last_at: Option<Instant>,
}

impl Scheduler {
    /// Start from the newest backup already in `dir`, so a restart does
    /// not immediately take another, and compare against what that
    /// backup holds, not against the membership now (fastetcd#225: a
    /// member added while the process was down is backed up at once).
    pub fn resume(dir: &Path) -> Scheduler {
        let newest = list(dir).into_iter().find_map(|p| verify(&p).ok());
        Scheduler {
            last: newest.as_ref().map(|h| Last { revision: h.revision, roles: h.roles.clone() }),
            last_at: newest.as_ref().and_then(|h| {
                let age = unix_ms().saturating_sub(h.created_unix_ms);
                Instant::now().checked_sub(Duration::from_millis(age))
            }),
        }
    }

    /// Take a backup if one is due. Returns it, or `None` if none was.
    pub async fn tick(
        &mut self,
        cfg: &BackupConfig,
        engine: &Arc<dyn KvStore>,
        mvcc: &MvccStore,
    ) -> anyhow::Result<Option<BackupInfo>> {
        let revision = mvcc.current_revision().await;
        let roles = current_roles(engine).await?;
        let since = self.last_at.map(|t| t.elapsed());
        if !due(cfg, self.last.as_ref(), since, revision, roles.as_ref()) {
            return Ok(None);
        }
        let info = take(engine, &cfg.dir, cfg.node_id, cfg.retain).await?;
        // What the backup holds, read from the same snapshot as its data.
        self.last = Some(Last { revision: info.header.revision, roles: info.header.roles.clone() });
        self.last_at = Some(Instant::now());
        Ok(Some(info))
    }
}

/// Run periodic backups for the life of the process.
pub fn spawn(cfg: BackupConfig, engine: Arc<dyn KvStore>, mvcc: MvccStore) {
    tokio::spawn(async move {
        let mut scheduler = Scheduler::resume(&cfg.dir);
        let tick = cfg.interval.min(Duration::from_secs(10)).max(Duration::from_secs(1));
        loop {
            tokio::time::sleep(tick).await;
            let started = Instant::now();
            match scheduler.tick(&cfg, &engine, &mvcc).await {
                Ok(None) => {}
                Ok(Some(info)) => {
                    tracing::info!(
                        target: "fastetcd::backup",
                        file = %info.path.display(),
                        revision = info.header.revision,
                        took_ms = started.elapsed().as_millis() as u64,
                        "backup written"
                    );
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

    fn dump(roles: Option<MembershipRoles>) -> Dump {
        Dump {
            header: BackupHeader {
                fastetcd_version: "test".into(),
                created_unix_ms: 1,
                node_id: 1,
                cluster_id: None,
                revision: 5,
                members: vec![1, 2],
                roles,
            },
            tables: vec![TableDump { name: "t".into(), rows: vec![(b"k".to_vec(), b"v".to_vec())] }],
        }
    }

    /// fastetcd#225: the roles trailer round-trips, a backup without it
    /// reads as unknown roles, and a reader that knows nothing of it (the
    /// 1.43.5 decode: magic, format, header, tables) still reads the file.
    #[test]
    fn the_roles_trailer_round_trips_and_older_readers_ignore_it() {
        let dir = tempfile::tempdir().unwrap();
        let roles = MembershipRoles { voter_sets: vec![vec![1], vec![1, 2]], learners: vec![] };
        for (name, roles) in [("with", Some(roles)), ("without", None)] {
            let d = dump(roles.clone());
            let path = dir.path().join(name);
            write_file(&path, &d).unwrap();
            let bytes = std::fs::read(&path).unwrap();
            assert_eq!(bytes.len() as u64, d.encoded_len().unwrap());
            assert_eq!(verify(&path).unwrap().roles, roles);
            let (h, tables) = read(&path, true).unwrap();
            assert_eq!(h, d.header);
            assert_eq!(tables[0].rows, d.tables[0].rows);

            let mut r = &bytes[12..bytes.len() - 32];
            let old: BackupHeader = bincode::deserialize_from(&mut r).unwrap();
            let old_tables: Vec<TableDump> = bincode::deserialize_from(&mut r).unwrap();
            assert_eq!(old.revision, 5);
            assert_eq!(old_tables[0].rows, d.tables[0].rows);
        }
    }

    #[test]
    fn a_backup_is_due_on_time_revisions_or_membership() {
        let c = cfg();
        let a = MembershipRoles { voter_sets: vec![vec![1]], learners: vec![2] };
        let promoted = MembershipRoles { voter_sets: vec![vec![1, 2]], learners: vec![] };
        let joint = MembershipRoles { voter_sets: vec![vec![1], vec![1, 2]], learners: vec![] };
        let last = Last { revision: 10, roles: Some(a.clone()) };
        let last = Some(&last);
        let fresh = Some(Duration::from_secs(5));
        let old = Some(Duration::from_secs(901));

        assert!(due(&c, None, None, 0, Some(&a)), "no backup yet");
        assert!(!due(&c, last, old, 10, Some(&a)), "nothing changed, however long");
        assert!(!due(&c, last, fresh, 11, Some(&a)), "a change, but not yet due");
        assert!(due(&c, last, old, 11, Some(&a)), "interval elapsed");
        assert!(due(&c, last, fresh, 110, Some(&a)), "revision trigger");
        // fastetcd#225: same ids, same revision, different roles.
        assert!(due(&c, last, fresh, 10, Some(&promoted)), "a learner promoted");
        assert!(due(&c, last, fresh, 10, Some(&joint)), "a joint configuration");
        let unknown = Last { revision: 10, roles: None };
        assert!(due(&c, Some(&unknown), fresh, 10, Some(&a)), "roles unknown");

        let no_rev_trigger = BackupConfig { every_revisions: 0, ..cfg() };
        assert!(!due(&no_rev_trigger, last, fresh, 1_000_000, Some(&a)));
    }
}
