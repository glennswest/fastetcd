//! On-disk store for raft snapshots, with retention and roll-off.
//!
//! A fastetcd snapshot is a full serialized copy of the MVCC database.
//! On a bounded data volume that is the single largest thing the node
//! writes, so *how many* copies are kept, and *when* the old ones go
//! away, decides whether the volume survives (fastetcd#14).
//!
//! Layout — one pair of files per retained snapshot, named by the log
//! index the snapshot covers, zero-padded so lexicographic order is
//! numeric order:
//!
//! ```text
//! snapshots/00000000000000012345.snap   # bincode SnapshotPayload
//! snapshots/00000000000000012345.meta   # bincode SnapshotMeta
//! ```
//!
//! Rules, all of them unconditional rather than best-effort:
//!
//! - **Roll off before writing, oldest first.** Retention is enforced
//!   *ahead* of the new write, not after it, so writing snapshot N+1
//!   never needs room for `retain + 1` copies at once. Writing into
//!   space the store just freed is what keeps the high-water mark at
//!   `retain` copies instead of `retain + 1`.
//! - **Prune on open.** A crash mid-write leaves temp files and can
//!   leave more snapshots than retention allows; both are reclaimed
//!   before the node starts serving.
//! - **Out of space means drop everything and retry.** openraft can
//!   always rebuild a snapshot from the state machine, so a node with no
//!   snapshot is recoverable while a node that cannot write one is not.

use std::io;
use std::path::{Path, PathBuf};

use openraft::SnapshotMeta;

use crate::types::NodeId;

type Meta = SnapshotMeta<NodeId, openraft::BasicNode>;

const SNAP_EXT: &str = "snap";
const META_EXT: &str = "meta";
const TMP_SUFFIX: &str = ".tmp";

/// Pre-v1.1 layout: a single snapshot overwritten in place. Migrated
/// into the indexed layout on open.
const LEGACY_SNAP: &str = "current.snap";
const LEGACY_META: &str = "current.meta";

/// Snapshots retained by default. One is enough for correctness —
/// openraft only ever reads the newest — and on a fixed-size volume each
/// extra copy is another whole database. Operators who want the older
/// copies as a safety net raise `--max-snapshots`.
pub const DEFAULT_RETAIN: usize = 1;

/// A directory of retained snapshots.
#[derive(Clone, Debug)]
pub struct SnapshotStore {
    dir: PathBuf,
    retain: usize,
}

impl SnapshotStore {
    /// Open (creating if needed) the snapshot directory, then reconcile
    /// it: drop temp files, drop half-written pairs, migrate the legacy
    /// single-snapshot layout, and roll off anything beyond `retain`.
    pub fn open(dir: impl Into<PathBuf>, retain: usize) -> io::Result<Self> {
        let store = Self {
            dir: dir.into(),
            retain: retain.max(1),
        };
        std::fs::create_dir_all(&store.dir)?;
        store.remove_temp_files();
        store.migrate_legacy();
        store.remove_orphans();
        store.prune_to(store.retain);
        Ok(store)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn retain(&self) -> usize {
        self.retain
    }

    /// Indices of the retained snapshots, oldest first.
    fn indices(&self) -> Vec<u64> {
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return Vec::new();
        };
        let mut out: Vec<u64> = entries
            .flatten()
            .filter_map(|e| {
                let path = e.path();
                if path.extension()?.to_str()? != SNAP_EXT {
                    return None;
                }
                path.file_stem()?.to_str()?.parse::<u64>().ok()
            })
            .collect();
        out.sort_unstable();
        out
    }

    fn snap_path(&self, index: u64) -> PathBuf {
        self.dir.join(format!("{index:020}.{SNAP_EXT}"))
    }

    fn meta_path(&self, index: u64) -> PathBuf {
        self.dir.join(format!("{index:020}.{META_EXT}"))
    }

    fn index_of(meta: &Meta) -> u64 {
        meta.last_log_id.map(|l| l.index).unwrap_or(0)
    }

    /// Metadata of the newest retained snapshot, if any.
    pub fn latest_meta(&self) -> Option<Meta> {
        for index in self.indices().into_iter().rev() {
            match std::fs::read(self.meta_path(index)) {
                Ok(bytes) => {
                    if let Ok(meta) = bincode::deserialize::<Meta>(&bytes) {
                        return Some(meta);
                    }
                }
                Err(_) => continue,
            }
        }
        None
    }

    /// Body of the snapshot covering `index`.
    pub fn read_body(&self, index: u64) -> io::Result<Vec<u8>> {
        std::fs::read(self.snap_path(index))
    }

    /// Body of the snapshot described by `meta`.
    pub fn read_body_for(&self, meta: &Meta) -> io::Result<Vec<u8>> {
        self.read_body(Self::index_of(meta))
    }

    /// Persist a snapshot, rolling off older ones first.
    ///
    /// Returns `Ok(true)` if the write only succeeded after discarding
    /// every retained snapshot to make room — the caller must then treat
    /// the previous snapshots as gone.
    pub fn store(&self, meta: &Meta, data: &[u8]) -> io::Result<bool> {
        // Roll off oldest-first *before* the write, leaving room for the
        // copy about to land. `retain - 1` because this new one takes
        // the last slot.
        self.prune_to(self.retain.saturating_sub(1));

        match self.write_pair(meta, data) {
            Ok(()) => Ok(false),
            Err(e) if is_out_of_space(&e) => {
                tracing::warn!(
                    target: "fastetcd::snapshot",
                    error = %e,
                    bytes = data.len(),
                    dir = %self.dir.display(),
                    "no space for a new snapshot — discarding every retained \
                     snapshot and retrying"
                );
                self.discard_all();
                self.write_pair(meta, data)?;
                Ok(true)
            }
            Err(e) => Err(e),
        }
    }

    /// Write the body then the meta, each through a temp file, so a meta
    /// never names a partial body.
    fn write_pair(&self, meta: &Meta, data: &[u8]) -> io::Result<()> {
        let index = Self::index_of(meta);
        let meta_bytes = bincode::serialize(meta).map_err(io::Error::other)?;
        write_atomic(&self.snap_path(index), data)?;
        write_atomic(&self.meta_path(index), &meta_bytes)
    }

    /// Delete every retained snapshot.
    pub fn discard_all(&self) {
        self.prune_to(0);
    }

    /// Roll off oldest-first until at most `keep` snapshots remain.
    fn prune_to(&self, keep: usize) {
        let indices = self.indices();
        if indices.len() <= keep {
            return;
        }
        let drop_count = indices.len() - keep;
        for index in indices.into_iter().take(drop_count) {
            let snap = self.snap_path(index);
            let freed = std::fs::metadata(&snap).map(|m| m.len()).unwrap_or(0);
            let _ = std::fs::remove_file(&snap);
            let _ = std::fs::remove_file(self.meta_path(index));
            tracing::info!(
                target: "fastetcd::snapshot",
                index,
                freed_bytes = freed,
                "rolled off an old snapshot"
            );
        }
    }

    /// Bytes the retained snapshots occupy.
    pub fn total_bytes(&self) -> u64 {
        fastetcd_storage::fs_space::dir_size(&self.dir)
    }

    fn remove_temp_files(&self) {
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let is_tmp = path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(TMP_SUFFIX));
            if is_tmp {
                match std::fs::remove_file(&path) {
                    Ok(()) => tracing::warn!(
                        target: "fastetcd::snapshot",
                        file = %path.display(),
                        "removed a leftover snapshot temp file"
                    ),
                    Err(e) => tracing::warn!(
                        target: "fastetcd::snapshot",
                        file = %path.display(),
                        error = %e,
                        "could not remove a leftover snapshot temp file"
                    ),
                }
            }
        }
    }

    /// A snapshot is only usable with both its body and its meta. Drop
    /// either half left alone by an interrupted write.
    fn remove_orphans(&self) {
        for index in self.indices() {
            if !self.meta_path(index).exists() {
                let _ = std::fs::remove_file(self.snap_path(index));
                tracing::warn!(
                    target: "fastetcd::snapshot",
                    index,
                    "removed a snapshot body with no metadata"
                );
            }
        }
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(index) = path
                .extension()
                .and_then(|e| e.to_str())
                .filter(|e| *e == META_EXT)
                .and_then(|_| path.file_stem())
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
            else {
                continue;
            };
            if !self.snap_path(index).exists() {
                let _ = std::fs::remove_file(&path);
                tracing::warn!(
                    target: "fastetcd::snapshot",
                    index,
                    "removed snapshot metadata with no body"
                );
            }
        }
    }

    /// Rename a pre-v1.1 `current.snap`/`current.meta` pair into the
    /// indexed layout, so an upgrade keeps the snapshot it already has
    /// and openraft can purge the log straight away instead of waiting
    /// for a fresh one.
    fn migrate_legacy(&self) {
        let legacy_snap = self.dir.join(LEGACY_SNAP);
        let legacy_meta = self.dir.join(LEGACY_META);
        if !legacy_snap.exists() || !legacy_meta.exists() {
            // A body with no meta (or the reverse) is unusable either
            // way; drop whichever half is there.
            let _ = std::fs::remove_file(&legacy_snap);
            let _ = std::fs::remove_file(&legacy_meta);
            return;
        }
        let index = std::fs::read(&legacy_meta)
            .ok()
            .and_then(|b| bincode::deserialize::<Meta>(&b).ok())
            .map(|m| Self::index_of(&m));
        let Some(index) = index else {
            tracing::warn!(
                target: "fastetcd::snapshot",
                "legacy snapshot metadata is unreadable — discarding it"
            );
            let _ = std::fs::remove_file(&legacy_snap);
            let _ = std::fs::remove_file(&legacy_meta);
            return;
        };
        if std::fs::rename(&legacy_snap, self.snap_path(index)).is_ok()
            && std::fs::rename(&legacy_meta, self.meta_path(index)).is_ok()
        {
            tracing::info!(
                target: "fastetcd::snapshot",
                index,
                "migrated the single-file snapshot into the retained layout"
            );
        }
    }
}

/// Write via a temp file and rename. A failed write cleans up its own
/// temp file — on a full volume that partial file is holding exactly the
/// space a retry needs.
fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension(format!(
        "{}{TMP_SUFFIX}",
        path.extension().and_then(|e| e.to_str()).unwrap_or("")
    ));
    if let Err(e) = std::fs::write(&tmp, bytes) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    std::fs::rename(&tmp, path)
}

/// True when an I/O error is "the volume is full". `ErrorKind::StorageFull`
/// is still unstable, so match the OS codes directly: ENOSPC on unix,
/// ERROR_HANDLE_DISK_FULL / ERROR_DISK_FULL on Windows.
fn is_out_of_space(e: &io::Error) -> bool {
    #[cfg(unix)]
    const CODES: &[i32] = &[28];
    #[cfg(windows)]
    const CODES: &[i32] = &[39, 112];
    #[cfg(not(any(unix, windows)))]
    const CODES: &[i32] = &[];
    e.raw_os_error().is_some_and(|c| CODES.contains(&c))
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{CommittedLeaderId, LogId};

    fn meta_at(index: u64) -> Meta {
        SnapshotMeta {
            last_log_id: Some(LogId::new(CommittedLeaderId::new(1, 0), index)),
            last_membership: Default::default(),
            snapshot_id: format!("snap-{index}"),
        }
    }

    #[test]
    fn retention_rolls_off_oldest_first() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path(), 2).unwrap();
        for index in 1..=5u64 {
            store.store(&meta_at(index), &[index as u8; 32]).unwrap();
        }
        assert_eq!(store.indices(), vec![4, 5], "only the newest two survive");
        assert_eq!(store.latest_meta().unwrap().snapshot_id, "snap-5");
        assert_eq!(store.read_body(5).unwrap(), vec![5u8; 32]);
    }

    #[test]
    fn a_write_never_needs_room_for_more_than_retention() {
        // The roll-off must happen before the new body is written, so
        // the directory never holds `retain + 1` snapshots even for an
        // instant. With retain = 1 that means the old snapshot is gone
        // before the new one lands.
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path(), 1).unwrap();
        store.store(&meta_at(1), &[1u8; 4096]).unwrap();
        let before = store.total_bytes();
        store.store(&meta_at(2), &[2u8; 4096]).unwrap();
        assert_eq!(store.indices(), vec![2]);
        assert_eq!(
            store.total_bytes(),
            before,
            "one snapshot in, one snapshot out — the footprint must not grow"
        );
    }

    #[test]
    fn open_prunes_beyond_retention_and_clears_temp_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path(), 5).unwrap();
        for index in 1..=4u64 {
            store.store(&meta_at(index), &[0u8; 16]).unwrap();
        }
        std::fs::write(dir.path().join("00000000000000000009.snap.tmp"), [0u8; 999]).unwrap();

        // Restarting with a tighter retention must reclaim immediately,
        // not wait for the next snapshot.
        let reopened = SnapshotStore::open(dir.path(), 2).unwrap();
        assert_eq!(reopened.indices(), vec![3, 4]);
        assert!(!dir.path().join("00000000000000000009.snap.tmp").exists());
    }

    #[test]
    fn half_written_pairs_are_discarded_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path(), 3).unwrap();
        store.store(&meta_at(1), &[1u8; 16]).unwrap();
        // Body with no meta, and meta with no body.
        std::fs::write(dir.path().join("00000000000000000002.snap"), [2u8; 16]).unwrap();
        std::fs::write(dir.path().join("00000000000000000003.meta"), [3u8; 16]).unwrap();

        let reopened = SnapshotStore::open(dir.path(), 3).unwrap();
        assert_eq!(reopened.indices(), vec![1]);
        assert!(!dir.path().join("00000000000000000003.meta").exists());
    }

    #[test]
    fn the_legacy_single_snapshot_layout_is_migrated() {
        let dir = tempfile::tempdir().unwrap();
        let meta = meta_at(77);
        std::fs::write(dir.path().join(LEGACY_SNAP), [7u8; 64]).unwrap();
        std::fs::write(
            dir.path().join(LEGACY_META),
            bincode::serialize(&meta).unwrap(),
        )
        .unwrap();

        let store = SnapshotStore::open(dir.path(), 2).unwrap();
        assert_eq!(store.indices(), vec![77], "the existing snapshot is kept");
        assert_eq!(store.read_body(77).unwrap(), vec![7u8; 64]);
        assert!(!dir.path().join(LEGACY_SNAP).exists());
    }

    #[test]
    fn discard_all_empties_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path(), 3).unwrap();
        store.store(&meta_at(1), &[0u8; 16]).unwrap();
        store.store(&meta_at(2), &[0u8; 16]).unwrap();
        store.discard_all();
        assert!(store.indices().is_empty());
        assert!(store.latest_meta().is_none());
        assert_eq!(store.total_bytes(), 0);
    }
}
