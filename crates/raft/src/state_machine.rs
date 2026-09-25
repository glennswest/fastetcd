//! `RaftStateMachine` impl that wraps [`fastetcd_storage::mvcc::MvccStore`].
//!
//! Every committed Raft log entry is decoded as a [`FastetcdLogEntry`]
//! and dispatched into the corresponding `MvccStore` operation. The
//! state machine tracks:
//!
//! - `last_applied_log_id` — required by openraft to know where to
//!   resume after restart.
//! - `last_membership` — also required by openraft.
//! - The current MVCC snapshot for serving `get_current_snapshot()`.
//!
//! Snapshot strategy: the entire MVCC state plus `last_applied_log_id`
//! + `last_membership`, `bincode`-encoded into one file per snapshot.
//! A snapshot moves between memory, disk and the network as a
//! [`SnapshotFile`]: it is serialized straight into its file, sent by
//! reading that file chunk by chunk, received into a temp file and
//! decoded from it, so no step holds the encoded snapshot in RAM
//! (fastetcd#30). Building still collects the three tables as `Vec`s,
//! and install still decodes them whole — streaming those needs a new
//! on-disk format.

use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::Snapshot;
use openraft::AnyError;
use openraft::ErrorSubject;
use openraft::ErrorVerb;
use openraft::LogId;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::StoredMembership;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use fastetcd_storage::mvcc::MvccStore;

use crate::snapshot_data::{Content, SnapshotFile};
use crate::snapshot_store::{self, SnapshotStore};
use crate::types::{FastetcdLogEntry, FastetcdLogResponse, NodeId, TypeConfig};

/// Concrete state machine type, clonable, owned by the openraft
/// internals.
#[derive(Clone)]
pub struct FastetcdStateMachine {
    inner: Arc<Mutex<Inner>>,
    mvcc: MvccStore,
    /// Retained snapshots on disk. The snapshot body lives in a file,
    /// never in RAM — the state machine used to hold the whole
    /// serialized database as a `Vec` in `current_snapshot`, which is
    /// why an idle node sat at gigabytes (fastetcd#13). Only the small
    /// [`SnapshotMeta`] is kept in memory. The store rolls old
    /// snapshots off so the volume holds a bounded number of copies
    /// (fastetcd#14).
    snapshots: SnapshotStore,
}

struct Inner {
    last_applied_log_id: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
    /// Metadata of the current on-disk snapshot (`current.snap`), if
    /// any. The body is read from disk on demand, not held here.
    current_snapshot: Option<SnapshotMeta<NodeId, openraft::BasicNode>>,
    snapshot_idx: u64,
}

/// Encoded snapshot payload. The MVCC state itself is large; we lean
/// on `MvccStore::snapshot` to read a consistent view and bincode the
/// raw `(key, KvRecord)` and `(key, KeyIndex)` pairs.
#[derive(Debug, Serialize, Deserialize)]
struct SnapshotPayload {
    last_applied_log_id: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
    // MVCC tables (raw bytes, engine-encoded). Order doesn't matter
    // for correctness; we re-apply via the KvStore directly.
    kv_table: Vec<(Vec<u8>, Vec<u8>)>,
    idx_table: Vec<(Vec<u8>, Vec<u8>)>,
    meta_table: Vec<(Vec<u8>, Vec<u8>)>,
}

impl FastetcdStateMachine {
    /// Open the state machine over a persistent `MvccStore`, restoring
    /// `last_applied_log_id` and `last_membership` from disk.
    ///
    /// Restoring these is what makes restart safe. They used to reset
    /// to `None` on every boot, so openraft saw an empty state machine
    /// sitting next to a populated MVCC store and replayed the log from
    /// index 0 — which either crash-looped, because a snapshot had
    /// already purged the early entries ("expected index [0, N), got
    /// [None, None)"), or silently double-applied every mutation when
    /// the log happened to be intact (fastetcd#9).
    pub async fn open(mvcc: MvccStore, snapshot_dir: impl Into<std::path::PathBuf>)
        -> Result<Self, anyhow::Error>
    {
        Self::open_with_retention(mvcc, snapshot_dir, snapshot_store::DEFAULT_RETAIN).await
    }

    /// Like [`open`](Self::open), with an explicit number of snapshots
    /// to retain on disk (`--max-snapshots`).
    pub async fn open_with_retention(
        mvcc: MvccStore,
        snapshot_dir: impl Into<std::path::PathBuf>,
        retain: usize,
    ) -> Result<Self, anyhow::Error> {
        // Opening the store reconciles the directory first: leftover
        // temp files, half-written pairs and anything beyond retention
        // are reclaimed before the node serves a request (fastetcd#14).
        let snapshots = SnapshotStore::open(snapshot_dir.into(), retain)?;
        let (applied_bytes, membership_bytes) = mvcc.read_raft_meta().await?;
        // Encoded as `Option<LogId>`, matching what `apply` stages and
        // what `install_snapshot` writes — decoding it as a bare
        // `LogId` would silently skip bincode's one-byte Option tag and
        // shift every field.
        let last_applied_log_id: Option<LogId<NodeId>> = match applied_bytes {
            Some(b) => bincode::deserialize(&b)?,
            None => None,
        };
        let last_membership: StoredMembership<NodeId, openraft::BasicNode> =
            match membership_bytes {
                Some(b) => bincode::deserialize(&b)?,
                None => StoredMembership::default(),
            };

        // Restore the newest retained snapshot's metadata, so openraft
        // can purge the log against it immediately after a restart
        // (without this, a restart lost the snapshot and purge stalled
        // — fastetcd#13).
        let current_snapshot = snapshots.latest_meta();

        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                last_applied_log_id,
                last_membership,
                current_snapshot,
                snapshot_idx: 0,
            })),
            mvcc,
            snapshots,
        })
    }

    /// Write a snapshot through the retained store and return a handle
    /// to its body.
    ///
    /// The payload is serialized straight into the snapshot file; the
    /// encoded snapshot never exists as a `Vec` (fastetcd#30). The store
    /// rolls older snapshots off before it writes and, on ENOSPC,
    /// discards every retained snapshot and retries (fastetcd#14).
    ///
    /// If the snapshot still cannot be written, it is encoded into
    /// memory instead and `persisted` is false. That is the pre-#30
    /// behaviour, kept as the fallback so a full volume never becomes a
    /// storage error: openraft treats one as fatal to the whole node,
    /// and on the reported node that meant even deleting keys to make
    /// room was refused.
    async fn write_snapshot(
        &self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        payload: SnapshotPayload,
    ) -> Result<WrittenSnapshot, StorageError<NodeId>> {
        let snapshots = self.snapshots.clone();
        let meta = meta.clone();
        let signature = meta.signature();
        tokio::task::spawn_blocking(move || -> Result<WrittenSnapshot, StorageError<NodeId>> {
            let written = snapshots
                .store_with(&meta, |f| serialize_payload(f, &payload))
                .and_then(|_| snapshots.open_body(&meta));
            match written {
                Ok(file) => Ok(WrittenSnapshot {
                    body: SnapshotFile::retained(file),
                    persisted: true,
                }),
                Err(e) => {
                    tracing::error!(
                        target: "fastetcd::snapshot",
                        error = %e,
                        dir = %snapshots.dir().display(),
                        "could not write the raft snapshot to disk — holding it in \
                         memory instead. Free space on the data volume (delete keys, \
                         `etcdctl compact`, `etcdctl defrag`, or `fastetcd defrag` \
                         with the server stopped)."
                    );
                    let bytes = bincode::serialize(&payload).map_err(|e| {
                        StorageIOError::write_snapshot(Some(meta.signature()), AnyError::new(&e))
                    })?;
                    Ok(WrittenSnapshot {
                        body: SnapshotFile::memory(bytes),
                        persisted: false,
                    })
                }
            }
        })
        .await
        .map_err(|e| StorageIOError::write_snapshot(Some(signature), AnyError::new(&e)))?
    }

    /// Build a snapshot now, on behalf of `get_current_snapshot`.
    async fn build_now(&self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        FastetcdSnapshotBuilder { sm: self.clone() }.build_snapshot().await
    }

    /// Bytes the retained snapshots occupy on the data volume. The space
    /// monitor counts this against the store's footprint — a snapshot is
    /// another full copy of the database, so leaving it out of the
    /// accounting understates occupancy by roughly half.
    pub fn snapshot_bytes(&self) -> u64 {
        self.snapshots.total_bytes()
    }

    /// How many snapshots this node retains on disk.
    pub fn snapshot_retention(&self) -> usize {
        self.snapshots.retain()
    }

    pub fn mvcc(&self) -> &MvccStore {
        &self.mvcc
    }

    /// Recover a data directory written before `last_applied_log_id`
    /// was persisted (fastetcd#9).
    ///
    /// Such a directory has MVCC data but no record of how far the log
    /// was applied, so openraft replays from index 0 and fails the
    /// moment it hits entries a snapshot already purged. `floor` is the
    /// log store's `last_purged_log_id`, which is a safe lower bound:
    /// openraft only purges entries it has both applied and captured in
    /// a snapshot, so the state machine is at least that far along.
    ///
    /// Adopting the floor means entries between it and the true applied
    /// position replay, which re-applies a bounded tail of mutations and
    /// can inflate the revision. That is a real cost, but the
    /// alternative for these directories is the documented workaround —
    /// deleting the data entirely — so this recovers strictly more.
    ///
    /// No-op unless the store holds data and has no applied position:
    /// a healthy or genuinely empty node is left alone.
    pub async fn recover_applied_floor(
        &self,
        floor: Option<LogId<NodeId>>,
    ) -> Result<Option<LogId<NodeId>>, anyhow::Error> {
        let Some(floor) = floor else {
            return Ok(None);
        };
        let mut g = self.inner.lock().await;
        if g.last_applied_log_id.is_some() || self.mvcc.current_revision().await == 0 {
            return Ok(None);
        }
        let bytes = bincode::serialize(&Some(floor))?;
        self.mvcc.stage_raft_meta(bytes, None).await;
        self.mvcc.flush_raft_meta().await?;
        g.last_applied_log_id = Some(floor);
        Ok(Some(floor))
    }

    /// True if the restored state machine has no voters — i.e. openraft
    /// would come up with an empty voter set and never elect a leader.
    /// A directory written before v1.0.1 never persisted membership, so
    /// once its log has been purged this is the state a restart lands in
    /// (fastetcd#11).
    pub async fn membership_is_empty(&self) -> bool {
        let g = self.inner.lock().await;
        g.last_membership.membership().voter_ids().next().is_none()
    }

    /// Install a `last_membership` reconstructed during upgrade recovery
    /// (from `--initial-cluster`, or a single-node set under
    /// `--force-new-cluster`) and persist it durably, so openraft loads
    /// the correct voter set at startup instead of an empty one.
    pub async fn recover_membership(
        &self,
        membership: StoredMembership<NodeId, openraft::BasicNode>,
    ) -> Result<(), anyhow::Error> {
        let bytes = bincode::serialize(&membership)?;
        self.mvcc.persist_membership(&bytes).await?;
        let mut g = self.inner.lock().await;
        g.last_membership = membership;
        Ok(())
    }
}

/// A snapshot body just written, and whether it reached the disk.
struct WrittenSnapshot {
    body: SnapshotFile,
    persisted: bool,
}

/// Unwrap bincode's error so an I/O failure keeps its OS error code:
/// the store recognises a full volume by it (ENOSPC).
fn bincode_io(e: bincode::Error) -> std::io::Error {
    match *e {
        bincode::ErrorKind::Io(io) => io,
        other => std::io::Error::other(other),
    }
}

fn serialize_payload(f: &mut std::fs::File, payload: &SnapshotPayload) -> std::io::Result<()> {
    let mut w = BufWriter::with_capacity(1 << 20, f);
    bincode::serialize_into(&mut w, payload).map_err(bincode_io)?;
    w.flush()
}

/// Decode a snapshot body. A file is read through a buffer, never
/// loaded whole.
fn decode_payload(content: &Content) -> Result<SnapshotPayload, std::io::Error> {
    let from_file = |mut f: &std::fs::File| -> std::io::Result<SnapshotPayload> {
        f.seek(SeekFrom::Start(0))?;
        bincode::deserialize_from(BufReader::with_capacity(1 << 20, f)).map_err(bincode_io)
    };
    match content {
        Content::Incoming { file, .. } => {
            // Durable before it is decoded and applied: it is about to
            // become this node's retained snapshot.
            file.sync_all()?;
            from_file(file)
        }
        Content::Retained(file) => from_file(file),
        Content::Memory(bytes) => bincode::deserialize(bytes).map_err(bincode_io),
    }
}

/// Keep a copy of an installed snapshot as this node's retained one.
fn retain_installed(
    snapshots: &SnapshotStore,
    meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
    content: Content,
) -> std::io::Result<()> {
    match content {
        // The received file becomes the retained snapshot as it is.
        Content::Incoming { file, path } => {
            let adopted = snapshots.adopt(meta, &file, &path);
            if adopted.is_err() {
                let _ = std::fs::remove_file(&path);
            }
            adopted
        }
        // Someone else's retained file (a local install, or tests):
        // copy it, never move it.
        Content::Retained(src) => snapshots
            .store_with(meta, |f| {
                (&src).seek(SeekFrom::Start(0))?;
                std::io::copy(&mut &src, f).map(|_| ())
            })
            .map(|_| ()),
        Content::Memory(bytes) => snapshots.store(meta, &bytes).map(|_| ()),
    }
}

impl RaftStateMachine<TypeConfig> for FastetcdStateMachine {
    type SnapshotBuilder = FastetcdSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, openraft::BasicNode>,
        ),
        StorageError<NodeId>,
    > {
        let g = self.inner.lock().await;
        Ok((g.last_applied_log_id, g.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<FastetcdLogResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut g = self.inner.lock().await;
        let mut responses = Vec::new();

        for entry in entries {
            let log_id = entry.log_id;
            let applied_bytes = bincode::serialize(&Some(log_id)).map_err(|e| {
                StorageIOError::new(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Write,
                    AnyError::error(format!("serialize last_applied: {e}")),
                )
            })?;

            // Membership-change entries are recorded but produce no
            // application-level mutation.
            if let openraft::EntryPayload::Membership(m) = &entry.payload {
                let membership = StoredMembership::new(Some(log_id), m.clone());
                let membership_bytes = bincode::serialize(&membership).map_err(|e| {
                    StorageIOError::new(
                        ErrorSubject::StateMachine,
                        ErrorVerb::Write,
                        AnyError::error(format!("serialize last_membership: {e}")),
                    )
                })?;
                self.mvcc
                    .stage_raft_meta(applied_bytes, Some(membership_bytes))
                    .await;
                g.last_membership = membership;
                // Match openraft's contract — every entry produces one response.
                let rev = self.mvcc.current_revision().await;
                responses.push(FastetcdLogResponse::Noop { revision: rev });
                g.last_applied_log_id = Some(log_id);
                continue;
            }

            // Staged before dispatch so the MVCC write below folds it
            // into the same atomic batch.
            self.mvcc.stage_raft_meta(applied_bytes, None).await;

            // Normal or Blank entry. Decode the AppData if present.
            let response = match &entry.payload {
                openraft::EntryPayload::Normal(data) => apply_data(&self.mvcc, data).await,
                openraft::EntryPayload::Blank => {
                    // Heartbeat-like blank entry; just advance applied_log_id.
                    let rev = self.mvcc.current_revision().await;
                    Ok(FastetcdLogResponse::Noop { revision: rev })
                }
                openraft::EntryPayload::Membership(_) => unreachable!("handled above"),
            };

            let response = response.map_err(|e| {
                StorageIOError::new(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Write,
                    AnyError::error(format!("apply failed: {e}")),
                )
            })?;

            responses.push(response);
            g.last_applied_log_id = Some(log_id);
        }

        // Membership and blank entries mutate no MVCC state, so nothing
        // folded their staged log id into a batch. Commit it here or a
        // restart would replay them.
        self.mvcc.flush_raft_meta().await.map_err(|e| {
            StorageIOError::new(
                ErrorSubject::StateMachine,
                ErrorVerb::Write,
                AnyError::error(format!("persist last_applied: {e}")),
            )
        })?;

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        FastetcdSnapshotBuilder {
            sm: self.clone(),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<SnapshotFile>, StorageError<NodeId>> {
        // Chunks go to a temp file, not a growing `Vec` (fastetcd#30).
        // Retention is enforced first, as before any snapshot write, so
        // with the default of one the old snapshot is rolled off now.
        // That is safe: `get_current_snapshot` rebuilds on demand if this
        // node needs one before the new snapshot lands.
        let snapshots = self.snapshots.clone();
        let opened = tokio::task::spawn_blocking(move || snapshots.begin_incoming()).await;
        self.inner.lock().await.current_snapshot = self.snapshots.latest_meta();
        let body = match opened {
            Ok(Ok((file, path))) => SnapshotFile::incoming(file, path, self.snapshots.clone()),
            Ok(Err(e)) => {
                // Same fallback as a failed write: never refuse a snapshot
                // because the disk is full.
                tracing::warn!(
                    target: "fastetcd::snapshot",
                    error = %e,
                    "cannot create a file to receive the snapshot into — receiving \
                     it in memory. Free space on the data volume."
                );
                SnapshotFile::memory(Vec::new())
            }
            Err(e) => {
                return Err(StorageIOError::write_snapshot(None, AnyError::new(&e)).into());
            }
        };
        Ok(Box::new(body))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<SnapshotFile>,
    ) -> Result<(), StorageError<NodeId>> {
        let content = snapshot.into_content();

        // Decode straight from the file (no raw-bytes copy in RAM). A
        // received file that fails to decode is deleted here; the live
        // database has not been touched.
        let (payload, content) = tokio::task::spawn_blocking(move || {
            let decoded = decode_payload(&content);
            if decoded.is_err() {
                if let Content::Incoming { path, .. } = &content {
                    let _ = std::fs::remove_file(path);
                }
            }
            decoded.map(|p| (p, content))
        })
        .await
        .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), AnyError::new(&e)))?
        .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), AnyError::new(&e)))?;

        // Replace the MVCC engine contents.
        let applied = rebuild_mvcc(&self.mvcc, &payload).await;
        if let Err(e) = applied {
            if let Content::Incoming { path, .. } = &content {
                let _ = std::fs::remove_file(path);
            }
            return Err(StorageIOError::write_snapshot(
                Some(meta.signature()),
                AnyError::error(format!("rebuild mvcc from snapshot: {e}")),
            )
            .into());
        }
        let SnapshotPayload {
            last_applied_log_id,
            last_membership,
            ..
        } = payload;

        // Keep the installed snapshot as this node's retained one, so it
        // survives restart; only the meta stays in RAM. A received file
        // is renamed into place — not written a second time.
        // Best-effort: the data is already durable in the MVCC store
        // above, so failing to keep a copy must not fail the install
        // (see `build_snapshot` for why a storage error here is fatal to
        // the whole node). `get_current_snapshot` rebuilds if asked.
        let snapshots = self.snapshots.clone();
        let for_store = meta.clone();
        let persisted =
            tokio::task::spawn_blocking(move || retain_installed(&snapshots, &for_store, content))
                .await
                .map_err(std::io::Error::other)
                .and_then(|r| r);
        if let Err(e) = &persisted {
            tracing::error!(
                target: "fastetcd::snapshot",
                error = %e,
                "installed snapshot could not be kept on disk — the data is applied \
                 and durable; a snapshot will be rebuilt when one is needed. Free \
                 space on the data volume."
            );
        }

        let mut g = self.inner.lock().await;
        g.last_applied_log_id = last_applied_log_id;
        g.last_membership = last_membership;
        g.current_snapshot = persisted.is_ok().then(|| meta.clone());
        Ok(())
    }

    /// The newest retained snapshot, read from its file on demand.
    ///
    /// Never `None` while the state machine has applied anything. When
    /// openraft's replication needs a snapshot for a lagging follower
    /// and gets `None`, it fails with a storage error rather than asking
    /// for one to be built (openraft only rebuilds a missing snapshot at
    /// startup). So if there is no usable retained snapshot — it could
    /// not be written, was rolled off to make room for an incoming one,
    /// or was deleted — build one now.
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let (meta, applied) = {
            let g = self.inner.lock().await;
            (g.current_snapshot.clone(), g.last_applied_log_id)
        };
        if let Some(meta) = meta {
            match self.snapshots.open_body(&meta) {
                Ok(file) => {
                    return Ok(Some(Snapshot {
                        meta,
                        snapshot: Box::new(SnapshotFile::retained(file)),
                    }))
                }
                Err(e) => {
                    tracing::warn!(
                        target: "fastetcd::snapshot",
                        error = %e,
                        snapshot = %meta.snapshot_id,
                        "the retained snapshot is gone — building a new one"
                    );
                    self.inner.lock().await.current_snapshot = None;
                }
            }
        }
        if applied.is_none() {
            return Ok(None);
        }
        self.build_now().await.map(Some)
    }
}

/// Builds a snapshot of the current MVCC state.
pub struct FastetcdSnapshotBuilder {
    sm: FastetcdStateMachine,
}

impl RaftSnapshotBuilder<TypeConfig> for FastetcdSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let payload = build_payload(&self.sm).await.map_err(|e| {
            StorageIOError::read_state_machine(AnyError::error(format!(
                "build snapshot payload: {e}"
            )))
        })?;

        let meta = {
            let mut g = self.sm.inner.lock().await;
            g.snapshot_idx += 1;
            SnapshotMeta {
                last_log_id: payload.last_applied_log_id,
                last_membership: payload.last_membership.clone(),
                snapshot_id: format!("snap-{}", g.snapshot_idx),
            }
        };
        // Serialized straight to disk (body + meta); only the meta stays
        // in RAM so an idle node doesn't hold the whole database (#13),
        // and the encoded snapshot never exists as a `Vec` (#30).
        //
        // A failure to write must not become a `StorageError`. openraft
        // treats a storage error as fatal to the whole node: it surfaces
        // on the linearizable read barrier and on every proposal, so a
        // full volume took the store down in both directions and even
        // deleting keys to make room was refused (fastetcd#14). The
        // state machine's data and `last_applied` are already committed,
        // so `write_snapshot` falls back to holding this snapshot in
        // memory, and no current snapshot is recorded on disk.
        let written = self.sm.write_snapshot(&meta, payload).await?;
        self.sm.inner.lock().await.current_snapshot =
            written.persisted.then(|| meta.clone());
        Ok(Snapshot {
            meta,
            snapshot: Box::new(written.body),
        })
    }
}

// ---------- helpers ----------

async fn apply_data(
    mvcc: &MvccStore,
    data: &FastetcdLogEntry,
) -> Result<FastetcdLogResponse, anyhow::Error> {
    match data {
        FastetcdLogEntry::Apply { mutations } => {
            let (revision, results) = mvcc.apply(mutations).await?;
            Ok(FastetcdLogResponse::Apply { revision, results })
        }
        FastetcdLogEntry::Txn {
            compares,
            success,
            failure,
        } => {
            let result = mvcc.txn(compares, success, failure).await?;
            Ok(FastetcdLogResponse::Txn(result))
        }
        FastetcdLogEntry::Compact { rev } => {
            let compact_rev = mvcc.compact(*rev).await?;
            Ok(FastetcdLogResponse::Compact { compact_rev })
        }
        FastetcdLogEntry::LeaseGrant {
            id,
            ttl_secs,
            now_unix,
        } => {
            let res = mvcc.apply_lease_grant(*id, *ttl_secs, *now_unix).await?;
            Ok(FastetcdLogResponse::LeaseGrant(res))
        }
        FastetcdLogEntry::LeaseRevoke { id } => {
            let res = mvcc.apply_lease_revoke(*id).await?;
            Ok(FastetcdLogResponse::LeaseRevoke(res))
        }
        FastetcdLogEntry::LeaseKeepAlive { id, now_unix } => {
            let res = mvcc.apply_lease_keepalive(*id, *now_unix).await?;
            Ok(FastetcdLogResponse::LeaseKeepAlive(res))
        }
        FastetcdLogEntry::Noop => {
            let rev = mvcc.current_revision().await;
            Ok(FastetcdLogResponse::Noop { revision: rev })
        }
    }
}

async fn build_payload(sm: &FastetcdStateMachine) -> Result<SnapshotPayload, anyhow::Error> {
    use std::ops::Bound;
    // Capture the consistent MVCC snapshot handle AND last_applied atomically
    // under the state-machine lock. `apply()` mutates the MVCC store and
    // last_applied together under this lock; if we took the snapshot outside it,
    // an apply could interleave and the snapshot would carry data from an
    // earlier revision than its last_applied_log_id. A learner installing that
    // mismatch is marked caught-up at a log id ahead of its data and never
    // receives the gap, so it stays stuck at the old revision (fastetcd#8).
    // The frozen snapshot handle is then scanned WITHOUT the lock so builds
    // don't block writes.
    let (snap, last_applied_log_id, last_membership) = {
        let g = sm.inner.lock().await;
        let snap = sm.mvcc.engine().snapshot().await?;
        (snap, g.last_applied_log_id, g.last_membership.clone())
    };

    let kv_table = snap
        .range("mvcc_kv", Bound::Unbounded, Bound::Unbounded, 0)
        .await?;
    let idx_table = snap
        .range("mvcc_idx", Bound::Unbounded, Bound::Unbounded, 0)
        .await?;
    let meta_table = snap
        .range("mvcc_meta", Bound::Unbounded, Bound::Unbounded, 0)
        .await?;

    Ok(SnapshotPayload {
        last_applied_log_id,
        last_membership,
        kv_table,
        idx_table,
        meta_table,
    })
}

async fn rebuild_mvcc(
    mvcc: &MvccStore,
    payload: &SnapshotPayload,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use fastetcd_storage::mvcc::store::{META_KEY_RAFT_APPLIED, META_KEY_RAFT_MEMBERSHIP};
    use fastetcd_storage::{WriteBatch, WriteOptions};

    let engine = mvcc.engine().clone();
    // Delete-everything is implemented as: delete_range over each
    // table's full key space.
    let mut batch = WriteBatch::new();
    batch.delete_range("mvcc_kv", b"", &[0xFFu8; 64]);
    batch.delete_range("mvcc_idx", b"", &[0xFFu8; 64]);
    batch.delete_range("mvcc_meta", b"", &[0xFFu8; 64]);
    for (k, v) in &payload.kv_table {
        batch.put("mvcc_kv", k, v);
    }
    for (k, v) in &payload.idx_table {
        batch.put("mvcc_idx", k, v);
    }
    for (k, v) in &payload.meta_table {
        batch.put("mvcc_meta", k, v);
    }
    // The installed data and the log id it corresponds to must land in
    // one batch. Otherwise a crash mid-install leaves a follower whose
    // MVCC state is the leader's but whose last_applied is its own old
    // one — it then replays already-applied entries over the snapshot.
    batch.put(
        "mvcc_meta",
        META_KEY_RAFT_APPLIED,
        &bincode::serialize(&payload.last_applied_log_id)?,
    );
    batch.put(
        "mvcc_meta",
        META_KEY_RAFT_MEMBERSHIP,
        &bincode::serialize(&payload.last_membership)?,
    );
    engine.commit(batch, WriteOptions::default()).await?;

    // The batch above went straight to the engine, so the MvccStore
    // handle is still serving the counters it cached at open. Pick up
    // the snapshot's revision before anyone reads or writes through it.
    mvcc.reload_write_state().await?;
    Ok(())
}
