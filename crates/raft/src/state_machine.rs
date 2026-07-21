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
//! Snapshot strategy: serialize the entire MVCC state plus
//! `last_applied_log_id` + `last_membership` into a single
//! `bincode`-encoded byte vector wrapped in `Cursor<Vec<u8>>` (the
//! `TypeConfig::SnapshotData` type). This is simple and correct for
//! datasets up to several hundred MB; production-scale clusters will
//! want a streaming snapshot replaced in a follow-up.

use std::io::Cursor;
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

use crate::types::{FastetcdLogEntry, FastetcdLogResponse, NodeId, TypeConfig};

/// Concrete state machine type, clonable, owned by the openraft
/// internals.
#[derive(Clone)]
pub struct FastetcdStateMachine {
    inner: Arc<Mutex<Inner>>,
    mvcc: MvccStore,
    /// Directory holding the persisted current snapshot. The snapshot
    /// body lives on disk (`current.snap`), never in RAM — the state
    /// machine used to hold the whole serialized database as a `Vec` in
    /// `current_snapshot`, which is why an idle node sat at gigabytes
    /// (fastetcd#13). Only the small [`SnapshotMeta`] is kept in memory.
    snapshot_dir: std::path::PathBuf,
}

struct Inner {
    last_applied_log_id: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
    /// Metadata of the current on-disk snapshot (`current.snap`), if
    /// any. The body is read from disk on demand, not held here.
    current_snapshot: Option<SnapshotMeta<NodeId, openraft::BasicNode>>,
    snapshot_idx: u64,
}

const SNAP_FILE: &str = "current.snap";
const SNAP_META_FILE: &str = "current.meta";

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
        let snapshot_dir = snapshot_dir.into();
        std::fs::create_dir_all(&snapshot_dir)?;
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

        // Restore the current snapshot's metadata from disk if both the
        // body and its sidecar meta are present, so openraft can purge
        // the log against it immediately after a restart (without this,
        // a restart lost the snapshot and purge stalled — fastetcd#13).
        let meta_path = snapshot_dir.join(SNAP_META_FILE);
        let snap_path = snapshot_dir.join(SNAP_FILE);
        let current_snapshot = if meta_path.exists() && snap_path.exists() {
            match std::fs::read(&meta_path) {
                Ok(bytes) => bincode::deserialize(&bytes).ok(),
                Err(_) => None,
            }
        } else {
            None
        };

        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                last_applied_log_id,
                last_membership,
                current_snapshot,
                snapshot_idx: 0,
            })),
            mvcc,
            snapshot_dir,
        })
    }

    fn snap_path(&self) -> std::path::PathBuf {
        self.snapshot_dir.join(SNAP_FILE)
    }

    /// Atomically persist the snapshot body + its metadata. The body is
    /// written and renamed first, then the meta, so the meta never
    /// points at a partial body.
    async fn persist_snapshot(
        &self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        data: &[u8],
    ) -> std::io::Result<()> {
        let dir = self.snapshot_dir.clone();
        let final_snap = dir.join(SNAP_FILE);
        let final_meta = dir.join(SNAP_META_FILE);
        let tmp_snap = dir.join(format!("{SNAP_FILE}.tmp"));
        let tmp_meta = dir.join(format!("{SNAP_META_FILE}.tmp"));
        let meta_bytes =
            bincode::serialize(meta).map_err(std::io::Error::other)?;
        let data = data.to_vec();
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            std::fs::write(&tmp_snap, &data)?;
            std::fs::rename(&tmp_snap, &final_snap)?;
            std::fs::write(&tmp_meta, &meta_bytes)?;
            std::fs::rename(&tmp_meta, &final_meta)?;
            Ok(())
        })
        .await
        .map_err(std::io::Error::other)?
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

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let payload: SnapshotPayload = bincode::deserialize(&data).map_err(|e| {
            StorageIOError::read_snapshot(Some(meta.signature()), AnyError::new(&e))
        })?;

        // Replace the MVCC engine contents.
        rebuild_mvcc(&self.mvcc, &payload).await.map_err(|e| {
            StorageIOError::write_snapshot(
                Some(meta.signature()),
                AnyError::error(format!("rebuild mvcc from snapshot: {e}")),
            )
        })?;

        // Persist the installed snapshot to disk (body + meta) so it
        // survives restart; keep only the meta in RAM.
        self.persist_snapshot(meta, &data).await.map_err(|e| {
            StorageIOError::write_snapshot(
                Some(meta.signature()),
                AnyError::error(format!("persist installed snapshot: {e}")),
            )
        })?;

        let mut g = self.inner.lock().await;
        g.last_applied_log_id = payload.last_applied_log_id;
        g.last_membership = payload.last_membership;
        g.current_snapshot = Some(meta.clone());
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let meta = {
            let g = self.inner.lock().await;
            match &g.current_snapshot {
                Some(m) => m.clone(),
                None => return Ok(None),
            }
        };
        // Read the body from disk on demand — never held in RAM.
        let path = self.snap_path();
        let data = tokio::task::spawn_blocking(move || std::fs::read(&path))
            .await
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), AnyError::new(&e)))?
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), AnyError::new(&e)))?;
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        }))
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
        let data = bincode::serialize(&payload).map_err(|e| {
            StorageIOError::read_state_machine(AnyError::new(&e))
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
        // Persist to disk (body + meta); keep only the meta in RAM so an
        // idle node doesn't hold the whole serialized database (#13).
        self.sm.persist_snapshot(&meta, &data).await.map_err(|e| {
            StorageIOError::write_snapshot(Some(meta.signature()), AnyError::new(&e))
        })?;
        self.sm.inner.lock().await.current_snapshot = Some(meta.clone());
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
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
