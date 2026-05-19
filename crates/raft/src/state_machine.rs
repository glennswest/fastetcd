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
}

struct Inner {
    last_applied_log_id: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
    /// The most recently built snapshot, if any. openraft asks for
    /// this via `get_current_snapshot`.
    current_snapshot: Option<StoredSnapshot>,
    snapshot_idx: u64,
}

#[derive(Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, openraft::BasicNode>,
    data: Vec<u8>,
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
    pub fn new(mvcc: MvccStore) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                last_applied_log_id: None,
                last_membership: StoredMembership::default(),
                current_snapshot: None,
                snapshot_idx: 0,
            })),
            mvcc,
        }
    }

    pub fn mvcc(&self) -> &MvccStore {
        &self.mvcc
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

            // Membership-change entries are recorded but produce no
            // application-level mutation.
            if let openraft::EntryPayload::Membership(m) = &entry.payload {
                g.last_membership = StoredMembership::new(Some(log_id), m.clone());
                // Match openraft's contract — every entry produces one response.
                let rev = self.mvcc.current_revision().await;
                responses.push(FastetcdLogResponse::Noop { revision: rev });
                g.last_applied_log_id = Some(log_id);
                continue;
            }

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

        let mut g = self.inner.lock().await;
        g.last_applied_log_id = payload.last_applied_log_id;
        g.last_membership = payload.last_membership;
        g.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let g = self.inner.lock().await;
        match &g.current_snapshot {
            Some(s) => Ok(Some(Snapshot {
                meta: s.meta.clone(),
                snapshot: Box::new(Cursor::new(s.data.clone())),
            })),
            None => Ok(None),
        }
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

        let mut g = self.sm.inner.lock().await;
        g.snapshot_idx += 1;
        let meta = SnapshotMeta {
            last_log_id: payload.last_applied_log_id,
            last_membership: payload.last_membership.clone(),
            snapshot_id: format!("snap-{}", g.snapshot_idx),
        };
        g.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });
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
        FastetcdLogEntry::Noop => {
            let rev = mvcc.current_revision().await;
            Ok(FastetcdLogResponse::Noop { revision: rev })
        }
    }
}

async fn build_payload(sm: &FastetcdStateMachine) -> Result<SnapshotPayload, anyhow::Error> {
    use std::ops::Bound;
    let engine = sm.mvcc.engine().clone();
    let snap = engine.snapshot().await?;

    let kv_table = snap
        .range("mvcc_kv", Bound::Unbounded, Bound::Unbounded, 0)
        .await?;
    let idx_table = snap
        .range("mvcc_idx", Bound::Unbounded, Bound::Unbounded, 0)
        .await?;
    let meta_table = snap
        .range("mvcc_meta", Bound::Unbounded, Bound::Unbounded, 0)
        .await?;

    let g = sm.inner.lock().await;
    Ok(SnapshotPayload {
        last_applied_log_id: g.last_applied_log_id,
        last_membership: g.last_membership.clone(),
        kv_table,
        idx_table,
        meta_table,
    })
}

async fn rebuild_mvcc(
    mvcc: &MvccStore,
    payload: &SnapshotPayload,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
    engine.commit(batch, WriteOptions::default()).await?;
    Ok(())
}
