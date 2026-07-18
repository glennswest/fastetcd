//! `MvccStore` — etcd-shaped MVCC layer over a [`KvStore`].
//!
//! Tables used in the underlying engine:
//!
//! | Table        | Key                              | Value                  |
//! |--------------|----------------------------------|------------------------|
//! | `mvcc_kv`    | `len(user_key) || user_key || rev` | `bincode(KvRecord)`  |
//! | `mvcc_idx`   | `user_key`                       | `bincode(KeyIndex)`    |
//! | `mvcc_meta`  | one of: `b"current_rev"`, `b"compact_rev"` | `i64 BE` |
//!
//! ## Invariants
//!
//! - Every mutation flows through `apply_*` methods on a single
//!   caller-owned task (e.g. the Raft apply loop). Concurrent writes
//!   are not supported at this layer.
//! - Reads are served from a [`KvStore::snapshot`], which sees a
//!   consistent point-in-time view including the in-memory state of
//!   pending writes only after `commit` returns.
//! - The `current_revision` advances by exactly `1` per `apply_*`
//!   call that produces at least one mutation. Multi-op transactions
//!   share a `main` revision and use distinct `sub` values.
//!
//! ## Lease handling (preview)
//!
//! `apply_put` and `apply_delete_range` record the `lease` field on
//! every written `KvRecord` and update lease attachment, but the
//! lease ticker / cascade-delete-on-expiry loop is **not** in this
//! file — it lives behind the `Lease` gRPC service (task #7) and
//! drives this `MvccStore` via the same `apply_*` entrypoints.

use std::ops::Bound;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{broadcast, Mutex};

use crate::kvstore::{KvStore, Snapshot, StorageError, WriteBatch, WriteOptions};

use super::event::{EventBatch, EventKind, MvccEvent};
use super::lease::{
    lease_id_key, lease_key_index, lease_keys_bounds, parse_lease_keys_key, LeaseId, LeaseRecord,
    TABLE_LEASE, TABLE_LEASE_KEYS,
};
use super::record::{KeyIndex, KvRecord};
use super::revision::{make_kv_key, Revision};

const TABLE_KV: &str = "mvcc_kv";
const TABLE_IDX: &str = "mvcc_idx";
const TABLE_META: &str = "mvcc_meta";

const META_KEY_CURRENT_REV: &[u8] = b"current_rev";
const META_KEY_COMPACT_REV: &[u8] = b"compact_rev";
const META_KEY_NEXT_LEASE_ID: &[u8] = b"next_lease_id";
/// Raft's `last_applied_log_id` / `last_membership`, stored as opaque
/// bytes so this crate stays free of an openraft dependency. The raft
/// crate encodes and decodes them; see [`MvccStore::stage_raft_meta`].
pub const META_KEY_RAFT_APPLIED: &[u8] = b"raft_applied";
pub const META_KEY_RAFT_MEMBERSHIP: &[u8] = b"raft_membership";

/// Errors specific to the MVCC layer (atop [`StorageError`]).
#[derive(Debug, Error)]
pub enum MvccError {
    #[error(transparent)]
    Storage(#[from] StorageError),

    /// The caller asked to read at a revision that has been compacted
    /// away. Matches etcd's `mvcc: required revision has been compacted`.
    #[error("required revision {requested} has been compacted (compact_rev = {compact_rev})")]
    Compacted { requested: i64, compact_rev: i64 },

    /// The caller asked to read at a revision that has not yet been
    /// committed. Matches etcd's `mvcc: future rev`.
    #[error("required revision {requested} is in the future (current_rev = {current_rev})")]
    FutureRevision { requested: i64, current_rev: i64 },

    #[error("mvcc internal: {0}")]
    Internal(String),
}

pub type MvccResult<T> = Result<T, MvccError>;

/// A single mutation request: put one key, or delete a range.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Mutation {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        lease: i64,
        /// If true, ignore `value` and keep the previously-stored value
        /// (matching etcd `Put.ignore_value`). Errors if the key doesn't
        /// exist.
        ignore_value: bool,
        /// If true, ignore `lease` and keep the previously-attached
        /// lease (matching etcd `Put.ignore_lease`).
        ignore_lease: bool,
        /// Return the previous KeyValue in the response.
        prev_kv: bool,
    },
    DeleteRange {
        key: Vec<u8>,
        range_end: Vec<u8>,
        prev_kv: bool,
    },
}

/// Outcome of an applied mutation, returned to the caller for shaping
/// the wire response.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MutationResult {
    /// Number of keys actually written or deleted.
    pub n: i64,
    /// Records that existed before the mutation, populated only when
    /// `prev_kv` was requested.
    pub prev_kvs: Vec<KvRecord>,
}

/// Outcome of a `range` query.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RangeResult {
    pub kvs: Vec<KvRecord>,
    /// True if more keys would have been returned but were excluded
    /// by `limit`. Maps to etcd `RangeResponse.more`.
    pub more: bool,
    /// Count of matching keys *before* the limit was applied.
    pub count: i64,
}

/// Operators for a [`Compare`] in a `Txn`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompareOp {
    Equal,
    NotEqual,
    Greater,
    Less,
}

/// Right-hand side of a [`Compare`]. Variant chooses which field of
/// the key's metadata is being compared.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompareTarget {
    Version(i64),
    CreateRevision(i64),
    ModRevision(i64),
    Value(Vec<u8>),
    Lease(i64),
}

/// One predicate within a `Txn.compare` list.
///
/// - `range_end == []` compares the single key `key`.
/// - `range_end == [0x00]` compares `[key, +Inf)`.
/// - Otherwise compares `[key, range_end)`.
///
/// When `range_end` is non-empty, the predicate must hold for **every**
/// key in the range. A key that is absent has version `0`,
/// create_revision `0`, mod_revision `0`, value `[]`, lease `0` —
/// matching etcd's semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Compare {
    pub key: Vec<u8>,
    pub range_end: Vec<u8>,
    pub op: CompareOp,
    pub target: CompareTarget,
}

/// A read operation within a `Txn` `success`/`failure` list.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RangeOp {
    pub key: Vec<u8>,
    pub range_end: Vec<u8>,
    pub limit: usize,
    pub revision: i64,
    pub keys_only: bool,
    pub count_only: bool,
}

/// A single op within a `Txn` `success`/`failure` list. Nested `Txn`
/// (etcd permits `Txn` inside `Txn`) is intentionally not represented
/// at this layer — the gRPC service flattens nested Txns into this
/// shape, or returns an error for unsupported nesting depth.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TxnOp {
    Range(RangeOp),
    Mutation(Mutation),
}

/// Per-op result within a [`TxnResult`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TxnOpResult {
    Range(RangeResult),
    Mutation(MutationResult),
}

/// Input for `bulk_load_records`. Each `BulkKey` carries one user
/// key with its (revision-sorted) put records, optionally followed
/// by a tombstone that closes the most recent generation. Multiple
/// generations per user key are not yet supported in this Phase-1
/// migration path — historical reads after the import will return
/// `None` for keys whose ancient generations were tombstoned (which
/// is also what etcd does after compaction).
#[derive(Debug, Clone)]
pub struct BulkKey {
    pub key: Vec<u8>,
    pub puts: Vec<KvRecord>,
    pub tombstone: Option<KvRecord>,
}

/// Outcome of a lease grant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseGrantResult {
    pub id: LeaseId,
    pub ttl_secs: i64,
    pub revision: i64,
}

/// Outcome of a lease revoke (cascade delete of attached keys).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LeaseRevokeResult {
    pub revision: i64,
    /// Keys that were deleted as part of the cascade.
    pub deleted_keys: i64,
}

/// Outcome of a `LeaseTimeToLive` query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseTtlResult {
    pub id: LeaseId,
    pub granted_ttl_secs: i64,
    /// Remaining TTL in seconds (clamped at 0; negative means expired
    /// but not yet revoked).
    pub remaining_ttl_secs: i64,
    /// Attached keys; populated only when the caller asked for them.
    pub keys: Vec<Vec<u8>>,
}

/// Outcome of a `Txn` call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxnResult {
    /// True if every `Compare` evaluated truthfully and the `success`
    /// branch was taken; false if the `failure` branch ran.
    pub succeeded: bool,
    /// Revision returned in the response header. If the chosen branch
    /// produced no mutations, this is the current revision (txn did
    /// not advance it). If it produced mutations, this is the new
    /// `main` revision they share.
    pub revision: i64,
    /// One entry per op in the chosen branch, in the same order.
    pub op_results: Vec<TxnOpResult>,
}

/// The MVCC layer. Thin handle; cheaply clonable.
#[derive(Clone)]
pub struct MvccStore {
    inner: Arc<Inner>,
}

struct Inner {
    engine: Arc<dyn KvStore>,
    /// Write-side state: current revision and compact revision.
    /// Protected by a mutex because `apply_*` is single-threaded by
    /// contract but exposing a clonable handle still requires interior
    /// mutability. Read methods do not contend on this lock.
    write_state: Mutex<WriteState>,
    /// Broadcast channel for events emitted by successful commits.
    /// Watch subscribers receive every committed batch in order. Slow
    /// subscribers may receive `Lagged`; they should treat that as a
    /// "force re-establish from a fresh start_revision" signal.
    event_tx: broadcast::Sender<EventBatch>,
    /// Raft metadata staged by the state machine before it invokes an
    /// `apply_*` call, folded into that call's `WriteBatch` so the
    /// mutation and the `last_applied_log_id` that describes it commit
    /// in a single atomic fsync. Anything still staged after the call
    /// (membership and blank entries mutate no MVCC state) is committed
    /// on its own by `flush_raft_meta`.
    pending_raft_meta: Mutex<Vec<(Vec<u8>, Vec<u8>)>>,
}

#[derive(Debug, Clone, Copy)]
struct WriteState {
    current_rev: i64,
    compact_rev: i64,
    /// High-water mark for auto-allocated lease IDs.
    next_lease_id: i64,
}

impl MvccStore {
    /// Open or initialize an MVCC layer over `engine`. On first open,
    /// initializes meta entries to `0`. On a populated engine, reads
    /// the stored counters.
    pub async fn open(engine: Arc<dyn KvStore>) -> MvccResult<Self> {
        let snap = engine.snapshot().await?;
        let current = read_i64(&*snap, META_KEY_CURRENT_REV).await?.unwrap_or(0);
        let compact = read_i64(&*snap, META_KEY_COMPACT_REV).await?.unwrap_or(0);
        let next_lease = read_i64(&*snap, META_KEY_NEXT_LEASE_ID).await?.unwrap_or(0);
        drop(snap);

        // If meta keys were absent, persist their initial values so
        // size_on_disk and subsequent opens see consistent state.
        if current == 0 && compact == 0 && next_lease == 0 {
            let mut b = WriteBatch::new();
            write_i64(&mut b, META_KEY_CURRENT_REV, 0);
            write_i64(&mut b, META_KEY_COMPACT_REV, 0);
            write_i64(&mut b, META_KEY_NEXT_LEASE_ID, 0);
            engine.commit(b, WriteOptions::default()).await?;
        }

        let (event_tx, _) = broadcast::channel(1024);
        Ok(Self {
            inner: Arc::new(Inner {
                engine,
                write_state: Mutex::new(WriteState {
                    current_rev: current,
                    compact_rev: compact,
                    next_lease_id: next_lease,
                }),
                event_tx,
                pending_raft_meta: Mutex::new(Vec::new()),
            }),
        })
    }

    /// Stage raft metadata to be written by the next `apply_*` commit.
    ///
    /// The state machine calls this immediately before dispatching an
    /// entry. Persisting `last_applied_log_id` in the *same* batch as
    /// the mutation it describes is what makes restart safe: a crash
    /// can leave both written or neither, never the mutation alone
    /// (which would replay it and double-apply) and never the log id
    /// alone (which would silently drop the mutation) — see fastetcd#9.
    ///
    /// `membership` is `None` for entries that don't change membership.
    pub async fn stage_raft_meta(&self, applied: Vec<u8>, membership: Option<Vec<u8>>) {
        let mut pending = self.inner.pending_raft_meta.lock().await;
        pending.retain(|(k, _)| {
            k.as_slice() != META_KEY_RAFT_APPLIED
                && (membership.is_none() || k.as_slice() != META_KEY_RAFT_MEMBERSHIP)
        });
        pending.push((META_KEY_RAFT_APPLIED.to_vec(), applied));
        if let Some(m) = membership {
            pending.push((META_KEY_RAFT_MEMBERSHIP.to_vec(), m));
        }
    }

    /// Commit any staged raft metadata that no `apply_*` call picked up.
    /// Membership and blank log entries touch no MVCC state, so without
    /// this their log ids would never reach disk and a restart would
    /// re-apply them.
    pub async fn flush_raft_meta(&self) -> MvccResult<()> {
        let mut batch = WriteBatch::new();
        if !self.fold_raft_meta(&mut batch).await {
            return Ok(());
        }
        self.inner
            .engine
            .commit(batch, WriteOptions::default())
            .await?;
        Ok(())
    }

    /// Read back the persisted raft metadata. Returns
    /// `(last_applied, last_membership)` as the opaque bytes that were
    /// staged. Called once at open to restore the state machine.
    pub async fn read_raft_meta(&self) -> MvccResult<(Option<Vec<u8>>, Option<Vec<u8>>)> {
        let snap = self.inner.engine.snapshot().await?;
        let applied = snap.get(TABLE_META, META_KEY_RAFT_APPLIED).await?;
        let membership = snap.get(TABLE_META, META_KEY_RAFT_MEMBERSHIP).await?;
        Ok((applied, membership))
    }

    /// Drain staged raft metadata into `batch`. Returns true if
    /// anything was added. Callers must hold no other MVCC lock that
    /// could be taken while `pending_raft_meta` is held.
    async fn fold_raft_meta(&self, batch: &mut WriteBatch) -> bool {
        let mut pending = self.inner.pending_raft_meta.lock().await;
        if pending.is_empty() {
            return false;
        }
        for (k, v) in pending.drain(..) {
            batch.put(TABLE_META, &k, &v);
        }
        true
    }

    /// Subscribe to the post-commit event stream. Each `EventBatch`
    /// corresponds to one successful `apply` / `txn` call that
    /// produced mutations; reads and no-op applies do not produce
    /// batches.
    pub fn subscribe(&self) -> broadcast::Receiver<EventBatch> {
        self.inner.event_tx.subscribe()
    }

    pub fn engine(&self) -> &Arc<dyn KvStore> {
        &self.inner.engine
    }

    pub async fn current_revision(&self) -> i64 {
        self.inner.write_state.lock().await.current_rev
    }

    pub async fn compact_revision(&self) -> i64 {
        self.inner.write_state.lock().await.compact_rev
    }

    /// Bulk-load records into a freshly-opened store, preserving the
    /// source revisions. Designed for migration tools that need
    /// post-migration `Range(rev)` and `Watch(start_rev)` to behave
    /// exactly as they did upstream.
    ///
    /// Caller invariants:
    /// - The store is empty (no prior `apply`/`bulk_load` since open).
    ///   This is enforced by checking `current_rev == 0`.
    /// - For each user key, `puts` are sorted strictly increasing by
    ///   revision. A `tombstone` (if present) is after every put it
    ///   closes.
    ///
    /// `next_rev` is the value the store's revision counter will be
    /// set to after the load (typically `max_observed_rev`); subsequent
    /// `apply` calls allocate from `next_rev + 1`.
    pub async fn bulk_load_records(
        &self,
        per_key: Vec<BulkKey>,
        next_rev: i64,
    ) -> MvccResult<()> {
        let mut state = self.inner.write_state.lock().await;
        if state.current_rev != 0 {
            return Err(MvccError::Internal(format!(
                "bulk_load_records requires an empty store (current_rev = {})",
                state.current_rev
            )));
        }

        let mut batch = WriteBatch::new();
        for entry in per_key {
            let BulkKey {
                key,
                puts,
                tombstone,
            } = entry;
            if puts.is_empty() && tombstone.is_none() {
                continue;
            }
            // Build KeyIndex from the supplied (sorted) records.
            let mut idx = KeyIndex::new(key.clone());
            let mut current_gen_revs: Vec<Revision> = Vec::new();
            let mut current_gen_created: Option<Revision> = None;
            for rec in &puts {
                let rev = Revision::new(rec.mod_revision, 0);
                if current_gen_created.is_none() {
                    current_gen_created = Some(rev);
                }
                current_gen_revs.push(rev);
                let kv_key = make_kv_key(&key, rev);
                let bytes = bincode::serialize(rec).map_err(|e| {
                    MvccError::Internal(format!("serialize KvRecord: {e}"))
                })?;
                batch.put(TABLE_KV, &kv_key, &bytes);
                if rec.lease != 0 {
                    batch.put(
                        TABLE_LEASE_KEYS,
                        &lease_key_index(rec.lease, &key),
                        &[],
                    );
                }
            }
            // Close the current generation if a tombstone is supplied.
            if let Some(tomb) = &tombstone {
                let rev = Revision::new(tomb.mod_revision, 0);
                idx.generations.push(super::record::Generation {
                    created: current_gen_created.unwrap_or(rev),
                    revs: std::mem::take(&mut current_gen_revs),
                    tombstone: Some(rev),
                });
                current_gen_created = None;
                let kv_key = make_kv_key(&key, rev);
                let bytes = bincode::serialize(tomb).map_err(|e| {
                    MvccError::Internal(format!("serialize tombstone: {e}"))
                })?;
                batch.put(TABLE_KV, &kv_key, &bytes);
            } else if !current_gen_revs.is_empty() {
                idx.generations.push(super::record::Generation {
                    created: current_gen_created.unwrap(),
                    revs: current_gen_revs,
                    tombstone: None,
                });
            }
            let idx_bytes = bincode::serialize(&idx)
                .map_err(|e| MvccError::Internal(format!("serialize KeyIndex: {e}")))?;
            batch.put(TABLE_IDX, &key, &idx_bytes);
        }
        write_i64(&mut batch, META_KEY_CURRENT_REV, next_rev);
        self.inner
            .engine
            .commit(batch, WriteOptions::default())
            .await?;
        state.current_rev = next_rev;
        Ok(())
    }

    /// Enumerate events in `[key, range_end)` whose revision is
    /// strictly greater than `start_rev` and at most the current
    /// revision. Returns events in `(revision, key)` order. Used by
    /// the Watch service to backfill from `start_revision`.
    ///
    /// Errors with [`MvccError::Compacted`] if `start_rev < compact_rev`.
    pub async fn range_events(
        &self,
        key: &[u8],
        range_end: &[u8],
        start_rev: i64,
    ) -> MvccResult<Vec<MvccEvent>> {
        let state = self.inner.write_state.lock().await;
        let current = state.current_rev;
        let compact = state.compact_rev;
        drop(state);

        if start_rev < compact && compact > 0 {
            return Err(MvccError::Compacted {
                requested: start_rev,
                compact_rev: compact,
            });
        }
        if start_rev > current {
            // Nothing to backfill; subscribe-only is correct.
            return Ok(Vec::new());
        }

        let snap = self.inner.engine.snapshot().await?;
        let (start, end) = range_bounds(key, range_end);
        let idx_entries = snap.range(TABLE_IDX, start, end, 0).await?;

        // Pre-collect (rev, user_key) pairs that fall in the window
        // so we can sort by revision later.
        let mut pairs: Vec<(Revision, Vec<u8>)> = Vec::new();
        for (user_key, idx_bytes) in &idx_entries {
            let idx: KeyIndex = bincode::deserialize(idx_bytes)
                .map_err(|e| MvccError::Internal(format!("deserialize KeyIndex: {e}")))?;
            for g in &idx.generations {
                for r in &g.revs {
                    if r.main > start_rev && r.main <= current {
                        pairs.push((*r, user_key.clone()));
                    }
                }
                if let Some(t) = g.tombstone {
                    if t.main > start_rev && t.main <= current {
                        pairs.push((t, user_key.clone()));
                    }
                }
            }
        }
        pairs.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        // For each pair, fetch the record and the previous live
        // record (if any) for prev_kv.
        let mut events = Vec::with_capacity(pairs.len());
        for (rev, user_key) in pairs {
            let kv_key = make_kv_key(&user_key, rev);
            let bytes = snap
                .get(TABLE_KV, &kv_key)
                .await
                .map_err(MvccError::Storage)?
                .ok_or_else(|| {
                    MvccError::Internal(format!(
                        "missing KvRecord for backfill key {:?} at rev {:?}",
                        user_key, rev
                    ))
                })?;
            let rec: KvRecord = bincode::deserialize(&bytes)
                .map_err(|e| MvccError::Internal(format!("deserialize KvRecord: {e}")))?;

            // prev_kv: most recent rec for this user_key strictly
            // before `rev` that isn't a tombstone. Re-load the
            // KeyIndex for this key (could cache; fine for now).
            let prev = previous_live_record(&*snap, &user_key, rev).await?;
            let kind = if rec.is_tombstone() {
                EventKind::Delete
            } else {
                EventKind::Put
            };
            events.push(MvccEvent {
                kind,
                kv: rec,
                prev_kv: prev,
            });
        }
        Ok(events)
    }

    /// Grant a lease with `ttl_secs` and (server-current) `now_unix`
    /// timestamp. `id == 0` allocates a fresh id (next sequential
    /// from a counter persisted in `mvcc_meta`); a non-zero id is
    /// used as-is. Goes through the meta lock so it serializes
    /// with apply()/txn() (matches our "all writes serialize"
    /// invariant).
    pub async fn apply_lease_grant(
        &self,
        id: LeaseId,
        ttl_secs: i64,
        now_unix: i64,
    ) -> MvccResult<LeaseGrantResult> {
        if ttl_secs <= 0 {
            return Err(MvccError::Internal(format!(
                "lease TTL must be positive, got {ttl_secs}"
            )));
        }
        let mut state = self.inner.write_state.lock().await;
        let mut batch = WriteBatch::new();
        let final_id = if id == 0 {
            let next = state.next_lease_id + 1;
            state.next_lease_id = next;
            write_i64(&mut batch, META_KEY_NEXT_LEASE_ID, next);
            next
        } else {
            // Track high-water-mark so auto-allocated ids don't collide.
            if id > state.next_lease_id {
                state.next_lease_id = id;
                write_i64(&mut batch, META_KEY_NEXT_LEASE_ID, id);
            }
            id
        };
        let rec = LeaseRecord {
            id: final_id,
            ttl_secs,
            deadline_unix_secs: now_unix.saturating_add(ttl_secs),
        };
        let bytes = bincode::serialize(&rec)
            .map_err(|e| MvccError::Internal(format!("serialize LeaseRecord: {e}")))?;
        batch.put(TABLE_LEASE, &lease_id_key(final_id), &bytes);
        self.fold_raft_meta(&mut batch).await;
        self.inner
            .engine
            .commit(batch, WriteOptions::default())
            .await?;
        Ok(LeaseGrantResult {
            id: final_id,
            ttl_secs,
            revision: state.current_rev,
        })
    }

    /// Refresh a lease's deadline. Returns the granted TTL (so the
    /// caller can pass it back to the client) or
    /// `MvccError::Internal` if the lease does not exist.
    pub async fn apply_lease_keepalive(
        &self,
        id: LeaseId,
        now_unix: i64,
    ) -> MvccResult<LeaseTtlResult> {
        // Hold the write lock so KeepAlive is serialized with other
        // lease mutations and the apply loop (matches our "all writes
        // serialize" invariant).
        let _state = self.inner.write_state.lock().await;
        let snap = self.inner.engine.snapshot().await?;
        let Some(rec_bytes) = snap.get(TABLE_LEASE, &lease_id_key(id)).await? else {
            return Err(MvccError::Internal(format!("lease {id} not found")));
        };
        let mut rec: LeaseRecord = bincode::deserialize(&rec_bytes)
            .map_err(|e| MvccError::Internal(format!("deserialize LeaseRecord: {e}")))?;
        rec.deadline_unix_secs = now_unix.saturating_add(rec.ttl_secs);
        let bytes = bincode::serialize(&rec)
            .map_err(|e| MvccError::Internal(format!("serialize LeaseRecord: {e}")))?;
        let mut batch = WriteBatch::new();
        batch.put(TABLE_LEASE, &lease_id_key(id), &bytes);
        self.fold_raft_meta(&mut batch).await;
        self.inner
            .engine
            .commit(batch, WriteOptions::default())
            .await?;
        Ok(LeaseTtlResult {
            id,
            granted_ttl_secs: rec.ttl_secs,
            remaining_ttl_secs: rec.ttl_secs,
            keys: Vec::new(),
        })
    }

    /// Look up a lease and (optionally) its attached keys.
    pub async fn lease_ttl(
        &self,
        id: LeaseId,
        include_keys: bool,
        now_unix: i64,
    ) -> MvccResult<Option<LeaseTtlResult>> {
        let snap = self.inner.engine.snapshot().await?;
        let Some(rec_bytes) = snap.get(TABLE_LEASE, &lease_id_key(id)).await? else {
            return Ok(None);
        };
        let rec: LeaseRecord = bincode::deserialize(&rec_bytes)
            .map_err(|e| MvccError::Internal(format!("deserialize LeaseRecord: {e}")))?;
        let remaining = rec.deadline_unix_secs.saturating_sub(now_unix);
        let keys = if include_keys {
            let (start, end) = lease_keys_bounds(id);
            let entries = snap.range(TABLE_LEASE_KEYS, start, end, 0).await?;
            entries
                .into_iter()
                .map(|(k, _)| parse_lease_keys_key(&k).map(|(_, uk)| uk))
                .collect::<MvccResult<Vec<_>>>()?
        } else {
            Vec::new()
        };
        Ok(Some(LeaseTtlResult {
            id,
            granted_ttl_secs: rec.ttl_secs,
            remaining_ttl_secs: remaining,
            keys,
        }))
    }

    /// List all lease IDs.
    pub async fn lease_list(&self) -> MvccResult<Vec<LeaseId>> {
        let snap = self.inner.engine.snapshot().await?;
        let entries = snap
            .range(TABLE_LEASE, Bound::Unbounded, Bound::Unbounded, 0)
            .await?;
        let mut out = Vec::with_capacity(entries.len());
        for (k, _) in entries {
            if k.len() != 8 {
                continue;
            }
            out.push(i64::from_be_bytes(k.try_into().expect("8 bytes")));
        }
        Ok(out)
    }

    /// Revoke a lease and cascade-delete every key associated with it.
    /// Each cascaded delete produces an MVCC event so watchers see
    /// the deletes; all deletes share one `main` revision.
    pub async fn apply_lease_revoke(&self, id: LeaseId) -> MvccResult<LeaseRevokeResult> {
        let mut state = self.inner.write_state.lock().await;
        let snap = self.inner.engine.snapshot().await?;

        // Collect attached keys.
        let (start, end) = lease_keys_bounds(id);
        let entries = snap.range(TABLE_LEASE_KEYS, start, end, 0).await?;
        let attached: Vec<Vec<u8>> = entries
            .iter()
            .map(|(k, _)| parse_lease_keys_key(k).map(|(_, uk)| uk))
            .collect::<MvccResult<Vec<_>>>()?;

        let mut ctx = ApplyContext::default();
        // Use a single DeleteRange-per-key mutation list. Reuse the
        // apply_inner pipeline so events fire normally.
        let mutations: Vec<Mutation> = attached
            .iter()
            .map(|k| Mutation::DeleteRange {
                key: k.clone(),
                range_end: Vec::new(),
                prev_kv: false,
            })
            .collect();

        let mut produced_any = false;
        let mut deleted_keys: i64 = 0;
        let revision = if !mutations.is_empty() {
            let (rev, results, produced) = self
                .apply_inner(&*snap, &mut ctx, &mut state, &mutations)
                .await?;
            for r in &results {
                deleted_keys += r.n;
            }
            produced_any = produced;
            rev
        } else {
            state.current_rev
        };

        // Remove the lease record itself and the lease_keys entries.
        ctx.batch.delete(TABLE_LEASE, &lease_id_key(id));
        for (k, _) in &entries {
            ctx.batch.delete(TABLE_LEASE_KEYS, k);
        }

        if produced_any {
            self.commit_ctx(&mut state, revision, ctx).await?;
        } else {
            // No cascade — just drop the lease record / index entries.
            let mut batch = ctx.batch;
            self.fold_raft_meta(&mut batch).await;
            self.inner
                .engine
                .commit(batch, WriteOptions::default())
                .await?;
        }

        Ok(LeaseRevokeResult {
            revision,
            deleted_keys,
        })
    }

    /// Compact the MVCC history at `rev`. After this call:
    ///
    /// - `Range` with `target_rev < rev` returns
    ///   [`MvccError::Compacted`].
    /// - `Range` at `target_rev >= rev` continues to work; for each
    ///   key the value at the largest `mod_revision <= rev` is
    ///   preserved.
    /// - Closed generations whose tombstone is `<= rev` are removed
    ///   entirely.
    ///
    /// Compaction itself does NOT consume a `main` revision; the
    /// current revision counter is unchanged.
    ///
    /// Errors if `rev` is `<= 0`, `> current_revision`, or
    /// `< compact_revision` (etcd treats Compact at the current
    /// compact rev as a no-op; we error to surface bugs — match
    /// upstream's behavior in a follow-up if tests demand it).
    pub async fn compact(&self, rev: i64) -> MvccResult<i64> {
        if rev <= 0 {
            return Err(MvccError::Internal(format!(
                "compact rev must be > 0, got {rev}"
            )));
        }
        let mut state = self.inner.write_state.lock().await;
        if rev > state.current_rev {
            return Err(MvccError::FutureRevision {
                requested: rev,
                current_rev: state.current_rev,
            });
        }
        if rev < state.compact_rev {
            return Err(MvccError::Internal(format!(
                "compact rev {rev} is below current compact_rev {}",
                state.compact_rev
            )));
        }
        if rev == state.compact_rev {
            return Ok(rev); // idempotent
        }

        let compact_rev_packed = Revision::new(rev, i64::MAX);

        // Walk every KeyIndex in mvcc_idx and apply compact.
        let snap = self.inner.engine.snapshot().await?;
        let entries = snap
            .range(TABLE_IDX, Bound::Unbounded, Bound::Unbounded, 0)
            .await?;

        let mut batch = WriteBatch::new();
        let mut dropped_records: u64 = 0;
        let mut dropped_indices: u64 = 0;
        for (key, idx_bytes) in entries {
            let mut idx: KeyIndex = bincode::deserialize(&idx_bytes)
                .map_err(|e| MvccError::Internal(format!("deserialize KeyIndex: {e}")))?;
            let dropped = idx.compact(compact_rev_packed);
            if dropped.is_empty() && !idx.generations.is_empty() {
                continue; // no change for this key
            }
            for r in dropped {
                let kv_key = make_kv_key(&key, r);
                batch.delete(TABLE_KV, &kv_key);
                dropped_records += 1;
            }
            if idx.generations.is_empty() {
                batch.delete(TABLE_IDX, &key);
                dropped_indices += 1;
            } else {
                let bytes = bincode::serialize(&idx)
                    .map_err(|e| MvccError::Internal(format!("serialize KeyIndex: {e}")))?;
                batch.put(TABLE_IDX, &key, &bytes);
            }
        }
        write_i64(&mut batch, META_KEY_COMPACT_REV, rev);
        self.fold_raft_meta(&mut batch).await;

        self.inner
            .engine
            .commit(batch, WriteOptions::default())
            .await?;
        state.compact_rev = rev;

        tracing::info!(
            target: "fastetcd::mvcc::compact",
            compact_rev = rev,
            dropped_records,
            dropped_indices,
            "compaction complete"
        );

        Ok(rev)
    }

    /// Apply a batch of mutations atomically at one new `main` revision.
    /// Distinct sub-revisions are assigned to each mutation in order;
    /// every put/delete shares the same `main`. Returns one
    /// [`MutationResult`] per input mutation.
    ///
    /// The `header_revision` of every response in the surrounding RPC
    /// should be the returned `main` revision.
    pub async fn apply(&self, mutations: &[Mutation]) -> MvccResult<(i64, Vec<MutationResult>)> {
        if mutations.is_empty() {
            let current = self.current_revision().await;
            return Ok((current, Vec::new()));
        }
        let mut state = self.inner.write_state.lock().await;
        let snap = self.inner.engine.snapshot().await?;
        let mut ctx = ApplyContext::default();
        let (revision, results, produced) = self
            .apply_inner(&*snap, &mut ctx, &mut state, mutations)
            .await?;
        if produced {
            self.commit_ctx(&mut state, revision, ctx).await?;
        }
        Ok((revision, results))
    }

    /// Range query at `target_rev` (or current state if `target_rev == 0`).
    pub async fn range(
        &self,
        key: &[u8],
        range_end: &[u8],
        limit: usize,
        target_rev: i64,
        keys_only: bool,
        count_only: bool,
    ) -> MvccResult<RangeResult> {
        let state = self.inner.write_state.lock().await;
        let state_copy = *state;
        drop(state);
        let snap = self.inner.engine.snapshot().await?;
        self.range_inner(
            &*snap,
            &ApplyContext::default(),
            state_copy,
            key,
            range_end,
            limit,
            target_rev,
            keys_only,
            count_only,
        )
        .await
    }

    /// Transactional execute: evaluate `compares` against the current
    /// snapshot; based on the AND of those results, execute either
    /// `success` or `failure` ops in order. All writes share one
    /// `main` revision (with distinct `sub`); reads within the txn
    /// observe the pre-mutation state.
    pub async fn txn(
        &self,
        compares: &[Compare],
        success: &[TxnOp],
        failure: &[TxnOp],
    ) -> MvccResult<TxnResult> {
        let mut state = self.inner.write_state.lock().await;
        let snap = self.inner.engine.snapshot().await?;

        let succeeded = self.evaluate_compares(&*snap, compares).await?;
        let ops: &[TxnOp] = if succeeded { success } else { failure };

        let mut ctx = ApplyContext::default();
        let mut op_results: Vec<TxnOpResult> = Vec::with_capacity(ops.len());
        let mut mutations: Vec<Mutation> = Vec::new();
        let mut produced_any = false;
        let proposed_main = state.current_rev + 1;
        let state_copy = *state;

        // First pass: separate reads from writes; reads run now against
        // the pre-mutation snapshot, writes are collected for the
        // post-pass apply. Op order is preserved by interleaving the
        // results vector with placeholders for writes.
        let mut write_slots: Vec<Option<usize>> = Vec::with_capacity(ops.len());
        for op in ops {
            match op {
                TxnOp::Range(r) => {
                    let res = self
                        .range_inner(
                            &*snap,
                            &ctx, // reads still see pre-mutation cache state
                            state_copy,
                            &r.key,
                            &r.range_end,
                            r.limit,
                            r.revision,
                            r.keys_only,
                            r.count_only,
                        )
                        .await?;
                    op_results.push(TxnOpResult::Range(res));
                    write_slots.push(None);
                }
                TxnOp::Mutation(m) => {
                    mutations.push(m.clone());
                    write_slots.push(Some(op_results.len()));
                    op_results.push(TxnOpResult::Mutation(MutationResult::default()));
                }
            }
        }

        // Second pass: run the mutations as one atomic apply.
        if !mutations.is_empty() {
            // We need to fill in mutation results back into the
            // op_results slots in their original positions, so we
            // run apply_inner directly and walk the result list.
            let (revision, mut results, produced) = self
                .apply_inner(&*snap, &mut ctx, &mut state, &mutations)
                .await?;
            produced_any = produced;
            if produced {
                self.commit_ctx(&mut state, revision, ctx).await?;
            }
            // Restore back into op order.
            results.reverse();
            for slot in write_slots {
                if let Some(idx) = slot {
                    if let Some(res) = results.pop() {
                        op_results[idx] = TxnOpResult::Mutation(res);
                    }
                }
            }
        }

        let revision = if produced_any { proposed_main } else { state.current_rev };
        Ok(TxnResult {
            succeeded,
            revision,
            op_results,
        })
    }

    // ---------- internal helpers ----------

    async fn apply_inner(
        &self,
        snap: &dyn Snapshot,
        ctx: &mut ApplyContext,
        state: &mut WriteState,
        mutations: &[Mutation],
    ) -> MvccResult<(i64, Vec<MutationResult>, bool)> {
        let main = state.current_rev + 1;
        let mut results: Vec<MutationResult> = Vec::with_capacity(mutations.len());
        let mut produced_any = false;

        for (sub_zero_based, mutation) in mutations.iter().enumerate() {
            let sub = sub_zero_based as i64;
            let rev = Revision::new(main, sub);
            match mutation {
                Mutation::Put {
                    key,
                    value,
                    lease,
                    ignore_value,
                    ignore_lease,
                    prev_kv,
                } => {
                    let mut idx =
                        load_or_init_index(snap, &ctx.idx_cache, key.as_slice()).await?;
                    let prev = if idx.is_live() {
                        load_latest_record(
                            snap,
                            &ctx.latest_record_cache,
                            key.as_slice(),
                            &idx,
                        )
                        .await?
                    } else {
                        None
                    };

                    if *ignore_value && prev.is_none() {
                        return Err(MvccError::Internal(
                            "ignore_value set on Put for a non-existent key".into(),
                        ));
                    }
                    if *ignore_lease && prev.is_none() {
                        return Err(MvccError::Internal(
                            "ignore_lease set on Put for a non-existent key".into(),
                        ));
                    }

                    let effective_value = if *ignore_value {
                        prev.as_ref().expect("checked").value.clone()
                    } else {
                        value.clone()
                    };
                    let effective_lease = if *ignore_lease {
                        prev.as_ref().expect("checked").lease
                    } else {
                        *lease
                    };

                    let (version, created) = idx.record_put(rev);
                    let record = KvRecord {
                        key: key.clone(),
                        value: effective_value,
                        create_revision: created.main,
                        mod_revision: rev.main,
                        version,
                        lease: effective_lease,
                        deleted: false,
                    };

                    let kv_key = make_kv_key(key, rev);
                    let record_bytes = bincode::serialize(&record)
                        .map_err(|e| MvccError::Internal(format!("serialize KvRecord: {e}")))?;
                    ctx.batch.put(TABLE_KV, &kv_key, &record_bytes);

                    let idx_bytes = bincode::serialize(&idx)
                        .map_err(|e| MvccError::Internal(format!("serialize KeyIndex: {e}")))?;
                    ctx.batch.put(TABLE_IDX, key, &idx_bytes);

                    // If the lease association changed, update the
                    // lease_keys reverse index. The previous lease (if
                    // any) needs its entry removed; the new lease (if
                    // non-zero) needs an entry added.
                    let old_lease = prev.as_ref().map(|p| p.lease).unwrap_or(0);
                    if old_lease != record.lease {
                        if old_lease != 0 {
                            ctx.batch.delete(
                                TABLE_LEASE_KEYS,
                                &lease_key_index(old_lease, key),
                            );
                        }
                        if record.lease != 0 {
                            ctx.batch
                                .put(TABLE_LEASE_KEYS, &lease_key_index(record.lease, key), &[]);
                        }
                    }

                    ctx.idx_cache.insert(key.clone(), idx);
                    ctx.latest_record_cache.insert(key.clone(), record.clone());

                    ctx.pending_events.push(MvccEvent {
                        kind: EventKind::Put,
                        kv: record,
                        prev_kv: prev.clone(),
                    });

                    results.push(MutationResult {
                        n: 1,
                        prev_kvs: if *prev_kv {
                            prev.into_iter().collect()
                        } else {
                            Vec::new()
                        },
                    });
                    produced_any = true;
                }
                Mutation::DeleteRange {
                    key,
                    range_end,
                    prev_kv,
                } => {
                    let live_keys = live_keys_in_range(
                        snap,
                        &ctx.idx_cache,
                        key.as_slice(),
                        range_end.as_slice(),
                    )
                    .await?;
                    let mut result = MutationResult::default();
                    for live_key in live_keys {
                        let mut idx = load_or_init_index(
                            snap,
                            &ctx.idx_cache,
                            live_key.as_slice(),
                        )
                        .await?;
                        // Always load the prev record (regardless of
                        // the request's prev_kv flag) so the broadcast
                        // event carries it for watchers that want it.
                        let prev = if idx.is_live() {
                            load_latest_record(
                                snap,
                                &ctx.latest_record_cache,
                                live_key.as_slice(),
                                &idx,
                            )
                            .await?
                        } else {
                            None
                        };
                        let closed = idx.record_delete(rev);
                        if !closed {
                            continue;
                        }
                        let tombstone = KvRecord {
                            key: live_key.clone(),
                            value: Vec::new(),
                            create_revision: 0,
                            mod_revision: rev.main,
                            version: 0,
                            lease: 0,
                            deleted: true,
                        };
                        let kv_key = make_kv_key(&live_key, rev);
                        let bytes = bincode::serialize(&tombstone)
                            .map_err(|e| MvccError::Internal(format!("serialize tombstone: {e}")))?;
                        ctx.batch.put(TABLE_KV, &kv_key, &bytes);
                        let idx_bytes = bincode::serialize(&idx)
                            .map_err(|e| MvccError::Internal(format!("serialize KeyIndex: {e}")))?;
                        ctx.batch.put(TABLE_IDX, &live_key, &idx_bytes);
                        // Drop the lease_keys reverse index entry if the
                        // deleted key was attached to a lease.
                        if let Some(p) = &prev {
                            if p.lease != 0 {
                                ctx.batch.delete(
                                    TABLE_LEASE_KEYS,
                                    &lease_key_index(p.lease, &live_key),
                                );
                            }
                        }
                        ctx.idx_cache.insert(live_key.clone(), idx);
                        ctx.latest_record_cache
                            .insert(live_key.clone(), tombstone.clone());

                        ctx.pending_events.push(MvccEvent {
                            kind: EventKind::Delete,
                            kv: tombstone,
                            prev_kv: prev.clone(),
                        });

                        result.n += 1;
                        if *prev_kv {
                            if let Some(p) = prev {
                                result.prev_kvs.push(p);
                            }
                        }
                    }
                    if result.n > 0 {
                        produced_any = true;
                    }
                    results.push(result);
                }
            }
        }
        Ok((main, results, produced_any))
    }

    async fn commit_ctx(
        &self,
        state: &mut WriteState,
        new_rev: i64,
        mut ctx: ApplyContext,
    ) -> MvccResult<()> {
        write_i64(&mut ctx.batch, META_KEY_CURRENT_REV, new_rev);
        self.fold_raft_meta(&mut ctx.batch).await;
        self.inner
            .engine
            .commit(ctx.batch, WriteOptions::default())
            .await?;
        state.current_rev = new_rev;
        // Broadcast the event batch. Send error here means no
        // subscribers — fine, just drop on the floor.
        if !ctx.pending_events.is_empty() {
            let batch = EventBatch {
                revision: new_rev,
                events: ctx.pending_events,
            };
            let _ = self.inner.event_tx.send(batch);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn range_inner(
        &self,
        snap: &dyn Snapshot,
        ctx: &ApplyContext,
        state: WriteState,
        key: &[u8],
        range_end: &[u8],
        limit: usize,
        target_rev: i64,
        keys_only: bool,
        count_only: bool,
    ) -> MvccResult<RangeResult> {
        let current_rev = state.current_rev;
        let compact_rev = state.compact_rev;

        if target_rev > 0 {
            if target_rev > current_rev {
                return Err(MvccError::FutureRevision {
                    requested: target_rev,
                    current_rev,
                });
            }
            if target_rev < compact_rev {
                return Err(MvccError::Compacted {
                    requested: target_rev,
                    compact_rev,
                });
            }
        }
        let read_rev = if target_rev == 0 {
            Revision::new(current_rev, i64::MAX)
        } else {
            Revision::new(target_rev, i64::MAX)
        };

        let (start, end) = range_bounds(key, range_end);
        let entries = snap
            .range(TABLE_IDX, start, end, 0)
            .await
            .map_err(MvccError::Storage)?;

        let mut matches: Vec<KvRecord> = Vec::new();
        let mut total: i64 = 0;
        // Avoid double-counting keys that also live in the ctx cache.
        let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();

        for (idx_key, idx_bytes) in entries {
            let idx: KeyIndex = match ctx.idx_cache.get(&idx_key) {
                Some(c) => c.clone(),
                None => bincode::deserialize(&idx_bytes)
                    .map_err(|e| MvccError::Internal(format!("deserialize KeyIndex: {e}")))?,
            };
            seen.insert(idx_key.clone());
            self.range_match_one(
                snap, ctx, &idx_key, &idx, read_rev, keys_only, count_only, &mut total, &mut matches,
            )
            .await?;
        }
        // Cache-only keys (created within this same apply batch).
        for (k, idx) in &ctx.idx_cache {
            if seen.contains(k) {
                continue;
            }
            if !in_range(k.as_slice(), key, range_end) {
                continue;
            }
            self.range_match_one(
                snap, ctx, k, idx, read_rev, keys_only, count_only, &mut total, &mut matches,
            )
            .await?;
        }

        matches.sort_by(|a, b| a.key.cmp(&b.key));

        let more = limit > 0 && matches.len() > limit;
        if more {
            matches.truncate(limit);
        }
        Ok(RangeResult {
            kvs: if count_only { Vec::new() } else { matches },
            more,
            count: total,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn range_match_one(
        &self,
        snap: &dyn Snapshot,
        ctx: &ApplyContext,
        idx_key: &[u8],
        idx: &KeyIndex,
        read_rev: Revision,
        keys_only: bool,
        count_only: bool,
        total: &mut i64,
        matches: &mut Vec<KvRecord>,
    ) -> MvccResult<()> {
        let Some(rec_rev) = idx.revision_at(read_rev) else {
            return Ok(());
        };
        *total += 1;
        if count_only {
            return Ok(());
        }
        // Prefer ctx cache for fresh-in-batch records.
        if let Some(rec) = ctx.latest_record_cache.get(idx_key) {
            if !rec.is_tombstone() {
                let mut r = rec.clone();
                if keys_only {
                    r.value.clear();
                }
                matches.push(r);
                return Ok(());
            }
        }
        let kv_key = make_kv_key(idx_key, rec_rev);
        let rec_bytes = snap
            .get(TABLE_KV, &kv_key)
            .await
            .map_err(MvccError::Storage)?
            .ok_or_else(|| {
                MvccError::Internal(format!(
                    "missing KvRecord for key {} at rev {:?}",
                    String::from_utf8_lossy(idx_key),
                    rec_rev
                ))
            })?;
        let mut rec: KvRecord = bincode::deserialize(&rec_bytes)
            .map_err(|e| MvccError::Internal(format!("deserialize KvRecord: {e}")))?;
        if keys_only {
            rec.value.clear();
        }
        matches.push(rec);
        Ok(())
    }

    async fn evaluate_compares(
        &self,
        snap: &dyn Snapshot,
        compares: &[Compare],
    ) -> MvccResult<bool> {
        for cmp in compares {
            if !self.evaluate_one_compare(snap, cmp).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn evaluate_one_compare(
        &self,
        snap: &dyn Snapshot,
        cmp: &Compare,
    ) -> MvccResult<bool> {
        // For a single-key compare, look up the latest live record (or
        // implicit zero-record if absent) and compare against the target.
        // For a range compare, every key in the range must satisfy.
        let (start, end) = range_bounds(&cmp.key, &cmp.range_end);
        let entries = snap
            .range(TABLE_IDX, start, end, 0)
            .await
            .map_err(MvccError::Storage)?;

        if cmp.range_end.is_empty() {
            // Single-key compare. If absent, use the implicit zero record.
            let rec = if entries.is_empty() {
                implicit_zero_record(&cmp.key)
            } else {
                let (k, idx_bytes) = &entries[0];
                debug_assert_eq!(k, &cmp.key);
                let idx: KeyIndex = bincode::deserialize(idx_bytes)
                    .map_err(|e| MvccError::Internal(format!("deserialize KeyIndex: {e}")))?;
                load_latest_or_zero(snap, &idx, &cmp.key).await?
            };
            return Ok(eval_compare(&rec, &cmp.op, &cmp.target));
        }

        // Range compare. All matching keys must satisfy. Empty range
        // matches vacuously (matches etcd).
        for (idx_key, idx_bytes) in entries {
            let idx: KeyIndex = bincode::deserialize(&idx_bytes)
                .map_err(|e| MvccError::Internal(format!("deserialize KeyIndex: {e}")))?;
            let rec = load_latest_or_zero(snap, &idx, &idx_key).await?;
            if !eval_compare(&rec, &cmp.op, &cmp.target) {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

// ---------- helpers ----------

/// Per-apply mutable state shared between read and write paths.
#[derive(Default)]
struct ApplyContext {
    batch: WriteBatch,
    idx_cache: std::collections::HashMap<Vec<u8>, KeyIndex>,
    latest_record_cache: std::collections::HashMap<Vec<u8>, KvRecord>,
    /// Events accumulated during this apply, to be broadcast after a
    /// successful commit. Order matches the order ops produced them.
    pending_events: Vec<MvccEvent>,
}

async fn previous_live_record(
    snap: &dyn Snapshot,
    user_key: &[u8],
    before_rev: Revision,
) -> MvccResult<Option<KvRecord>> {
    let Some(idx_bytes) = snap
        .get(TABLE_IDX, user_key)
        .await
        .map_err(MvccError::Storage)?
    else {
        return Ok(None);
    };
    let idx: KeyIndex = bincode::deserialize(&idx_bytes)
        .map_err(|e| MvccError::Internal(format!("deserialize KeyIndex: {e}")))?;
    // Walk generations newest-to-oldest; find the latest rev strictly
    // less than `before_rev` and not a tombstone.
    let mut candidate: Option<Revision> = None;
    'outer: for g in idx.generations.iter().rev() {
        for r in g.revs.iter().rev() {
            if *r < before_rev {
                candidate = Some(*r);
                break 'outer;
            }
        }
        // If we ran out of puts in this generation, the only thing
        // older is a closing tombstone, which yields None.
    }
    let Some(rev) = candidate else { return Ok(None) };
    let kv_key = make_kv_key(user_key, rev);
    let bytes = snap
        .get(TABLE_KV, &kv_key)
        .await
        .map_err(MvccError::Storage)?;
    let Some(bytes) = bytes else { return Ok(None) };
    let rec: KvRecord = bincode::deserialize(&bytes)
        .map_err(|e| MvccError::Internal(format!("deserialize KvRecord: {e}")))?;
    if rec.is_tombstone() {
        Ok(None)
    } else {
        Ok(Some(rec))
    }
}

fn implicit_zero_record(key: &[u8]) -> KvRecord {
    KvRecord {
        key: key.to_vec(),
        value: Vec::new(),
        create_revision: 0,
        mod_revision: 0,
        version: 0,
        lease: 0,
        deleted: false,
    }
}

async fn load_latest_or_zero(
    snap: &dyn Snapshot,
    idx: &KeyIndex,
    key: &[u8],
) -> MvccResult<KvRecord> {
    if let Some((rev, _ver)) = idx.current() {
        let kv_key = make_kv_key(key, rev);
        if let Some(bytes) = snap
            .get(TABLE_KV, &kv_key)
            .await
            .map_err(MvccError::Storage)?
        {
            let rec: KvRecord = bincode::deserialize(&bytes)
                .map_err(|e| MvccError::Internal(format!("deserialize KvRecord: {e}")))?;
            if !rec.is_tombstone() {
                return Ok(rec);
            }
        }
    }
    Ok(implicit_zero_record(key))
}

fn eval_compare(rec: &KvRecord, op: &CompareOp, target: &CompareTarget) -> bool {
    use std::cmp::Ordering;
    let ordering: Ordering = match target {
        CompareTarget::Version(v) => rec.version.cmp(v),
        CompareTarget::CreateRevision(v) => rec.create_revision.cmp(v),
        CompareTarget::ModRevision(v) => rec.mod_revision.cmp(v),
        CompareTarget::Lease(v) => rec.lease.cmp(v),
        CompareTarget::Value(v) => rec.value.as_slice().cmp(v.as_slice()),
    };
    match op {
        CompareOp::Equal => ordering == Ordering::Equal,
        CompareOp::NotEqual => ordering != Ordering::Equal,
        CompareOp::Greater => ordering == Ordering::Greater,
        CompareOp::Less => ordering == Ordering::Less,
    }
}

async fn read_i64(snap: &dyn Snapshot, key: &[u8]) -> Result<Option<i64>, StorageError> {
    let bytes = snap.get(TABLE_META, key).await?;
    Ok(bytes.and_then(|b| {
        if b.len() == 8 {
            Some(i64::from_be_bytes(b.as_slice().try_into().expect("8 bytes")))
        } else {
            None
        }
    }))
}

fn write_i64(batch: &mut WriteBatch, key: &[u8], value: i64) {
    batch.put(TABLE_META, key, &value.to_be_bytes());
}

async fn load_or_init_index(
    snap: &dyn Snapshot,
    cache: &std::collections::HashMap<Vec<u8>, KeyIndex>,
    key: &[u8],
) -> Result<KeyIndex, MvccError> {
    if let Some(idx) = cache.get(key) {
        return Ok(idx.clone());
    }
    match snap.get(TABLE_IDX, key).await.map_err(MvccError::Storage)? {
        Some(bytes) => bincode::deserialize::<KeyIndex>(&bytes)
            .map_err(|e| MvccError::Internal(format!("deserialize KeyIndex: {e}"))),
        None => Ok(KeyIndex::new(key.to_vec())),
    }
}

async fn load_latest_record(
    snap: &dyn Snapshot,
    cache: &std::collections::HashMap<Vec<u8>, KvRecord>,
    key: &[u8],
    idx: &KeyIndex,
) -> Result<Option<KvRecord>, MvccError> {
    if let Some(r) = cache.get(key) {
        return Ok(if r.is_tombstone() {
            None
        } else {
            Some(r.clone())
        });
    }
    let Some((rev, _ver)) = idx.current() else {
        return Ok(None);
    };
    let kv_key = make_kv_key(key, rev);
    let bytes = snap
        .get(TABLE_KV, &kv_key)
        .await
        .map_err(MvccError::Storage)?;
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    let rec: KvRecord = bincode::deserialize(&bytes)
        .map_err(|e| MvccError::Internal(format!("deserialize KvRecord: {e}")))?;
    if rec.is_tombstone() {
        Ok(None)
    } else {
        Ok(Some(rec))
    }
}

async fn live_keys_in_range(
    snap: &dyn Snapshot,
    cache: &std::collections::HashMap<Vec<u8>, KeyIndex>,
    key: &[u8],
    range_end: &[u8],
) -> Result<Vec<Vec<u8>>, MvccError> {
    let (start, end) = range_bounds(key, range_end);
    // Pull all index entries in the byte range from the snapshot.
    let entries = snap
        .range(TABLE_IDX, start, end, 0)
        .await
        .map_err(MvccError::Storage)?;
    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(entries.len());
    for (k, v) in entries {
        // Index cache supersedes snapshot for in-batch changes.
        let idx: KeyIndex = match cache.get(&k) {
            Some(c) => c.clone(),
            None => bincode::deserialize(&v)
                .map_err(|e| MvccError::Internal(format!("deserialize KeyIndex: {e}")))?,
        };
        if idx.is_live() {
            keys.push(k);
        }
    }
    // Also include any keys present only in the cache (i.e., a key
    // first created in this same apply batch).
    for (cache_key, idx) in cache {
        if in_range(cache_key.as_slice(), key, range_end) && idx.is_live() && !keys.contains(cache_key) {
            keys.push(cache_key.clone());
        }
    }
    keys.sort();
    Ok(keys)
}

fn range_bounds(key: &[u8], range_end: &[u8]) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    // etcd's range semantics:
    //   - range_end == "":     single-key query for `key`
    //   - range_end == "\x00": "from key to end" (full upper-open range)
    //   - else:                [key, range_end) half-open
    if range_end.is_empty() {
        return (
            Bound::Included(key.to_vec()),
            Bound::Included(key.to_vec()),
        );
    }
    if range_end == [0u8] {
        return (Bound::Included(key.to_vec()), Bound::Unbounded);
    }
    (
        Bound::Included(key.to_vec()),
        Bound::Excluded(range_end.to_vec()),
    )
}

fn in_range(probe: &[u8], key: &[u8], range_end: &[u8]) -> bool {
    if range_end.is_empty() {
        return probe == key;
    }
    if range_end == [0u8] {
        return probe >= key;
    }
    probe >= key && probe < range_end
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redb_engine::RedbEngine;
    use tempfile::tempdir;

    async fn open_mvcc() -> (tempfile::TempDir, MvccStore) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("mvcc.redb");
        let eng = RedbEngine::open(&path).unwrap();
        let store = MvccStore::open(Arc::new(eng)).await.unwrap();
        (dir, store)
    }

    fn put(key: &[u8], value: &[u8]) -> Mutation {
        Mutation::Put {
            key: key.to_vec(),
            value: value.to_vec(),
            lease: 0,
            ignore_value: false,
            ignore_lease: false,
            prev_kv: false,
        }
    }

    fn put_prev(key: &[u8], value: &[u8]) -> Mutation {
        Mutation::Put {
            key: key.to_vec(),
            value: value.to_vec(),
            lease: 0,
            ignore_value: false,
            ignore_lease: false,
            prev_kv: true,
        }
    }

    fn del_range(key: &[u8], end: &[u8]) -> Mutation {
        Mutation::DeleteRange {
            key: key.to_vec(),
            range_end: end.to_vec(),
            prev_kv: false,
        }
    }

    fn del_prev(key: &[u8], end: &[u8]) -> Mutation {
        Mutation::DeleteRange {
            key: key.to_vec(),
            range_end: end.to_vec(),
            prev_kv: true,
        }
    }

    #[tokio::test]
    async fn fresh_store_starts_at_rev_zero() {
        let (_d, s) = open_mvcc().await;
        assert_eq!(s.current_revision().await, 0);
        assert_eq!(s.compact_revision().await, 0);
    }

    #[tokio::test]
    async fn put_advances_revision_and_round_trips() {
        let (_d, s) = open_mvcc().await;
        let (rev, _) = s.apply(&[put(b"hello", b"world")]).await.unwrap();
        assert_eq!(rev, 1);
        let out = s.range(b"hello", b"", 0, 0, false, false).await.unwrap();
        assert_eq!(out.kvs.len(), 1);
        assert_eq!(out.kvs[0].key, b"hello");
        assert_eq!(out.kvs[0].value, b"world");
        assert_eq!(out.kvs[0].create_revision, 1);
        assert_eq!(out.kvs[0].mod_revision, 1);
        assert_eq!(out.kvs[0].version, 1);
        assert_eq!(out.count, 1);
    }

    #[tokio::test]
    async fn second_put_increments_version_keeps_create_rev() {
        let (_d, s) = open_mvcc().await;
        s.apply(&[put(b"k", b"v0")]).await.unwrap();
        s.apply(&[put(b"k", b"v1")]).await.unwrap();
        let out = s.range(b"k", b"", 0, 0, false, false).await.unwrap();
        assert_eq!(out.kvs[0].value, b"v1");
        assert_eq!(out.kvs[0].create_revision, 1);
        assert_eq!(out.kvs[0].mod_revision, 2);
        assert_eq!(out.kvs[0].version, 2);
    }

    #[tokio::test]
    async fn range_returns_lex_ordered_keys() {
        let (_d, s) = open_mvcc().await;
        s.apply(&[
            put(b"c", b"3"),
            put(b"a", b"1"),
            put(b"b", b"2"),
        ])
        .await
        .unwrap();
        let out = s.range(b"a", b"z", 0, 0, false, false).await.unwrap();
        let keys: Vec<&[u8]> = out.kvs.iter().map(|r| r.key.as_slice()).collect();
        assert_eq!(keys, [b"a".as_ref(), b"b", b"c"]);
    }

    #[tokio::test]
    async fn delete_range_returns_count_and_makes_key_absent() {
        let (_d, s) = open_mvcc().await;
        s.apply(&[
            put(b"k1", b"v"),
            put(b"k2", b"v"),
            put(b"k3", b"v"),
        ])
        .await
        .unwrap();
        let (_rev, r) = s.apply(&[del_range(b"k1", b"k3")]).await.unwrap();
        assert_eq!(r[0].n, 2);
        let out = s.range(b"k1", b"k4", 0, 0, false, false).await.unwrap();
        let keys: Vec<&[u8]> = out.kvs.iter().map(|r| r.key.as_slice()).collect();
        assert_eq!(keys, [b"k3".as_ref()]);
    }

    #[tokio::test]
    async fn put_with_prev_kv_returns_prior_value() {
        let (_d, s) = open_mvcc().await;
        s.apply(&[put(b"k", b"v0")]).await.unwrap();
        let (_rev, results) = s.apply(&[put_prev(b"k", b"v1")]).await.unwrap();
        assert_eq!(results[0].prev_kvs.len(), 1);
        assert_eq!(results[0].prev_kvs[0].value, b"v0");
    }

    #[tokio::test]
    async fn delete_with_prev_kv_returns_prior_values() {
        let (_d, s) = open_mvcc().await;
        s.apply(&[put(b"k1", b"a"), put(b"k2", b"b")]).await.unwrap();
        let (_rev, results) = s.apply(&[del_prev(b"k1", b"k3")]).await.unwrap();
        assert_eq!(results[0].n, 2);
        assert_eq!(results[0].prev_kvs.len(), 2);
        let mut vals: Vec<&[u8]> = results[0].prev_kvs.iter().map(|r| r.value.as_slice()).collect();
        vals.sort();
        assert_eq!(vals, [b"a".as_ref(), b"b"]);
    }

    #[tokio::test]
    async fn delete_then_put_resurrects_key_with_version_one() {
        let (_d, s) = open_mvcc().await;
        s.apply(&[put(b"k", b"v0")]).await.unwrap();
        s.apply(&[del_range(b"k", b"")]).await.unwrap();
        let (_rev, _) = s.apply(&[put(b"k", b"v2")]).await.unwrap();
        let out = s.range(b"k", b"", 0, 0, false, false).await.unwrap();
        assert_eq!(out.kvs.len(), 1);
        assert_eq!(out.kvs[0].value, b"v2");
        assert_eq!(out.kvs[0].create_revision, 3);
        assert_eq!(out.kvs[0].mod_revision, 3);
        assert_eq!(out.kvs[0].version, 1);
    }

    #[tokio::test]
    async fn historical_range_returns_old_value() {
        let (_d, s) = open_mvcc().await;
        s.apply(&[put(b"k", b"v0")]).await.unwrap();
        s.apply(&[put(b"k", b"v1")]).await.unwrap();
        s.apply(&[put(b"k", b"v2")]).await.unwrap();

        let at1 = s.range(b"k", b"", 0, 1, false, false).await.unwrap();
        assert_eq!(at1.kvs[0].value, b"v0");
        let at2 = s.range(b"k", b"", 0, 2, false, false).await.unwrap();
        assert_eq!(at2.kvs[0].value, b"v1");
        let at3 = s.range(b"k", b"", 0, 3, false, false).await.unwrap();
        assert_eq!(at3.kvs[0].value, b"v2");
    }

    #[tokio::test]
    async fn future_revision_errors() {
        let (_d, s) = open_mvcc().await;
        s.apply(&[put(b"k", b"v0")]).await.unwrap();
        let err = s
            .range(b"k", b"", 0, 999, false, false)
            .await
            .err()
            .expect("future rev errors");
        assert!(matches!(err, MvccError::FutureRevision { .. }));
    }

    #[tokio::test]
    async fn range_limit_marks_more() {
        let (_d, s) = open_mvcc().await;
        s.apply(&[put(b"a", b""), put(b"b", b""), put(b"c", b"")])
            .await
            .unwrap();
        let out = s.range(b"a", b"z", 2, 0, false, false).await.unwrap();
        assert_eq!(out.kvs.len(), 2);
        assert!(out.more);
        assert_eq!(out.count, 3);
    }

    #[tokio::test]
    async fn count_only_returns_count_without_values() {
        let (_d, s) = open_mvcc().await;
        s.apply(&[put(b"a", b""), put(b"b", b""), put(b"c", b"")])
            .await
            .unwrap();
        let out = s.range(b"a", b"z", 0, 0, false, true).await.unwrap();
        assert!(out.kvs.is_empty());
        assert_eq!(out.count, 3);
    }

    #[tokio::test]
    async fn keys_only_drops_values() {
        let (_d, s) = open_mvcc().await;
        s.apply(&[put(b"a", b"AAA"), put(b"b", b"BBB")])
            .await
            .unwrap();
        let out = s.range(b"a", b"z", 0, 0, true, false).await.unwrap();
        for r in &out.kvs {
            assert!(r.value.is_empty());
        }
    }

    #[tokio::test]
    async fn revisions_survive_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("persist.redb");
        {
            let eng = RedbEngine::open(&path).unwrap();
            let s = MvccStore::open(Arc::new(eng)).await.unwrap();
            s.apply(&[put(b"k", b"v0"), put(b"k", b"v1")]).await.unwrap();
            assert_eq!(s.current_revision().await, 1);
        }
        let eng = RedbEngine::open(&path).unwrap();
        let s = MvccStore::open(Arc::new(eng)).await.unwrap();
        assert_eq!(s.current_revision().await, 1);
        let out = s.range(b"k", b"", 0, 0, false, false).await.unwrap();
        assert_eq!(out.kvs[0].value, b"v1");
        assert_eq!(out.kvs[0].version, 2);
    }

    #[tokio::test]
    async fn compact_makes_older_reads_error_but_preserves_floor() {
        let (_d, s) = open_mvcc().await;
        s.apply(&[put(b"k", b"v0")]).await.unwrap();
        s.apply(&[put(b"k", b"v1")]).await.unwrap();
        s.apply(&[put(b"k", b"v2")]).await.unwrap();

        // Compact at rev 2: rev 1 history is gone; rev 2 is the floor.
        s.compact(2).await.unwrap();
        assert_eq!(s.compact_revision().await, 2);

        // Reads strictly below compact_rev now error.
        let err = s.range(b"k", b"", 0, 1, false, false).await.err().unwrap();
        assert!(matches!(err, MvccError::Compacted { .. }));

        // Reads at the compact rev still succeed and return the floor value.
        let out = s.range(b"k", b"", 0, 2, false, false).await.unwrap();
        assert_eq!(out.kvs[0].value, b"v1");

        // Reads at newer revs unaffected.
        let out = s.range(b"k", b"", 0, 3, false, false).await.unwrap();
        assert_eq!(out.kvs[0].value, b"v2");

        // Current reads unaffected.
        let out = s.range(b"k", b"", 0, 0, false, false).await.unwrap();
        assert_eq!(out.kvs[0].value, b"v2");
    }

    #[tokio::test]
    async fn compact_drops_tombstoned_keys_entirely() {
        let (_d, s) = open_mvcc().await;
        s.apply(&[put(b"k", b"v0")]).await.unwrap();
        s.apply(&[del_range(b"k", b"")]).await.unwrap();
        // After compact at rev 2, the index entry for k is fully gone.
        s.compact(2).await.unwrap();
        let out = s.range(b"k", b"", 0, 0, false, false).await.unwrap();
        assert!(out.kvs.is_empty());
        assert_eq!(out.count, 0);
    }

    #[tokio::test]
    async fn compact_at_future_rev_errors() {
        let (_d, s) = open_mvcc().await;
        s.apply(&[put(b"k", b"v0")]).await.unwrap();
        let err = s.compact(50).await.err().unwrap();
        assert!(matches!(err, MvccError::FutureRevision { .. }));
    }

    #[tokio::test]
    async fn compact_at_or_before_compact_rev_is_idempotent_or_errors() {
        let (_d, s) = open_mvcc().await;
        s.apply(&[put(b"k", b"v0")]).await.unwrap();
        s.apply(&[put(b"k", b"v1")]).await.unwrap();
        s.compact(2).await.unwrap();
        // Same rev: no-op.
        s.compact(2).await.unwrap();
        // Older rev: error.
        let err = s.compact(1).await.err().unwrap();
        assert!(matches!(err, MvccError::Internal(_)));
    }

    #[tokio::test]
    async fn compact_persists_across_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("compact.redb");
        {
            let eng = RedbEngine::open(&path).unwrap();
            let s = MvccStore::open(Arc::new(eng)).await.unwrap();
            s.apply(&[put(b"k", b"v0"), put(b"k", b"v1")]).await.unwrap();
            // rev = 1 after one apply (batched). Add more so we can compact.
            s.apply(&[put(b"k", b"v2")]).await.unwrap();
            s.compact(2).await.unwrap();
        }
        let eng = RedbEngine::open(&path).unwrap();
        let s = MvccStore::open(Arc::new(eng)).await.unwrap();
        assert_eq!(s.compact_revision().await, 2);
        // Old revisions still gone after reopen.
        let err = s.range(b"k", b"", 0, 1, false, false).await.err().unwrap();
        assert!(matches!(err, MvccError::Compacted { .. }));
    }

    fn cmp_value_eq(key: &[u8], value: &[u8]) -> Compare {
        Compare {
            key: key.to_vec(),
            range_end: Vec::new(),
            op: CompareOp::Equal,
            target: CompareTarget::Value(value.to_vec()),
        }
    }

    fn cmp_version_eq(key: &[u8], version: i64) -> Compare {
        Compare {
            key: key.to_vec(),
            range_end: Vec::new(),
            op: CompareOp::Equal,
            target: CompareTarget::Version(version),
        }
    }

    fn txn_put(key: &[u8], value: &[u8]) -> TxnOp {
        TxnOp::Mutation(put(key, value))
    }

    fn txn_range(key: &[u8], range_end: &[u8]) -> TxnOp {
        TxnOp::Range(RangeOp {
            key: key.to_vec(),
            range_end: range_end.to_vec(),
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn txn_success_branch_runs_when_compares_pass() {
        let (_d, s) = open_mvcc().await;
        s.apply(&[put(b"k", b"v0")]).await.unwrap();
        let r = s
            .txn(
                &[cmp_value_eq(b"k", b"v0")],
                &[txn_put(b"k", b"v1")],
                &[txn_put(b"k", b"failure")],
            )
            .await
            .unwrap();
        assert!(r.succeeded);
        assert_eq!(r.op_results.len(), 1);
        let out = s.range(b"k", b"", 0, 0, false, false).await.unwrap();
        assert_eq!(out.kvs[0].value, b"v1");
    }

    #[tokio::test]
    async fn txn_failure_branch_runs_when_compares_fail() {
        let (_d, s) = open_mvcc().await;
        s.apply(&[put(b"k", b"v0")]).await.unwrap();
        let r = s
            .txn(
                &[cmp_value_eq(b"k", b"nope")],
                &[txn_put(b"k", b"success")],
                &[txn_put(b"k", b"failure")],
            )
            .await
            .unwrap();
        assert!(!r.succeeded);
        let out = s.range(b"k", b"", 0, 0, false, false).await.unwrap();
        assert_eq!(out.kvs[0].value, b"failure");
    }

    #[tokio::test]
    async fn txn_compare_on_absent_key_uses_zero_record() {
        let (_d, s) = open_mvcc().await;
        // Compare version == 0 should pass for an absent key.
        let r = s
            .txn(
                &[cmp_version_eq(b"missing", 0)],
                &[txn_put(b"missing", b"created")],
                &[],
            )
            .await
            .unwrap();
        assert!(r.succeeded);
        let out = s.range(b"missing", b"", 0, 0, false, false).await.unwrap();
        assert_eq!(out.kvs[0].value, b"created");
    }

    #[tokio::test]
    async fn txn_with_mixed_read_and_write_ops_preserves_order() {
        let (_d, s) = open_mvcc().await;
        s.apply(&[put(b"a", b"A"), put(b"b", b"B")]).await.unwrap();
        let r = s
            .txn(
                &[], // no compares -> success branch
                &[
                    txn_range(b"a", b"c"),
                    txn_put(b"c", b"C"),
                    txn_range(b"a", b"d"),
                ],
                &[],
            )
            .await
            .unwrap();
        assert!(r.succeeded);
        // First range observes pre-mutation state (no "c").
        let first = match &r.op_results[0] {
            TxnOpResult::Range(rr) => rr.kvs.iter().map(|k| k.key.clone()).collect::<Vec<_>>(),
            _ => panic!("expected Range"),
        };
        assert_eq!(first, vec![b"a".to_vec(), b"b".to_vec()]);
        // Mutation result.
        match &r.op_results[1] {
            TxnOpResult::Mutation(m) => assert_eq!(m.n, 1),
            _ => panic!("expected Mutation"),
        };
        // Second range — etcd's behavior: reads inside the txn see the
        // pre-mutation snapshot. So "c" should NOT be observable here.
        let second = match &r.op_results[2] {
            TxnOpResult::Range(rr) => rr.kvs.iter().map(|k| k.key.clone()).collect::<Vec<_>>(),
            _ => panic!("expected Range"),
        };
        assert_eq!(second, vec![b"a".to_vec(), b"b".to_vec()]);
    }

    #[tokio::test]
    async fn txn_with_no_mutations_does_not_advance_revision() {
        let (_d, s) = open_mvcc().await;
        s.apply(&[put(b"k", b"v0")]).await.unwrap();
        let r_before = s.current_revision().await;
        let r = s
            .txn(&[cmp_value_eq(b"k", b"v0")], &[txn_range(b"k", b"")], &[])
            .await
            .unwrap();
        assert!(r.succeeded);
        assert_eq!(r.revision, r_before);
        assert_eq!(s.current_revision().await, r_before);
    }

    #[tokio::test]
    async fn txn_mutations_share_one_main_revision() {
        let (_d, s) = open_mvcc().await;
        s.apply(&[put(b"k", b"v0")]).await.unwrap();
        let r = s
            .txn(
                &[],
                &[txn_put(b"a", b"1"), txn_put(b"b", b"2"), txn_put(b"c", b"3")],
                &[],
            )
            .await
            .unwrap();
        assert!(r.succeeded);
        assert_eq!(r.revision, 2);
        for key in [b"a".as_ref(), b"b", b"c"] {
            let out = s.range(key, b"", 0, 0, false, false).await.unwrap();
            assert_eq!(out.kvs[0].mod_revision, 2);
            assert_eq!(out.kvs[0].create_revision, 2);
        }
    }

    #[tokio::test]
    async fn txn_range_compare_must_hold_for_every_key_in_range() {
        let (_d, s) = open_mvcc().await;
        s.apply(&[put(b"a", b"v"), put(b"b", b"v"), put(b"c", b"x")])
            .await
            .unwrap();
        // Range compare: all keys in [a, d) must equal "v" — fails on "c".
        let r = s
            .txn(
                &[Compare {
                    key: b"a".to_vec(),
                    range_end: b"d".to_vec(),
                    op: CompareOp::Equal,
                    target: CompareTarget::Value(b"v".to_vec()),
                }],
                &[txn_put(b"result", b"success")],
                &[txn_put(b"result", b"failure")],
            )
            .await
            .unwrap();
        assert!(!r.succeeded);
        let out = s.range(b"result", b"", 0, 0, false, false).await.unwrap();
        assert_eq!(out.kvs[0].value, b"failure");
    }

    #[tokio::test]
    async fn batched_puts_share_main_revision_distinct_sub() {
        let (_d, s) = open_mvcc().await;
        let (rev, _) = s
            .apply(&[put(b"a", b"1"), put(b"b", b"2"), put(b"c", b"3")])
            .await
            .unwrap();
        assert_eq!(rev, 1);
        let out = s.range(b"a", b"z", 0, 0, false, false).await.unwrap();
        for r in &out.kvs {
            assert_eq!(r.mod_revision, 1);
            assert_eq!(r.create_revision, 1);
        }
    }
}
