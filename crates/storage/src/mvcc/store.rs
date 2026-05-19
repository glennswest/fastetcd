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

use thiserror::Error;
use tokio::sync::Mutex;

use crate::kvstore::{KvStore, Snapshot, StorageError, WriteBatch, WriteOptions};

use super::record::{KeyIndex, KvRecord};
use super::revision::{make_kv_key, Revision};

const TABLE_KV: &str = "mvcc_kv";
const TABLE_IDX: &str = "mvcc_idx";
const TABLE_META: &str = "mvcc_meta";

const META_KEY_CURRENT_REV: &[u8] = b"current_rev";
const META_KEY_COMPACT_REV: &[u8] = b"compact_rev";

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
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone, Default)]
pub struct MutationResult {
    /// Number of keys actually written or deleted.
    pub n: i64,
    /// Records that existed before the mutation, populated only when
    /// `prev_kv` was requested.
    pub prev_kvs: Vec<KvRecord>,
}

/// Outcome of a `range` query.
#[derive(Debug, Clone, Default)]
pub struct RangeResult {
    pub kvs: Vec<KvRecord>,
    /// True if more keys would have been returned but were excluded
    /// by `limit`. Maps to etcd `RangeResponse.more`.
    pub more: bool,
    /// Count of matching keys *before* the limit was applied.
    pub count: i64,
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
}

#[derive(Debug, Clone, Copy)]
struct WriteState {
    current_rev: i64,
    compact_rev: i64,
}

impl MvccStore {
    /// Open or initialize an MVCC layer over `engine`. On first open,
    /// initializes meta entries to `0`. On a populated engine, reads
    /// the stored counters.
    pub async fn open(engine: Arc<dyn KvStore>) -> MvccResult<Self> {
        let snap = engine.snapshot().await?;
        let current = read_i64(&*snap, META_KEY_CURRENT_REV).await?.unwrap_or(0);
        let compact = read_i64(&*snap, META_KEY_COMPACT_REV).await?.unwrap_or(0);
        drop(snap);

        // If meta keys were absent, persist their initial values so
        // size_on_disk and subsequent opens see consistent state.
        if current == 0 && compact == 0 {
            let mut b = WriteBatch::new();
            write_i64(&mut b, META_KEY_CURRENT_REV, 0);
            write_i64(&mut b, META_KEY_COMPACT_REV, 0);
            engine.commit(b, WriteOptions::default()).await?;
        }

        Ok(Self {
            inner: Arc::new(Inner {
                engine,
                write_state: Mutex::new(WriteState {
                    current_rev: current,
                    compact_rev: compact,
                }),
            }),
        })
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

    /// Apply a batch of mutations atomically at one new `main` revision.
    /// Distinct sub-revisions are assigned to each mutation in order;
    /// every put/delete shares the same `main`. Returns one
    /// [`MutationResult`] per input mutation.
    ///
    /// The `header_revision` of every response in the surrounding RPC
    /// should be the returned `main` revision.
    pub async fn apply(&self, mutations: &[Mutation]) -> MvccResult<(i64, Vec<MutationResult>)> {
        if mutations.is_empty() {
            // Reads at "no mutation" don't advance the revision; behave
            // as if a no-op was applied.
            let current = self.current_revision().await;
            return Ok((current, Vec::new()));
        }

        let mut state = self.inner.write_state.lock().await;
        let main = state.current_rev + 1;
        let mut batch = WriteBatch::new();
        let mut results: Vec<MutationResult> = Vec::with_capacity(mutations.len());
        let mut produced_any = false;
        let snap = self.inner.engine.snapshot().await?;

        // In-memory KeyIndex cache: when one mutation puts/deletes a
        // key and a later mutation in the same `apply` touches the
        // same key, the second must see the first's effect.
        let mut idx_cache: std::collections::HashMap<Vec<u8>, KeyIndex> =
            std::collections::HashMap::new();
        // Track records we wrote in this batch but haven't committed
        // yet, so prev_kv lookups within the batch see them.
        let mut latest_record_cache: std::collections::HashMap<Vec<u8>, KvRecord> =
            std::collections::HashMap::new();

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
                    let mut idx = load_or_init_index(
                        &*snap,
                        &idx_cache,
                        key.as_slice(),
                    )
                    .await?;

                    // Resolve prev record (only if requested or needed for ignore_*).
                    let prev = if idx.is_live() {
                        load_latest_record(
                            &*snap,
                            &latest_record_cache,
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
                        create_revision: created.main, // see note below
                        mod_revision: rev.main,
                        version,
                        lease: effective_lease,
                        deleted: false,
                    };
                    // etcd's KeyValue uses the main revision for both
                    // create_revision and mod_revision. We mirror that
                    // here so wire responses match exactly.

                    let kv_key = make_kv_key(key, rev);
                    let record_bytes = bincode::serialize(&record)
                        .map_err(|e| MvccError::Internal(format!("serialize KvRecord: {e}")))?;
                    batch.put(TABLE_KV, &kv_key, &record_bytes);

                    let idx_bytes = bincode::serialize(&idx)
                        .map_err(|e| MvccError::Internal(format!("serialize KeyIndex: {e}")))?;
                    batch.put(TABLE_IDX, key, &idx_bytes);

                    idx_cache.insert(key.clone(), idx);
                    latest_record_cache.insert(key.clone(), record);

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
                        &*snap,
                        &idx_cache,
                        key.as_slice(),
                        range_end.as_slice(),
                    )
                    .await?;

                    let mut result = MutationResult::default();
                    for live_key in live_keys {
                        let mut idx = load_or_init_index(
                            &*snap,
                            &idx_cache,
                            live_key.as_slice(),
                        )
                        .await?;
                        let prev = if *prev_kv && idx.is_live() {
                            load_latest_record(
                                &*snap,
                                &latest_record_cache,
                                live_key.as_slice(),
                                &idx,
                            )
                            .await?
                        } else {
                            None
                        };
                        let closed = idx.record_delete(rev);
                        if !closed {
                            continue; // already deleted (raced with prior op in batch)
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
                        batch.put(TABLE_KV, &kv_key, &bytes);

                        let idx_bytes = bincode::serialize(&idx)
                            .map_err(|e| MvccError::Internal(format!("serialize KeyIndex: {e}")))?;
                        batch.put(TABLE_IDX, &live_key, &idx_bytes);

                        idx_cache.insert(live_key.clone(), idx);
                        latest_record_cache.insert(live_key.clone(), tombstone);

                        result.n += 1;
                        if let Some(p) = prev {
                            result.prev_kvs.push(p);
                        }
                    }
                    if result.n > 0 {
                        produced_any = true;
                    }
                    results.push(result);
                }
            }
        }

        if !produced_any {
            // No-op apply (e.g. delete on empty range). Don't advance
            // the revision counter; commit nothing.
            return Ok((state.current_rev, results));
        }

        // Persist the new current_rev as part of the same atomic batch.
        write_i64(&mut batch, META_KEY_CURRENT_REV, main);
        self.inner
            .engine
            .commit(batch, WriteOptions::default())
            .await?;
        state.current_rev = main;
        Ok((main, results))
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
        let snap = self.inner.engine.snapshot().await?;
        let state = self.inner.write_state.lock().await;
        let current_rev = state.current_rev;
        let compact_rev = state.compact_rev;
        drop(state);

        if target_rev > 0 {
            if target_rev > current_rev {
                return Err(MvccError::FutureRevision {
                    requested: target_rev,
                    current_rev,
                });
            }
            if target_rev <= compact_rev && compact_rev > 0 {
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

        // Range over the key index.
        let (start, end) = range_bounds(key, range_end);
        let entries = snap
            .range(TABLE_IDX, start, end, 0)
            .await
            .map_err(MvccError::Storage)?;

        let mut matches: Vec<KvRecord> = Vec::new();
        let mut total: i64 = 0;
        for (idx_key, idx_bytes) in entries {
            let idx: KeyIndex = bincode::deserialize(&idx_bytes)
                .map_err(|e| MvccError::Internal(format!("deserialize KeyIndex: {e}")))?;
            let Some(rec_rev) = idx.revision_at(read_rev) else {
                continue;
            };
            total += 1;
            if count_only {
                continue;
            }
            // Fetch the record.
            let kv_key = make_kv_key(&idx_key, rec_rev);
            let rec_bytes = snap
                .get(TABLE_KV, &kv_key)
                .await
                .map_err(MvccError::Storage)?
                .ok_or_else(|| {
                    MvccError::Internal(format!(
                        "missing KvRecord for key {} at rev {:?}",
                        String::from_utf8_lossy(&idx_key),
                        rec_rev
                    ))
                })?;
            let mut rec: KvRecord = bincode::deserialize(&rec_bytes)
                .map_err(|e| MvccError::Internal(format!("deserialize KvRecord: {e}")))?;
            if keys_only {
                rec.value.clear();
            }
            matches.push(rec);
        }

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
}

// ---------- helpers ----------

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
