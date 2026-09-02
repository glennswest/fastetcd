//! Engine-agnostic key-value store trait.
//!
//! Every concrete storage engine (redb today, io_uring tomorrow) implements
//! the [`KvStore`] trait. Higher layers — the MVCC state machine, the Raft
//! log adapter — depend on the trait, never on a concrete engine.
//!
//! ## Shape
//!
//! - A store contains one or more named **tables**. Tables provide
//!   key-space separation (MVCC index, MVCC values, leases, Raft log,
//!   Raft snapshots, etc.). Tables are auto-created on first use.
//! - **Reads** go through a [`Snapshot`], which is a consistent
//!   point-in-time view of the entire store. A snapshot is cheap to take
//!   and may be held across awaits.
//! - **Writes** go into a concrete [`WriteBatch`] value (engine-agnostic;
//!   a simple op-list), then atomically committed via
//!   [`KvStore::commit`]. Engines may fsync at commit (the default) or
//!   batch fsyncs across commits if the caller opts in via
//!   [`WriteOptions::sync = false`].
//! - All methods are async to accommodate engines whose I/O is naturally
//!   async (io_uring); engines built on synchronous primitives (redb)
//!   wrap blocking work via `spawn_blocking`.
//!
//! ## Error model
//!
//! Engines return [`StorageError`]. The variant is the contract; the
//! `source` chain carries engine-specific detail.

use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

/// Result alias for all storage operations.
pub type StorageResult<T> = Result<T, StorageError>;

/// Coarse-grained storage error. Engine-specific details live in `source`.
#[derive(Debug, Error)]
pub enum StorageError {
    /// I/O failed at the engine layer (disk full, permission denied, etc.).
    #[error("storage io: {0}")]
    Io(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// A transaction conflicted and must be retried. Engines that
    /// serialize writes through a single apply loop will not produce
    /// this; engines that allow concurrent writers may.
    #[error("storage conflict: {0}")]
    Conflict(String),

    /// Caller-side misuse: bad table name, malformed key, etc.
    #[error("storage misuse: {0}")]
    Misuse(String),

    /// Engine has been closed or is shutting down.
    #[error("storage closed")]
    Closed,
}

impl StorageError {
    /// Wrap an arbitrary `std::error::Error` as an [`StorageError::Io`].
    pub fn io<E>(e: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        StorageError::Io(Box::new(e))
    }
}

/// Options that modify how a write batch is committed.
#[derive(Debug, Clone, Copy)]
pub struct WriteOptions {
    /// If true (default), the engine must ensure the batch is durably
    /// flushed (fsync, `O_DSYNC`, or equivalent) before commit returns.
    /// Setting this to false lets the engine batch fsyncs across commits
    /// — useful for non-critical writes; never used for Raft log appends.
    pub sync: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self { sync: true }
    }
}

/// One mutation within a [`WriteBatch`]. Engines iterate these in order.
#[derive(Debug, Clone)]
pub enum BatchOp {
    Put {
        table: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        table: String,
        key: Vec<u8>,
    },
    /// Delete every key in `[start, end)` in `table`.
    DeleteRange {
        table: String,
        start: Vec<u8>,
        end: Vec<u8>,
    },
}

/// Engine-agnostic write batch. Built imperatively, then passed to
/// [`KvStore::commit`]. The op list is exposed so engines can iterate it
/// directly — keep the type concrete so we don't need trait-object
/// downcasts.
#[derive(Debug, Default, Clone)]
pub struct WriteBatch {
    ops: Vec<BatchOp>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            ops: Vec::with_capacity(cap),
        }
    }

    pub fn put(&mut self, table: &str, key: &[u8], value: &[u8]) -> &mut Self {
        self.ops.push(BatchOp::Put {
            table: table.to_string(),
            key: key.to_vec(),
            value: value.to_vec(),
        });
        self
    }

    pub fn delete(&mut self, table: &str, key: &[u8]) -> &mut Self {
        self.ops.push(BatchOp::Delete {
            table: table.to_string(),
            key: key.to_vec(),
        });
        self
    }

    pub fn delete_range(&mut self, table: &str, start: &[u8], end: &[u8]) -> &mut Self {
        self.ops.push(BatchOp::DeleteRange {
            table: table.to_string(),
            start: start.to_vec(),
            end: end.to_vec(),
        });
        self
    }

    pub fn ops(&self) -> &[BatchOp] {
        &self.ops
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }
}

/// A consistent point-in-time view of the store. Cheap to construct;
/// holding many snapshots is fine but the engine may need to retain
/// reachable versions until snapshots are dropped, so callers should
/// drop snapshots promptly when done.
#[async_trait]
pub trait Snapshot: Send + Sync {
    /// Point read. Returns `Ok(None)` if the key is absent.
    async fn get(&self, table: &str, key: &[u8]) -> StorageResult<Option<Vec<u8>>>;

    /// Range scan. `start` and `end` are `Bound`s over the keyspace
    /// (etcd's range model — `start` inclusive, `end` exclusive, but
    /// callers may pass any `Bound` explicitly). `limit = 0` means no
    /// limit. The returned vector preserves byte-lexicographic key
    /// order.
    async fn range(
        &self,
        table: &str,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Count the number of keys in a range without materializing values.
    async fn count(
        &self,
        table: &str,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    ) -> StorageResult<u64>;

    /// Return the entry with the largest key in `table`, or `None` if the
    /// table is empty.
    ///
    /// The default materializes the whole table and takes the last row —
    /// engines should override this with a reverse/last B-tree lookup.
    /// The Raft log store depends on the override: it reads the last log
    /// entry on every startup, and a log that has grown to tens of
    /// thousands of entries otherwise loads gigabytes into RAM before the
    /// node can bind its peer port and hold an election (fastetcd#13).
    async fn last(&self, table: &str) -> StorageResult<Option<(Vec<u8>, Vec<u8>)>> {
        Ok(self
            .range(table, Bound::Unbounded, Bound::Unbounded, 0)
            .await?
            .pop())
    }
}

/// How much space an engine is occupying, and how much of that is
/// actually holding live data.
///
/// A copy-on-write B-tree never shrinks its file on its own: pages
/// freed by a delete go on a free list and are reused, so the file
/// stays at its high-water mark. `file_bytes` is therefore what the
/// filesystem sees; `in_use_bytes` is what the data would occupy after
/// a defragment. The gap between them is what
/// [`KvStore::defragment`] can hand back to the volume, and knowing it
/// is what lets the server reclaim space *before* it hits ENOSPC
/// (fastetcd#14).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreUsage {
    /// Size of the engine's backing file(s) on disk.
    pub file_bytes: u64,
    /// Bytes holding live keys, values and index metadata.
    pub in_use_bytes: u64,
    /// Bytes lost to fragmentation within allocated pages.
    pub fragmented_bytes: u64,
}

impl StoreUsage {
    /// Upper bound on what a defragment could return to the filesystem:
    /// the part of the file not held by live pages.
    ///
    /// An upper bound, not a promise. An allocator can usually only give
    /// back whole regions at the end of the file, so a store whose live
    /// pages are spread across it may free less — or nothing. Treat this
    /// as "is a defragment worth the pause", never as "you will get this
    /// many bytes".
    pub fn reclaimable_bytes(&self) -> u64 {
        self.file_bytes.saturating_sub(self.in_use_bytes)
    }
}

/// The top-level storage engine trait. Implementations are expected to
/// be cheaply `Clone` (e.g., `Arc`-wrapping their state) so the server
/// can hand copies to multiple subsystems.
#[async_trait]
pub trait KvStore: Send + Sync + 'static {
    /// Open or create a snapshot of the current committed state.
    async fn snapshot(&self) -> StorageResult<Arc<dyn Snapshot>>;

    /// Atomically apply `batch` and (if `opts.sync`) flush durably.
    async fn commit(&self, batch: WriteBatch, opts: WriteOptions) -> StorageResult<()>;

    /// Explicit flush of any data that has been committed with `sync =
    /// false`. Engines that always fsync on commit may make this a no-op.
    async fn sync(&self) -> StorageResult<()>;

    /// Best-effort estimate of the on-disk size in bytes. Used for
    /// `Maintenance.Status.dbSize`. May be approximate.
    async fn size_on_disk(&self) -> StorageResult<u64>;

    /// Space accounting: file size vs. bytes actually holding data.
    ///
    /// The default reports the file size for both, i.e. "no reclaimable
    /// space" — safe for engines that can't tell the difference, since
    /// it never promises a defragment will free anything.
    async fn usage(&self) -> StorageResult<StoreUsage> {
        let file_bytes = self.size_on_disk().await?;
        Ok(StoreUsage {
            file_bytes,
            in_use_bytes: file_bytes,
            fragmented_bytes: 0,
        })
    }

    /// Engine-defined name for logging and metrics (`"redb"`, `"iouring"`).
    fn engine_name(&self) -> &'static str;

    /// Best-effort online defragmentation. Engines that don't
    /// support compaction may return `Ok(())` as a no-op. May
    /// block (under `spawn_blocking`) for the duration of the
    /// rewrite — callers should not hold open snapshots while
    /// invoking.
    async fn defragment(&self) -> StorageResult<()> {
        Ok(())
    }
}

/// Conformance tests for any [`KvStore`] implementation. Engines link to
/// these in their own test modules so we exercise the trait surface
/// identically across engines.
#[cfg(any(test, feature = "conformance"))]
pub mod conformance {
    use super::*;

    /// Run the full conformance suite against `store`. Panics on the
    /// first failure with a descriptive message; intended to be called
    /// from `#[tokio::test]`.
    pub async fn run_all<S: KvStore>(store: &S) {
        put_get_delete(store).await;
        range_scan_lex_order(store).await;
        delete_range_is_atomic(store).await;
        snapshot_is_consistent_under_writes(store).await;
        count_matches_range_len(store).await;
    }

    pub async fn put_get_delete<S: KvStore>(store: &S) {
        let mut batch = WriteBatch::new();
        batch.put("kv", b"alpha", b"one");
        batch.put("kv", b"bravo", b"two");
        store
            .commit(batch, WriteOptions::default())
            .await
            .expect("commit");

        let snap = store.snapshot().await.expect("snapshot");
        assert_eq!(
            snap.get("kv", b"alpha").await.unwrap().as_deref(),
            Some(b"one".as_ref())
        );
        assert_eq!(
            snap.get("kv", b"bravo").await.unwrap().as_deref(),
            Some(b"two".as_ref())
        );
        assert_eq!(snap.get("kv", b"charlie").await.unwrap(), None);

        let mut batch = WriteBatch::new();
        batch.delete("kv", b"alpha");
        store
            .commit(batch, WriteOptions::default())
            .await
            .expect("commit");

        let snap = store.snapshot().await.expect("snapshot");
        assert_eq!(snap.get("kv", b"alpha").await.unwrap(), None);
        assert_eq!(
            snap.get("kv", b"bravo").await.unwrap().as_deref(),
            Some(b"two".as_ref())
        );
    }

    pub async fn range_scan_lex_order<S: KvStore>(store: &S) {
        let mut batch = WriteBatch::new();
        for (i, key) in [b"c", b"a", b"d", b"b"].iter().enumerate() {
            batch.put("range", key.as_slice(), &[i as u8]);
        }
        store
            .commit(batch, WriteOptions::default())
            .await
            .expect("commit");

        let snap = store.snapshot().await.unwrap();
        let out = snap
            .range(
                "range",
                Bound::Included(b"a".to_vec()),
                Bound::Excluded(b"e".to_vec()),
                0,
            )
            .await
            .unwrap();
        let keys: Vec<&[u8]> = out.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(keys, [b"a".as_ref(), b"b", b"c", b"d"]);
    }

    pub async fn delete_range_is_atomic<S: KvStore>(store: &S) {
        let mut batch = WriteBatch::new();
        for k in [b"k1", b"k2", b"k3", b"k4"] {
            batch.put("del", k.as_slice(), b"v");
        }
        store
            .commit(batch, WriteOptions::default())
            .await
            .unwrap();

        let mut batch = WriteBatch::new();
        batch.delete_range("del", b"k2", b"k4");
        store
            .commit(batch, WriteOptions::default())
            .await
            .unwrap();

        let snap = store.snapshot().await.unwrap();
        let out = snap
            .range("del", Bound::Unbounded, Bound::Unbounded, 0)
            .await
            .unwrap();
        let keys: Vec<&[u8]> = out.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(keys, [b"k1".as_ref(), b"k4"]);
    }

    pub async fn snapshot_is_consistent_under_writes<S: KvStore>(store: &S) {
        let mut batch = WriteBatch::new();
        batch.put("iso", b"x", b"v0");
        store
            .commit(batch, WriteOptions::default())
            .await
            .unwrap();

        let snap = store.snapshot().await.unwrap();

        // Mutate after snapshot.
        let mut batch = WriteBatch::new();
        batch.put("iso", b"x", b"v1");
        batch.put("iso", b"y", b"v1");
        store
            .commit(batch, WriteOptions::default())
            .await
            .unwrap();

        // Snapshot must still observe v0 and not see y.
        assert_eq!(snap.get("iso", b"x").await.unwrap().as_deref(), Some(b"v0".as_ref()));
        assert_eq!(snap.get("iso", b"y").await.unwrap(), None);

        // A fresh snapshot sees the new state.
        let snap2 = store.snapshot().await.unwrap();
        assert_eq!(snap2.get("iso", b"x").await.unwrap().as_deref(), Some(b"v1".as_ref()));
        assert_eq!(snap2.get("iso", b"y").await.unwrap().as_deref(), Some(b"v1".as_ref()));
    }

    pub async fn count_matches_range_len<S: KvStore>(store: &S) {
        let mut batch = WriteBatch::new();
        for i in 0u8..16 {
            batch.put("cnt", &[i], &[i]);
        }
        store
            .commit(batch, WriteOptions::default())
            .await
            .unwrap();

        let snap = store.snapshot().await.unwrap();
        let out = snap
            .range("cnt", Bound::Unbounded, Bound::Unbounded, 0)
            .await
            .unwrap();
        let count = snap
            .count("cnt", Bound::Unbounded, Bound::Unbounded)
            .await
            .unwrap();
        assert_eq!(out.len() as u64, count);
        assert_eq!(count, 16);
    }
}
