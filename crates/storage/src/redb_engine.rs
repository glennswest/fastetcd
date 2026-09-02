//! `redb` implementation of the [`KvStore`](crate::kvstore::KvStore) trait.
//!
//! redb is a native-Rust, single-file, ACID, copy-on-write B-tree. Each
//! commit is a full transaction with an fsync; readers run against a
//! consistent point-in-time view obtained from a read transaction.
//!
//! This engine is the default for fastetcd because it builds and runs
//! everywhere — no native deps, no Linux-specific syscalls, single file
//! to back up or copy.

use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use redb::{Database, ReadableTable, TableDefinition};
use tokio::sync::{Notify, RwLock};
use tokio::task;

use crate::kvstore::{
    BatchOp, KvStore, Snapshot, StorageError, StorageResult, StoreUsage, WriteBatch,
    WriteOptions,
};

/// redb-backed engine.
///
/// Clone-safe: cloning shares the same underlying [`Database`] handle.
#[derive(Clone)]
pub struct RedbEngine {
    inner: Arc<RedbInner>,
}

struct RedbInner {
    db: RwLock<Database>,
    path: PathBuf,
    /// Snapshots currently outstanding. redb refuses to compact while
    /// any read transaction is live, and a `RedbSnapshot` keeps its
    /// transaction open for as long as the caller holds it — well after
    /// the lock taken to create it was released. Under steady load
    /// there is therefore almost always one live, which is why a
    /// defragment on a busy server used to fail outright with "a
    /// transaction is still in progress" (fastetcd#14). Defragment now
    /// takes the write lock (which stops *new* snapshots) and waits
    /// here for the outstanding ones to drain.
    live_readers: Arc<AtomicUsize>,
    readers_drained: Arc<Notify>,
}

impl RedbEngine {
    /// Open or create a redb-backed engine at `path`. Parent directory
    /// must already exist.
    pub fn open<P: AsRef<Path>>(path: P) -> StorageResult<Self> {
        let path = path.as_ref().to_path_buf();
        let db = Database::create(&path).map_err(StorageError::io)?;
        Ok(Self {
            inner: Arc::new(RedbInner {
                db: RwLock::new(db),
                path,
                live_readers: Arc::new(AtomicUsize::new(0)),
                readers_drained: Arc::new(Notify::new()),
            }),
        })
    }

    fn table_def(name: &str) -> TableDefinition<'_, &'static [u8], &'static [u8]> {
        TableDefinition::new(name)
    }
}

#[async_trait]
impl KvStore for RedbEngine {
    async fn snapshot(&self) -> StorageResult<Arc<dyn Snapshot>> {
        let inner = self.inner.clone();
        let db_guard = inner.db.read().await;
        let txn = db_guard.begin_read().map_err(StorageError::io)?;
        drop(db_guard);
        inner.live_readers.fetch_add(1, Ordering::AcqRel);
        Ok(Arc::new(RedbSnapshot {
            live_readers: inner.live_readers.clone(),
            readers_drained: inner.readers_drained.clone(),
            _engine: inner.clone(),
            txn: Arc::new(txn),
        }))
    }

    async fn commit(&self, batch: WriteBatch, _opts: WriteOptions) -> StorageResult<()> {
        let inner = self.inner.clone();
        // Hold the read lock across the spawn_blocking. We use a
        // read lock because redb begin_write only needs &self;
        // exclusive access is only required for compact (taken via
        // db.write().await below).
        let db_guard = inner.db.read().await;
        let txn = db_guard.begin_write().map_err(StorageError::io)?;
        // Move ownership of txn into the blocking task. The
        // db_guard must outlive the txn; we keep it on the calling
        // task and the blocking task only sees the txn.
        let result = task::spawn_blocking(move || -> StorageResult<()> {
            let txn = txn;
            for op in batch.ops() {
                match op {
                    BatchOp::Put { table, key, value } => {
                        let mut t = txn
                            .open_table(RedbEngine::table_def(table))
                            .map_err(StorageError::io)?;
                        t.insert(key.as_slice(), value.as_slice())
                            .map_err(StorageError::io)?;
                    }
                    BatchOp::Delete { table, key } => {
                        let mut t = txn
                            .open_table(RedbEngine::table_def(table))
                            .map_err(StorageError::io)?;
                        t.remove(key.as_slice()).map_err(StorageError::io)?;
                    }
                    BatchOp::DeleteRange { table, start, end } => {
                        let mut t = txn
                            .open_table(RedbEngine::table_def(table))
                            .map_err(StorageError::io)?;
                        // Collect keys first to avoid invalidating
                        // the iterator while we mutate.
                        let mut to_remove: Vec<Vec<u8>> = Vec::new();
                        {
                            let r = t
                                .range::<&[u8]>(start.as_slice()..end.as_slice())
                                .map_err(StorageError::io)?;
                            for entry in r {
                                let (k, _v) = entry.map_err(StorageError::io)?;
                                to_remove.push(k.value().to_vec());
                            }
                        }
                        for k in to_remove {
                            t.remove(k.as_slice()).map_err(StorageError::io)?;
                        }
                    }
                }
            }
            txn.commit().map_err(StorageError::io)?;
            Ok(())
        })
        .await
        .map_err(|e| StorageError::Io(Box::new(e)))?;
        drop(db_guard);
        result?;
        Ok(())
    }

    async fn sync(&self) -> StorageResult<()> {
        // redb fsyncs on every `commit()`; nothing else to flush.
        Ok(())
    }

    async fn size_on_disk(&self) -> StorageResult<u64> {
        let path = self.inner.path.clone();
        let sz = task::spawn_blocking(move || -> StorageResult<u64> {
            std::fs::metadata(&path)
                .map(|m| m.len())
                .map_err(StorageError::io)
        })
        .await
        .map_err(|e| StorageError::Io(Box::new(e)))??;
        Ok(sz)
    }

    /// redb reports live vs. allocated space through a write
    /// transaction's `stats()`. That walk visits every B-tree page, so
    /// it is O(size of the database) and holds the write lock for its
    /// duration — call it on demand (deciding whether a defragment is
    /// worth it, answering `Maintenance.Status`), never on a hot path.
    /// The transaction is aborted, so nothing is written.
    ///
    /// "In use" is `allocated_pages * page_size` — the pages redb is
    /// holding — not `stored_bytes`, which counts the key and value
    /// bytes inside them. The difference is not academic: a store full
    /// of live 4 KiB values reports roughly a quarter of its file as
    /// `stored_bytes`, so using that number told an operator 22 MB was
    /// recoverable from a store where a defragment freed nothing
    /// (fastetcd#14). Only pages the allocator has actually released
    /// can come back to the filesystem.
    ///
    /// One consequence to know about: redb releases the pages a delete
    /// frees on a *later* commit, so immediately after a large delete
    /// this still counts them. The figure catches up within a commit or
    /// two, which is why the reclaim path compacts (a commit) before it
    /// measures and decides whether a defragment is worth it.
    async fn usage(&self) -> StorageResult<StoreUsage> {
        let file_bytes = self.size_on_disk().await?;
        let inner = self.inner.clone();
        let db_guard = inner.db.read().await;
        let txn = db_guard.begin_write().map_err(StorageError::io)?;
        let stats = task::spawn_blocking(move || -> StorageResult<(u64, u64)> {
            let txn = txn;
            let stats = txn.stats().map_err(StorageError::io)?;
            let in_use = stats
                .allocated_pages()
                .saturating_mul(stats.page_size() as u64);
            let fragmented = stats.fragmented_bytes();
            // Nothing was mutated; abort so the transaction doesn't
            // commit an empty write (which would cost an fsync).
            txn.abort().map_err(StorageError::io)?;
            Ok((in_use, fragmented))
        })
        .await
        .map_err(|e| StorageError::Io(Box::new(e)))?;
        drop(db_guard);
        let (in_use_bytes, fragmented_bytes) = stats?;
        Ok(StoreUsage {
            file_bytes,
            in_use_bytes,
            fragmented_bytes,
        })
    }

    fn engine_name(&self) -> &'static str {
        "redb"
    }

    async fn defragment(&self) -> StorageResult<()> {
        // Take the exclusive lock first: `snapshot()` needs the read
        // side, so from here on no *new* read transaction can be
        // created. Reads and writes already in flight finish normally.
        let mut db_guard = self.inner.db.write().await;

        // Then wait for the read transactions that were already
        // outstanding to be dropped. redb refuses to compact while any
        // of them is live, and on a busy server one essentially always
        // is — this is why a defragment on a loaded node failed
        // immediately with "a transaction is still in progress"
        // (fastetcd#14), exactly when an operator most needs it to run.
        const DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
        let deadline = tokio::time::Instant::now() + DRAIN_TIMEOUT;
        while self.inner.live_readers.load(Ordering::Acquire) > 0 {
            let notified = self.inner.readers_drained.notified();
            // Re-check after arming the waiter: the last reader may have
            // been dropped between the load above and this point, and
            // its notification would then have nobody to wake.
            if self.inner.live_readers.load(Ordering::Acquire) == 0 {
                break;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Err(StorageError::Misuse(format!(
                    "defragment could not start: {} read transaction(s) stayed open \
                     for {DRAIN_TIMEOUT:?} (a long-running range scan or watch). \
                     Retry, or stop the server and run `fastetcd defrag`.",
                    self.inner.live_readers.load(Ordering::Acquire)
                )));
            }
        }

        let _changed = db_guard.compact().map_err(StorageError::io)?;
        Ok(())
    }
}

/// A redb read transaction that satisfies the `Snapshot` contract.
struct RedbSnapshot {
    live_readers: Arc<AtomicUsize>,
    readers_drained: Arc<Notify>,
    // Kept alive so the read txn doesn't outlive the db handle.
    _engine: Arc<RedbInner>,
    txn: Arc<redb::ReadTransaction>,
}

impl Drop for RedbSnapshot {
    fn drop(&mut self) {
        // Releasing the last outstanding read transaction is what lets a
        // waiting defragment start.
        if self.live_readers.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.readers_drained.notify_waiters();
        }
    }
}

#[async_trait]
impl Snapshot for RedbSnapshot {
    async fn get(&self, table: &str, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let txn = self.txn.clone();
        let table = table.to_string();
        let key = key.to_vec();
        task::spawn_blocking(move || -> StorageResult<Option<Vec<u8>>> {
            let t = match txn.open_table(RedbEngine::table_def(&table)) {
                Ok(t) => t,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(e) => return Err(StorageError::io(e)),
            };
            match t.get(key.as_slice()).map_err(StorageError::io)? {
                Some(v) => Ok(Some(v.value().to_vec())),
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| StorageError::Io(Box::new(e)))?
    }

    async fn range(
        &self,
        table: &str,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let txn = self.txn.clone();
        let table = table.to_string();
        task::spawn_blocking(move || -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
            let t = match txn.open_table(RedbEngine::table_def(&table)) {
                Ok(t) => t,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(e) => return Err(StorageError::io(e)),
            };
            let bounds = (bound_ref(&start), bound_ref(&end));
            let iter = t.range::<&[u8]>(bounds).map_err(StorageError::io)?;
            let mut out = Vec::new();
            for (i, entry) in iter.enumerate() {
                if limit != 0 && i >= limit {
                    break;
                }
                let (k, v) = entry.map_err(StorageError::io)?;
                out.push((k.value().to_vec(), v.value().to_vec()));
            }
            Ok(out)
        })
        .await
        .map_err(|e| StorageError::Io(Box::new(e)))?
    }

    async fn count(
        &self,
        table: &str,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    ) -> StorageResult<u64> {
        let txn = self.txn.clone();
        let table = table.to_string();
        task::spawn_blocking(move || -> StorageResult<u64> {
            let t = match txn.open_table(RedbEngine::table_def(&table)) {
                Ok(t) => t,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
                Err(e) => return Err(StorageError::io(e)),
            };
            let bounds = (bound_ref(&start), bound_ref(&end));
            let iter = t.range::<&[u8]>(bounds).map_err(StorageError::io)?;
            let mut n: u64 = 0;
            for entry in iter {
                entry.map_err(StorageError::io)?;
                n += 1;
            }
            Ok(n)
        })
        .await
        .map_err(|e| StorageError::Io(Box::new(e)))?
    }

    /// Efficient last-row lookup via redb's B-tree, so reading the tail of
    /// a large table (e.g. the Raft log at startup) is O(log n) and does
    /// not materialize every value (fastetcd#13).
    async fn last(&self, table: &str) -> StorageResult<Option<(Vec<u8>, Vec<u8>)>> {
        let txn = self.txn.clone();
        let table = table.to_string();
        task::spawn_blocking(move || -> StorageResult<Option<(Vec<u8>, Vec<u8>)>> {
            let t = match txn.open_table(RedbEngine::table_def(&table)) {
                Ok(t) => t,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(e) => return Err(StorageError::io(e)),
            };
            let out = t
                .last()
                .map_err(StorageError::io)?
                .map(|(k, v)| (k.value().to_vec(), v.value().to_vec()));
            Ok(out)
        })
        .await
        .map_err(|e| StorageError::Io(Box::new(e)))?
    }
}

fn bound_ref(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Included(v) => Bound::Included(v.as_slice()),
        Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kvstore::conformance;
    use tempfile::tempdir;

    fn open_temp() -> (tempfile::TempDir, RedbEngine) {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("test.redb");
        let engine = RedbEngine::open(&path).expect("open redb");
        (dir, engine)
    }

    #[tokio::test]
    async fn engine_name_is_redb() {
        let (_dir, eng) = open_temp();
        assert_eq!(eng.engine_name(), "redb");
    }

    #[tokio::test]
    async fn conformance_put_get_delete() {
        let (_dir, eng) = open_temp();
        conformance::put_get_delete(&eng).await;
    }

    #[tokio::test]
    async fn conformance_range_scan() {
        let (_dir, eng) = open_temp();
        conformance::range_scan_lex_order(&eng).await;
    }

    #[tokio::test]
    async fn conformance_delete_range() {
        let (_dir, eng) = open_temp();
        conformance::delete_range_is_atomic(&eng).await;
    }

    #[tokio::test]
    async fn conformance_snapshot_isolation() {
        let (_dir, eng) = open_temp();
        conformance::snapshot_is_consistent_under_writes(&eng).await;
    }

    #[tokio::test]
    async fn conformance_count() {
        let (_dir, eng) = open_temp();
        conformance::count_matches_range_len(&eng).await;
    }

    #[tokio::test]
    async fn usage_separates_live_bytes_from_file_size_and_defragment_reclaims() {
        let (_dir, eng) = open_temp();
        // Write a few MB, then delete most of it. redb frees the pages
        // onto its free list but does not shrink the file, so the file
        // stays at its high-water mark while in-use bytes collapse —
        // exactly the gap a bounded volume runs out of space in
        // (fastetcd#14).
        let value = vec![7u8; 4096];
        for chunk in 0..8 {
            let mut b = WriteBatch::new();
            for i in 0u32..64 {
                b.put("big", &(chunk * 64 + i).to_be_bytes(), &value);
            }
            eng.commit(b, WriteOptions::default()).await.unwrap();
        }
        let full = eng.usage().await.unwrap();
        assert!(full.in_use_bytes >= 512 * 4096, "in_use={}", full.in_use_bytes);

        let mut b = WriteBatch::new();
        b.delete_range("big", &0u32.to_be_bytes(), &500u32.to_be_bytes());
        eng.commit(b, WriteOptions::default()).await.unwrap();

        // redb hands the freed pages back to its allocator on a later
        // commit, so the accounting only catches up once more writes
        // land. This is why the reclaim path compacts before measuring.
        for i in 0..4u32 {
            let mut b = WriteBatch::new();
            b.put("settle", &i.to_be_bytes(), b"x");
            eng.commit(b, WriteOptions::default()).await.unwrap();
        }

        let after_delete = eng.usage().await.unwrap();
        assert!(
            after_delete.in_use_bytes < full.in_use_bytes / 2,
            "delete should drop the pages held: {} -> {}",
            full.in_use_bytes,
            after_delete.in_use_bytes
        );
        assert!(
            after_delete.reclaimable_bytes() > 0,
            "file {} should exceed the pages held {}",
            after_delete.file_bytes,
            after_delete.in_use_bytes
        );

        eng.defragment().await.unwrap();
        let after_defrag = eng.usage().await.unwrap();
        assert!(
            after_defrag.file_bytes < after_delete.file_bytes,
            "defragment should shrink the file: {} -> {}",
            after_delete.file_bytes,
            after_defrag.file_bytes
        );
    }

    /// The gap between `file_bytes` and `in_use_bytes` is advertised to
    /// operators as "what a defragment would give back", and the space
    /// monitor uses it to decide whether the pause is worth it. So it
    /// must not over-promise: a store that is genuinely full of live
    /// data has to report ~nothing reclaimable, even though its logical
    /// key+value bytes are a fraction of the file (fastetcd#14 — the
    /// first cut used redb's `stored_bytes`, which measures the payload
    /// rather than the pages holding it, and told an operator 22 MB was
    /// recoverable from a store where a defragment freed zero).
    #[tokio::test]
    async fn reclaimable_bytes_does_not_over_promise_on_live_data() {
        let (_dir, eng) = open_temp();
        let value = vec![9u8; 4096];
        for chunk in 0..16u32 {
            let mut b = WriteBatch::new();
            for i in 0..64u32 {
                b.put("live", &(chunk * 64 + i).to_be_bytes(), &value);
            }
            eng.commit(b, WriteOptions::default()).await.unwrap();
        }

        let usage = eng.usage().await.unwrap();
        // Every byte of what was written is live, so the pages redb
        // holds must account for it. `stored_bytes` would report only
        // the payload (~4 MiB here) and make three quarters of the file
        // look recoverable.
        let payload = 16 * 64 * 4096;
        assert!(
            usage.in_use_bytes >= payload,
            "in_use {} must cover the {payload} bytes of live data",
            usage.in_use_bytes
        );

        // The advertised figure is an upper bound on what a defragment
        // can return, and it must hold: telling an operator that 22 MB
        // is recoverable and then freeing zero is the failure this
        // guards against.
        let before = usage.file_bytes;
        eng.defragment().await.unwrap();
        let actually_freed = before.saturating_sub(eng.size_on_disk().await.unwrap());
        assert!(
            usage.reclaimable_bytes() >= actually_freed,
            "a defragment freed {actually_freed} but only {} was advertised",
            usage.reclaimable_bytes()
        );

    }

    /// A defragment must not fail just because a request is in flight.
    /// redb refuses to compact while a read transaction is live, and on
    /// a busy server one essentially always is — so a defragment used to
    /// fail immediately, exactly when an operator most needs it to run
    /// (fastetcd#14).
    #[tokio::test]
    async fn defragment_waits_for_an_outstanding_snapshot_instead_of_failing() {
        let (_dir, eng) = open_temp();
        let mut b = WriteBatch::new();
        for i in 0..128u32 {
            b.put("t", &i.to_be_bytes(), &[3u8; 1024]);
        }
        eng.commit(b, WriteOptions::default()).await.unwrap();

        // Hold a snapshot, as an in-flight range scan or watch would.
        let snap = eng.snapshot().await.unwrap();

        let engine = eng.clone();
        let defrag = tokio::spawn(async move { engine.defragment().await });

        // Give the defragment time to take the lock and start waiting,
        // then release the reader.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(!defrag.is_finished(), "defragment must wait, not fail");
        drop(snap);

        defrag
            .await
            .expect("defragment task")
            .expect("defragment must succeed once the reader is gone");
    }

    #[tokio::test]
    async fn last_returns_the_max_key_without_scanning_all() {
        let (_dir, eng) = open_temp();
        // Empty table → None.
        let snap = eng.snapshot().await.unwrap();
        assert!(snap.last("t").await.unwrap().is_none());
        drop(snap);

        // Insert keys out of order; last() must return the largest key.
        let mut b = WriteBatch::new();
        for i in [5u32, 1, 9, 3, 7] {
            b.put("t", &i.to_be_bytes(), format!("v{i}").as_bytes());
        }
        eng.commit(b, WriteOptions::default()).await.unwrap();

        let snap = eng.snapshot().await.unwrap();
        let (k, v) = snap.last("t").await.unwrap().expect("non-empty");
        assert_eq!(k, 9u32.to_be_bytes().to_vec());
        assert_eq!(v, b"v9".to_vec());
    }
}
