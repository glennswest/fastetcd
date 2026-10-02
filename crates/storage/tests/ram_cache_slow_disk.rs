//! A GET of a hot key must not wait on the disk (fastetcd#82).
//!
//! The engine here is redb with every read made `SEEK` slower and
//! counted, as on the spinning disk in the issue, where each read that
//! missed memory cost a seek. The test counts engine reads per GET (the
//! assertion) and prints the latencies (the illustration):
//!
//! - value cache on, hot key: no engine read at all;
//! - value cache on, first GET after a restart: one read (the record;
//!   the key's index is already in RAM), which then fills the cache;
//! - value cache off: one read per GET, where before #82 it was two
//!   (the key's index, then the record).

use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use fastetcd_storage::mvcc::cache::DEFAULT_MAX_ENTRY_BYTES;
use fastetcd_storage::mvcc::{CacheConfig, Mutation, MvccStore};
use fastetcd_storage::redb_engine::RedbEngine;
use fastetcd_storage::{KvStore, Snapshot, StorageResult, WriteBatch, WriteOptions};

const SEEK: Duration = Duration::from_millis(4);
const KEYS: usize = 200;

struct SlowReads {
    inner: RedbEngine,
    reads: Arc<AtomicU64>,
}

struct SlowSnap {
    inner: Arc<dyn Snapshot>,
    reads: Arc<AtomicU64>,
}

#[async_trait]
impl Snapshot for SlowSnap {
    async fn get(&self, table: &str, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(SEEK).await;
        self.inner.get(table, key).await
    }
    async fn range(
        &self,
        table: &str,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(SEEK).await;
        self.inner.range(table, start, end, limit).await
    }
    async fn count(
        &self,
        table: &str,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    ) -> StorageResult<u64> {
        self.inner.count(table, start, end).await
    }
}

#[async_trait]
impl KvStore for SlowReads {
    async fn snapshot(&self) -> StorageResult<Arc<dyn Snapshot>> {
        Ok(Arc::new(SlowSnap {
            inner: self.inner.snapshot().await?,
            reads: self.reads.clone(),
        }))
    }
    async fn commit(&self, batch: WriteBatch, opts: WriteOptions) -> StorageResult<()> {
        self.inner.commit(batch, opts).await
    }
    async fn sync(&self) -> StorageResult<()> {
        self.inner.sync().await
    }
    async fn size_on_disk(&self) -> StorageResult<u64> {
        self.inner.size_on_disk().await
    }
    fn engine_name(&self) -> &'static str {
        "slow-reads"
    }
}

fn key(i: usize) -> Vec<u8> {
    format!("/registry/leases/kube-system/lease-{i:04}").into_bytes()
}

async fn open(path: &std::path::Path, budget: u64) -> (MvccStore, Arc<AtomicU64>) {
    let reads = Arc::new(AtomicU64::new(0));
    let engine = SlowReads {
        inner: RedbEngine::open(path).unwrap(),
        reads: reads.clone(),
    };
    let store = MvccStore::open_with(
        Arc::new(engine),
        CacheConfig {
            value_cache_bytes: budget,
            max_entry_bytes: DEFAULT_MAX_ENTRY_BYTES,
        },
    )
    .await
    .unwrap();
    (store, reads)
}

/// GET every key once; returns engine reads per GET and the p50/p99.
async fn get_all(s: &MvccStore, reads: &AtomicU64) -> (f64, Duration, Duration) {
    let before = reads.load(Ordering::Relaxed);
    let mut took = Vec::with_capacity(KEYS);
    for i in 0..KEYS {
        let t = Instant::now();
        let out = s.range(&key(i), b"", 0, 0, false, false).await.unwrap();
        took.push(t.elapsed());
        assert_eq!(out.kvs.len(), 1);
        assert_eq!(out.kvs[0].value, format!("holder-{i}").into_bytes());
    }
    took.sort();
    let per_get = (reads.load(Ordering::Relaxed) - before) as f64 / KEYS as f64;
    (per_get, took[KEYS / 2], took[KEYS * 99 / 100])
}

#[tokio::test]
async fn a_hot_get_reads_nothing_from_a_slow_disk() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("slow.redb");
    {
        let (s, _) = open(&path, 64 << 20).await;
        for i in 0..KEYS {
            s.apply(&[Mutation::Put {
                key: key(i),
                value: format!("holder-{i}").into_bytes(),
                lease: 0,
                ignore_value: false,
                ignore_lease: false,
                prev_kv: false,
            }])
            .await
            .unwrap();
        }
        // Written through: hot from the first GET.
        let (per_get, p50, p99) = get_all(&s, &AtomicU64::new(0)).await;
        println!("after writes: {per_get} reads/GET, p50 {p50:?}, p99 {p99:?}");
        assert_eq!(s.cache_stats().value_misses, 0);
    }

    // A restart: the index is loaded, the value cache is empty.
    let (s, reads) = open(&path, 64 << 20).await;
    let (cold, cold_p50, cold_p99) = get_all(&s, &reads).await;
    let (hot, hot_p50, hot_p99) = get_all(&s, &reads).await;
    println!("cache on, cold: {cold} reads/GET, p50 {cold_p50:?}, p99 {cold_p99:?}");
    println!("cache on, hot:  {hot} reads/GET, p50 {hot_p50:?}, p99 {hot_p99:?}");
    assert_eq!(cold, 1.0, "a cold GET reads only the record");
    assert_eq!(hot, 0.0, "a hot GET reads nothing from the engine");
    assert!(hot_p99 < SEEK, "a hot GET waited on a seek: p99 {hot_p99:?}");
    drop(s);

    let (s, reads) = open(&path, 0).await;
    let (off, off_p50, off_p99) = get_all(&s, &reads).await;
    let (off2, _, _) = get_all(&s, &reads).await;
    println!("cache off:      {off} reads/GET, p50 {off_p50:?}, p99 {off_p99:?}");
    assert_eq!((off, off2), (1.0, 1.0), "with no value cache, the index still saves a read");
}
