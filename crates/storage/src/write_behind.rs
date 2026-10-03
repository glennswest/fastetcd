//! Write-behind in front of an engine: non-durable commits land in RAM
//! and reach the engine in batches (fastetcd#85).
//!
//! With the raft log in its own WAL, the state machine's applies need
//! not be durable on their own (#71): the WAL replays them. They used to
//! be non-durable redb commits all the same, and a redb commit takes
//! redb's single writer, which a durable commit (a checkpoint) holds for
//! its whole fsync. So an apply, and the client waiting on it, queued
//! behind every checkpoint's flush of the data file.
//!
//! Here a commit with `sync = false` becomes an immutable in-RAM
//! **layer** and returns. Reads see it at once: a [`Snapshot`] is the
//! list of layers plus an engine snapshot, taken together under one
//! lock, and every read merges the layers (newest wins) over the
//! engine. [`WriteBehind::flush`] writes the layers into the engine as
//! one non-durable commit and drops them from the list under the same
//! lock, so no snapshot ever sees a layer twice or misses one; [`sync`]
//! (the checkpoint) flushes and then commits durably. Every engine write
//! is serialized behind one flush lock, so the engine's writer is never
//! contended by an apply.
//!
//! A commit with `sync = true` flushes first and then commits to the
//! engine, so order is kept. If the layers hold more than the byte
//! budget, a commit flushes before returning (back-pressure).
//!
//! [`sync`]: KvStore::sync

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use tokio::sync::{Mutex, RwLock};

use crate::kvstore::{
    BatchOp, KvStore, Snapshot, StorageResult, StoreUsage, WriteBatch, WriteOptions,
};

/// Default byte budget of the layers before a commit has to flush.
pub const DEFAULT_MAX_BYTES: usize = 64 * 1024 * 1024;

/// One committed batch, normalized: within a table, the point results
/// (`Some` = put, `None` = deleted) win over the range deletes, which
/// cover only what was there before this batch.
#[derive(Default, Debug)]
struct Layer {
    tables: HashMap<String, TableLayer>,
    bytes: usize,
}

#[derive(Default, Debug)]
struct TableLayer {
    entries: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    range_dels: Vec<(Vec<u8>, Vec<u8>)>,
}

impl TableLayer {
    fn covered(&self, key: &[u8]) -> bool {
        self.range_dels
            .iter()
            .any(|(s, e)| key >= s.as_slice() && key < e.as_slice())
    }
}

impl Layer {
    fn from_batch(batch: &WriteBatch) -> Layer {
        let mut layer = Layer::default();
        for op in batch.ops() {
            match op {
                BatchOp::Put { table, key, value } => {
                    layer.bytes += key.len() + value.len() + 64;
                    let t = layer.tables.entry(table.clone()).or_default();
                    t.entries.insert(key.clone(), Some(value.clone()));
                }
                BatchOp::Delete { table, key } => {
                    layer.bytes += key.len() + 64;
                    let t = layer.tables.entry(table.clone()).or_default();
                    t.entries.insert(key.clone(), None);
                }
                BatchOp::DeleteRange { table, start, end } => {
                    layer.bytes += start.len() + end.len() + 64;
                    let t = layer.tables.entry(table.clone()).or_default();
                    if start < end {
                        // Earlier point writes of this batch in the range
                        // are gone; later ones go back into `entries`.
                        let keys: Vec<Vec<u8>> = t
                            .entries
                            .range::<[u8], _>((
                                Bound::Included(start.as_slice()),
                                Bound::Excluded(end.as_slice()),
                            ))
                            .map(|(k, _)| k.clone())
                            .collect();
                        for k in keys {
                            t.entries.remove(&k);
                        }
                        t.range_dels.push((start.clone(), end.clone()));
                    }
                }
            }
        }
        layer
    }

    /// The same effect as one batch of engine ops.
    fn to_ops(&self, batch: &mut WriteBatch) {
        for (table, t) in &self.tables {
            for (s, e) in &t.range_dels {
                batch.delete_range(table, s, e);
            }
            for (k, v) in &t.entries {
                match v {
                    Some(v) => batch.put(table, k, v),
                    None => batch.delete(table, k),
                };
            }
        }
    }
}

struct Layers {
    /// Oldest first.
    list: Arc<Vec<Arc<Layer>>>,
    bytes: usize,
}

/// Counters for `/metrics`.
#[derive(Debug, Default)]
pub struct WriteBehindStats {
    pub layer_bytes: AtomicU64,
    pub layers: AtomicU64,
    pub flushes: AtomicU64,
    pub flush_nanos: AtomicU64,
    /// Commits that had to flush first because the layers were over
    /// their budget.
    pub backpressure: AtomicU64,
}

struct Inner {
    base: Arc<dyn KvStore>,
    layers: RwLock<Layers>,
    /// Held by everything that writes to the engine.
    flush_lock: Mutex<()>,
    max_bytes: usize,
    stats: Arc<WriteBehindStats>,
}

/// See the module docs. Cheap to clone.
#[derive(Clone)]
pub struct WriteBehind {
    inner: Arc<Inner>,
}

impl WriteBehind {
    pub fn new(base: Arc<dyn KvStore>, max_bytes: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                base,
                layers: RwLock::new(Layers { list: Arc::new(Vec::new()), bytes: 0 }),
                flush_lock: Mutex::new(()),
                max_bytes,
                stats: Arc::new(WriteBehindStats::default()),
            }),
        }
    }

    pub fn stats(&self) -> Arc<WriteBehindStats> {
        self.inner.stats.clone()
    }

    /// Write every layer into the engine (one non-durable commit).
    pub async fn flush(&self) -> StorageResult<()> {
        let _g = self.inner.flush_lock.lock().await;
        self.flush_locked().await
    }

    async fn flush_locked(&self) -> StorageResult<()> {
        let list = self.inner.layers.read().await.list.clone();
        if list.is_empty() {
            return Ok(());
        }
        let t = Instant::now();
        let mut batch = WriteBatch::new();
        for layer in list.iter() {
            layer.to_ops(&mut batch);
        }
        // The engine changes and the layers go in one step for readers:
        // a snapshot is taken under the read side of this lock.
        let mut layers = self.inner.layers.write().await;
        self.inner.base.commit(batch, WriteOptions { sync: false }).await?;
        let n = list.len();
        let rest: Vec<Arc<Layer>> = layers.list[n..].to_vec();
        let flushed: usize = list.iter().map(|l| l.bytes).sum();
        layers.list = Arc::new(rest);
        layers.bytes -= flushed;
        self.publish(&layers);
        drop(layers);
        self.inner.stats.flushes.fetch_add(1, Ordering::Relaxed);
        self.inner
            .stats
            .flush_nanos
            .fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
        Ok(())
    }

    fn publish(&self, layers: &Layers) {
        self.inner.stats.layer_bytes.store(layers.bytes as u64, Ordering::Relaxed);
        self.inner.stats.layers.store(layers.list.len() as u64, Ordering::Relaxed);
    }
}

#[async_trait]
impl KvStore for WriteBehind {
    async fn snapshot(&self) -> StorageResult<Arc<dyn Snapshot>> {
        let layers = self.inner.layers.read().await;
        let base = self.inner.base.snapshot().await?;
        let list = layers.list.clone();
        drop(layers);
        Ok(Arc::new(OverlaySnapshot { base, layers: list }))
    }

    async fn commit(&self, batch: WriteBatch, opts: WriteOptions) -> StorageResult<()> {
        if opts.sync {
            let _g = self.inner.flush_lock.lock().await;
            self.flush_locked().await?;
            return self.inner.base.commit(batch, opts).await;
        }
        let layer = Arc::new(Layer::from_batch(&batch));
        let over = {
            let mut layers = self.inner.layers.write().await;
            let mut list: Vec<Arc<Layer>> = layers.list.as_ref().clone();
            layers.bytes += layer.bytes;
            list.push(layer);
            layers.list = Arc::new(list);
            self.publish(&layers);
            layers.bytes > self.inner.max_bytes
        };
        if over {
            self.inner.stats.backpressure.fetch_add(1, Ordering::Relaxed);
            self.flush().await?;
        }
        Ok(())
    }

    async fn sync(&self) -> StorageResult<()> {
        let _g = self.inner.flush_lock.lock().await;
        self.flush_locked().await?;
        self.inner.base.sync().await
    }

    async fn size_on_disk(&self) -> StorageResult<u64> {
        self.inner.base.size_on_disk().await
    }

    async fn usage(&self) -> StorageResult<StoreUsage> {
        // Opens an engine write transaction: keep it off the flush path.
        let _g = self.inner.flush_lock.lock().await;
        self.inner.base.usage().await
    }

    fn engine_name(&self) -> &'static str {
        self.inner.base.engine_name()
    }

    async fn defragment(&self) -> StorageResult<()> {
        let _g = self.inner.flush_lock.lock().await;
        self.flush_locked().await?;
        self.inner.base.defragment().await
    }
}

/// A consistent view: the engine as of the snapshot plus the layers not
/// yet in it.
struct OverlaySnapshot {
    base: Arc<dyn Snapshot>,
    layers: Arc<Vec<Arc<Layer>>>,
}

fn in_bounds(key: &[u8], start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> bool {
    let lo = match start {
        Bound::Included(s) => key >= s.as_slice(),
        Bound::Excluded(s) => key > s.as_slice(),
        Bound::Unbounded => true,
    };
    let hi = match end {
        Bound::Included(e) => key <= e.as_slice(),
        Bound::Excluded(e) => key < e.as_slice(),
        Bound::Unbounded => true,
    };
    lo && hi
}

/// What the layers say about a table's keys in a range.
struct TableView {
    /// Point results, newest layer winning.
    entries: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// Range deletes: an engine key under one, and not in `entries`, is
    /// gone.
    dels: Vec<(Vec<u8>, Vec<u8>)>,
}

impl TableView {
    fn hides(&self, key: &[u8]) -> bool {
        self.entries.contains_key(key)
            || self
                .dels
                .iter()
                .any(|(s, e)| key >= s.as_slice() && key < e.as_slice())
    }
}

impl OverlaySnapshot {
    fn touches(&self, table: &str) -> bool {
        self.layers.iter().any(|l| l.tables.contains_key(table))
    }

    fn view(&self, table: &str, start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> TableView {
        let mut v = TableView { entries: BTreeMap::new(), dels: Vec::new() };
        for layer in self.layers.iter() {
            let Some(t) = layer.tables.get(table) else { continue };
            for (s, e) in &t.range_dels {
                let gone: Vec<Vec<u8>> = v
                    .entries
                    .range::<[u8], _>((Bound::Included(s.as_slice()), Bound::Excluded(e.as_slice())))
                    .map(|(k, _)| k.clone())
                    .collect();
                for k in gone {
                    v.entries.insert(k, None);
                }
                v.dels.push((s.clone(), e.clone()));
            }
            for (k, val) in &t.entries {
                if in_bounds(k, start, end) {
                    v.entries.insert(k.clone(), val.clone());
                }
            }
        }
        v
    }

    /// Merge the engine's rows in `[start, end)` with the layers, up to
    /// `limit` rows (0 = all). Reads the engine in pages, so a limited
    /// range does not read the whole table.
    async fn merged(
        &self,
        table: &str,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        if !self.touches(table) {
            return self.base.range(table, start, end, limit).await;
        }
        let view = self.view(table, &start, &end);
        // Layer keys with a value, in order; `next` is the first not yet
        // emitted. Owned, so nothing borrowed lives across an await.
        let pending: Vec<(Vec<u8>, Vec<u8>)> = view
            .entries
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|v| (k.clone(), v.clone())))
            .collect();
        let mut next = 0usize;
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let page = if limit == 0 { 0 } else { (limit * 2).max(256) };
        let mut cursor = start.clone();
        loop {
            let rows = self.base.range(table, cursor.clone(), end.clone(), page).await?;
            let exhausted = page == 0 || rows.len() < page;
            let page_hi = rows.last().map(|(k, _)| k.clone());
            for (k, v) in rows {
                while next < pending.len() && pending[next].0 < k {
                    out.push(pending[next].clone());
                    next += 1;
                    if limit > 0 && out.len() >= limit {
                        return Ok(out);
                    }
                }
                if !view.hides(&k) {
                    out.push((k, v));
                    if limit > 0 && out.len() >= limit {
                        return Ok(out);
                    }
                }
            }
            if exhausted {
                break;
            }
            // Layer keys up to this page's last key belong before the
            // next page.
            let hi = page_hi.expect("a full page has a last row");
            while next < pending.len() && pending[next].0 <= hi {
                out.push(pending[next].clone());
                next += 1;
                if limit > 0 && out.len() >= limit {
                    return Ok(out);
                }
            }
            cursor = Bound::Excluded(hi);
        }
        while next < pending.len() {
            out.push(pending[next].clone());
            next += 1;
            if limit > 0 && out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl Snapshot for OverlaySnapshot {
    async fn get(&self, table: &str, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        for layer in self.layers.iter().rev() {
            if let Some(t) = layer.tables.get(table) {
                if let Some(v) = t.entries.get(key) {
                    return Ok(v.clone());
                }
                if t.covered(key) {
                    return Ok(None);
                }
            }
        }
        self.base.get(table, key).await
    }

    async fn range(
        &self,
        table: &str,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.merged(table, start, end, limit).await
    }

    async fn count(
        &self,
        table: &str,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    ) -> StorageResult<u64> {
        if !self.touches(table) {
            return self.base.count(table, start, end).await;
        }
        Ok(self.merged(table, start, end, 0).await?.len() as u64)
    }

    async fn last(&self, table: &str) -> StorageResult<Option<(Vec<u8>, Vec<u8>)>> {
        if !self.touches(table) {
            return self.base.last(table).await;
        }
        let view = self.view(table, &Bound::Unbounded, &Bound::Unbounded);
        let top = view
            .entries
            .iter()
            .rev()
            .find_map(|(k, v)| v.as_ref().map(|v| (k.clone(), v.clone())));
        match self.base.last(table).await? {
            Some((k, v)) if !view.hides(&k) => {
                Ok(match top {
                    Some(t) if t.0 > k => Some(t),
                    _ => Some((k, v)),
                })
            }
            // The engine's last key is overwritten or deleted: merge it all.
            Some(_) => Ok(self.merged(table, Bound::Unbounded, Bound::Unbounded, 0).await?.pop()),
            None => Ok(top),
        }
    }

    async fn table_names(&self) -> StorageResult<Vec<String>> {
        let mut names = self.base.table_names().await?;
        for layer in self.layers.iter() {
            for (name, t) in &layer.tables {
                if t.entries.values().any(|v| v.is_some()) && !names.contains(name) {
                    names.push(name.clone());
                }
            }
        }
        Ok(names)
    }
}

#[cfg(all(test, feature = "redb-engine"))]
mod tests {
    use super::*;
    use crate::redb_engine::RedbEngine;

    fn open() -> (tempfile::TempDir, WriteBehind) {
        let dir = tempfile::tempdir().unwrap();
        let base: Arc<dyn KvStore> = Arc::new(RedbEngine::open(dir.path().join("db")).unwrap());
        (dir, WriteBehind::new(base, DEFAULT_MAX_BYTES))
    }

    #[tokio::test]
    async fn conformance() {
        let (_d, s) = open();
        crate::kvstore::conformance::run_all(&s).await;
    }

    /// A tiny deterministic generator, so a failure reproduces.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn key(n: u64) -> Vec<u8> {
        format!("k{n:03}").into_bytes()
    }

    /// Random puts, deletes and range deletes, non-durable and durable,
    /// with flushes and syncs in between; every read must equal a plain
    /// map's.
    #[tokio::test]
    async fn reads_match_a_model_through_layers_and_flushes() {
        let (_d, s) = open();
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        for step in 0..3000u64 {
            let mut batch = WriteBatch::new();
            for _ in 0..(1 + rng.below(4)) {
                match rng.below(10) {
                    0..=5 => {
                        let k = key(rng.below(200));
                        let v = format!("v{step}").into_bytes();
                        batch.put("t", &k, &v);
                        model.insert(k, v);
                    }
                    6..=7 => {
                        let k = key(rng.below(200));
                        batch.delete("t", &k);
                        model.remove(&k);
                    }
                    _ => {
                        let a = rng.below(200);
                        let b = a + rng.below(20);
                        let (s_, e_) = (key(a), key(b));
                        batch.delete_range("t", &s_, &e_);
                        let gone: Vec<Vec<u8>> =
                            model.range(s_.clone()..e_.clone()).map(|(k, _)| k.clone()).collect();
                        for k in gone {
                            model.remove(&k);
                        }
                    }
                }
            }
            let sync = rng.below(20) == 0;
            s.commit(batch, WriteOptions { sync }).await.unwrap();
            match rng.below(30) {
                0 => s.flush().await.unwrap(),
                1 => s.sync().await.unwrap(),
                _ => {}
            }
            if step % 7 == 0 {
                let snap = s.snapshot().await.unwrap();
                let k = key(rng.below(200));
                assert_eq!(snap.get("t", &k).await.unwrap(), model.get(&k).cloned(), "get at {step}");
                let a = rng.below(200);
                let b = a + rng.below(60);
                let limit = rng.below(4) as usize * 7;
                let got = snap
                    .range("t", Bound::Included(key(a)), Bound::Excluded(key(b)), limit)
                    .await
                    .unwrap();
                let mut want: Vec<(Vec<u8>, Vec<u8>)> = model
                    .range(key(a)..key(b))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                if limit > 0 {
                    want.truncate(limit);
                }
                assert_eq!(got, want, "range [{a},{b}) limit {limit} at {step}");
                let all = snap.range("t", Bound::Unbounded, Bound::Unbounded, 0).await.unwrap();
                assert_eq!(all.len(), model.len(), "full range at {step}");
                assert_eq!(
                    snap.count("t", Bound::Unbounded, Bound::Unbounded).await.unwrap(),
                    model.len() as u64
                );
                assert_eq!(
                    snap.last("t").await.unwrap(),
                    model.iter().next_back().map(|(k, v)| (k.clone(), v.clone())),
                    "last at {step}"
                );
            }
        }
        // And all of it reaches the engine.
        s.sync().await.unwrap();
        assert_eq!(s.stats().layers.load(Ordering::Relaxed), 0);
        let base = s.inner.base.snapshot().await.unwrap();
        let all = base.range("t", Bound::Unbounded, Bound::Unbounded, 0).await.unwrap();
        let want: Vec<(Vec<u8>, Vec<u8>)> = model.into_iter().collect();
        assert_eq!(all, want);
    }

    #[tokio::test]
    async fn a_snapshot_keeps_its_view_across_a_flush() {
        let (_d, s) = open();
        let mut b = WriteBatch::new();
        b.put("t", b"a", b"1");
        s.commit(b, WriteOptions { sync: false }).await.unwrap();
        let snap = s.snapshot().await.unwrap();
        let mut b = WriteBatch::new();
        b.put("t", b"a", b"2");
        b.put("t", b"b", b"2");
        s.commit(b, WriteOptions { sync: false }).await.unwrap();
        s.flush().await.unwrap();
        assert_eq!(snap.get("t", b"a").await.unwrap().as_deref(), Some(&b"1"[..]));
        assert_eq!(snap.get("t", b"b").await.unwrap(), None);
        let now = s.snapshot().await.unwrap();
        assert_eq!(now.get("t", b"a").await.unwrap().as_deref(), Some(&b"2"[..]));
    }

    #[tokio::test]
    async fn over_budget_commits_flush() {
        let dir = tempfile::tempdir().unwrap();
        let base: Arc<dyn KvStore> = Arc::new(RedbEngine::open(dir.path().join("db")).unwrap());
        let s = WriteBehind::new(base, 4096);
        for i in 0..100u32 {
            let mut b = WriteBatch::new();
            b.put("t", &i.to_be_bytes(), &[0u8; 100]);
            s.commit(b, WriteOptions { sync: false }).await.unwrap();
        }
        assert!(s.stats().backpressure.load(Ordering::Relaxed) > 0);
        assert!(s.stats().layer_bytes.load(Ordering::Relaxed) <= 4096 + 200);
        let snap = s.snapshot().await.unwrap();
        assert_eq!(snap.count("t", Bound::Unbounded, Bound::Unbounded).await.unwrap(), 100);
    }
}
