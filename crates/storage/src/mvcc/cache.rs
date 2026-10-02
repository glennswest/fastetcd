//! What the MVCC layer keeps in RAM (fastetcd#82).
//!
//! - [`KeyIndex`es](super::record::KeyIndex) of **every** key, the way
//!   etcd keeps its `treeIndex`: a `BTreeMap` of key → generations. A
//!   read finds the revision it needs without touching the engine, and
//!   only the record itself can cost a disk read. Memory is roughly the
//!   key bytes plus 16 bytes per retained revision.
//! - The **latest record** of recently used keys, an LRU bounded in
//!   bytes ([`ValueCache`]). A hit costs no engine read at all.
//!
//! Neither is ever ahead of the engine. The MVCC store changes them only
//! after the engine commit that they describe returned, and only while it
//! holds its write-state lock, so a reader that takes the index guard
//! under that lock, next to its engine snapshot, sees the two agree. A
//! value-cache entry carries the exact [`Revision`] it was written at and
//! is used only when the index names that revision for the read: a stale
//! entry can miss, never answer.
//!
//! The on-disk `mvcc_idx` table is still written as before; the resident
//! index is rebuilt from it at open, so the file format is unchanged.

use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, HashMap};
use std::hash::BuildHasher;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::record::{Generation, KeyIndex, KvRecord};
use super::revision::Revision;

/// Default value-cache budget when none is configured: 128 MiB.
pub const DEFAULT_VALUE_CACHE_BYTES: u64 = 128 * 1024 * 1024;
/// Default largest value the cache takes: 256 KiB.
pub const DEFAULT_MAX_ENTRY_BYTES: u64 = 256 * 1024;
/// Shards of the value cache, so concurrent readers rarely share a lock.
const SHARDS: usize = 16;
/// Fixed per-entry overhead counted against the budget: the map slot,
/// the LRU order entry, the `Arc` and the record's other fields.
const ENTRY_OVERHEAD: u64 = 160;
/// Fixed per-key overhead counted for the index: the map node share and
/// the `Vec` headers.
const INDEX_KEY_OVERHEAD: u64 = 64;

/// Sizing of the RAM cache.
#[derive(Debug, Clone, Copy)]
pub struct CacheConfig {
    /// Byte budget of the latest-value cache. 0 turns it off; the key
    /// index is always resident.
    pub value_cache_bytes: u64,
    /// Records whose value is larger than this are never cached.
    pub max_entry_bytes: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            value_cache_bytes: DEFAULT_VALUE_CACHE_BYTES,
            max_entry_bytes: DEFAULT_MAX_ENTRY_BYTES,
        }
    }
}

/// Counters and sizes for `/metrics`.
#[derive(Debug, Clone, Copy, Default)]
pub struct CacheStats {
    pub value_hits: u64,
    pub value_misses: u64,
    pub value_evictions: u64,
    pub value_bytes: u64,
    pub value_entries: u64,
    pub value_budget_bytes: u64,
    pub index_keys: u64,
    pub index_bytes: u64,
}

// ---------- resident key index ----------

/// Every key's generations, in key order.
#[derive(Default)]
pub struct ResidentIndex {
    map: RwLock<BTreeMap<Vec<u8>, Vec<Generation>>>,
    bytes: AtomicU64,
}

/// A read view of the index: hold it only for synchronous work.
pub struct IndexRead<'a>(RwLockReadGuard<'a, BTreeMap<Vec<u8>, Vec<Generation>>>);

/// A write view of the index, for the MVCC store's write paths.
pub struct IndexWrite<'a> {
    map: RwLockWriteGuard<'a, BTreeMap<Vec<u8>, Vec<Generation>>>,
    bytes: &'a AtomicU64,
}

fn index_entry_bytes(key: &[u8], gens: &[Generation]) -> u64 {
    let revs: usize = gens.iter().map(|g| g.revs.len()).sum();
    key.len() as u64
        + INDEX_KEY_OVERHEAD
        + (gens.len() * std::mem::size_of::<Generation>()) as u64
        + (revs * std::mem::size_of::<Revision>()) as u64
}

impl ResidentIndex {
    pub fn read(&self) -> IndexRead<'_> {
        IndexRead(self.map.read().unwrap_or_else(|p| p.into_inner()))
    }

    pub fn write(&self) -> IndexWrite<'_> {
        IndexWrite {
            map: self.map.write().unwrap_or_else(|p| p.into_inner()),
            bytes: &self.bytes,
        }
    }

    pub fn approx_bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    pub fn len(&self) -> usize {
        self.read().0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl IndexRead<'_> {
    /// The key's index (with `key` filled in), or `None` if the key has
    /// no history.
    pub fn get(&self, key: &[u8]) -> Option<KeyIndex> {
        self.0.get(key).map(|g| KeyIndex {
            key: key.to_vec(),
            generations: g.clone(),
        })
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        self.0.contains_key(key)
    }

    /// Keys in `[start, end]` with their generations, in key order.
    pub fn range(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> impl Iterator<Item = (&Vec<u8>, &Vec<Generation>)> {
        self.0
            .range::<[u8], _>((start, end))
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Vec<Generation>)> {
        self.0.iter()
    }
}

impl IndexWrite<'_> {
    /// Set a key's index; an index with no generations removes the key.
    pub fn set(&mut self, key: &[u8], gens: Vec<Generation>) {
        if gens.is_empty() {
            self.remove(key);
            return;
        }
        let add = index_entry_bytes(key, &gens);
        if let Some(old) = self.map.insert(key.to_vec(), gens) {
            self.bytes
                .fetch_sub(index_entry_bytes(key, &old), Ordering::Relaxed);
        }
        self.bytes.fetch_add(add, Ordering::Relaxed);
    }

    pub fn remove(&mut self, key: &[u8]) {
        if let Some(old) = self.map.remove(key) {
            self.bytes
                .fetch_sub(index_entry_bytes(key, &old), Ordering::Relaxed);
        }
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.bytes.store(0, Ordering::Relaxed);
    }
}

// ---------- latest-value cache ----------

struct Slot {
    rev: Revision,
    rec: Arc<KvRecord>,
    tick: u64,
    size: u64,
}

#[derive(Default)]
struct Shard {
    map: HashMap<Vec<u8>, Slot>,
    /// Least recently used first.
    order: BTreeMap<u64, Vec<u8>>,
    bytes: u64,
    tick: u64,
}

impl Shard {
    fn next_tick(&mut self) -> u64 {
        self.tick += 1;
        self.tick
    }

    fn remove(&mut self, key: &[u8]) -> Option<u64> {
        let slot = self.map.remove(key)?;
        self.order.remove(&slot.tick);
        self.bytes -= slot.size;
        Some(slot.size)
    }
}

/// The latest record of recently used keys, bounded in bytes. Each of
/// [`SHARDS`] shards has an equal share of the budget and evicts its
/// least recently used entries to stay inside it.
pub struct ValueCache {
    shards: Vec<Mutex<Shard>>,
    hasher: RandomState,
    shard_budget: u64,
    budget: u64,
    max_entry: u64,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    bytes: AtomicU64,
    entries: AtomicU64,
}

impl ValueCache {
    pub fn new(cfg: CacheConfig) -> Self {
        let shard_budget = cfg.value_cache_bytes / SHARDS as u64;
        Self {
            shards: (0..SHARDS).map(|_| Mutex::new(Shard::default())).collect(),
            hasher: RandomState::new(),
            shard_budget,
            budget: shard_budget * SHARDS as u64,
            // An entry may take at most a quarter of its shard.
            max_entry: cfg.max_entry_bytes.min(shard_budget / 4),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            entries: AtomicU64::new(0),
        }
    }

    pub fn enabled(&self) -> bool {
        self.budget > 0
    }

    fn shard(&self, key: &[u8]) -> std::sync::MutexGuard<'_, Shard> {
        let i = (self.hasher.hash_one(key) as usize) % SHARDS;
        self.shards[i].lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The cached record of `key` if it is the one written at `rev`.
    /// Counts a hit or a miss.
    pub fn get(&self, key: &[u8], rev: Revision) -> Option<Arc<KvRecord>> {
        if !self.enabled() {
            return None;
        }
        let mut shard = self.shard(key);
        let tick = shard.next_tick();
        let found = match shard.map.get_mut(key) {
            Some(slot) if slot.rev == rev => {
                let old = std::mem::replace(&mut slot.tick, tick);
                Some((old, slot.rec.clone()))
            }
            _ => None,
        };
        match found {
            Some((old, rec)) => {
                if let Some(k) = shard.order.remove(&old) {
                    shard.order.insert(tick, k);
                }
                drop(shard);
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(rec)
            }
            None => {
                drop(shard);
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Cache `rec`, written at `rev`, as `key`'s latest record. With
    /// `only_if_newer`, an entry at a revision >= `rev` is kept instead
    /// (a read filling the cache must not replace what a later write
    /// put there). A record too large to cache removes any entry.
    pub fn insert(&self, key: &[u8], rev: Revision, rec: Arc<KvRecord>, only_if_newer: bool) {
        if !self.enabled() {
            return;
        }
        let size = ENTRY_OVERHEAD + 2 * key.len() as u64 + rec.value.len() as u64;
        let mut shard = self.shard(key);
        if only_if_newer {
            if let Some(slot) = shard.map.get(key) {
                if slot.rev >= rev {
                    return;
                }
            }
        }
        if let Some(freed) = shard.remove(key) {
            self.bytes.fetch_sub(freed, Ordering::Relaxed);
            self.entries.fetch_sub(1, Ordering::Relaxed);
        }
        if rec.value.len() as u64 > self.max_entry {
            return;
        }
        let tick = shard.next_tick();
        shard.order.insert(tick, key.to_vec());
        shard.map.insert(
            key.to_vec(),
            Slot {
                rev,
                rec,
                tick,
                size,
            },
        );
        shard.bytes += size;
        self.bytes.fetch_add(size, Ordering::Relaxed);
        self.entries.fetch_add(1, Ordering::Relaxed);
        while shard.bytes > self.shard_budget {
            let Some((_, victim)) = shard.order.pop_first() else {
                break;
            };
            if let Some(slot) = shard.map.remove(&victim) {
                shard.bytes -= slot.size;
                self.bytes.fetch_sub(slot.size, Ordering::Relaxed);
                self.entries.fetch_sub(1, Ordering::Relaxed);
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn remove(&self, key: &[u8]) {
        if !self.enabled() {
            return;
        }
        if let Some(freed) = self.shard(key).remove(key) {
            self.bytes.fetch_sub(freed, Ordering::Relaxed);
            self.entries.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub fn clear(&self) {
        for s in &self.shards {
            let mut s = s.lock().unwrap_or_else(|p| p.into_inner());
            let n = s.map.len() as u64;
            let b = s.bytes;
            *s = Shard::default();
            self.bytes.fetch_sub(b, Ordering::Relaxed);
            self.entries.fetch_sub(n, Ordering::Relaxed);
        }
    }

    pub fn budget(&self) -> u64 {
        self.budget
    }
}

/// The index and the value cache together, owned by the MVCC store.
pub struct MvccCache {
    pub index: ResidentIndex,
    pub values: ValueCache,
}

impl MvccCache {
    pub fn new(cfg: CacheConfig) -> Self {
        Self {
            index: ResidentIndex::default(),
            values: ValueCache::new(cfg),
        }
    }

    pub fn stats(&self) -> CacheStats {
        let v = &self.values;
        CacheStats {
            value_hits: v.hits.load(Ordering::Relaxed),
            value_misses: v.misses.load(Ordering::Relaxed),
            value_evictions: v.evictions.load(Ordering::Relaxed),
            value_bytes: v.bytes.load(Ordering::Relaxed),
            value_entries: v.entries.load(Ordering::Relaxed),
            value_budget_bytes: v.budget(),
            index_keys: self.index.len() as u64,
            index_bytes: self.index.approx_bytes(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(key: &[u8], value_len: usize, rev: i64) -> Arc<KvRecord> {
        Arc::new(KvRecord {
            key: key.to_vec(),
            value: vec![7u8; value_len],
            create_revision: rev,
            mod_revision: rev,
            version: 1,
            lease: 0,
            deleted: false,
        })
    }

    fn cache(budget: u64) -> ValueCache {
        ValueCache::new(CacheConfig {
            value_cache_bytes: budget,
            max_entry_bytes: DEFAULT_MAX_ENTRY_BYTES,
        })
    }

    #[test]
    fn hit_only_at_the_exact_revision() {
        let c = cache(1 << 20);
        c.insert(b"k", Revision::new(5, 0), rec(b"k", 10, 5), false);
        assert!(c.get(b"k", Revision::new(5, 0)).is_some());
        assert!(c.get(b"k", Revision::new(5, 1)).is_none());
        assert!(c.get(b"k", Revision::new(6, 0)).is_none());
        assert!(c.get(b"other", Revision::new(5, 0)).is_none());
        assert_eq!(c.hits.load(Ordering::Relaxed), 1);
        assert_eq!(c.misses.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn a_read_fill_never_replaces_a_newer_write() {
        let c = cache(1 << 20);
        c.insert(b"k", Revision::new(9, 0), rec(b"k", 10, 9), false);
        c.insert(b"k", Revision::new(5, 0), rec(b"k", 10, 5), true);
        assert!(c.get(b"k", Revision::new(9, 0)).is_some());
        assert!(c.get(b"k", Revision::new(5, 0)).is_none());
    }

    #[test]
    fn stays_inside_its_budget_and_evicts_least_recently_used() {
        let budget = 16 * 64 * 1024; // 64 KiB a shard
        let c = cache(budget);
        for i in 0..2000u32 {
            let k = format!("key-{i}");
            c.insert(k.as_bytes(), Revision::new(i as i64 + 1, 0), rec(k.as_bytes(), 1000, 1), false);
            assert!(c.bytes.load(Ordering::Relaxed) <= budget);
        }
        assert!(c.evictions.load(Ordering::Relaxed) > 0);
        // The newest key is still there; the oldest went first.
        assert!(c.get(b"key-1999", Revision::new(2000, 0)).is_some());
        assert!(c.get(b"key-0", Revision::new(1, 0)).is_none());
        let sum: u64 = c
            .shards
            .iter()
            .map(|s| s.lock().unwrap().bytes)
            .sum();
        assert_eq!(sum, c.bytes.load(Ordering::Relaxed));
    }

    #[test]
    fn a_used_entry_survives_eviction() {
        let c = cache(16 * 8 * 1024);
        // Fill one key, then keep touching it while others stream past.
        c.insert(b"hot", Revision::new(1, 0), rec(b"hot", 100, 1), false);
        for i in 0..5000u32 {
            let k = format!("cold-{i}");
            c.insert(k.as_bytes(), Revision::new(2, 0), rec(k.as_bytes(), 300, 2), false);
            assert!(c.get(b"hot", Revision::new(1, 0)).is_some(), "evicted at {i}");
        }
    }

    #[test]
    fn large_values_are_not_cached_and_drop_the_old_entry() {
        let c = cache(1 << 30);
        c.insert(b"k", Revision::new(1, 0), rec(b"k", 10, 1), false);
        c.insert(
            b"k",
            Revision::new(2, 0),
            rec(b"k", DEFAULT_MAX_ENTRY_BYTES as usize + 1, 2),
            false,
        );
        assert!(c.get(b"k", Revision::new(1, 0)).is_none());
        assert!(c.get(b"k", Revision::new(2, 0)).is_none());
        assert_eq!(c.entries.load(Ordering::Relaxed), 0);
        assert_eq!(c.bytes.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn zero_budget_is_off() {
        let c = cache(0);
        c.insert(b"k", Revision::new(1, 0), rec(b"k", 1, 1), false);
        assert!(c.get(b"k", Revision::new(1, 0)).is_none());
        assert_eq!(c.misses.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn remove_and_clear_return_the_bytes() {
        let c = cache(1 << 20);
        c.insert(b"a", Revision::new(1, 0), rec(b"a", 10, 1), false);
        c.insert(b"b", Revision::new(1, 1), rec(b"b", 10, 1), false);
        c.remove(b"a");
        assert_eq!(c.entries.load(Ordering::Relaxed), 1);
        c.clear();
        assert_eq!(c.entries.load(Ordering::Relaxed), 0);
        assert_eq!(c.bytes.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn index_tracks_bytes_and_removes_empty_keys() {
        let idx = ResidentIndex::default();
        let gens = vec![Generation {
            created: Revision::new(1, 0),
            revs: vec![Revision::new(1, 0), Revision::new(2, 0)],
            tombstone: None,
        }];
        idx.write().set(b"a", gens.clone());
        idx.write().set(b"b", gens.clone());
        assert_eq!(idx.len(), 2);
        let two = idx.approx_bytes();
        idx.write().set(b"a", gens.clone());
        assert_eq!(idx.approx_bytes(), two);
        idx.write().set(b"a", Vec::new());
        assert_eq!(idx.len(), 1);
        assert_eq!(idx.approx_bytes(), index_entry_bytes(b"b", &gens));
        let got = idx.read().get(b"b").unwrap();
        assert_eq!(got.key, b"b".to_vec());
        assert_eq!(got.generations, gens);
        let keys: Vec<_> = idx
            .read()
            .range(Bound::Included(b"a".as_slice()), Bound::Unbounded)
            .map(|(k, _)| k.clone())
            .collect();
        assert_eq!(keys, vec![b"b".to_vec()]);
    }
}
