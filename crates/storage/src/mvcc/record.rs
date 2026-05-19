//! Stored value records for the MVCC layer.
//!
//! Two persisted types:
//!
//! - [`KvRecord`] — the actual key/value payload, stored in the
//!   `mvcc_kv` table keyed by `(user_key, revision)`. One record per
//!   `(key, revision)` pair: a put writes one with `value` set; a
//!   delete writes a tombstone with `deleted = true`.
//!
//! - [`KeyIndex`] — per-key metadata stored in the `mvcc_idx` table.
//!   Tracks the lifecycle of a key as a sequence of [`Generation`]s.
//!   A generation starts at a put-after-absence and ends at a delete.
//!   The index is what makes historical reads tractable: given a
//!   target revision, we find the generation containing it and the
//!   newest revision in that generation `<=` the target.

use serde::{Deserialize, Serialize};

use super::revision::Revision;

/// The value stored in the `mvcc_kv` table at `(user_key, revision)`.
///
/// `value` is empty for tombstones; check `deleted`. The `key` field
/// is stored redundantly so we can reconstruct an `mvccpb::KeyValue`
/// without re-parsing the composite storage key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    /// Revision at which this key was first put within its current
    /// generation. Matches etcd's `KeyValue.create_revision`.
    pub create_revision: i64,
    /// Revision at which this record was written. Matches etcd's
    /// `KeyValue.mod_revision`.
    pub mod_revision: i64,
    /// Number of puts since the current generation began. Matches
    /// etcd's `KeyValue.version`. Always `1` on the create-put.
    pub version: i64,
    /// Lease ID attached to this key. `0` means no lease.
    pub lease: i64,
    /// True if this record is a tombstone for a delete. Tombstones
    /// carry `value = []`.
    pub deleted: bool,
}

impl KvRecord {
    pub fn is_tombstone(&self) -> bool {
        self.deleted
    }
}

/// A "generation" of a key — the period from a create-put through the
/// matching delete (if any). Generations are appended in order; the
/// last one may be open (`tombstone = None`).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Generation {
    /// Revision the generation was created at (first put).
    pub created: Revision,
    /// All revisions in this generation, in order. The last entry is
    /// the most recent put.
    pub revs: Vec<Revision>,
    /// Set when the generation has been closed by a delete.
    pub tombstone: Option<Revision>,
}

impl Generation {
    pub fn is_open(&self) -> bool {
        self.tombstone.is_none()
    }

    /// The revision the next put in this generation would create at —
    /// used to assign `version` and `mod_revision`.
    pub fn version_for_next_put(&self) -> i64 {
        (self.revs.len() as i64) + 1
    }

    /// The latest revision in this generation that is `<= target`,
    /// considering both puts and the closing tombstone. Returns
    /// `Some(rev)` if any matching revision exists, else `None`.
    /// If the matched revision is the tombstone, the second element
    /// of the tuple is `true` (signals "key is deleted at that rev").
    pub fn latest_at_or_before(&self, target: Revision) -> Option<(Revision, bool)> {
        let mut best: Option<(Revision, bool)> = None;
        for &r in &self.revs {
            if r <= target {
                best = match best {
                    Some((b, _)) if b >= r => Some((b, false)),
                    _ => Some((r, false)),
                };
            }
        }
        if let Some(t) = self.tombstone {
            if t <= target {
                best = match best {
                    Some((b, _)) if b > t => best,
                    _ => Some((t, true)),
                };
            }
        }
        best
    }
}

/// Per-key MVCC metadata. Stored in `mvcc_idx` keyed by the raw user
/// key.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct KeyIndex {
    pub key: Vec<u8>,
    pub generations: Vec<Generation>,
}

impl KeyIndex {
    pub fn new(key: Vec<u8>) -> Self {
        Self {
            key,
            generations: Vec::new(),
        }
    }

    /// True if the key is currently live (last generation is open).
    pub fn is_live(&self) -> bool {
        matches!(self.generations.last(), Some(g) if g.is_open())
    }

    /// Latest revision and version of the key at current state, or
    /// `None` if the key is currently deleted.
    pub fn current(&self) -> Option<(Revision, i64)> {
        let g = self.generations.last()?;
        if !g.is_open() {
            return None;
        }
        let rev = *g.revs.last()?;
        Some((rev, g.revs.len() as i64))
    }

    /// Apply a new put: opens a new generation if currently deleted,
    /// else appends to the current generation. Returns the
    /// `(version, create_revision)` to embed in the new [`KvRecord`].
    pub fn record_put(&mut self, rev: Revision) -> (i64, Revision) {
        let needs_new = match self.generations.last() {
            None => true,
            Some(g) => !g.is_open(),
        };
        if needs_new {
            self.generations.push(Generation {
                created: rev,
                revs: vec![rev],
                tombstone: None,
            });
            (1, rev)
        } else {
            let g = self.generations.last_mut().expect("checked above");
            g.revs.push(rev);
            (g.revs.len() as i64, g.created)
        }
    }

    /// Apply a delete: closes the current open generation with a
    /// tombstone revision. Returns `true` if a generation was actually
    /// closed (i.e., the key was live); `false` if the key was already
    /// deleted (in which case the caller should treat the delete as a
    /// no-op and not write a tombstone record).
    pub fn record_delete(&mut self, rev: Revision) -> bool {
        match self.generations.last_mut() {
            Some(g) if g.is_open() => {
                g.tombstone = Some(rev);
                true
            }
            _ => false,
        }
    }

    /// Find the revision to use when reading this key at `target_rev`.
    /// Returns `Some(rev)` if the key existed at or before that rev;
    /// `None` if the key did not exist at that rev (either never
    /// created or last seen as a tombstone).
    pub fn revision_at(&self, target_rev: Revision) -> Option<Revision> {
        // Walk generations in reverse so we hit the most recent first.
        for g in self.generations.iter().rev() {
            // If the generation started after the target, skip it.
            if g.created > target_rev {
                continue;
            }
            if let Some((rev, is_tombstone)) = g.latest_at_or_before(target_rev) {
                if is_tombstone {
                    // Key was deleted at this generation by `target_rev`.
                    return None;
                }
                return Some(rev);
            }
        }
        None
    }

    /// Compact the index against `compact_rev`. Returns the set of
    /// revisions whose `KvRecord`s in `mvcc_kv` are no longer
    /// reachable and may be physically deleted. After compaction:
    ///
    /// - Each generation whose tombstone is `<= compact_rev` is
    ///   entirely dropped (all its puts plus the tombstone).
    /// - In the generation that contains `compact_rev` (or whose
    ///   latest rev is `<= compact_rev`), all puts strictly older
    ///   than the latest put `<= compact_rev` are dropped. The latest
    ///   such put is kept so that `range_at(compact_rev)` for the key
    ///   continues to return the right value.
    /// - Puts whose revision is `> compact_rev` are untouched.
    ///
    /// If after compaction no generations remain, the caller should
    /// also drop this `KeyIndex` from `mvcc_idx`.
    pub fn compact(&mut self, compact_rev: Revision) -> Vec<Revision> {
        let mut dropped: Vec<Revision> = Vec::new();
        let mut keep: Vec<Generation> = Vec::with_capacity(self.generations.len());

        for g in std::mem::take(&mut self.generations) {
            match g.tombstone {
                Some(t) if t <= compact_rev => {
                    // Entire generation can go.
                    for r in &g.revs {
                        dropped.push(*r);
                    }
                    dropped.push(t);
                }
                _ => {
                    // Find the latest put with revision <= compact_rev.
                    let mut floor_idx: Option<usize> = None;
                    for (i, r) in g.revs.iter().enumerate() {
                        if *r <= compact_rev {
                            floor_idx = Some(i);
                        } else {
                            break;
                        }
                    }
                    let mut new_revs: Vec<Revision> = Vec::with_capacity(g.revs.len());
                    if let Some(fi) = floor_idx {
                        // Drop revs[..fi], keep revs[fi..].
                        for (i, r) in g.revs.iter().enumerate() {
                            if i < fi {
                                dropped.push(*r);
                            } else {
                                new_revs.push(*r);
                            }
                        }
                    } else {
                        // No revs at or before compact_rev — keep them all.
                        new_revs.extend_from_slice(&g.revs);
                    }
                    if new_revs.is_empty() && g.tombstone.is_none() {
                        // Live generation with no surviving revs is impossible —
                        // an open generation always has at least one put. Defensive.
                        continue;
                    }
                    keep.push(Generation {
                        created: g.created,
                        revs: new_revs,
                        tombstone: g.tombstone,
                    });
                }
            }
        }

        self.generations = keep;
        dropped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_index_is_not_live() {
        let idx = KeyIndex::new(b"k".to_vec());
        assert!(!idx.is_live());
        assert_eq!(idx.current(), None);
        assert_eq!(idx.revision_at(Revision::new(100, 0)), None);
    }

    #[test]
    fn put_then_get_records_generation_and_version() {
        let mut idx = KeyIndex::new(b"k".to_vec());
        let r1 = Revision::new(5, 0);
        let (ver, created) = idx.record_put(r1);
        assert_eq!(ver, 1);
        assert_eq!(created, r1);
        assert!(idx.is_live());
        assert_eq!(idx.current(), Some((r1, 1)));

        let r2 = Revision::new(7, 0);
        let (ver, created) = idx.record_put(r2);
        assert_eq!(ver, 2);
        assert_eq!(created, r1); // same generation
        assert_eq!(idx.current(), Some((r2, 2)));
    }

    #[test]
    fn delete_closes_generation_and_returns_none_for_current() {
        let mut idx = KeyIndex::new(b"k".to_vec());
        idx.record_put(Revision::new(5, 0));
        idx.record_put(Revision::new(7, 0));
        let closed = idx.record_delete(Revision::new(8, 0));
        assert!(closed);
        assert!(!idx.is_live());
        assert_eq!(idx.current(), None);
    }

    #[test]
    fn double_delete_is_noop() {
        let mut idx = KeyIndex::new(b"k".to_vec());
        idx.record_put(Revision::new(5, 0));
        assert!(idx.record_delete(Revision::new(6, 0)));
        assert!(!idx.record_delete(Revision::new(7, 0))); // no live generation
    }

    #[test]
    fn put_after_delete_opens_new_generation() {
        let mut idx = KeyIndex::new(b"k".to_vec());
        idx.record_put(Revision::new(5, 0));
        idx.record_delete(Revision::new(6, 0));
        let (ver, created) = idx.record_put(Revision::new(10, 0));
        assert_eq!(ver, 1);
        assert_eq!(created, Revision::new(10, 0));
        assert_eq!(idx.generations.len(), 2);
    }

    #[test]
    fn compact_drops_closed_generation_entirely() {
        let mut idx = KeyIndex::new(b"k".to_vec());
        idx.record_put(Revision::new(5, 0));
        idx.record_delete(Revision::new(6, 0));
        idx.record_put(Revision::new(10, 0));

        let dropped = idx.compact(Revision::new(7, 0));
        // Old generation (puts at 5 + tombstone at 6) entirely gone.
        let mut set: std::collections::HashSet<_> = dropped.into_iter().collect();
        assert!(set.remove(&Revision::new(5, 0)));
        assert!(set.remove(&Revision::new(6, 0)));
        assert!(set.is_empty());
        // Live generation preserved.
        assert_eq!(idx.generations.len(), 1);
        assert_eq!(idx.generations[0].revs, vec![Revision::new(10, 0)]);
    }

    #[test]
    fn compact_keeps_floor_in_live_generation() {
        let mut idx = KeyIndex::new(b"k".to_vec());
        idx.record_put(Revision::new(2, 0));
        idx.record_put(Revision::new(5, 0));
        idx.record_put(Revision::new(8, 0));

        let dropped = idx.compact(Revision::new(6, 0));
        // rev 2 dropped (older than floor); rev 5 kept (latest <= compact_rev);
        // rev 8 kept (newer).
        assert_eq!(dropped, vec![Revision::new(2, 0)]);
        assert_eq!(
            idx.generations[0].revs,
            vec![Revision::new(5, 0), Revision::new(8, 0)]
        );
    }

    #[test]
    fn compact_below_first_rev_is_noop() {
        let mut idx = KeyIndex::new(b"k".to_vec());
        idx.record_put(Revision::new(5, 0));
        idx.record_put(Revision::new(7, 0));
        let dropped = idx.compact(Revision::new(2, 0));
        assert!(dropped.is_empty());
        assert_eq!(idx.generations[0].revs.len(), 2);
    }

    #[test]
    fn revision_at_historical_lookup() {
        let mut idx = KeyIndex::new(b"k".to_vec());
        idx.record_put(Revision::new(5, 0));
        idx.record_put(Revision::new(7, 0));
        idx.record_delete(Revision::new(8, 0));
        idx.record_put(Revision::new(10, 0));

        // Before any revision exists: None.
        assert_eq!(idx.revision_at(Revision::new(4, 0)), None);
        // Inside first generation.
        assert_eq!(
            idx.revision_at(Revision::new(5, 0)),
            Some(Revision::new(5, 0))
        );
        assert_eq!(
            idx.revision_at(Revision::new(6, 0)),
            Some(Revision::new(5, 0))
        );
        assert_eq!(
            idx.revision_at(Revision::new(7, 0)),
            Some(Revision::new(7, 0))
        );
        // After tombstone, before re-creation: None.
        assert_eq!(idx.revision_at(Revision::new(8, 0)), None);
        assert_eq!(idx.revision_at(Revision::new(9, 0)), None);
        // After re-creation.
        assert_eq!(
            idx.revision_at(Revision::new(10, 0)),
            Some(Revision::new(10, 0))
        );
        assert_eq!(
            idx.revision_at(Revision::new(50, 0)),
            Some(Revision::new(10, 0))
        );
    }
}
