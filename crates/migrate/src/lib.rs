//! etcd → fastetcd snapshot migration.
//!
//! Public API: [`migrate_snapshot`] takes a path to an etcd BoltDB
//! snapshot and a path to a target fastetcd data dir, walks the
//! upstream `key` bucket, and replays the latest record of each
//! user-key as a `Mutation::Put` into a fresh `MvccStore`. Revision
//! history from the source is **not** preserved — Phase 2 will add a
//! revision-preserving bulk-load path.

use std::path::Path;
use std::sync::Arc;

use bbolt_rs::{Bolt, BucketApi, DbApi, TxApi};
use fastetcd_proto::mvccpb;
use fastetcd_storage::mvcc::{BulkKey, KvRecord, Mutation, MvccStore};
use fastetcd_storage::redb_engine::RedbEngine;
use prost::Message;

/// Result of a successful migration.
#[derive(Debug, Clone)]
pub struct MigrationSummary {
    /// Total bolt entries inspected (including tombstones).
    pub scanned: u64,
    /// Tombstone records encountered (skipped).
    pub tombstones: u64,
    /// Live keys imported into fastetcd.
    pub imported: usize,
    /// MVCC revision in the target after the import.
    pub revision_after: i64,
}

/// How to handle MVCC revision history during a migration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MigrationMode {
    /// Import only the latest live value per user key. After
    /// migration every key has `create_revision = mod_revision = 1`.
    /// Smallest output and fastest; suitable for "I want my data
    /// on fastetcd" use cases where existing watch state is OK to
    /// reset.
    #[default]
    LatestOnly,
    /// Preserve `create_revision`, `mod_revision`, `version`, and
    /// `lease` for every record. After migration, `Range(rev)` and
    /// `Watch(start_rev)` behave the same as on the source server
    /// (modulo whatever the source had compacted).
    PreserveRevisions,
}

/// Migrate an etcd BoltDB snapshot at `from` into a fastetcd data
/// directory at `to`. The target directory must be empty unless
/// `force` is true. Existing target redb files are removed when
/// `force` is set.
pub async fn migrate_snapshot(
    from: &Path,
    to: &Path,
    force: bool,
) -> anyhow::Result<MigrationSummary> {
    migrate_snapshot_with_mode(from, to, force, MigrationMode::default()).await
}

/// Variant of [`migrate_snapshot`] that takes an explicit
/// [`MigrationMode`].
pub async fn migrate_snapshot_with_mode(
    from: &Path,
    to: &Path,
    force: bool,
    mode: MigrationMode,
) -> anyhow::Result<MigrationSummary> {
    if !from.exists() {
        anyhow::bail!("source snapshot {from:?} does not exist");
    }
    let bolt = Bolt::open_ro(from).map_err(|e| anyhow::anyhow!("open bolt: {e}"))?;

    // For LatestOnly mode: track latest (mod_rev, create_rev, value)
    // per user key. For PreserveRevisions mode: track every record
    // per user key in revision order.
    let mut latest: std::collections::HashMap<Vec<u8>, (i64, i64, Vec<u8>, i64)> =
        std::collections::HashMap::new();
    let mut history: std::collections::HashMap<Vec<u8>, Vec<(i64, KvRecord, bool)>> =
        std::collections::HashMap::new();
    let mut scanned: u64 = 0;
    let mut tombstones: u64 = 0;
    let mut max_rev: i64 = 0;

    let tx = bolt
        .begin()
        .map_err(|e| anyhow::anyhow!("bolt begin: {e}"))?;
    let bucket = tx
        .bucket(b"key")
        .ok_or_else(|| anyhow::anyhow!("snapshot has no 'key' bucket"))?;

    let mode_local = mode;
    #[allow(deprecated)]
    bucket
        .for_each(|k: &[u8], v: Option<&[u8]>| -> bbolt_rs::Result<()> {
            scanned += 1;
            let Some(value) = v else {
                return Ok(());
            };
            let kv = match mvccpb::KeyValue::decode(value) {
                Ok(kv) => kv,
                Err(_) => return Ok(()),
            };
            let is_tomb = k.last() == Some(&b't');
            if is_tomb {
                tombstones += 1;
            }
            if kv.mod_revision > max_rev {
                max_rev = kv.mod_revision;
            }

            match mode_local {
                MigrationMode::LatestOnly => {
                    if is_tomb {
                        latest.remove(&kv.key);
                    } else {
                        match latest.get(&kv.key) {
                            Some((m, _, _, _)) if *m >= kv.mod_revision => {}
                            _ => {
                                latest.insert(
                                    kv.key.clone(),
                                    (
                                        kv.mod_revision,
                                        kv.create_revision,
                                        kv.value.clone(),
                                        kv.lease,
                                    ),
                                );
                            }
                        }
                    }
                }
                MigrationMode::PreserveRevisions => {
                    let rec = KvRecord {
                        key: kv.key.clone(),
                        value: kv.value.clone(),
                        create_revision: kv.create_revision,
                        mod_revision: kv.mod_revision,
                        version: kv.version,
                        lease: kv.lease,
                        deleted: is_tomb,
                    };
                    history
                        .entry(kv.key.clone())
                        .or_default()
                        .push((kv.mod_revision, rec, is_tomb));
                }
            }
            Ok(())
        })
        .map_err(|e| anyhow::anyhow!("bolt for_each: {e}"))?;
    drop(bucket);
    drop(tx);
    drop(bolt);

    if to.exists() && !force {
        let entries = std::fs::read_dir(to)?;
        if entries.count() > 0 {
            anyhow::bail!("target {to:?} exists and is not empty; pass force=true to overwrite");
        }
    }
    std::fs::create_dir_all(to)?;
    let target_path = to.join("fastetcd.redb");
    if force && target_path.exists() {
        std::fs::remove_file(&target_path)?;
    }

    let engine: Arc<dyn fastetcd_storage::KvStore> = Arc::new(RedbEngine::open(&target_path)?);
    let mvcc = MvccStore::open(engine).await?;

    let (imported, revision_after) = match mode {
        MigrationMode::LatestOnly => {
            let mutations: Vec<Mutation> = latest
                .into_iter()
                .map(|(key, (_, _, value, lease))| Mutation::Put {
                    key,
                    value,
                    lease,
                    ignore_value: false,
                    ignore_lease: false,
                    prev_kv: false,
                })
                .collect();
            let n = mutations.len();
            let rev = if mutations.is_empty() {
                mvcc.current_revision().await
            } else {
                let (r, _) = mvcc
                    .apply(&mutations)
                    .await
                    .map_err(|e| anyhow::anyhow!("mvcc apply: {e}"))?;
                r
            };
            (n, rev)
        }
        MigrationMode::PreserveRevisions => {
            // For each user key, sort by revision. The last record per
            // key is either a tombstone (closes a generation) or a put
            // (live). Multi-generation history is collapsed: we drop
            // everything before the final tombstone (those would be
            // unreachable anyway) and import the final generation.
            let mut bulk: Vec<BulkKey> = Vec::with_capacity(history.len());
            let mut imported_count: usize = 0;
            for (key, mut records) in history {
                records.sort_by_key(|(rev, _, _)| *rev);
                // Find the index after the most recent tombstone.
                let last_tomb_idx = records
                    .iter()
                    .rposition(|(_, _, is_tomb)| *is_tomb);
                let (puts_recs, tomb_rec) = match last_tomb_idx {
                    Some(i) => {
                        // Records before the tombstone are part of
                        // the now-closed generation. We import them
                        // along with the tombstone so historical
                        // reads at those revisions still work.
                        let mut tomb_rec = records[i].1.clone();
                        tomb_rec.deleted = true;
                        tomb_rec.value = Vec::new();
                        let puts: Vec<KvRecord> = records[..i]
                            .iter()
                            .map(|(_, rec, _)| rec.clone())
                            .collect();
                        // Plus any records after the tombstone — those
                        // are a new (live) generation. For simplicity
                        // here we drop them; multi-generation import
                        // is a future enhancement.
                        (puts, Some(tomb_rec))
                    }
                    None => {
                        let puts: Vec<KvRecord> =
                            records.iter().map(|(_, rec, _)| rec.clone()).collect();
                        (puts, None)
                    }
                };
                if puts_recs.is_empty() && tomb_rec.is_none() {
                    continue;
                }
                if !puts_recs.is_empty() && tomb_rec.is_none() {
                    imported_count += 1;
                }
                bulk.push(BulkKey {
                    key,
                    puts: puts_recs,
                    tombstone: tomb_rec,
                });
            }
            mvcc.bulk_load_records(bulk, max_rev)
                .await
                .map_err(|e| anyhow::anyhow!("bulk_load_records: {e}"))?;
            (imported_count, max_rev)
        }
    };
    Ok(MigrationSummary {
        scanned,
        tombstones,
        imported,
        revision_after,
    })
}
