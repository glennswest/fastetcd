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
use fastetcd_storage::mvcc::{Mutation, MvccStore};
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

/// Migrate an etcd BoltDB snapshot at `from` into a fastetcd data
/// directory at `to`. The target directory must be empty unless
/// `force` is true. Existing target redb files are removed when
/// `force` is set.
pub async fn migrate_snapshot(
    from: &Path,
    to: &Path,
    force: bool,
) -> anyhow::Result<MigrationSummary> {
    if !from.exists() {
        anyhow::bail!("source snapshot {from:?} does not exist");
    }
    let bolt = Bolt::open_ro(from).map_err(|e| anyhow::anyhow!("open bolt: {e}"))?;

    let mut latest: std::collections::HashMap<Vec<u8>, (i64, i64, Vec<u8>)> =
        std::collections::HashMap::new();
    let mut scanned: u64 = 0;
    let mut tombstones: u64 = 0;

    let tx = bolt
        .begin()
        .map_err(|e| anyhow::anyhow!("bolt begin: {e}"))?;
    let bucket = tx
        .bucket(b"key")
        .ok_or_else(|| anyhow::anyhow!("snapshot has no 'key' bucket"))?;

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
            if k.last() == Some(&b't') {
                tombstones += 1;
                latest.remove(&kv.key);
                return Ok(());
            }
            match latest.get(&kv.key) {
                Some((m, _, _)) if *m >= kv.mod_revision => {}
                _ => {
                    latest.insert(
                        kv.key.clone(),
                        (kv.mod_revision, kv.create_revision, kv.value.clone()),
                    );
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

    let mutations: Vec<Mutation> = latest
        .into_iter()
        .map(|(key, (_, _, value))| Mutation::Put {
            key,
            value,
            lease: 0,
            ignore_value: false,
            ignore_lease: false,
            prev_kv: false,
        })
        .collect();
    let imported = mutations.len();
    let revision_after = if mutations.is_empty() {
        mvcc.current_revision().await
    } else {
        let (rev, _) = mvcc
            .apply(&mutations)
            .await
            .map_err(|e| anyhow::anyhow!("mvcc apply: {e}"))?;
        rev
    };
    Ok(MigrationSummary {
        scanned,
        tombstones,
        imported,
        revision_after,
    })
}
