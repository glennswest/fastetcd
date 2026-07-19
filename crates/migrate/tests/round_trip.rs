//! End-to-end test: build a synthetic etcd snapshot using bbolt-rs in
//! rw mode, run `migrate_snapshot`, then open the resulting fastetcd
//! data dir and read the migrated values back through `MvccStore`.

use std::ops::Bound;
use std::sync::Arc;

use bbolt_rs::{Bolt, BucketRwApi, DbApi, DbRwAPI, TxRwRefApi};
use fastetcd_proto::mvccpb;
use fastetcd_storage::mvcc::MvccStore;
use fastetcd_storage::redb_engine::RedbEngine;
use prost::Message;
use tempfile::tempdir;

use fastetcd_migrate::{migrate_snapshot, migrate_snapshot_with_mode, MigrationMode};

/// Build a bolt key in etcd's format: `main_rev_be(8) || sub_rev_be(8)`
/// optionally followed by `b't'` for tombstones.
fn bolt_key(main: i64, sub: i64, tombstone: bool) -> Vec<u8> {
    let mut k = Vec::with_capacity(17);
    k.extend_from_slice(&main.to_be_bytes());
    k.extend_from_slice(&sub.to_be_bytes());
    if tombstone {
        k.push(b't');
    }
    k
}

fn kv_bytes(key: &[u8], value: &[u8], create_rev: i64, mod_rev: i64, version: i64) -> Vec<u8> {
    let kv = mvccpb::KeyValue {
        key: key.to_vec(),
        create_revision: create_rev,
        mod_revision: mod_rev,
        version,
        value: value.to_vec(),
        lease: 0,
    };
    kv.encode_to_vec()
}

#[tokio::test]
async fn migrates_latest_value_per_key_and_skips_tombstones() {
    let dir = tempdir().unwrap();
    let snap_path = dir.path().join("snapshot.db");

    // 1. Build a synthetic etcd snapshot with three logical keys:
    //    - `alpha`: two puts (rev 1, 5); latest value should win
    //    - `bravo`: one put then a tombstone (rev 2, 3); should not import
    //    - `charlie`: single put (rev 4); should import
    {
        let mut db = Bolt::open(&snap_path).expect("open rw bolt");
        db.update(|mut tx| -> bbolt_rs::Result<()> {
            let mut bucket = tx.create_bucket(b"key")?;
            bucket.put(bolt_key(1, 0, false), kv_bytes(b"alpha", b"v0", 1, 1, 1))?;
            bucket.put(bolt_key(2, 0, false), kv_bytes(b"bravo", b"bv0", 2, 2, 1))?;
            bucket.put(bolt_key(3, 0, true), kv_bytes(b"bravo", b"", 0, 3, 0))?;
            bucket.put(bolt_key(4, 0, false), kv_bytes(b"charlie", b"cv0", 4, 4, 1))?;
            bucket.put(bolt_key(5, 0, false), kv_bytes(b"alpha", b"v1", 1, 5, 2))?;
            Ok(())
        })
        .expect("populate snapshot");
        db.close();
    }

    // 2. Migrate into a fresh target.
    let target = dir.path().join("fastetcd-data");
    let summary = migrate_snapshot(&snap_path, &target, false).await.unwrap();
    assert_eq!(summary.scanned, 5);
    assert_eq!(summary.tombstones, 1);
    assert_eq!(summary.imported, 2); // alpha + charlie
    assert!(summary.revision_after >= 1);

    // 3. Re-open the target MvccStore and verify contents.
    let engine: Arc<dyn fastetcd_storage::KvStore> =
        Arc::new(RedbEngine::open(target.join("fastetcd.redb")).unwrap());
    let mvcc = MvccStore::open(engine).await.unwrap();
    let snap = engine_snapshot(&mvcc).await;
    assert!(snap.contains_key(b"alpha".as_slice()));
    assert!(snap.contains_key(b"charlie".as_slice()));
    assert!(!snap.contains_key(b"bravo".as_slice()));
    assert_eq!(snap.get(b"alpha".as_slice()).unwrap(), &b"v1".to_vec()); // latest version
    assert_eq!(snap.get(b"charlie".as_slice()).unwrap(), &b"cv0".to_vec());
}

async fn engine_snapshot(mvcc: &MvccStore) -> std::collections::HashMap<Vec<u8>, Vec<u8>> {
    let r = mvcc
        .range(b"\x00", &[0u8], 0, 0, false, false)
        .await
        .unwrap();
    r.kvs
        .into_iter()
        .map(|rec| (rec.key, rec.value))
        .collect()
}

#[allow(dead_code)]
fn _bound_helper() {
    // Avoid an unused-imports warning if Bound is added later.
    let _ = Bound::<Vec<u8>>::Unbounded;
}

#[tokio::test]
async fn preserve_revisions_keeps_history_visible_via_range_at_rev() {
    let dir = tempdir().unwrap();
    let snap_path = dir.path().join("snapshot.db");

    // Build a snapshot with three versions of the same key.
    {
        let mut db = Bolt::open(&snap_path).expect("open rw bolt");
        db.update(|mut tx| -> bbolt_rs::Result<()> {
            let mut bucket = tx.create_bucket(b"key")?;
            bucket.put(bolt_key(1, 0, false), kv_bytes(b"k", b"v1", 1, 1, 1))?;
            bucket.put(bolt_key(2, 0, false), kv_bytes(b"k", b"v2", 1, 2, 2))?;
            bucket.put(bolt_key(3, 0, false), kv_bytes(b"k", b"v3", 1, 3, 3))?;
            Ok(())
        })
        .expect("populate");
        db.close();
    }

    let target = dir.path().join("fastetcd-data");
    let summary = migrate_snapshot_with_mode(
        &snap_path,
        &target,
        false,
        MigrationMode::PreserveRevisions,
    )
    .await
    .unwrap();
    assert_eq!(summary.scanned, 3);
    assert_eq!(summary.imported, 1);
    assert_eq!(summary.revision_after, 3);

    // Open the target and verify Range at each historical revision.
    let engine: Arc<dyn fastetcd_storage::KvStore> =
        Arc::new(RedbEngine::open(target.join("fastetcd.redb")).unwrap());
    let mvcc = MvccStore::open(engine).await.unwrap();
    for (rev, want_value) in [(1i64, b"v1".as_ref()), (2, b"v2"), (3, b"v3")] {
        let r = mvcc
            .range(b"k", b"", 0, rev, false, false)
            .await
            .unwrap();
        assert_eq!(r.kvs.len(), 1, "rev {rev}: expected one record");
        assert_eq!(
            r.kvs[0].value, want_value,
            "rev {rev}: expected value {want_value:?}"
        );
        assert_eq!(r.kvs[0].create_revision, 1);
        assert_eq!(r.kvs[0].mod_revision, rev);
    }
}
