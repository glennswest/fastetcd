//! Tests for the offline data-directory toolkit: backup, restore, fsck.

use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::BasicNode;
use tempfile::tempdir;

use fastetcd_server::admin;
use fastetcd_storage::mvcc::{Mutation, MvccStore};
use fastetcd_storage::redb_engine::RedbEngine;
use fastetcd_storage::KvStore;

/// Write `keys` more puts into the data dir (creating it if needed) and
/// return the resulting revision. Drops the engine before returning so
/// the exclusive redb lock is released for the admin commands.
async fn seed(dir: &std::path::Path, keys: usize) -> i64 {
    let engine: Arc<dyn KvStore> = Arc::new(RedbEngine::open(admin::data_file(dir)).unwrap());
    let mvcc = MvccStore::open(engine).await.unwrap();
    for i in 0..keys {
        mvcc.apply(&[Mutation::Put {
            key: format!("k{i}").into_bytes(),
            value: b"v".to_vec(),
            lease: 0,
            prev_kv: false,
            ignore_value: false,
            ignore_lease: false,
        }])
        .await
        .unwrap();
    }
    mvcc.current_revision().await
}

async fn revision(dir: &std::path::Path) -> i64 {
    let engine: Arc<dyn KvStore> = Arc::new(RedbEngine::open(admin::data_file(dir)).unwrap());
    MvccStore::open(engine).await.unwrap().current_revision().await
}

#[tokio::test]
async fn backup_then_restore_round_trips_and_guards_newer() {
    let dir = tempdir().unwrap();
    let rev1 = seed(dir.path(), 5).await;

    let backup = dir.path().join("snap.redb");
    admin::cmd_backup(dir.path(), &backup).await.unwrap();
    assert!(backup.exists() && backup.metadata().unwrap().len() > 0);

    // Advance the live dir past the backup.
    let rev2 = seed(dir.path(), 3).await;
    assert!(rev2 > rev1);

    // Restoring an older backup over a newer dir must be refused...
    assert!(
        admin::cmd_restore(dir.path(), &backup, false).await.is_err(),
        "restore must refuse to overwrite a newer data dir without --force"
    );
    // ...unless forced, which rolls the revision back to the backup's.
    admin::cmd_restore(dir.path(), &backup, true).await.unwrap();
    assert_eq!(revision(dir.path()).await, rev1);

    // The pre-restore file is kept, so the restore itself is reversible.
    let kept = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().contains("replaced-"));
    assert!(kept, "pre-restore data file must be preserved");
}

#[tokio::test]
async fn fsck_reports_then_repairs_a_legacy_dir() {
    let dir = tempdir().unwrap();
    seed(dir.path(), 4).await; // data present, but no membership / format marker

    let node_id = 42u64;
    let mut all = BTreeMap::new();
    all.insert(node_id, BasicNode::new("http://127.0.0.1:2380"));

    // Report-only: a legacy dir has problems (empty membership, no format).
    assert_eq!(
        admin::cmd_fsck(dir.path(), &all, node_id, false).await.unwrap(),
        1,
        "legacy dir should report problems"
    );

    // Repair recovers membership from config and stamps the format.
    assert_eq!(
        admin::cmd_fsck(dir.path(), &all, node_id, true).await.unwrap(),
        1
    );

    // A second check is now clean.
    assert_eq!(
        admin::cmd_fsck(dir.path(), &all, node_id, false).await.unwrap(),
        0,
        "dir should be clean after repair"
    );
}

#[tokio::test]
async fn backup_refuses_while_locked() {
    let dir = tempdir().unwrap();
    let _rev = seed(dir.path(), 1).await;
    // Hold the data dir open (as a running server would).
    let engine: Arc<dyn KvStore> = Arc::new(RedbEngine::open(admin::data_file(dir.path())).unwrap());
    let _held = MvccStore::open(engine).await.unwrap();

    let out = dir.path().join("snap.redb");
    assert!(
        admin::cmd_backup(dir.path(), &out).await.is_err(),
        "backup must refuse while the data file is locked by another opener"
    );
}

#[tokio::test]
async fn backs_up_only_when_the_version_changes() {
    let dir = tempdir().unwrap();
    seed(dir.path(), 2).await;
    let backups = dir.path().join("backups");

    let engine: Arc<dyn KvStore> = Arc::new(RedbEngine::open(admin::data_file(dir.path())).unwrap());
    let mvcc = MvccStore::open(engine).await.unwrap();
    mvcc.write_open_version("1.0.2").await.unwrap();

    // Same version → no backup.
    assert!(
        admin::backup_before_version(&mvcc, dir.path(), &backups, "1.0.2")
            .await
            .unwrap()
            .is_none(),
        "no backup when the version is unchanged"
    );
    // Newer version → a backup is written.
    let made = admin::backup_before_version(&mvcc, dir.path(), &backups, "1.0.3")
        .await
        .unwrap();
    assert!(made.is_some() && made.unwrap().exists(), "backup on version change");
}

#[tokio::test]
async fn no_startup_backup_for_an_empty_dir() {
    let dir = tempdir().unwrap();
    // Create an empty store (no data), as a fresh install would.
    {
        let engine: Arc<dyn KvStore> =
            Arc::new(RedbEngine::open(admin::data_file(dir.path())).unwrap());
        let _ = MvccStore::open(engine).await.unwrap();
    }
    let engine: Arc<dyn KvStore> = Arc::new(RedbEngine::open(admin::data_file(dir.path())).unwrap());
    let mvcc = MvccStore::open(engine).await.unwrap();
    assert!(
        admin::backup_before_version(&mvcc, dir.path(), &dir.path().join("backups"), "1.0.3")
            .await
            .unwrap()
            .is_none(),
        "an empty directory has nothing to back up"
    );
}

#[tokio::test]
async fn a_failed_backup_surfaces_an_error_so_startup_can_continue() {
    let dir = tempdir().unwrap();
    seed(dir.path(), 2).await;

    let engine: Arc<dyn KvStore> = Arc::new(RedbEngine::open(admin::data_file(dir.path())).unwrap());
    let mvcc = MvccStore::open(engine).await.unwrap();
    mvcc.write_open_version("1.0.3").await.unwrap();

    // Put a regular file where the backup *directory* should be, so
    // create_dir_all fails — the same class of failure as a read-only or
    // full disk. backup_before_version must return the error rather than
    // panic; startup logs it and continues (verified by the functional
    // upgrade test).
    let blocked = dir.path().join("blocked-backups");
    std::fs::write(&blocked, b"not a directory").unwrap();

    let res = admin::backup_before_version(&mvcc, dir.path(), &blocked, "1.0.4").await;
    assert!(res.is_err(), "a backup that cannot be written must surface an error");
}
