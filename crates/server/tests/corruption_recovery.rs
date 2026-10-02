//! fastetcd#37: survive a corrupt data file.
//!
//! The reported failure: after a power cut on a device that lost fsync'd
//! writes, redb refused to open the store ("Failed to repair database.
//! All roots are corrupted") and the node crash-looped. These tests make
//! that file for real, by zeroing every page after redb's header and
//! marking the file as needing recovery (as an unclean shutdown leaves
//! it), and check what fastetcd does with it.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::maintenance_server::Maintenance;
use fastetcd_raft::types::NodeId;
use fastetcd_server::backup;
use fastetcd_server::maintenance::MaintenanceService;
use fastetcd_server::recovery::{open_or_recover, OnCorruption, OpenOptions, RecoveryAlarm};
use fastetcd_storage::mvcc::{Mutation, MvccStore};
use fastetcd_storage::redb_engine::RedbEngine;
use fastetcd_storage::{KvStore, StorageError, WriteBatch, WriteOptions};

mod common;

const NODE: NodeId = 1;

struct Dirs {
    _tmp: tempfile::TempDir,
    data_dir: PathBuf,
    data_file: PathBuf,
    backup_dir: PathBuf,
}

fn dirs() -> Dirs {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let backup_dir = tmp.path().join("backups");
    std::fs::create_dir_all(&data_dir).unwrap();
    Dirs {
        data_file: data_dir.join("fastetcd.redb"),
        data_dir,
        backup_dir,
        _tmp: tmp,
    }
}

fn opts(d: &Dirs, members: usize) -> OpenOptions {
    OpenOptions {
        backup_dir: Some(d.backup_dir.clone()),
        on_corruption: OnCorruption::Restore,
        node_id: NODE,
        configured_members: members,
        engine_cache_bytes: None,
    }
}

fn put(key: String) -> Mutation {
    Mutation::Put {
        key: key.into_bytes(),
        value: b"v".to_vec(),
        lease: 0,
        prev_kv: false,
        ignore_value: false,
        ignore_lease: false,
    }
}

async fn put_keys(mvcc: &MvccStore, from: usize, to: usize) {
    for i in from..to {
        mvcc.apply(&[put(format!("key/{i:04}"))]).await.unwrap();
    }
}

/// Record a raft membership of `members` in the store, as the state
/// machine does when it applies a membership entry.
async fn set_members(mvcc: &MvccStore, members: &[NodeId]) {
    let nodes: std::collections::BTreeMap<NodeId, openraft::BasicNode> = members
        .iter()
        .map(|id| (*id, openraft::BasicNode::new(format!("http://node-{id}"))))
        .collect();
    let voters: std::collections::BTreeSet<NodeId> = members.iter().copied().collect();
    let membership = openraft::StoredMembership::new(
        Some(openraft::LogId::new(openraft::CommittedLeaderId::new(1, NODE), 1)),
        openraft::Membership::new(vec![voters], nodes),
    );
    mvcc.persist_membership(&bincode::serialize(&membership).unwrap())
        .await
        .unwrap();
}

async fn visible(mvcc: &MvccStore) -> usize {
    mvcc.range(b"key/", b"key0", 0, 0, true, false).await.unwrap().kvs.len()
}

/// Leave the file as a power cut on a lying device did: the header says
/// recovery is needed, and every page behind it is gone, so both commit
/// slots point at roots whose checksums fail.
fn zero_the_roots(file: &Path) {
    let mut bytes = std::fs::read(file).unwrap();
    let page_size = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    assert!(page_size.is_power_of_two() && page_size < bytes.len());
    bytes[9] |= 2; // god byte: RECOVERY_REQUIRED
    for b in &mut bytes[page_size..] {
        *b = 0;
    }
    std::fs::write(file, bytes).unwrap();
}

/// A lone member with 50 keys backed up, then 20 more written after the
/// backup, then the file destroyed. Returns the cluster id it had.
async fn lone_member_then_corruption(d: &Dirs) -> u64 {
    let cluster_id;
    {
        let engine: Arc<dyn KvStore> = Arc::new(RedbEngine::open(&d.data_file).unwrap());
        cluster_id = fastetcd_server::cluster_id::resolve(&engine, None, true, Some("prod"))
            .await
            .unwrap()
            .0;
        let mvcc = MvccStore::open(engine.clone()).await.unwrap();
        set_members(&mvcc, &[NODE]).await;
        put_keys(&mvcc, 0, 50).await;
        backup::take(&engine, &d.backup_dir, NODE, 4).await.unwrap();
        // The window a restore loses.
        put_keys(&mvcc, 50, 70).await;
    }
    std::fs::create_dir_all(d.data_dir.join("snapshots")).unwrap();
    std::fs::create_dir_all(d.data_dir.join("wal")).unwrap();
    zero_the_roots(&d.data_file);
    cluster_id
}

#[tokio::test]
async fn zeroed_roots_are_reported_as_corruption() {
    let d = dirs();
    lone_member_then_corruption(&d).await;
    match RedbEngine::open(&d.data_file) {
        Err(StorageError::Corrupted(msg)) => println!("redb says: {msg}"),
        Err(e) => panic!("expected Corrupted, got {e}"),
        Ok(_) => panic!("a file with zeroed roots must not open"),
    }
}

#[tokio::test]
async fn a_lone_member_restores_its_newest_backup_and_raises_the_alarm() {
    let d = dirs();
    let cluster_id = lone_member_then_corruption(&d).await;

    let (engine, record) = open_or_recover(&d.data_file, &opts(&d, 1)).await.unwrap();
    let record = record.expect("a recovery must be reported");
    assert_eq!(record.backup_revision, 50);
    assert!(record.reason.contains("orrupt"), "{}", record.reason);

    let engine: Arc<dyn KvStore> = Arc::new(engine);
    let mvcc = MvccStore::open(engine.clone()).await.unwrap();
    assert_eq!(mvcc.current_revision().await, 50);
    assert_eq!(visible(&mvcc).await, 50, "everything up to the backup is back");

    // The corrupt file is kept, never deleted; the snapshots and the
    // raft WAL went with it (#85).
    assert!(Path::new(&record.corrupt_file).exists());
    assert!(!d.data_dir.join("snapshots").exists());
    assert!(!d.data_dir.join("wal").exists());
    for moved in ["snapshots.corrupt.", "wal.corrupt."] {
        assert!(std::fs::read_dir(&d.data_dir)
            .unwrap()
            .flatten()
            .any(|e| e.file_name().to_string_lossy().starts_with(moved)));
    }

    // The node-local cluster id came back with the store.
    assert_eq!(fastetcd_server::cluster_id::read(&engine).await.unwrap(), Some(cluster_id));

    // The alarm is on record, and survives a restart.
    let alarm = RecoveryAlarm::load(&engine).await.unwrap();
    assert_eq!(alarm.active(), Some(record.clone()));
    assert_eq!(alarm.recoveries(), 1);
    drop(mvcc);
    drop(engine);
    let (engine, again) = open_or_recover(&d.data_file, &opts(&d, 1)).await.unwrap();
    assert!(again.is_none(), "a healthy file is opened, not restored");
    let engine: Arc<dyn KvStore> = Arc::new(engine);
    let alarm = RecoveryAlarm::load(&engine).await.unwrap();
    assert_eq!(alarm.active(), Some(record));

    // Disarming clears it for good but keeps the count.
    alarm.disarm(&engine).await.unwrap();
    let alarm = RecoveryAlarm::load(&engine).await.unwrap();
    assert!(alarm.active().is_none());
    assert_eq!(alarm.recoveries(), 1);
}

/// A power cut between moving the corrupt file aside and moving the
/// restored one into place leaves no data file. The next start must
/// finish the swap, not create an empty store where the data was.
#[tokio::test]
async fn a_restore_interrupted_mid_swap_is_finished_on_the_next_start() {
    let d = dirs();
    lone_member_then_corruption(&d).await;
    let (engine, record) = open_or_recover(&d.data_file, &opts(&d, 1)).await.unwrap();
    let record = record.unwrap();
    drop(engine);

    // The state the crash leaves: the corrupt file is aside, the
    // complete restored file (recovery already recorded in it) has not
    // been moved in, and the snapshots have not been moved yet.
    let restored = d.data_file.with_extension("redb.restored");
    std::fs::rename(&d.data_file, &restored).unwrap();
    std::fs::create_dir_all(d.data_dir.join("snapshots")).unwrap();

    let (engine, again) = open_or_recover(&d.data_file, &opts(&d, 1)).await.unwrap();
    assert!(again.is_none(), "the swap is finished, not a second restore");
    assert!(!restored.exists());
    assert!(!d.data_dir.join("snapshots").exists(), "snapshots moved aside");
    let engine: Arc<dyn KvStore> = Arc::new(engine);
    let alarm = RecoveryAlarm::load(&engine).await.unwrap();
    assert_eq!(alarm.active(), Some(record), "the alarm came with the restored file");
    let mvcc = MvccStore::open(engine).await.unwrap();
    assert_eq!(mvcc.current_revision().await, 50);
    assert_eq!(visible(&mvcc).await, 50);
}

#[tokio::test]
async fn a_backup_that_fails_its_checksum_is_skipped_for_the_next_older() {
    let d = dirs();
    {
        let engine: Arc<dyn KvStore> = Arc::new(RedbEngine::open(&d.data_file).unwrap());
        let mvcc = MvccStore::open(engine.clone()).await.unwrap();
        set_members(&mvcc, &[NODE]).await;
        put_keys(&mvcc, 0, 30).await;
        backup::take(&engine, &d.backup_dir, NODE, 4).await.unwrap();
        // Distinct file names are by millisecond.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        put_keys(&mvcc, 30, 50).await;
        backup::take(&engine, &d.backup_dir, NODE, 4).await.unwrap();
    }
    let newest = backup::list(&d.backup_dir)[0].clone();
    assert_eq!(backup::verify(&newest).unwrap().revision, 50);
    let mut bytes = std::fs::read(&newest).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    std::fs::write(&newest, bytes).unwrap();
    assert!(backup::verify(&newest).is_err(), "a flipped byte must fail the checksum");

    zero_the_roots(&d.data_file);
    let (engine, record) = open_or_recover(&d.data_file, &opts(&d, 1)).await.unwrap();
    assert_eq!(record.unwrap().backup_revision, 30, "fell back to the older backup");
    let mvcc = MvccStore::open(Arc::new(engine)).await.unwrap();
    assert_eq!(visible(&mvcc).await, 30);
}

/// A member of a multi-node cluster never starts from a backup, and the
/// corrupt file is left exactly as it was.
#[tokio::test]
async fn a_member_of_a_cluster_refuses_and_leaves_the_file() {
    let d = dirs();
    lone_member_then_corruption(&d).await;
    let before = std::fs::read(&d.data_file).unwrap();

    let err = open_or_recover(&d.data_file, &opts(&d, 3)).await.err().expect("must refuse");
    let msg = err.to_string();
    assert!(msg.contains("member remove"), "{msg}");
    assert_eq!(std::fs::read(&d.data_file).unwrap(), before, "file untouched");
    assert!(d.data_dir.join("snapshots").exists());
}

/// `--initial-cluster` may name only this node while the backup shows it
/// had been joined by others: the backup's membership wins.
#[tokio::test]
async fn a_backup_showing_other_members_refuses() {
    let d = dirs();
    {
        let engine: Arc<dyn KvStore> = Arc::new(RedbEngine::open(&d.data_file).unwrap());
        let mvcc = MvccStore::open(engine.clone()).await.unwrap();
        set_members(&mvcc, &[NODE, 2, 3]).await;
        put_keys(&mvcc, 0, 10).await;
        backup::take(&engine, &d.backup_dir, NODE, 4).await.unwrap();
    }
    zero_the_roots(&d.data_file);
    let before = std::fs::read(&d.data_file).unwrap();
    assert!(open_or_recover(&d.data_file, &opts(&d, 1)).await.is_err());
    assert_eq!(std::fs::read(&d.data_file).unwrap(), before);
}

#[tokio::test]
async fn no_backup_dir_refuse_policy_and_no_usable_backup_all_refuse() {
    let d = dirs();
    lone_member_then_corruption(&d).await;
    let before = std::fs::read(&d.data_file).unwrap();

    let no_dir = OpenOptions { backup_dir: None, ..opts(&d, 1) };
    assert!(open_or_recover(&d.data_file, &no_dir).await.is_err());

    let refuse = OpenOptions { on_corruption: OnCorruption::Refuse, ..opts(&d, 1) };
    assert!(open_or_recover(&d.data_file, &refuse).await.is_err());

    let other_node = OpenOptions { node_id: 99, ..opts(&d, 1) };
    assert!(
        open_or_recover(&d.data_file, &other_node).await.is_err(),
        "another node's backups are not used"
    );
    assert_eq!(std::fs::read(&d.data_file).unwrap(), before);
}

/// redb would silently initialise a fresh database over an empty file,
/// discarding the store (and a member's raft vote).
#[tokio::test]
async fn an_empty_data_file_is_corruption_not_a_new_store() {
    let d = dirs();
    std::fs::write(&d.data_file, b"").unwrap();
    assert!(matches!(RedbEngine::open(&d.data_file), Err(StorageError::Corrupted(_))));
}

/// A backup is every table, from one point in time: restoring it gives
/// back exactly the store it was taken from.
#[tokio::test]
async fn a_backup_restores_every_table_exactly() {
    let d = dirs();
    let engine: Arc<dyn KvStore> = Arc::new(RedbEngine::open(&d.data_file).unwrap());
    let mvcc = MvccStore::open(engine.clone()).await.unwrap();
    put_keys(&mvcc, 0, 25).await;
    let mut batch = WriteBatch::new();
    batch.put("raft_log", b"\x00\x00\x00\x00\x00\x00\x00\x07", b"entry");
    batch.put("raft_meta", b"vote", b"v");
    batch.put("lease", b"\x01", b"lease-record");
    batch.put("auth_users", b"alice", b"user-record");
    batch.put("node_meta", b"cluster_id", &42u64.to_be_bytes());
    engine.commit(batch, WriteOptions::default()).await.unwrap();

    let info = backup::take(&engine, &d.backup_dir, NODE, 4).await.unwrap();
    assert_eq!(info.header.cluster_id, Some(42));
    let restored_file = d.data_dir.join("restored.redb");
    backup::restore_to(&info.path, &restored_file).await.unwrap();

    let restored: Arc<dyn KvStore> = Arc::new(RedbEngine::open(&restored_file).unwrap());
    let (a, b) = (engine.snapshot().await.unwrap(), restored.snapshot().await.unwrap());
    let mut names = a.table_names().await.unwrap();
    names.sort();
    let mut restored_names = b.table_names().await.unwrap();
    restored_names.sort();
    assert_eq!(names, restored_names);
    for name in &names {
        let all = |s: &Arc<dyn fastetcd_storage::Snapshot>| {
            let s = s.clone();
            let name = name.clone();
            async move {
                s.range(&name, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded, 0)
                    .await
                    .unwrap()
            }
        };
        assert_eq!(all(&a).await, all(&b).await, "table {name} differs");
    }
}

/// The alarm and the metrics, as a client and a scraper see them.
#[tokio::test]
async fn the_corrupt_alarm_is_visible_and_can_be_disarmed() {
    let h = common::start_test_server_full().await;
    let engine = h.state.sm.mvcc().engine().clone();
    let record = fastetcd_server::recovery::RecoveryRecord {
        recovered_unix_ms: 1,
        reason: "All roots are corrupted".into(),
        corrupt_file: "/data/fastetcd.redb.corrupt.1".into(),
        backup_file: "/backups/fastetcd-backup-1-rev50.fbak".into(),
        backup_created_unix_ms: 1,
        backup_revision: 50,
    };
    let mut batch = WriteBatch::new();
    batch.put("node_meta", b"recovered_from_backup", &bincode::serialize(&record).unwrap());
    batch.put("node_meta", b"recoveries_total", &1u64.to_be_bytes());
    engine.commit(batch, WriteOptions::default()).await.unwrap();
    let alarm = Arc::new(RecoveryAlarm::load(&engine).await.unwrap());
    let state = Arc::new((*h.state).clone().with_recovery(alarm));
    let svc = MaintenanceService::new(state.clone());

    let list = |svc: MaintenanceService| async move {
        svc.alarm(tonic::Request::new(pb::AlarmRequest {
            action: pb::alarm_request::AlarmAction::Get as i32,
            member_id: 0,
            alarm: pb::AlarmType::None as i32,
        }))
        .await
        .unwrap()
        .into_inner()
        .alarms
    };
    let alarms = list(svc.clone()).await;
    assert_eq!(alarms.len(), 1);
    assert_eq!(alarms[0].alarm, pb::AlarmType::Corrupt as i32);
    let status = svc
        .status(tonic::Request::new(pb::StatusRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert!(status.errors.contains(&"CORRUPT".to_string()), "{:?}", status.errors);

    let metrics = fastetcd_server::metrics::Metrics::new();
    metrics.refresh(&state).await;
    let mut text = String::new();
    prometheus_client::encoding::text::encode(&mut text, &*metrics.registry.lock().await).unwrap();
    assert!(text.contains("\nfastetcd_recovered_from_backup_total 1\n"), "{text}");
    assert!(text.contains("\nfastetcd_recovered_revision 50\n"), "{text}");

    // `etcdctl alarm disarm`
    svc.alarm(tonic::Request::new(pb::AlarmRequest {
        action: pb::alarm_request::AlarmAction::Deactivate as i32,
        member_id: 0,
        alarm: pb::AlarmType::Corrupt as i32,
    }))
    .await
    .unwrap();
    assert!(list(svc.clone()).await.is_empty());
    assert!(RecoveryAlarm::load(&engine).await.unwrap().active().is_none(), "persisted");
    metrics.refresh(&state).await;
    let mut text = String::new();
    prometheus_client::encoding::text::encode(&mut text, &*metrics.registry.lock().await).unwrap();
    assert!(text.contains("\nfastetcd_recovered_revision 0\n"), "{text}");
    assert!(text.contains("\nfastetcd_recovered_from_backup_total 1\n"), "{text}");
}
