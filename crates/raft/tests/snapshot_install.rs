//! Regression tests for fastetcd#8 — a node that installs a leader
//! snapshot must actually serve the snapshot's data.
//!
//! The reported symptom was a rejoined member whose raftIndex caught up
//! but whose MVCC revision stayed far behind (2927 vs 17918), holding
//! only half the keys. `rebuild_mvcc` writes every MVCC table straight
//! through the engine, so the `MvccStore` handle kept serving the
//! counters it had cached at open: reads clamped to the old revision
//! and new writes allocated revisions that collided with the
//! snapshot's.

use std::sync::Arc;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{Entry, EntryPayload, LeaderId, LogId};
use tempfile::tempdir;

use fastetcd_raft::types::{FastetcdLogEntry, NodeId, TypeConfig};
use fastetcd_raft::FastetcdStateMachine;
use fastetcd_storage::mvcc::{Mutation, MvccStore};
use fastetcd_storage::redb_engine::RedbEngine;
use fastetcd_storage::KvStore;

fn log_id(term: u64, index: u64) -> LogId<NodeId> {
    LogId {
        leader_id: LeaderId::new(term, 1),
        index,
    }
}

fn put_entry(index: u64, key: &str, value: &str) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(1, index),
        payload: EntryPayload::Normal(FastetcdLogEntry::Apply {
            mutations: vec![Mutation::Put {
                key: key.as_bytes().to_vec(),
                value: value.as_bytes().to_vec(),
                lease: 0,
                prev_kv: false,
                ignore_value: false,
                ignore_lease: false,
            }],
        }),
    }
}

async fn open_sm(path: &std::path::Path) -> (FastetcdStateMachine, MvccStore) {
    let engine: Arc<dyn KvStore> = Arc::new(RedbEngine::open(path).unwrap());
    let mvcc = MvccStore::open(engine).await.unwrap();
    let sm = FastetcdStateMachine::open(mvcc.clone(), path.parent().unwrap().join("snapshots")).await.unwrap();
    (sm, mvcc)
}

/// Count keys visible through a full-range read — this is what the
/// issue's "504 of 1004 keys" measured.
async fn visible_keys(mvcc: &MvccStore) -> usize {
    let res = mvcc
        .range(b"", &[0xFF; 16], 0, 0, true, false)
        .await
        .unwrap();
    res.kvs.len()
}

#[tokio::test]
async fn learner_serves_installed_snapshot_data() {
    let dir = tempdir().unwrap();

    // Leader: 50 keys, ending at revision 50.
    let (mut leader, leader_mvcc) = open_sm(&dir.path().join("leader.redb")).await;
    for i in 1..=50u64 {
        leader
            .apply(vec![put_entry(i, &format!("k{i:03}"), "v")])
            .await
            .unwrap();
    }
    assert_eq!(leader_mvcc.current_revision().await, 50);
    assert_eq!(visible_keys(&leader_mvcc).await, 50);

    let snapshot = leader.get_snapshot_builder().await.build_snapshot().await.unwrap();
    let meta = snapshot.meta.clone();
    let data = snapshot.snapshot;

    // Learner: empty data dir, installs the snapshot.
    let (mut learner, learner_mvcc) = open_sm(&dir.path().join("learner.redb")).await;
    assert_eq!(learner_mvcc.current_revision().await, 0);
    learner.install_snapshot(&meta, data).await.unwrap();

    // This is the bug: the data was on disk, but the handle still
    // reported revision 0 and reads clamped to it, hiding every key.
    assert_eq!(
        learner_mvcc.current_revision().await,
        50,
        "installing a snapshot must advance the visible MVCC revision"
    );
    assert_eq!(
        visible_keys(&learner_mvcc).await,
        50,
        "every key in the snapshot must be visible after install"
    );
    assert_eq!(learner.applied_state().await.unwrap().0, Some(log_id(1, 50)));
}

/// After installing, the learner applies the log tail. Those entries
/// must allocate revisions above the snapshot's, not collide with it.
#[tokio::test]
async fn applies_after_install_do_not_collide_with_snapshot_revisions() {
    let dir = tempdir().unwrap();

    let (mut leader, _leader_mvcc) = open_sm(&dir.path().join("leader.redb")).await;
    for i in 1..=20u64 {
        leader
            .apply(vec![put_entry(i, &format!("k{i:03}"), "v")])
            .await
            .unwrap();
    }
    let snapshot = leader.get_snapshot_builder().await.build_snapshot().await.unwrap();
    let (meta, data) = (snapshot.meta.clone(), snapshot.snapshot);

    let (mut learner, learner_mvcc) = open_sm(&dir.path().join("learner.redb")).await;
    learner.install_snapshot(&meta, data).await.unwrap();

    // Apply the tail, as a catching-up learner does.
    learner
        .apply(vec![put_entry(21, "k021", "v"), put_entry(22, "k022", "v")])
        .await
        .unwrap();

    assert_eq!(
        learner_mvcc.current_revision().await,
        22,
        "tail applies must continue from the snapshot's revision"
    );
    assert_eq!(
        visible_keys(&learner_mvcc).await,
        22,
        "snapshot keys and tail keys must both be visible"
    );
}

/// The install must also survive a restart of the learner.
#[tokio::test]
async fn installed_snapshot_survives_restart() {
    let dir = tempdir().unwrap();
    let learner_path = dir.path().join("learner.redb");

    let (mut leader, _m) = open_sm(&dir.path().join("leader.redb")).await;
    for i in 1..=10u64 {
        leader
            .apply(vec![put_entry(i, &format!("k{i:03}"), "v")])
            .await
            .unwrap();
    }
    let snapshot = leader.get_snapshot_builder().await.build_snapshot().await.unwrap();
    let (meta, data) = (snapshot.meta.clone(), snapshot.snapshot);

    {
        let (mut learner, _m) = open_sm(&learner_path).await;
        learner.install_snapshot(&meta, data).await.unwrap();
    }

    let (mut learner, learner_mvcc) = open_sm(&learner_path).await;
    assert_eq!(learner_mvcc.current_revision().await, 10);
    assert_eq!(visible_keys(&learner_mvcc).await, 10);
    assert_eq!(
        learner.applied_state().await.unwrap().0,
        Some(log_id(1, 10)),
        "the installed applied position must be persisted, not just in memory"
    );
}

/// #13 memory + purge-resume: a built snapshot must live on disk (not be
/// held in RAM) and survive a restart, so openraft can purge the log
/// against it immediately after a restart instead of stalling.
#[tokio::test]
async fn snapshot_persists_to_disk_and_survives_restart() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s.redb");
    let snap_dir = path.parent().unwrap().join("snapshots");
    // Snapshots are named by the log index they cover, zero-padded.
    let snap_file = snap_dir.join("00000000000000000002.snap");

    {
        let (mut sm, _mvcc) = open_sm(&path).await;
        sm.apply(vec![put_entry(1, "a", "1"), put_entry(2, "b", "2")])
            .await
            .unwrap();
        let s = sm.get_snapshot_builder().await.build_snapshot().await.unwrap();
        assert_eq!(s.meta.last_log_id.map(|l| l.index), Some(2));
    }
    assert!(snap_file.exists(), "snapshot body must be persisted to disk");

    // Reopen (simulated restart): the snapshot must still be available.
    {
        let (mut sm, _mvcc) = open_sm(&path).await;
        let cur = sm
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("snapshot must survive restart so purge can resume");
        assert_eq!(cur.meta.last_log_id.map(|l| l.index), Some(2));
    }
}

/// The snapshot directory must not grow with every snapshot taken: each
/// one is a full copy of the database, and on a fixed-size volume that
/// is what fills it (fastetcd#14). Retention rolls the old ones off.
#[tokio::test]
async fn repeated_snapshots_do_not_accumulate_on_disk() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s.redb");
    let snap_dir = path.parent().unwrap().join("snapshots");

    let (mut sm, _mvcc) = open_sm(&path).await;
    let mut sizes = Vec::new();
    for index in 1..=5u64 {
        sm.apply(vec![put_entry(index, &format!("k{index}"), "v")])
            .await
            .unwrap();
        sm.get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        sizes.push(fastetcd_storage::fs_space::dir_size(&snap_dir));
    }

    let bodies = std::fs::read_dir(&snap_dir)
        .unwrap()
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|x| x == "snap"))
        .count();
    assert_eq!(bodies, 1, "the default retention keeps exactly one snapshot");

    // Five snapshots of a store that grew by one small key each time
    // must not have produced five snapshots' worth of files.
    assert!(
        sizes[4] < sizes[0] * 3,
        "snapshot directory grew from {} to {} over five snapshots",
        sizes[0],
        sizes[4]
    );
    assert_eq!(
        sm.snapshot_bytes(),
        fastetcd_storage::fs_space::dir_size(&snap_dir),
        "snapshot_bytes must report what is actually on the volume"
    );
}

/// With a larger retention, older snapshots are kept — and still rolled
/// off oldest-first once the limit is reached.
#[tokio::test]
async fn retention_above_one_keeps_that_many_snapshots() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s.redb");
    let snap_dir = path.parent().unwrap().join("snapshots");

    let engine: Arc<dyn KvStore> = Arc::new(RedbEngine::open(&path).unwrap());
    let mvcc = MvccStore::open(engine).await.unwrap();
    let mut sm = FastetcdStateMachine::open_with_retention(mvcc, &snap_dir, 3)
        .await
        .unwrap();
    assert_eq!(sm.snapshot_retention(), 3);

    for index in 1..=6u64 {
        sm.apply(vec![put_entry(index, &format!("k{index}"), "v")])
            .await
            .unwrap();
        sm.get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
    }

    let mut kept: Vec<String> = std::fs::read_dir(&snap_dir)
        .unwrap()
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|x| x == "snap"))
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    kept.sort();
    assert_eq!(
        kept,
        vec![
            "00000000000000000004.snap".to_string(),
            "00000000000000000005.snap".to_string(),
            "00000000000000000006.snap".to_string(),
        ],
        "the three newest snapshots survive, oldest-first roll-off"
    );

    // The newest is still the one openraft gets.
    let cur = sm.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(cur.meta.last_log_id.map(|l| l.index), Some(6));
}

/// A snapshot that cannot be written must not take the node down
/// (fastetcd#14).
///
/// openraft treats a `StorageError` from the snapshot builder as fatal:
/// it comes back out of the linearizable read barrier *and* out of
/// every proposal, so on the reported node a full volume made reads and
/// writes fail together — and deleting keys to make room failed for the
/// same reason. The snapshot body is a durability convenience; the
/// state machine's data and `last_applied` are already committed. So a
/// failed write must degrade to "no current snapshot" (the log just
/// isn't purged yet), not to a dead node.
#[tokio::test]
async fn a_snapshot_that_cannot_be_written_does_not_fail_the_node() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s.redb");
    let snap_dir = path.parent().unwrap().join("snapshots");

    let (mut sm, mvcc) = open_sm(&path).await;
    sm.apply(vec![put_entry(1, "a", "1"), put_entry(2, "b", "2")])
        .await
        .unwrap();

    // Block the write by parking a directory where the snapshot body
    // has to land: the rename onto it fails for any user, root
    // included, which a read-only directory would not.
    std::fs::create_dir_all(snap_dir.join("00000000000000000002.snap")).unwrap();

    let built = sm
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .expect("an unwritable snapshot must not surface as a storage error");
    assert_eq!(built.meta.last_log_id.map(|l| l.index), Some(2));

    // No snapshot is reported, so openraft simply doesn't purge yet.
    assert!(
        sm.get_current_snapshot().await.unwrap().is_none(),
        "a snapshot that was not persisted must not be reported as current"
    );

    // And the state machine keeps working — which is the whole point.
    sm.apply(vec![put_entry(3, "c", "3")])
        .await
        .expect("applies must continue after a failed snapshot write");
    assert_eq!(mvcc.current_revision().await, 3);
    assert_eq!(visible_keys(&mvcc).await, 3);
}
