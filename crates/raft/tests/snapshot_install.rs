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
    let sm = FastetcdStateMachine::open(mvcc.clone()).await.unwrap();
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
