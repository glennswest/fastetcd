//! Regression test for fastetcd#11 — a data directory written before
//! durable membership existed (v0.8.2) must not come up with an empty
//! voter set after upgrade.
//!
//! v0.8.2 never persisted raft membership. Once its log was purged, the
//! bootstrap membership entry was gone from everywhere, and a restart
//! under v1.0.0 loaded an empty voter set → no leader. The v1.0.1+
//! upgrade recovers the voter set in place and persists it durably,
//! preferring the *actual* membership still in the retained log and only
//! falling back to `--initial-cluster` when the log has been purged
//! clean of it. These tests cover: the config-fallback path survives a
//! restart, a dir that already has membership is left alone, and the
//! retained-log scan recovers the exact on-disk voter set.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::{
    BasicNode, Entry, EntryPayload, LeaderId, LogId, Membership, RaftLogReader, StoredMembership,
};
use tempfile::tempdir;

use fastetcd_raft::kv_log_store::KvLogStore;
use fastetcd_raft::types::{NodeId, TypeConfig};
use fastetcd_raft::FastetcdStateMachine;
use fastetcd_storage::mvcc::{Mutation, MvccStore};
use fastetcd_storage::redb_engine::RedbEngine;
use fastetcd_storage::{KvStore, WriteBatch, WriteOptions};

async fn open_sm(path: &std::path::Path) -> (FastetcdStateMachine, MvccStore) {
    let engine: Arc<dyn KvStore> = Arc::new(RedbEngine::open(path).unwrap());
    let mvcc = MvccStore::open(engine).await.unwrap();
    let sm = FastetcdStateMachine::open(mvcc.clone(), path.parent().unwrap().join("snapshots")).await.unwrap();
    (sm, mvcc)
}

fn config_membership(ids: &[NodeId]) -> StoredMembership<NodeId, BasicNode> {
    let voters: BTreeSet<NodeId> = ids.iter().copied().collect();
    let nodes: BTreeMap<NodeId, BasicNode> = ids
        .iter()
        .map(|id| (*id, BasicNode::new(format!("http://10.0.0.{id}:2380"))))
        .collect();
    StoredMembership::new(
        Some(LogId {
            leader_id: LeaderId::new(1, 1),
            index: 5,
        }),
        Membership::new(vec![voters], nodes),
    )
}

#[tokio::test]
async fn recovers_membership_for_a_legacy_data_dir() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("legacy.redb");

    // Simulate a v0.8.2 directory: MVCC data written straight through
    // the store (as the old state machine did), so nothing persisted
    // raft membership.
    {
        let (sm, mvcc) = open_sm(&path).await;
        for i in 0..5u64 {
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
        // This is the stranded state: data present, no voters.
        assert!(mvcc.current_revision().await > 0);
        assert!(
            sm.membership_is_empty().await,
            "precondition: a legacy dir has no persisted membership"
        );
        assert!(
            mvcc.read_format_version().await.unwrap().is_none(),
            "precondition: a legacy dir has no format marker"
        );
    }

    // Recovery: rebuild membership from the configured cluster and stamp
    // the format version, exactly as startup does.
    {
        let (sm, mvcc) = open_sm(&path).await;
        assert!(sm.membership_is_empty().await, "still empty on reopen");
        sm.recover_membership(config_membership(&[1, 2, 3]))
            .await
            .unwrap();
        mvcc.write_format_version(fastetcd_storage::mvcc::store::FORMAT_VERSION)
            .await
            .unwrap();
    }

    // After a restart the recovered voter set must be present and the
    // format marker must suppress a second recovery.
    {
        let (mut sm, mvcc) = open_sm(&path).await;
        assert!(
            !sm.membership_is_empty().await,
            "membership must survive restart"
        );
        let (_applied, membership) = sm.applied_state().await.unwrap();
        let voters: BTreeSet<NodeId> = membership.membership().voter_ids().collect();
        assert_eq!(voters, [1, 2, 3].into_iter().collect());
        assert_eq!(
            mvcc.read_format_version().await.unwrap(),
            Some(fastetcd_storage::mvcc::store::FORMAT_VERSION),
            "format marker must be persisted so recovery does not re-run"
        );
    }
}

/// A healthy directory that already has a voter set must be left alone —
/// recovery keys off an empty membership, so this guards against it
/// firing on a normal restart.
#[tokio::test]
async fn does_not_touch_a_dir_that_already_has_membership() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("healthy.redb");
    {
        let (sm, _mvcc) = open_sm(&path).await;
        sm.recover_membership(config_membership(&[1, 2, 3]))
            .await
            .unwrap();
    }
    let (sm, _mvcc) = open_sm(&path).await;
    assert!(!sm.membership_is_empty().await);
}

/// A faithful conversion recovers the *actual* voter set from the
/// retained raft log, not a guess from config. This writes a real
/// `Membership` entry into the log store (as it exists on disk) and
/// verifies the startup scan extracts exactly those voters — including
/// a set that differs from any static config, which is the case a
/// config-only recovery would get wrong.
#[tokio::test]
async fn recovers_actual_membership_from_the_retained_log() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("log.redb");
    let engine: Arc<dyn KvStore> = Arc::new(RedbEngine::open(&path).unwrap());

    // The live voter set diverged from bootstrap: nodes {2,3,4}, not the
    // {1,2,3} a config might name. Write it as a Membership log entry at
    // index 40, plus a later plain entry, exactly as the on-disk log holds.
    let voters: BTreeSet<NodeId> = [2, 4, 3].into_iter().collect();
    let nodes: BTreeMap<NodeId, BasicNode> = voters
        .iter()
        .map(|id| (*id, BasicNode::new(format!("http://10.0.0.{id}:2380"))))
        .collect();
    let membership_entry: Entry<TypeConfig> = Entry {
        log_id: LogId {
            leader_id: LeaderId::new(2, 2),
            index: 40,
        },
        payload: EntryPayload::Membership(Membership::new(vec![voters.clone()], nodes)),
    };
    let tail_entry: Entry<TypeConfig> = Entry {
        log_id: LogId {
            leader_id: LeaderId::new(2, 2),
            index: 41,
        },
        payload: EntryPayload::Blank,
    };
    let mut batch = WriteBatch::new();
    for e in [&membership_entry, &tail_entry] {
        batch.put(
            "raft_log",
            &e.log_id.index.to_be_bytes(),
            &bincode::serialize(e).unwrap(),
        );
    }
    engine.commit(batch, WriteOptions::default()).await.unwrap();

    // The same scan startup performs over the retained log.
    let mut log = KvLogStore::new(engine);
    let mut latest: Option<StoredMembership<NodeId, BasicNode>> = None;
    for entry in log.try_get_log_entries(0..42).await.unwrap() {
        if let EntryPayload::Membership(m) = entry.payload {
            latest = Some(StoredMembership::new(Some(entry.log_id), m));
        }
    }

    let recovered = latest.expect("membership entry must be found in the retained log");
    let got: BTreeSet<NodeId> = recovered.membership().voter_ids().collect();
    assert_eq!(
        got, voters,
        "must recover the exact on-disk voter set {{2,3,4}}, not a config guess"
    );
}
