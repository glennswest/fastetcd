//! Regression tests for fastetcd#9 — a restart must not lose the state
//! machine's `last_applied_log_id`.
//!
//! Before the fix, `FastetcdStateMachine` rebuilt `last_applied_log_id`
//! as `None` on every open. openraft then saw an empty state machine
//! next to a populated MVCC store and replayed the log from index 0,
//! which crash-looped once a snapshot had purged the early entries
//! ("expected index [0, N), got [None, None)") and double-applied every
//! mutation when it hadn't.

use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::{Entry, EntryPayload, LeaderId, LogId, Membership, StoredMembership};
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

/// Open an engine at `path`, run `f` against a fresh state machine over
/// it, then drop everything — simulating a process restart.
async fn with_reopened<F, Fut, T>(path: &std::path::Path, f: F) -> T
where
    F: FnOnce(FastetcdStateMachine, MvccStore) -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let engine: Arc<dyn KvStore> = Arc::new(RedbEngine::open(path).unwrap());
    let mvcc = MvccStore::open(engine).await.unwrap();
    let sm = FastetcdStateMachine::open(mvcc.clone()).await.unwrap();
    f(sm, mvcc).await
}

#[tokio::test]
async fn last_applied_survives_restart() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("raft.redb");

    // First "process": apply three normal entries.
    with_reopened(&path, |mut sm, mvcc| async move {
        sm.apply(vec![
            put_entry(1, "a", "1"),
            put_entry(2, "b", "2"),
            put_entry(3, "c", "3"),
        ])
        .await
        .unwrap();
        assert_eq!(sm.applied_state().await.unwrap().0, Some(log_id(1, 3)));
        assert_eq!(mvcc.current_revision().await, 3);
    })
    .await;

    // Second "process": the state machine must come back at index 3,
    // not at None. A None here is exactly the bug — openraft would ask
    // the log store for [0, 4) and fail if those entries were purged.
    with_reopened(&path, |mut sm, mvcc| async move {
        let (applied, _) = sm.applied_state().await.unwrap();
        assert_eq!(
            applied,
            Some(log_id(1, 3)),
            "last_applied_log_id must be restored from disk after restart"
        );
        assert_eq!(mvcc.current_revision().await, 3);
    })
    .await;
}

#[tokio::test]
async fn membership_entries_persist_their_log_id() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("raft.redb");

    // Membership and blank entries mutate no MVCC state, so nothing
    // folds their staged log id into a write batch — flush_raft_meta
    // has to commit it or the restart replays them.
    with_reopened(&path, |mut sm, _mvcc| async move {
        let members = Membership::new(vec![[1, 2, 3].into_iter().collect()], None);
        sm.apply(vec![
            put_entry(1, "a", "1"),
            Entry {
                log_id: log_id(1, 2),
                payload: EntryPayload::Membership(members),
            },
            Entry {
                log_id: log_id(1, 3),
                payload: EntryPayload::Blank,
            },
        ])
        .await
        .unwrap();
    })
    .await;

    with_reopened(&path, |mut sm, _mvcc| async move {
        let (applied, membership) = sm.applied_state().await.unwrap();
        assert_eq!(
            applied,
            Some(log_id(1, 3)),
            "a trailing blank entry's log id must reach disk"
        );
        assert_ne!(
            membership,
            StoredMembership::default(),
            "last_membership must be restored from disk after restart"
        );
    })
    .await;
}

/// `recover_applied_floor` rewrites the applied position, so the guards
/// that keep it from firing on a healthy node matter more than the
/// recovery itself.
#[tokio::test]
async fn recovery_floor_only_applies_to_damaged_stores() {
    let dir = tempdir().unwrap();
    let floor = log_id(1, 7);

    // Empty store: nothing to recover, even with a floor available.
    let empty = dir.path().join("empty.redb");
    with_reopened(&empty, |sm, _mvcc| async move {
        assert_eq!(sm.recover_applied_floor(Some(floor)).await.unwrap(), None);
    })
    .await;

    // Healthy store: has data AND a persisted applied position, so the
    // floor must not clobber it.
    let healthy = dir.path().join("healthy.redb");
    with_reopened(&healthy, |mut sm, _mvcc| async move {
        sm.apply(vec![put_entry(1, "a", "1")]).await.unwrap();
    })
    .await;
    with_reopened(&healthy, |mut sm, _mvcc| async move {
        assert_eq!(sm.recover_applied_floor(Some(floor)).await.unwrap(), None);
        assert_eq!(sm.applied_state().await.unwrap().0, Some(log_id(1, 1)));
    })
    .await;

    // Damaged store: data present, applied position missing. This is
    // the #9 shape — recovery adopts the floor and persists it.
    let damaged = dir.path().join("damaged.redb");
    with_reopened(&damaged, |_sm, mvcc| async move {
        // Data written without any raft meta, as pre-fix builds did.
        mvcc.apply(&[Mutation::Put {
            key: b"a".to_vec(),
            value: b"1".to_vec(),
            lease: 0,
            prev_kv: false,
            ignore_value: false,
            ignore_lease: false,
        }])
        .await
        .unwrap();
    })
    .await;
    with_reopened(&damaged, |sm, _mvcc| async move {
        assert_eq!(
            sm.recover_applied_floor(Some(floor)).await.unwrap(),
            Some(floor)
        );
    })
    .await;
    // And it survives the next restart without needing recovery again.
    with_reopened(&damaged, |mut sm, _mvcc| async move {
        assert_eq!(sm.applied_state().await.unwrap().0, Some(floor));
        assert_eq!(sm.recover_applied_floor(Some(floor)).await.unwrap(), None);
    })
    .await;
}

/// The failure that actually took the cluster down: replaying an
/// already-applied entry must not mutate MVCC a second time. With
/// last_applied restored, openraft never asks; this asserts the data
/// itself is unchanged across a restart with no new entries.
#[tokio::test]
async fn restart_does_not_double_apply() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("raft.redb");

    with_reopened(&path, |mut sm, _mvcc| async move {
        sm.apply(vec![put_entry(1, "k", "v1"), put_entry(2, "k", "v2")])
            .await
            .unwrap();
    })
    .await;

    with_reopened(&path, |_sm, mvcc| async move {
        assert_eq!(
            mvcc.current_revision().await,
            2,
            "reopening must not advance the revision"
        );
    })
    .await;
}
