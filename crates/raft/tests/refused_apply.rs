//! A request that cannot apply is refused, never a state-machine error
//! (fastetcd#49).
//!
//! openraft turns an error from `apply` into a `StorageError` that stops
//! RaftCore, and every member applies the same entry, so one bad request
//! used to stop the whole cluster (and again on every restart, replaying
//! it). Each such request now answers `Refused`, changes nothing, and
//! the entries around it, inside a batch too, apply as usual.

use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::{Entry, EntryPayload, LogId};
use tempfile::tempdir;

use fastetcd_raft::types::{FastetcdLogEntry, FastetcdLogResponse, TypeConfig};
use fastetcd_raft::FastetcdStateMachine;
use fastetcd_storage::mvcc::{Mutation, MvccStore, RangeOp, Refusal, TxnOp};
use fastetcd_storage::redb_engine::RedbEngine;

fn put_mutation(key: &str, ignore_value: bool, ignore_lease: bool) -> Mutation {
    Mutation::Put {
        key: key.as_bytes().to_vec(),
        value: b"v".to_vec(),
        lease: 0,
        ignore_value,
        ignore_lease,
        prev_kv: false,
    }
}

fn put(key: &str) -> FastetcdLogEntry {
    FastetcdLogEntry::Apply { mutations: vec![put_mutation(key, false, false)] }
}

fn entry(index: u64, data: FastetcdLogEntry) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId { leader_id: Default::default(), index },
        payload: EntryPayload::Normal(data),
    }
}

/// Every request the state machine refuses, with the refusal expected,
/// on a store holding only revisions 1..=2 and nothing compacted.
fn refused_requests() -> Vec<(&'static str, FastetcdLogEntry, Refusal)> {
    let range_at = |revision| TxnOp::Range(RangeOp {
        key: b"a".to_vec(),
        range_end: Vec::new(),
        limit: 0,
        revision,
        keys_only: false,
        count_only: false,
    });
    vec![
        (
            "ignore_value put on a missing key",
            FastetcdLogEntry::Apply { mutations: vec![put_mutation("missing", true, false)] },
            Refusal::KeyNotFound,
        ),
        (
            "ignore_lease put on a missing key, after a put in the same entry",
            FastetcdLogEntry::Apply {
                mutations: vec![put_mutation("z", false, false), put_mutation("missing", false, true)],
            },
            Refusal::KeyNotFound,
        ),
        (
            "txn branch with an ignore_value put on a missing key",
            FastetcdLogEntry::Txn {
                compares: vec![],
                success: vec![
                    TxnOp::Mutation(put_mutation("z", false, false)),
                    TxnOp::Mutation(put_mutation("missing", true, false)),
                ],
                failure: vec![],
            },
            Refusal::KeyNotFound,
        ),
        (
            "txn range at a future revision",
            FastetcdLogEntry::Txn {
                compares: vec![],
                success: vec![TxnOp::Mutation(put_mutation("z", false, false)), range_at(99)],
                failure: vec![],
            },
            Refusal::FutureRevision,
        ),
        ("compact at a future revision", FastetcdLogEntry::Compact { rev: 99 }, Refusal::FutureRevision),
        (
            "compact at revision 0",
            FastetcdLogEntry::Compact { rev: 0 },
            Refusal::InvalidArgument("compact rev must be > 0, got 0".into()),
        ),
        (
            "keep-alive of a lease that does not exist",
            FastetcdLogEntry::LeaseKeepAlive { id: 4242, now_unix: 0 },
            Refusal::LeaseNotFound,
        ),
        (
            "lease grant with TTL 0",
            FastetcdLogEntry::LeaseGrant { id: 0, ttl_secs: 0, now_unix: 0 },
            Refusal::InvalidArgument("lease TTL must be positive, got 0".into()),
        ),
    ]
}

#[tokio::test]
async fn each_refused_request_answers_and_changes_nothing() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(RedbEngine::open(dir.path().join("db.redb")).unwrap());
    let mvcc = MvccStore::open(engine).await.unwrap();
    let mut sm = FastetcdStateMachine::open(mvcc.clone(), dir.path().join("snapshots"))
        .await
        .unwrap();
    sm.apply(vec![entry(1, put("a")), entry(2, put("b"))]).await.unwrap();
    assert_eq!(mvcc.current_revision().await, 2);

    let mut index = 2;
    for (what, request, want) in refused_requests() {
        index += 1;
        let out = sm
            .apply(vec![entry(index, request)])
            .await
            .unwrap_or_else(|e| panic!("{what}: apply stopped the state machine: {e}"));
        match &out[0] {
            FastetcdLogResponse::Refused { revision, refusal } => {
                assert_eq!(refusal, &want, "{what}");
                assert_eq!(*revision, 2, "{what}");
            }
            other => panic!("{what}: not refused: {other:?}"),
        }
        assert_eq!(mvcc.current_revision().await, 2, "{what} changed the store");
        assert_eq!(*sm.applied_index().borrow(), index + 1, "{what}: applied position");
    }
    let z = mvcc.range(b"z", b"", 0, 0, false, false).await.unwrap();
    assert!(z.kvs.is_empty(), "a refused entry's earlier put was applied");
    assert_eq!(mvcc.compact_revision().await, 0);

    // The state machine keeps applying.
    index += 1;
    let out = sm.apply(vec![entry(index, put("c"))]).await.unwrap();
    assert_eq!(out[0].header_revision(), 3);

    // The refused entries' log ids were persisted: a restart resumes
    // after them rather than replaying them.
    drop(sm);
    let sm = FastetcdStateMachine::open(mvcc.clone(), dir.path().join("snapshots"))
        .await
        .unwrap();
    assert_eq!(*sm.applied_index().borrow(), index + 1);
}

#[tokio::test]
async fn a_refused_proposal_in_a_batch_leaves_its_neighbours_applied() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(RedbEngine::open(dir.path().join("db.redb")).unwrap());
    let mvcc = MvccStore::open(engine).await.unwrap();
    let mut sm = FastetcdStateMachine::open(mvcc.clone(), dir.path().join("snapshots"))
        .await
        .unwrap();

    let bad = FastetcdLogEntry::Apply { mutations: vec![put_mutation("missing", true, false)] };
    let batch = || FastetcdLogEntry::Batch(vec![put("a"), bad.clone(), put("c")]);
    let out = sm.apply(vec![entry(1, batch())]).await.expect("the batch applies");
    let FastetcdLogResponse::Batch(rs) = &out[0] else { panic!("{out:?}") };
    assert_eq!(rs.len(), 3);
    assert_eq!(rs[0].header_revision(), 1);
    assert!(
        matches!(&rs[1], FastetcdLogResponse::Refused { refusal: Refusal::KeyNotFound, revision: 1 }),
        "{rs:?}"
    );
    assert_eq!(rs[2].header_revision(), 2, "the proposal after the refused one");
    assert_eq!(*sm.applied_index().borrow(), 2);
    for key in ["a", "c"] {
        let r = mvcc.range(key.as_bytes(), b"", 0, 0, false, false).await.unwrap();
        assert_eq!(r.kvs.len(), 1, "{key} not applied");
    }

    // A replay of the same batch (what a restart before the batch's
    // last commit does, from no progress) refuses the same proposal and
    // stops nothing.
    let dir2 = tempdir().unwrap();
    let engine2 = Arc::new(RedbEngine::open(dir2.path().join("db.redb")).unwrap());
    let mvcc2 = MvccStore::open(engine2).await.unwrap();
    let mut sm2 = FastetcdStateMachine::open(mvcc2.clone(), dir2.path().join("snapshots"))
        .await
        .unwrap();
    let out2 = sm2.apply(vec![entry(1, batch())]).await.unwrap();
    let FastetcdLogResponse::Batch(rs2) = &out2[0] else { panic!("{out2:?}") };
    assert!(matches!(&rs2[1], FastetcdLogResponse::Refused { .. }));
    assert_eq!(mvcc2.current_revision().await, mvcc.current_revision().await);
}
