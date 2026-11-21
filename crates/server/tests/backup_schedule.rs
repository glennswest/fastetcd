//! fastetcd#225: the backup scheduler notices every membership change,
//! with no KV write in between, including across a restart.
//!
//! - A learner promoted to a voter keeps the same node ids and the same
//!   revision; the scheduler hashed only the ids, so the newest backup
//!   kept the learner configuration and a restore undid the promotion.
//! - On a restart, the scheduler took the membership *now* to be what
//!   the newest backup held, so a member added while the process was
//!   down was never backed up until the next KV write.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use fastetcd_raft::types::NodeId;
use fastetcd_server::backup::{self, BackupConfig, MembershipRoles, Scheduler};
use fastetcd_storage::mvcc::{Mutation, MvccStore};
use fastetcd_storage::redb_engine::RedbEngine;
use fastetcd_storage::KvStore;

struct Node {
    _tmp: tempfile::TempDir,
    cfg: BackupConfig,
    engine: Arc<dyn KvStore>,
    mvcc: MvccStore,
}

async fn node() -> Node {
    let tmp = tempfile::tempdir().unwrap();
    let engine: Arc<dyn KvStore> =
        Arc::new(RedbEngine::open(tmp.path().join("fastetcd.redb")).unwrap());
    let mvcc = MvccStore::open(engine.clone()).await.unwrap();
    let cfg = BackupConfig {
        dir: tmp.path().join("backups"),
        // Long enough that only a change of membership makes one due.
        interval: Duration::from_secs(3600),
        every_revisions: 0,
        retain: 8,
        node_id: 1,
    };
    Node { _tmp: tmp, cfg, engine, mvcc }
}

/// Record a membership in the store, as the state machine does when it
/// applies a membership entry: `configs` are the voter sets (two for a
/// joint configuration), `learners` the rest.
async fn set_membership(mvcc: &MvccStore, configs: &[&[NodeId]], learners: &[NodeId]) {
    let voters: BTreeSet<NodeId> = configs.iter().flat_map(|c| c.iter().copied()).collect();
    let nodes: BTreeMap<NodeId, openraft::BasicNode> = voters
        .iter()
        .chain(learners)
        .map(|id| (*id, openraft::BasicNode::new(format!("http://node-{id}"))))
        .collect();
    let configs: Vec<BTreeSet<NodeId>> =
        configs.iter().map(|c| c.iter().copied().collect()).collect();
    let membership = openraft::StoredMembership::new(
        Some(openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), 1)),
        openraft::Membership::new(configs, nodes),
    );
    mvcc.persist_membership(&bincode::serialize(&membership).unwrap()).await.unwrap();
}

async fn put(mvcc: &MvccStore, key: &str) {
    mvcc.apply(&[Mutation::Put {
        key: key.as_bytes().to_vec(),
        value: b"v".to_vec(),
        lease: 0,
        prev_kv: false,
        ignore_value: false,
        ignore_lease: false,
    }])
    .await
    .unwrap();
}

fn roles(voter_sets: &[&[NodeId]], learners: &[NodeId]) -> MembershipRoles {
    MembershipRoles {
        voter_sets: voter_sets.iter().map(|v| v.to_vec()).collect(),
        learners: learners.to_vec(),
    }
}

#[tokio::test]
async fn a_promotion_with_no_kv_write_is_backed_up() {
    let n = node().await;
    set_membership(&n.mvcc, &[&[1]], &[2]).await;
    put(&n.mvcc, "a").await;

    let mut s = Scheduler::resume(&n.cfg.dir);
    let first = s.tick(&n.cfg, &n.engine, &n.mvcc).await.unwrap().expect("first backup");
    assert_eq!(first.header.roles, Some(roles(&[&[1]], &[2])));
    assert!(s.tick(&n.cfg, &n.engine, &n.mvcc).await.unwrap().is_none(), "nothing changed");

    // The joint step and then the promotion: same ids, same revision.
    set_membership(&n.mvcc, &[&[1], &[1, 2]], &[]).await;
    let joint = s.tick(&n.cfg, &n.engine, &n.mvcc).await.unwrap().expect("joint backed up");
    assert_eq!(joint.header.revision, first.header.revision);
    assert_eq!(joint.header.roles, Some(roles(&[&[1], &[1, 2]], &[])));

    set_membership(&n.mvcc, &[&[1, 2]], &[]).await;
    let promoted = s.tick(&n.cfg, &n.engine, &n.mvcc).await.unwrap().expect("promotion backed up");
    assert_eq!(promoted.header.members, first.header.members, "same ids");
    assert_eq!(promoted.header.roles, Some(roles(&[&[1, 2]], &[])));
    assert!(s.tick(&n.cfg, &n.engine, &n.mvcc).await.unwrap().is_none());

    // What is on disk says so too, for the next start and for restore.
    let newest = backup::verify(&backup::list(&n.cfg.dir)[0]).unwrap();
    assert_eq!(newest.roles, Some(roles(&[&[1, 2]], &[])));
}

#[tokio::test]
async fn a_member_added_before_a_restart_is_backed_up_after_it() {
    let n = node().await;
    set_membership(&n.mvcc, &[&[1]], &[]).await;
    put(&n.mvcc, "a").await;
    {
        let mut s = Scheduler::resume(&n.cfg.dir);
        s.tick(&n.cfg, &n.engine, &n.mvcc).await.unwrap().expect("lone member backed up");
    }

    // A restart with nothing changed takes no backup ...
    let mut s = Scheduler::resume(&n.cfg.dir);
    assert!(s.tick(&n.cfg, &n.engine, &n.mvcc).await.unwrap().is_none());

    // ... but a member that joined before it does, with no KV write.
    set_membership(&n.mvcc, &[&[1]], &[2]).await;
    let mut s = Scheduler::resume(&n.cfg.dir);
    let after = s.tick(&n.cfg, &n.engine, &n.mvcc).await.unwrap().expect("join backed up");
    assert_eq!(after.header.members, vec![1, 2]);
    assert_eq!(backup::list(&n.cfg.dir).len(), 2);
}
