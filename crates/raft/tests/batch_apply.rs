//! Batched log entries (group commit, fastetcd#75).
//!
//! A `Batch` entry applies each proposal at its own revision; a replay
//! of a batch that was partly on disk skips exactly the part that was;
//! and the proposer turns concurrent proposals into few log entries.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use openraft::storage::RaftStateMachine;
use openraft::{Config, Entry, EntryPayload, LogId, Raft, RaftMetrics};
use tempfile::tempdir;

use fastetcd_raft::kv_log_store::KvLogStore;
use fastetcd_raft::types::{FastetcdLogEntry, FastetcdLogResponse, NodeId, TypeConfig};
use fastetcd_raft::{empty_peers, FastetcdStateMachine, Proposer, WriteForwarder};
use fastetcd_storage::mvcc::{Mutation, MvccStore};
use fastetcd_storage::redb_engine::RedbEngine;
use fastetcd_storage::{KvStore, Snapshot, StorageResult, WriteBatch, WriteOptions};

const FSYNC: Duration = Duration::from_millis(100);

/// redb with every durable commit made `FSYNC` slower, as on a slow
/// disk. Non-durable commits (the deferred applies) are not slowed.
struct SlowSync(RedbEngine);

#[async_trait]
impl KvStore for SlowSync {
    async fn snapshot(&self) -> StorageResult<Arc<dyn Snapshot>> {
        self.0.snapshot().await
    }
    async fn commit(&self, batch: WriteBatch, opts: WriteOptions) -> StorageResult<()> {
        if opts.sync {
            tokio::time::sleep(FSYNC).await;
        }
        self.0.commit(batch, opts).await
    }
    async fn sync(&self) -> StorageResult<()> {
        tokio::time::sleep(FSYNC).await;
        self.0.sync().await
    }
    async fn size_on_disk(&self) -> StorageResult<u64> {
        self.0.size_on_disk().await
    }
    fn engine_name(&self) -> &'static str {
        "slow-sync"
    }
}

/// Minimal in-process "network" that errors on any peer message. A
/// single-node cluster never sends peer messages once it's elected.
#[derive(Clone)]
struct NopNetwork;

impl openraft::network::RaftNetworkFactory<TypeConfig> for NopNetwork {
    type Network = NopNet;
    async fn new_client(&mut self, _target: NodeId, _node: &openraft::BasicNode) -> Self::Network {
        NopNet
    }
}

struct NopNet;

impl openraft::network::RaftNetwork<TypeConfig> for NopNet {
    async fn append_entries(
        &mut self,
        _rpc: openraft::raft::AppendEntriesRequest<TypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::AppendEntriesResponse<NodeId>,
        openraft::error::RPCError<NodeId, openraft::BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        Err(openraft::error::RPCError::Network(
            openraft::error::NetworkError::new(&std::io::Error::other(
                "no network in single-node test",
            )),
        ))
    }

    async fn install_snapshot(
        &mut self,
        _rpc: openraft::raft::InstallSnapshotRequest<TypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<NodeId>,
        openraft::error::RPCError<
            NodeId,
            openraft::BasicNode,
            openraft::error::RaftError<NodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        Err(openraft::error::RPCError::Network(
            openraft::error::NetworkError::new(&std::io::Error::other(
                "no network in single-node test",
            )),
        ))
    }

    async fn vote(
        &mut self,
        _rpc: openraft::raft::VoteRequest<NodeId>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::VoteResponse<NodeId>,
        openraft::error::RPCError<NodeId, openraft::BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        Err(openraft::error::RPCError::Network(
            openraft::error::NetworkError::new(&std::io::Error::other(
                "no network in single-node test",
            )),
        ))
    }
}

fn put(key: &str) -> FastetcdLogEntry {
    FastetcdLogEntry::Apply { mutations: vec![put_mutation(key)] }
}

fn put_mutation(key: &str) -> Mutation {
    Mutation::Put {
        key: key.as_bytes().to_vec(),
        value: b"v".to_vec(),
        lease: 0,
        ignore_value: false,
        ignore_lease: false,
        prev_kv: false,
    }
}

fn entry(index: u64, data: FastetcdLogEntry) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId { leader_id: Default::default(), index },
        payload: EntryPayload::Normal(data),
    }
}

/// The state machine's encoding of batch progress: index, then count.
fn progress(index: u64, done: u64) -> Vec<u8> {
    let mut b = index.to_be_bytes().to_vec();
    b.extend_from_slice(&done.to_be_bytes());
    b
}

async fn mod_revision(mvcc: &MvccStore, key: &str) -> i64 {
    let r = mvcc.range(key.as_bytes(), b"", 0, 0, false, false).await.unwrap();
    assert_eq!(r.kvs.len(), 1, "{key} missing");
    r.kvs[0].mod_revision
}

#[tokio::test]
async fn a_batch_applies_each_proposal_at_its_own_revision() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(RedbEngine::open(dir.path().join("db.redb")).unwrap());
    let mvcc = MvccStore::open(engine).await.unwrap();
    let mut sm = FastetcdStateMachine::open(mvcc.clone(), dir.path().join("snapshots"))
        .await
        .unwrap();
    let applied = sm.applied_index();

    let out = sm
        .apply(vec![entry(1, FastetcdLogEntry::Batch(vec![put("a"), put("b"), put("c")]))])
        .await
        .unwrap();
    let FastetcdLogResponse::Batch(rs) = &out[0] else { panic!("{out:?}") };
    let revs: Vec<i64> = rs.iter().map(|r| r.header_revision()).collect();
    assert_eq!(revs, vec![1, 2, 3]);
    assert_eq!(*applied.borrow(), 2, "applied index 1 (+1)");
    assert_eq!(mod_revision(&mvcc, "c").await, 3);
    assert_eq!(mvcc.read_batch_progress().await.unwrap(), Some(progress(1, 3)));

    // A nested batch is refused, never applied.
    let nested = FastetcdLogEntry::Batch(vec![FastetcdLogEntry::Batch(vec![put("x")])]);
    assert!(sm.apply(vec![entry(2, nested)]).await.is_err());
}

#[tokio::test]
async fn a_replayed_batch_skips_what_reached_disk() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(RedbEngine::open(dir.path().join("db.redb")).unwrap());
    let mvcc = MvccStore::open(engine).await.unwrap();

    // What a crash in the middle of applying the batch at index 1 leaves:
    // its first two proposals on disk with progress (1, 2), and no
    // applied position (the batch's last commit carries that).
    mvcc.stage_batch_progress(progress(1, 1)).await;
    mvcc.apply(&[put_mutation("a")]).await.unwrap();
    mvcc.stage_batch_progress(progress(1, 2)).await;
    mvcc.apply(&[put_mutation("b")]).await.unwrap();

    // The restart replays the entry.
    let mut sm = FastetcdStateMachine::open(mvcc.clone(), dir.path().join("snapshots"))
        .await
        .unwrap();
    let out = sm
        .apply(vec![entry(1, FastetcdLogEntry::Batch(vec![put("a"), put("b"), put("c")]))])
        .await
        .unwrap();
    let FastetcdLogResponse::Batch(rs) = &out[0] else { panic!("{out:?}") };
    assert!(matches!(rs[0], FastetcdLogResponse::Noop { .. }), "{rs:?}");
    assert!(matches!(rs[1], FastetcdLogResponse::Noop { .. }), "{rs:?}");
    assert_eq!(rs[2].header_revision(), 3);
    // a and b kept their revisions: applied once, not twice.
    assert_eq!(mod_revision(&mvcc, "a").await, 1);
    assert_eq!(mod_revision(&mvcc, "b").await, 2);
    assert_eq!(mod_revision(&mvcc, "c").await, 3);
    assert_eq!(mvcc.current_revision().await, 3);
}

async fn wait_for_leader(raft: &Raft<TypeConfig>) {
    let mut rx: tokio::sync::watch::Receiver<RaftMetrics<NodeId, openraft::BasicNode>> =
        raft.metrics();
    tokio::time::timeout(
        Duration::from_secs(10),
        rx.wait_for(|m| matches!(m.state, openraft::ServerState::Leader)),
    )
    .await
    .expect("leader within 10s")
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_proposer_batches_concurrent_proposals() {
    let dir = tempdir().unwrap();
    let engine: Arc<dyn KvStore> =
        Arc::new(SlowSync(RedbEngine::open(dir.path().join("db.redb")).unwrap()));
    let mvcc = MvccStore::open(engine.clone()).await.unwrap();
    let sm = FastetcdStateMachine::open(mvcc.clone(), dir.path().join("snapshots"))
        .await
        .unwrap();
    mvcc.defer_apply_sync();
    let log = KvLogStore::new(engine);
    let progress = log.progress();
    let config = Arc::new(
        Config {
            heartbeat_interval: 100,
            election_timeout_min: 300,
            election_timeout_max: 600,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let raft = Raft::<TypeConfig>::new(1, config, NopNetwork, log, sm.clone())
        .await
        .unwrap();
    raft.initialize(BTreeSet::from([1])).await.unwrap();
    wait_for_leader(&raft).await;
    let before = raft.metrics().borrow().last_log_index.unwrap();

    // The redb log's append waits for its fsync.
    let proposer = Proposer::spawn(
        raft.clone(),
        progress,
        WriteForwarder::new(empty_peers()),
        fastetcd_raft::proposer::IN_FLIGHT_BLOCKING_LOG,
    );
    let tasks: Vec<_> = (0..40)
        .map(|i| {
            let p = proposer.clone();
            tokio::spawn(async move { p.propose(put(&format!("k{i:02}"))).await })
        })
        .collect();
    let mut revs = Vec::new();
    for t in tasks {
        let r = t.await.unwrap().expect("proposal applied");
        let FastetcdLogResponse::Apply { revision, .. } = r else { panic!("{r:?}") };
        revs.push(revision);
    }
    revs.sort();
    assert_eq!(revs, (1..=40).collect::<Vec<i64>>(), "each proposal its own revision");
    for i in 0..40 {
        mod_revision(&mvcc, &format!("k{i:02}")).await;
    }

    let entries = raft.metrics().borrow().last_log_index.unwrap() - before;
    let s = proposer.stats();
    let (batches, batched, single) = (
        s.batches.load(std::sync::atomic::Ordering::Relaxed),
        s.batched.load(std::sync::atomic::Ordering::Relaxed),
        s.single.load(std::sync::atomic::Ordering::Relaxed),
    );
    eprintln!("40 proposals → {entries} log entries ({batches} batches of {batched}, {single} alone)");
    assert_eq!(batched + single, 40);
    assert!(batches >= 1);
    assert!(entries <= 10, "40 proposals took {entries} log entries");
}
