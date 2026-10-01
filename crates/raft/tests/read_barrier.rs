//! The read barrier does not queue behind writes (fastetcd#71).
//!
//! A sole-voter leader serves its read index from the log store and the
//! state machine (`read_barrier` with a `LocalReadIndex`). openraft's
//! `ensure_linearizable` is a message to RaftCore, which handles one
//! client write at a time and awaits its log fsync first, so a read
//! waited behind every write queued ahead of it. Here every durable
//! commit takes 100 ms (a slow disk), 30 writes are queued, and the
//! local barrier must answer in a fraction of the queue's 3 s while
//! still seeing every write acknowledged before it began.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use openraft::{Config, Raft, RaftMetrics};
use tempfile::tempdir;

use fastetcd_raft::kv_log_store::KvLogStore;
use fastetcd_raft::types::{FastetcdLogEntry, NodeId, TypeConfig};
use fastetcd_raft::{read_barrier, FastetcdStateMachine, LocalReadIndex};
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

fn put(key: String) -> FastetcdLogEntry {
    FastetcdLogEntry::Apply {
        mutations: vec![Mutation::Put {
            key: key.into_bytes(),
            value: b"v".to_vec(),
            lease: 0,
            ignore_value: false,
            ignore_lease: false,
            prev_kv: false,
        }],
    }
}

/// Queue `n` writes; each records its key once acknowledged.
fn spawn_writes(
    raft: &Raft<TypeConfig>,
    prefix: &str,
    n: usize,
    done: &Arc<Mutex<Vec<String>>>,
) -> Vec<tokio::task::JoinHandle<()>> {
    (0..n)
        .map(|i| {
            let raft = raft.clone();
            let done = done.clone();
            let key = format!("{prefix}{i:03}");
            tokio::spawn(async move {
                raft.client_write(put(key.clone())).await.expect("write");
                done.lock().unwrap().push(key);
            })
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sole_voter_read_does_not_wait_for_queued_writes() {
    let dir = tempdir().unwrap();
    let engine: Arc<dyn KvStore> =
        Arc::new(SlowSync(RedbEngine::open(dir.path().join("db.redb")).unwrap()));
    let mvcc = MvccStore::open(engine.clone()).await.unwrap();
    let sm = FastetcdStateMachine::open(mvcc.clone(), dir.path().join("snapshots"))
        .await
        .unwrap();
    mvcc.defer_apply_sync();
    let log = KvLogStore::new(engine);
    let local = LocalReadIndex::new(log.progress(), sm.applied_index());

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

    // Not yet a leader: the local path does not apply, and openraft's
    // barrier refuses.
    assert!(read_barrier(&raft, Some(&local)).await.is_err());

    raft.initialize(BTreeSet::from([1])).await.unwrap();
    wait_for_leader(&raft).await;
    raft.client_write(put("warm".into())).await.unwrap();

    let done = Arc::new(Mutex::new(Vec::new()));

    // Control: openraft's own barrier waits behind the queue.
    let writes = spawn_writes(&raft, "a", 30, &done);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let t = Instant::now();
    raft.ensure_linearizable().await.unwrap();
    let control = t.elapsed();
    for w in writes {
        w.await.unwrap();
    }

    // The local barrier does not.
    let writes = spawn_writes(&raft, "b", 30, &done);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut slowest = Duration::ZERO;
    for _ in 0..10 {
        let acked: Vec<String> = done.lock().unwrap().clone();
        let t = Instant::now();
        read_barrier(&raft, Some(&local)).await.unwrap();
        slowest = slowest.max(t.elapsed());
        // Linearizable: every write acknowledged before the read began
        // is visible after the barrier.
        for key in &acked {
            let r = mvcc.range(key.as_bytes(), b"", 0, 0, false, true).await.unwrap();
            assert_eq!(r.count, 1, "{key} was acknowledged before the read but is not visible");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for w in writes {
        w.await.unwrap();
    }

    eprintln!("ensure_linearizable behind 30 writes: {control:?}; local barrier slowest: {slowest:?}");
    assert!(
        control > Duration::from_secs(1),
        "control: expected ensure_linearizable to queue behind the writes, took {control:?}"
    );
    assert!(
        slowest < 3 * FSYNC,
        "local read barrier took {slowest:?} behind queued writes"
    );
}
