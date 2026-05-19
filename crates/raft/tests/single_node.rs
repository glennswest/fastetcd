//! End-to-end test: bring up a single-node openraft cluster, propose
//! a few entries, observe them apply against the MVCC state machine.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use openraft::Config;
use openraft::Raft;
use openraft::RaftMetrics;
use tempfile::tempdir;

use fastetcd_raft::log_store::MemLogStore;
use fastetcd_raft::types::{FastetcdLogEntry, FastetcdLogResponse, NodeId, TypeConfig};
use fastetcd_raft::FastetcdStateMachine;
use fastetcd_storage::mvcc::{Mutation, MvccStore};
use fastetcd_storage::redb_engine::RedbEngine;

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
            openraft::error::NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::Other,
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
            openraft::error::NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::Other,
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
            openraft::error::NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::Other,
                "no network in single-node test",
            )),
        ))
    }
}

async fn wait_for_leader(
    metrics_rx: &mut tokio::sync::watch::Receiver<RaftMetrics<NodeId, openraft::BasicNode>>,
) {
    // Tick the metrics channel until we see ourselves as Leader.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!("did not become leader in 5s");
        }
        let m = metrics_rx.borrow_and_update().clone();
        if matches!(m.state, openraft::ServerState::Leader) {
            return;
        }
        tokio::time::timeout(Duration::from_millis(100), metrics_rx.changed())
            .await
            .ok();
    }
}

#[tokio::test]
async fn one_node_cluster_applies_entries_to_mvcc() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("raft.redb");
    let engine = Arc::new(RedbEngine::open(&path).unwrap());
    let mvcc = MvccStore::open(engine).await.unwrap();
    let sm = FastetcdStateMachine::new(mvcc.clone());
    let log = MemLogStore::new();

    let config = Arc::new(
        Config {
            heartbeat_interval: 100,
            election_timeout_min: 200,
            election_timeout_max: 500,
            // Snapshot-related tunables left at defaults.
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );

    let raft = Raft::<TypeConfig>::new(1, config, NopNetwork, log, sm.clone())
        .await
        .unwrap();

    // Bootstrap as a one-member cluster.
    let mut members: BTreeSet<NodeId> = BTreeSet::new();
    members.insert(1);
    raft.initialize(members).await.unwrap();

    let mut metrics_rx = raft.metrics();
    wait_for_leader(&mut metrics_rx).await;

    // Propose an Apply entry: put k=v.
    let entry = FastetcdLogEntry::Apply {
        mutations: vec![Mutation::Put {
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
            lease: 0,
            ignore_value: false,
            ignore_lease: false,
            prev_kv: false,
        }],
    };
    let resp = raft.client_write(entry).await.unwrap();
    let revision = match resp.response() {
        FastetcdLogResponse::Apply { revision, .. } => *revision,
        other => panic!("unexpected response: {other:?}"),
    };
    assert_eq!(revision, 1);

    // Observe the value via the MVCC store directly (single-node, so
    // we don't need a read-index round trip).
    let out = sm
        .mvcc()
        .range(b"hello", b"", 0, 0, false, false)
        .await
        .unwrap();
    assert_eq!(out.kvs.len(), 1);
    assert_eq!(out.kvs[0].value, b"world");
}
