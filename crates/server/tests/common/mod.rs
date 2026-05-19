//! Shared test harness: spin up an in-process fastetcd server with
//! KV + Cluster + Maintenance services on an ephemeral port.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use openraft::{Config, Raft};
use tempfile::TempDir;
use tokio::time::sleep;

use fastetcd_proto::etcdserverpb::cluster_server::ClusterServer;
use fastetcd_proto::etcdserverpb::kv_server::KvServer;
use fastetcd_proto::etcdserverpb::maintenance_server::MaintenanceServer;
use fastetcd_raft::log_store::MemLogStore;
use fastetcd_raft::types::{NodeId, TypeConfig};
use fastetcd_raft::FastetcdStateMachine;
use fastetcd_server::cluster::ClusterService;
use fastetcd_server::kv::KvService;
use fastetcd_server::maintenance::MaintenanceService;
use fastetcd_server::ServerState;
use fastetcd_storage::mvcc::MvccStore;
use fastetcd_storage::redb_engine::RedbEngine;

#[derive(Clone)]
pub struct NopNet;

impl openraft::network::RaftNetworkFactory<TypeConfig> for NopNet {
    type Network = NopNetConn;
    async fn new_client(
        &mut self,
        _target: NodeId,
        _node: &openraft::BasicNode,
    ) -> Self::Network {
        NopNetConn
    }
}

pub struct NopNetConn;

fn err() -> openraft::error::RPCError<
    NodeId,
    openraft::BasicNode,
    openraft::error::RaftError<NodeId>,
> {
    openraft::error::RPCError::Network(openraft::error::NetworkError::new(&std::io::Error::new(
        std::io::ErrorKind::Other,
        "test: no peers",
    )))
}

impl openraft::network::RaftNetwork<TypeConfig> for NopNetConn {
    async fn append_entries(
        &mut self,
        _: openraft::raft::AppendEntriesRequest<TypeConfig>,
        _: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::AppendEntriesResponse<NodeId>,
        openraft::error::RPCError<NodeId, openraft::BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        Err(err())
    }
    async fn install_snapshot(
        &mut self,
        _: openraft::raft::InstallSnapshotRequest<TypeConfig>,
        _: openraft::network::RPCOption,
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
                "test: no peers",
            )),
        ))
    }
    async fn vote(
        &mut self,
        _: openraft::raft::VoteRequest<NodeId>,
        _: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::VoteResponse<NodeId>,
        openraft::error::RPCError<NodeId, openraft::BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        Err(err())
    }
}

pub async fn wait_for_leader(raft: &Raft<TypeConfig>) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut rx = raft.metrics();
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!("did not become leader in 5s");
        }
        let m = rx.borrow_and_update().clone();
        if matches!(m.state, openraft::ServerState::Leader) {
            return;
        }
        tokio::time::timeout(Duration::from_millis(100), rx.changed())
            .await
            .ok();
    }
}

/// All handles a test might want, plus the gRPC endpoint string.
pub struct TestServerHandles {
    pub _dir: TempDir,
    pub endpoint: String,
}

pub async fn start_test_server_full() -> TestServerHandles {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kv.redb");
    let engine = Arc::new(RedbEngine::open(&path).unwrap());
    let mvcc = MvccStore::open(engine).await.unwrap();
    let sm = FastetcdStateMachine::new(mvcc);
    let log = MemLogStore::new();
    let config = Arc::new(
        Config {
            heartbeat_interval: 100,
            election_timeout_min: 200,
            election_timeout_max: 500,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let raft = Raft::<TypeConfig>::new(1, config, NopNet, log.clone(), sm.clone())
        .await
        .unwrap();
    let mut members: BTreeSet<NodeId> = BTreeSet::new();
    members.insert(1);
    raft.initialize(members).await.unwrap();
    wait_for_leader(&raft).await;

    let state = Arc::new(ServerState::new(raft, sm, 7, 1, log));

    let kv = KvService::new(state.clone());
    let cluster = ClusterService::new(
        state.clone(),
        "test-node".to_string(),
        vec!["http://test-peer:0".to_string()],
        vec!["http://test-client:0".to_string()],
    );
    let maintenance = MaintenanceService::new(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(KvServer::new(kv))
            .add_service(ClusterServer::new(cluster))
            .add_service(MaintenanceServer::new(maintenance))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    sleep(Duration::from_millis(50)).await;

    TestServerHandles {
        _dir: dir,
        endpoint: format!("http://{addr}"),
    }
}
