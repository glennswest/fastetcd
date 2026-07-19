#![allow(dead_code)] // shared scaffolding; each test binary uses a subset

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
use fastetcd_proto::etcdserverpb::lease_server::LeaseServer;
use fastetcd_proto::etcdserverpb::auth_server::AuthServer;
use fastetcd_proto::etcdserverpb::watch_server::WatchServer;
use fastetcd_raft::kv_log_store::KvLogStore;
use fastetcd_raft::types::{NodeId, TypeConfig};
use fastetcd_raft::FastetcdStateMachine;
use fastetcd_server::cluster::ClusterService;
use fastetcd_server::kv::KvService;
use fastetcd_server::maintenance::MaintenanceService;
use fastetcd_server::auth::{AuthInterceptor, AuthService, AuthState};
use fastetcd_server::lease::LeaseService;
use fastetcd_server::watch::WatchService;
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
    openraft::error::RPCError::Network(openraft::error::NetworkError::new(&std::io::Error::other(
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
            openraft::error::NetworkError::new(&std::io::Error::other(
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
    pub state: std::sync::Arc<ServerState>,
}

pub async fn start_test_server_full() -> TestServerHandles {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kv.redb");
    let engine: Arc<dyn fastetcd_storage::KvStore> = Arc::new(RedbEngine::open(&path).unwrap());
    let mvcc = MvccStore::open(engine.clone()).await.unwrap();
    let sm = FastetcdStateMachine::open(mvcc).await.unwrap();
    let log = KvLogStore::new(engine);
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

    let auth_state = AuthState::default();
    let peers = fastetcd_raft::network::empty_peers();
    let forwarder = fastetcd_raft::WriteForwarder::new(peers.clone());
    let state = Arc::new(ServerState::new(raft, sm, 7, 1, auth_state.clone(), forwarder));
    let _ = log;

    let kv = KvService::new(state.clone());
    let directory: fastetcd_server::cluster::MemberDirectory =
        std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::BTreeMap::new()));
    ClusterService::seed_self(
        &directory,
        1,
        "test-node".to_string(),
        vec!["http://test-peer:0".to_string()],
        vec!["http://test-client:0".to_string()],
    )
    .await;
    let cluster = ClusterService::new(state.clone(), 1, peers, directory);
    let maintenance = MaintenanceService::new(state.clone());
    let watch = WatchService::new(state.clone());
    let lease = LeaseService::new(state.clone());
    let auth = AuthService::new(state.clone(), auth_state.clone());
    let interceptor = AuthInterceptor::new(auth_state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(KvServer::with_interceptor(kv, interceptor.clone()))
            .add_service(ClusterServer::with_interceptor(cluster, interceptor.clone()))
            .add_service(MaintenanceServer::with_interceptor(
                maintenance,
                interceptor.clone(),
            ))
            .add_service(WatchServer::with_interceptor(watch, interceptor.clone()))
            .add_service(LeaseServer::with_interceptor(lease, interceptor))
            .add_service(AuthServer::new(auth))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    sleep(Duration::from_millis(50)).await;

    TestServerHandles {
        _dir: dir,
        endpoint: format!("http://{addr}"),
        state,
    }
}

/// Same as `start_test_server_full` but additionally spawns the
/// lease auto-expiry ticker at 200ms cadence (faster than prod so
/// tests don't have to sleep for a full second).
pub async fn start_test_server_full_with_expiry_ticker() -> TestServerHandles {
    let h = start_test_server_full().await;
    fastetcd_server::lease_expiry::spawn_with_tick(
        h.state.clone(),
        Duration::from_millis(200),
    );
    h
}
