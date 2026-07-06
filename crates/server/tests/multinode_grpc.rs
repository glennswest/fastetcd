//! Three-node integration test exercising the real gRPC peer
//! transport. Each node runs in-process on its own ephemeral ports,
//! and they discover each other via `--initial-cluster`-style
//! configuration.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::{Config, Raft};
use tempfile::TempDir;
use tokio::sync::RwLock;
use tokio::time::sleep;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::cluster_server::ClusterServer;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::kv_server::KvServer;
use fastetcd_proto::etcdserverpb::lease_server::LeaseServer;
use fastetcd_proto::etcdserverpb::maintenance_server::MaintenanceServer;
use fastetcd_proto::etcdserverpb::watch_server::WatchServer;
use fastetcd_proto::fastetcd_raft::raft_peer_server::RaftPeerServer;
use fastetcd_raft::kv_log_store::KvLogStore;
use fastetcd_raft::network::{GrpcNetworkFactory, RaftPeerService};
use fastetcd_raft::types::{NodeId, TypeConfig};
use fastetcd_raft::FastetcdStateMachine;
use fastetcd_server::cluster::ClusterService;
use fastetcd_server::kv::KvService;
use fastetcd_server::lease::LeaseService;
use fastetcd_server::maintenance::MaintenanceService;
use fastetcd_server::watch::WatchService;
use fastetcd_server::ServerState;
use fastetcd_storage::mvcc::MvccStore;
use fastetcd_storage::redb_engine::RedbEngine;

struct Node {
    _dir: TempDir,
    client_endpoint: String,
    raft: Raft<TypeConfig>,
}

async fn start_node(
    id: NodeId,
    members: &BTreeMap<NodeId, String>, // node_id -> peer URL
) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.redb");
    let engine: Arc<dyn fastetcd_storage::KvStore> = Arc::new(RedbEngine::open(&path).unwrap());
    let mvcc = MvccStore::open(engine.clone()).await.unwrap();
    let sm = FastetcdStateMachine::new(mvcc);
    let log = KvLogStore::new(engine);

    let config = Arc::new(
        Config {
            heartbeat_interval: 100,
            election_timeout_min: 400,
            election_timeout_max: 900,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );

    // Peers map for this node = all members except self.
    let mut peers: BTreeMap<NodeId, String> = members.clone();
    peers.remove(&id);
    let peers = Arc::new(RwLock::new(peers.into_iter().collect()));

    let factory = GrpcNetworkFactory::new(peers.clone());
    let raft = Raft::<TypeConfig>::new(id, config, factory, log, sm.clone())
        .await
        .unwrap();

    let auth_state = fastetcd_server::auth::AuthState::default();
    let forwarder = fastetcd_raft::WriteForwarder::new(peers);
    let state = Arc::new(ServerState::new(raft.clone(), sm, 7, id, auth_state, forwarder));
    let kv = KvService::new(state.clone());
    let test_peers = fastetcd_raft::network::empty_peers();
    let test_dir: fastetcd_server::cluster::MemberDirectory =
        std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::BTreeMap::new()));
    ClusterService::seed_self(
        &test_dir,
        id,
        format!("node-{id}"),
        vec![format!("http://peer-{id}:0")],
        vec![format!("http://client-{id}:0")],
    )
    .await;
    let cluster_svc = ClusterService::new(state.clone(), id, test_peers, test_dir);
    let maintenance = MaintenanceService::new(state.clone());
    let watch = WatchService::new(state.clone());
    let lease = LeaseService::new(state);
    let peer_service = RaftPeerService::new(raft.clone());

    // Listen on the URL the peers were told about.
    let peer_url = &members[&id];
    let peer_addr: std::net::SocketAddr = peer_url
        .strip_prefix("http://")
        .unwrap()
        .parse()
        .unwrap();
    let peer_listener = tokio::net::TcpListener::bind(peer_addr).await.unwrap();
    let peer_addr_bound = peer_listener.local_addr().unwrap();
    let peer_incoming = tokio_stream::wrappers::TcpListenerStream::new(peer_listener);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(RaftPeerServer::new(peer_service))
            .serve_with_incoming(peer_incoming)
            .await
            .unwrap();
    });
    assert_eq!(peer_addr_bound, peer_addr);

    // Client port on an ephemeral address.
    let client_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_addr = client_listener.local_addr().unwrap();
    let client_incoming = tokio_stream::wrappers::TcpListenerStream::new(client_listener);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(KvServer::new(kv))
            .add_service(ClusterServer::new(cluster_svc))
            .add_service(MaintenanceServer::new(maintenance))
            .add_service(WatchServer::new(watch))
            .add_service(LeaseServer::new(lease))
            .serve_with_incoming(client_incoming)
            .await
            .unwrap();
    });

    Node {
        _dir: dir,
        client_endpoint: format!("http://{client_addr}"),
        raft,
    }
}

async fn pick_free_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    port
}

#[tokio::test]
async fn three_node_cluster_replicates_via_grpc_transport() {
    // Allocate peer ports up front so members[] is consistent.
    let p1 = pick_free_port().await;
    let p2 = pick_free_port().await;
    let p3 = pick_free_port().await;
    let mut members: BTreeMap<NodeId, String> = BTreeMap::new();
    members.insert(1, format!("http://127.0.0.1:{p1}"));
    members.insert(2, format!("http://127.0.0.1:{p2}"));
    members.insert(3, format!("http://127.0.0.1:{p3}"));

    // Bring all three up in parallel.
    let (n1, n2, n3) = tokio::join!(
        start_node(1, &members),
        start_node(2, &members),
        start_node(3, &members),
    );

    // Give the peer servers a brief moment to start accepting before
    // we initialize (so the first AppendEntries doesn't trigger a dial
    // before the listener is ready).
    sleep(Duration::from_millis(150)).await;

    // Only node 1 calls initialize; openraft will replicate the
    // membership to the others. Address by peer URL, matching
    // main.rs's bootstrap — a bare `BTreeSet<NodeId>` defaults every
    // member's `BasicNode.addr` to empty (see #4).
    let mut all: BTreeMap<NodeId, openraft::BasicNode> = BTreeMap::new();
    for (id, url) in &members {
        all.insert(*id, openraft::BasicNode::new(url.clone()));
    }
    n1.raft.initialize(all).await.unwrap();

    // Wait for a leader to emerge.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let leader_id;
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!("no leader elected in 10s");
        }
        let m = n1.raft.metrics().borrow().clone();
        if let Some(l) = m.current_leader {
            leader_id = l;
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    let leader_node = match leader_id {
        1 => &n1,
        2 => &n2,
        3 => &n3,
        other => panic!("unexpected leader id {other}"),
    };

    // Put on the leader.
    let mut kv_leader = KvClient::connect(leader_node.client_endpoint.clone())
        .await
        .unwrap();
    let put = kv_leader
        .put(pb::PutRequest {
            key: b"replicated-key".to_vec(),
            value: b"replicated-value".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let put_rev = put.header.unwrap().revision;
    assert_eq!(put_rev, 1);

    // Range on every node — they should all have the value applied.
    // Give followers a heartbeat tick to apply.
    sleep(Duration::from_millis(300)).await;
    for n in [&n1, &n2, &n3] {
        let mut kv = KvClient::connect(n.client_endpoint.clone()).await.unwrap();
        let r = kv
            .range(pb::RangeRequest {
                key: b"replicated-key".to_vec(),
                serializable: true,
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(r.kvs.len(), 1, "node {} did not see the value", n.client_endpoint);
        assert_eq!(r.kvs[0].value, b"replicated-value");
    }

    // Regression test for #4: a write sent to a FOLLOWER must still
    // succeed — fastetcd forwards it to the leader over the peer
    // channel rather than erroring with "has to forward request to:
    // ... BasicNode { addr: "" }".
    let follower = [&n1, &n2, &n3]
        .into_iter()
        .find(|n| n.client_endpoint != leader_node.client_endpoint)
        .unwrap();
    let mut kv_follower = KvClient::connect(follower.client_endpoint.clone())
        .await
        .unwrap();
    let put = kv_follower
        .put(pb::PutRequest {
            key: b"forwarded-key".to_vec(),
            value: b"forwarded-value".to_vec(),
            ..Default::default()
        })
        .await
        .expect("PUT on a follower must be forwarded to the leader, not fail")
        .into_inner();
    assert!(put.header.unwrap().revision > put_rev);

    sleep(Duration::from_millis(300)).await;
    for n in [&n1, &n2, &n3] {
        let mut kv = KvClient::connect(n.client_endpoint.clone()).await.unwrap();
        let r = kv
            .range(pb::RangeRequest {
                key: b"forwarded-key".to_vec(),
                serializable: true,
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            r.kvs.len(),
            1,
            "node {} did not see the forwarded write",
            n.client_endpoint
        );
        assert_eq!(r.kvs[0].value, b"forwarded-value");
    }
}
