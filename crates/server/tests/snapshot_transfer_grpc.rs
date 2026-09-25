//! fastetcd#30 end to end: a learner that joins after the log has been
//! purged is caught up by a real chunked `InstallSnapshot` over the gRPC
//! peer transport, sent from the leader's snapshot file and received
//! into a file on the learner.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use openraft::{Config, Raft, SnapshotPolicy};
use tempfile::TempDir;
use tokio::sync::RwLock;
use tokio::time::{sleep, Instant};

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::kv_server::KvServer;
use fastetcd_proto::fastetcd_raft::raft_peer_server::RaftPeerServer;
use fastetcd_raft::kv_log_store::KvLogStore;
use fastetcd_raft::network::{GrpcNetworkFactory, PeerEndpoints, RaftPeerService};
use fastetcd_raft::types::{NodeId, TypeConfig};
use fastetcd_raft::FastetcdStateMachine;
use fastetcd_server::kv::KvService;
use fastetcd_server::ServerState;
use fastetcd_storage::mvcc::MvccStore;
use fastetcd_storage::redb_engine::RedbEngine;

const KEYS: usize = 200;

struct Node {
    _dir: TempDir,
    snap_dir: PathBuf,
    peer_url: String,
    client_endpoint: String,
    peers: PeerEndpoints,
    raft: Raft<TypeConfig>,
}

async fn start_node(id: NodeId) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let snap_dir = dir.path().join("snapshots");
    let engine: Arc<dyn fastetcd_storage::KvStore> =
        Arc::new(RedbEngine::open(dir.path().join("data.redb")).unwrap());
    let mvcc = MvccStore::open(engine.clone()).await.unwrap();
    let sm = FastetcdStateMachine::open(mvcc, &snap_dir).await.unwrap();
    let log = KvLogStore::new(engine);

    let config = Arc::new(
        Config {
            heartbeat_interval: 100,
            election_timeout_min: 400,
            election_timeout_max: 900,
            // Snapshot and purge aggressively so a late joiner cannot be
            // caught up from the log.
            snapshot_policy: SnapshotPolicy::LogsSinceLast(20),
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: 1,
            // Many small chunks, so offsets and reassembly are exercised.
            snapshot_max_chunk_size: 8 * 1024,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );

    let peers: PeerEndpoints = Arc::new(RwLock::new(HashMap::new()));
    let raft = Raft::<TypeConfig>::new(id, config, GrpcNetworkFactory::new(peers.clone()), log, sm.clone())
        .await
        .unwrap();

    let forwarder = fastetcd_raft::WriteForwarder::new(peers.clone());
    let state = Arc::new(ServerState::new(
        raft.clone(),
        sm,
        7,
        id,
        fastetcd_server::auth::AuthState::default(),
        forwarder,
    ));
    let peer_service = RaftPeerService::new(raft.clone(), state.sm.mvcc().clone());
    let kv = KvService::new(state);

    let peer_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_url = format!("http://{}", peer_listener.local_addr().unwrap());
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(RaftPeerServer::new(peer_service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(peer_listener))
            .await
            .unwrap();
    });
    let client_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_endpoint = format!("http://{}", client_listener.local_addr().unwrap());
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(KvServer::new(kv))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(client_listener))
            .await
            .unwrap();
    });

    Node {
        _dir: dir,
        snap_dir,
        peer_url,
        client_endpoint,
        peers,
        raft,
    }
}

fn files_in(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .map(|d| {
            d.flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

async fn wait_for(what: &str, mut ok: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !ok() {
        if Instant::now() > deadline {
            panic!("timed out waiting for {what}");
        }
        sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_late_learner_is_caught_up_by_a_file_backed_snapshot() {
    let leader = start_node(1).await;
    let mut members = std::collections::BTreeMap::new();
    members.insert(1, openraft::BasicNode::new(leader.peer_url.clone()));
    leader.raft.initialize(members).await.unwrap();
    wait_for("leader", || leader.raft.metrics().borrow().current_leader == Some(1)).await;

    let mut kv = KvClient::connect(leader.client_endpoint.clone()).await.unwrap();
    for i in 0..KEYS {
        kv.put(pb::PutRequest {
            key: format!("key/{i:05}").into_bytes(),
            value: vec![b'v'; 512],
            ..Default::default()
        })
        .await
        .unwrap();
    }

    // The log must be purged past the first write, so only a snapshot can
    // bring a new member up to date.
    wait_for("the leader to snapshot and purge its log", || {
        let m = leader.raft.metrics().borrow().clone();
        m.snapshot.is_some() && m.purged.is_some_and(|p| p.index > 10)
    })
    .await;
    let leader_snapshot = leader.raft.metrics().borrow().snapshot.unwrap();

    let learner = start_node(2).await;
    leader.peers.write().await.insert(2, learner.peer_url.clone());
    learner.peers.write().await.insert(1, leader.peer_url.clone());
    leader
        .raft
        .add_learner(2, openraft::BasicNode::new(learner.peer_url.clone()), true)
        .await
        .unwrap();

    wait_for("the learner to install the snapshot", || {
        learner
            .raft
            .metrics()
            .borrow()
            .snapshot
            .is_some_and(|s| s.index >= leader_snapshot.index)
    })
    .await;
    wait_for("the learner to apply everything", || {
        let leader_applied = leader.raft.metrics().borrow().last_applied;
        learner.raft.metrics().borrow().last_applied >= leader_applied
    })
    .await;

    // Every key is there.
    let mut learner_kv = KvClient::connect(learner.client_endpoint.clone()).await.unwrap();
    let all = learner_kv
        .range(pb::RangeRequest {
            key: b"key/".to_vec(),
            range_end: b"key0".to_vec(),
            serializable: true,
            count_only: true,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(all.count, KEYS as i64);

    // The learner kept the received file as its retained snapshot and
    // left no temp file behind.
    let files = files_in(&learner.snap_dir);
    assert!(
        files.iter().any(|f| f.ends_with(".snap")) && files.iter().any(|f| f.ends_with(".meta")),
        "the learner retains the installed snapshot: {files:?}"
    );
    assert!(
        !files.iter().any(|f| f.ends_with(".tmp")),
        "no temp file left behind: {files:?}"
    );
}
