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
use fastetcd_raft::wal_log_store::{wal_dir, WalLogOptions, WalLogStore};
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
    let sm = FastetcdStateMachine::open(mvcc, dir.path().join("snapshots")).await.unwrap();
    // The raft log in a WAL, as the server runs it (#85).
    let log = WalLogStore::open(&wal_dir(dir.path()), engine, WalLogOptions::default())
        .await
        .unwrap();

    let config = Arc::new(
        Config {
            // Generous election timeouts: the build box runs several jobs
            // at once and fsync can stall the leader past a tight
            // timeout, moving leadership mid-test (#44).
            heartbeat_interval: 100,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
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

    let forwarder = fastetcd_raft::WriteForwarder::new(peers);
    let state = Arc::new(ServerState::new(raft.clone(), sm, 7, id, forwarder));
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
    let peer_mvcc = state.sm.mvcc().clone();
    let lease = LeaseService::new(state);
    let peer_service = RaftPeerService::new(raft.clone(), peer_mvcc);

    // Listen on the URL the peers were told about.
    let peer_url = &members[&id];
    let peer_addr: std::net::SocketAddr = peer_url
        .strip_prefix("http://")
        .unwrap()
        .parse()
        .unwrap();
    let reserved = RESERVED
        .lock()
        .unwrap()
        .remove(&peer_addr.port())
        .expect("peer port reserved by pick_free_port");
    reserved.set_nonblocking(true).unwrap();
    let peer_listener = tokio::net::TcpListener::from_std(reserved).unwrap();
    let peer_addr_bound = peer_listener.local_addr().unwrap();
    let peer_incoming = tokio_stream::wrappers::TcpListenerStream::new(peer_listener);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(RaftPeerServer::new(peer_service)
                .max_decoding_message_size(fastetcd_raft::network::PEER_MAX_DECODE_BYTES))
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

/// Peer listeners reserved by [`pick_free_port`], waiting for their
/// node to start.
static RESERVED: std::sync::Mutex<BTreeMap<u16, std::net::TcpListener>> =
    std::sync::Mutex::new(BTreeMap::new());

/// Reserve a free port for a node's peer listener. The listener stays
/// bound until [`start_node`] takes it. Closing it and binding the port
/// again later let another process on a shared build box take it in
/// between (`AddrInUse`, #40). A std listener, because each test runs
/// its own tokio runtime.
async fn pick_free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    RESERVED.lock().unwrap().insert(port, l);
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
    // Followers apply on a later heartbeat; poll rather than sleep a
    // fixed tick, which a loaded build box can outlast (#44).
    for n in [&n1, &n2, &n3] {
        let mut kv = KvClient::connect(n.client_endpoint.clone()).await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let r = loop {
            let r = kv
                .range(pb::RangeRequest {
                    key: b"replicated-key".to_vec(),
                    serializable: true,
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner();
            if !r.kvs.is_empty() || tokio::time::Instant::now() > deadline {
                break r;
            }
            sleep(Duration::from_millis(100)).await;
        };
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

/// fastetcd#7 — etcd forwards cluster-membership changes to the leader,
/// so `etcdctl member add/remove` works against any endpoint. fastetcd
/// used to surface openraft's ForwardToLeader error instead ("raft
/// change_membership: has to forward request to: Some(...)"), which
/// broke etcdctl, kubeadm, and the rustkube master-replacement runbook.
#[tokio::test]
async fn membership_changes_forward_from_a_follower() {
    use fastetcd_proto::etcdserverpb::cluster_client::ClusterClient;

    let p1 = pick_free_port().await;
    let p2 = pick_free_port().await;
    let p3 = pick_free_port().await;
    let mut members: BTreeMap<NodeId, String> = BTreeMap::new();
    members.insert(1, format!("http://127.0.0.1:{p1}"));
    members.insert(2, format!("http://127.0.0.1:{p2}"));
    members.insert(3, format!("http://127.0.0.1:{p3}"));

    let (n1, n2, n3) = tokio::join!(
        start_node(1, &members),
        start_node(2, &members),
        start_node(3, &members),
    );
    sleep(Duration::from_millis(150)).await;

    let mut all: BTreeMap<NodeId, openraft::BasicNode> = BTreeMap::new();
    for (id, url) in &members {
        all.insert(*id, openraft::BasicNode::new(url.clone()));
    }
    n1.raft.initialize(all).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let leader_id = loop {
        if tokio::time::Instant::now() > deadline {
            panic!("no leader elected in 10s");
        }
        if let Some(l) = n1.raft.metrics().borrow().current_leader {
            break l;
        }
        sleep(Duration::from_millis(100)).await;
    };

    // Target a node that is definitely NOT the leader.
    let by_id = |id: NodeId| match id {
        1 => &n1,
        2 => &n2,
        3 => &n3,
        other => panic!("unexpected node id {other}"),
    };

    // A follower learns `current_leader` from the leader's first
    // AppendEntries, before the initial membership (log index 0) is
    // committed. A membership change before then is refused with
    // "already undergoing a configuration change", so wait until the
    // leader has applied its first-term entry (fastetcd#44).
    by_id(leader_id)
        .raft
        .wait(Some(Duration::from_secs(10)))
        .applied_index_at_least(Some(1), "initial membership committed")
        .await
        .expect("leader applies its first entry");
    let follower_id = (1..=3u64).find(|id| *id != leader_id).unwrap();
    let follower = by_id(follower_id);
    let mut cluster = ClusterClient::connect(follower.client_endpoint.clone())
        .await
        .unwrap();

    // MemberAdd against the follower. Before the fix this failed with
    // "has to forward request to: Some(...)".
    let added = cluster
        .member_add(pb::MemberAddRequest {
            peer_ur_ls: vec!["http://127.0.0.1:59999".to_string()],
            is_learner: true,
        })
        .await
        .expect("MemberAdd on a follower must forward to the leader, not fail")
        .into_inner();
    let new_id = added.member.expect("MemberAddResponse.member").id;
    assert_ne!(new_id, 0);

    // The leader must actually have the learner, proving the change was
    // applied there rather than only recorded in the follower's local
    // directory.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let m = by_id(leader_id).raft.metrics().borrow().clone();
        if m.membership_config.membership().nodes().any(|(id, _)| *id == new_id) {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("leader never saw the forwarded add_learner for {new_id}");
        }
        sleep(Duration::from_millis(100)).await;
    }

    // MemberRemove of a real voter, also via the follower — the exact
    // shape of the replace-master runbook. Remove a node that is
    // neither the leader nor the one serving this RPC.
    let victim = (1..=3u64)
        .find(|id| *id != leader_id && *id != follower_id)
        .unwrap();
    cluster
        .member_remove(pb::MemberRemoveRequest { id: victim })
        .await
        .expect("MemberRemove on a follower must forward to the leader, not fail");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let voters: Vec<NodeId> = by_id(leader_id)
            .raft
            .metrics()
            .borrow()
            .membership_config
            .voter_ids()
            .collect();
        if !voters.contains(&victim) {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("leader still lists {victim} as a voter after forwarded remove");
        }
        sleep(Duration::from_millis(100)).await;
    }

    let _ = n2;
    let _ = n3;
}

/// Reproduction for #10: a single client doing sequential
/// read-modify-write with a `Compare::mod_revision(Equal)` guard and no
/// concurrent writer should never see a spurious CAS failure. Runs the
/// loop against every member (leader and followers) using the default
/// linearizable read.
#[tokio::test]
async fn rmw_cas_loop_has_no_spurious_conflicts() {
    use fastetcd_proto::etcdserverpb::compare::{CompareResult, CompareTarget, TargetUnion};

    let p1 = pick_free_port().await;
    let p2 = pick_free_port().await;
    let p3 = pick_free_port().await;
    let mut members: BTreeMap<NodeId, String> = BTreeMap::new();
    members.insert(1, format!("http://127.0.0.1:{p1}"));
    members.insert(2, format!("http://127.0.0.1:{p2}"));
    members.insert(3, format!("http://127.0.0.1:{p3}"));

    let (n1, n2, n3) = tokio::join!(
        start_node(1, &members),
        start_node(2, &members),
        start_node(3, &members),
    );
    sleep(Duration::from_millis(150)).await;
    let mut all: BTreeMap<NodeId, openraft::BasicNode> = BTreeMap::new();
    for (id, url) in &members {
        all.insert(*id, openraft::BasicNode::new(url.clone()));
    }
    n1.raft.initialize(all).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let leader_id = loop {
        if tokio::time::Instant::now() > deadline {
            panic!("no leader in 10s");
        }
        if let Some(l) = n1.raft.metrics().borrow().current_leader {
            break l;
        }
        sleep(Duration::from_millis(100)).await;
    };
    let by_id = |id: NodeId| match id {
        1 => &n1,
        2 => &n2,
        3 => &n3,
        o => panic!("bad id {o}"),
    };

    // Seed the key once via the leader.
    let key = b"rmw".to_vec();
    let mut seed = KvClient::connect(by_id(leader_id).client_endpoint.clone())
        .await
        .unwrap();
    seed.put(pb::PutRequest {
        key: key.clone(),
        value: b"seed".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    // Let every follower apply the seed before we start reading locally.
    sleep(Duration::from_millis(300)).await;

    for target in [leader_id, (1..=3).find(|i| *i != leader_id).unwrap()] {
        let endpoint = by_id(target).client_endpoint.clone();
        let mut kv = KvClient::connect(endpoint.clone()).await.unwrap();
        let iters = 30;
        let mut ok = 0;
        // What this test guards is spurious CAS *conflicts*. If
        // leadership moves mid-loop (a loaded build box, #44) a request
        // fails with `Unavailable`, as it does in etcd, and a client
        // retries it. Do the same: settle, re-read, and retry, bounded.
        let mut unavailable = 0;
        let mut retry = |e: tonic::Status| {
            assert_eq!(e.code(), tonic::Code::Unavailable, "unexpected error: {e}");
            unavailable += 1;
            assert!(unavailable <= 20, "still unavailable after 20 retries: {e}");
        };
        let mut i = 0;
        while i < iters {
            // Linearizable GET (serializable = false, etcd default).
            let g = match kv
                .range(pb::RangeRequest {
                    key: key.clone(),
                    ..Default::default()
                })
                .await
            {
                Ok(r) => r.into_inner(),
                Err(e) => {
                    retry(e);
                    sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };
            let rv = g.kvs.first().map(|k| k.mod_revision).unwrap_or(0);

            let txn = match kv
                .txn(pb::TxnRequest {
                    compare: vec![pb::Compare {
                        result: CompareResult::Equal as i32,
                        target: CompareTarget::Mod as i32,
                        key: key.clone(),
                        target_union: Some(TargetUnion::ModRevision(rv)),
                        range_end: Vec::new(),
                    }],
                    success: vec![pb::RequestOp {
                        request: Some(pb::request_op::Request::RequestPut(pb::PutRequest {
                            key: key.clone(),
                            value: format!("v{i}").into_bytes(),
                            ..Default::default()
                        })),
                    }],
                    failure: Vec::new(),
                })
                .await
            {
                Ok(r) => r.into_inner(),
                Err(e) => {
                    // It may or may not have applied; the re-read after
                    // settling sees either way, so no conflict follows.
                    retry(e);
                    sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };
            if txn.succeeded {
                ok += 1;
            }
            i += 1;
        }
        let role = if target == leader_id { "leader" } else { "follower" };
        assert_eq!(
            ok, iters,
            "{role} ({endpoint}): {ok}/{iters} CAS succeeded — spurious conflicts (#10)"
        );
    }
    let _ = (&n1, &n2, &n3);
}


/// Deterministic guard for the #10 barrier: a node that cannot confirm
/// leadership must not serve a default (linearizable) read from local
/// state — it must fail. A lone, uninitialized node has no leader, so
/// `ensure_linearizable` cannot pass and there is nobody to forward to.
///
/// Without the barrier this returns an empty result with `Ok` (stale
/// local read); with it, the linearizable read errors while an explicit
/// `serializable` read still returns local state. This is the leader
/// side of the report ("~25% even on the leader"): a deposed or
/// unconfirmed leader answering reads from stale local state.
#[tokio::test]
async fn linearizable_read_without_a_leader_fails_instead_of_serving_stale() {
    // Two-member config, but only node 1 is started and it is never
    // initialized — so it stays a follower with no elected leader.
    let p1 = pick_free_port().await;
    let p2 = pick_free_port().await;
    let mut members: BTreeMap<NodeId, String> = BTreeMap::new();
    members.insert(1, format!("http://127.0.0.1:{p1}"));
    members.insert(2, format!("http://127.0.0.1:{p2}"));
    // Node 2 is never started: release its port, so nothing listens there.
    RESERVED.lock().unwrap().remove(&p2);
    let n1 = start_node(1, &members).await;
    sleep(Duration::from_millis(200)).await;
    assert!(
        n1.raft.metrics().borrow().current_leader.is_none(),
        "precondition: node has no leader"
    );

    let mut kv = KvClient::connect(n1.client_endpoint.clone()).await.unwrap();

    // Serializable (explicit opt-in to a possibly-stale local read):
    // succeeds, returns empty local state.
    let serial = kv
        .range(pb::RangeRequest {
            key: b"k".to_vec(),
            serializable: true,
            ..Default::default()
        })
        .await;
    assert!(serial.is_ok(), "serializable read should still serve local state");

    // Linearizable (default): must fail rather than return stale/empty
    // local state, because leadership can't be confirmed.
    let lin = kv
        .range(pb::RangeRequest {
            key: b"k".to_vec(),
            ..Default::default()
        })
        .await;
    assert!(
        lin.is_err(),
        "linearizable read with no confirmable leader must fail, got {lin:?}"
    );
}

/// fastetcd#19 on a cluster: the lease check runs on the leader, so a
/// lease granted through one member can be used at once through any
/// other (a follower that has not applied the grant yet must not refuse
/// it), and a lease that does not exist is refused through a follower
/// too, with etcd's NotFound.
#[tokio::test]
async fn a_put_names_a_lease_through_any_member() {
    use fastetcd_proto::etcdserverpb::lease_client::LeaseClient;

    let p1 = pick_free_port().await;
    let p2 = pick_free_port().await;
    let p3 = pick_free_port().await;
    let mut members: BTreeMap<NodeId, String> = BTreeMap::new();
    members.insert(1, format!("http://127.0.0.1:{p1}"));
    members.insert(2, format!("http://127.0.0.1:{p2}"));
    members.insert(3, format!("http://127.0.0.1:{p3}"));
    let (n1, n2, n3) = tokio::join!(
        start_node(1, &members),
        start_node(2, &members),
        start_node(3, &members),
    );
    sleep(Duration::from_millis(150)).await;
    let mut all: BTreeMap<NodeId, openraft::BasicNode> = BTreeMap::new();
    for (id, url) in &members {
        all.insert(*id, openraft::BasicNode::new(url.clone()));
    }
    n1.raft.initialize(all).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let leader_id = loop {
        assert!(tokio::time::Instant::now() < deadline, "no leader in 10s");
        if let Some(l) = n1.raft.metrics().borrow().current_leader {
            break l;
        }
        sleep(Duration::from_millis(100)).await;
    };
    let nodes = [&n1, &n2, &n3];
    let leader = nodes[(leader_id - 1) as usize];
    leader
        .raft
        .wait(Some(Duration::from_secs(10)))
        .applied_index_at_least(Some(1), "first entry applied")
        .await
        .unwrap();

    for granter in nodes {
        let id = LeaseClient::connect(granter.client_endpoint.clone())
            .await
            .unwrap()
            .lease_grant(pb::LeaseGrantRequest { ttl: 60, id: 0 })
            .await
            .unwrap()
            .into_inner()
            .id;
        // Immediately, through every member.
        for (i, via) in nodes.iter().enumerate() {
            KvClient::connect(via.client_endpoint.clone())
                .await
                .unwrap()
                .put(pb::PutRequest {
                    key: format!("leased/{id}/{i}").into_bytes(),
                    value: b"v".to_vec(),
                    lease: id,
                    ..Default::default()
                })
                .await
                .unwrap_or_else(|e| panic!("lease {id} refused via {}: {e:?}", via.client_endpoint));
        }
    }

    for via in nodes {
        let err = KvClient::connect(via.client_endpoint.clone())
            .await
            .unwrap()
            .put(pb::PutRequest {
                key: b"dangling".to_vec(),
                value: b"v".to_vec(),
                lease: 0x7777,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound, "via {}: {err:?}", via.client_endpoint);
        assert_eq!(err.message(), "etcdserver: requested lease not found");
    }
}

/// fastetcd#50 on a cluster: a LIST's header revision is the revision
/// its contents were read at, on the leader (local read) and on a
/// follower (the read is forwarded, and the header used to be the
/// follower's own revision, not the leader's). Writers go through both;
/// LIST readers run through both at the same time. Then every LIST must
/// hold exactly the keys created at or below its header revision, and a
/// WATCH from the next revision on the member that served the LIST must
/// deliver every other key: nothing is skipped between LIST and WATCH.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_revision_matches_contents_and_watch_continues_it() {
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicBool, Ordering};

    use fastetcd_proto::etcdserverpb::watch_client::WatchClient;
    use tokio_stream::wrappers::ReceiverStream;
    use tokio_stream::StreamExt;

    let p1 = pick_free_port().await;
    let p2 = pick_free_port().await;
    let p3 = pick_free_port().await;
    let mut members: BTreeMap<NodeId, String> = BTreeMap::new();
    members.insert(1, format!("http://127.0.0.1:{p1}"));
    members.insert(2, format!("http://127.0.0.1:{p2}"));
    members.insert(3, format!("http://127.0.0.1:{p3}"));
    let (n1, n2, n3) = tokio::join!(
        start_node(1, &members),
        start_node(2, &members),
        start_node(3, &members),
    );
    sleep(Duration::from_millis(150)).await;
    let mut all: BTreeMap<NodeId, openraft::BasicNode> = BTreeMap::new();
    for (id, url) in &members {
        all.insert(*id, openraft::BasicNode::new(url.clone()));
    }
    n1.raft.initialize(all).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let leader_id = loop {
        if tokio::time::Instant::now() > deadline {
            panic!("no leader in 10s");
        }
        if let Some(l) = n1.raft.metrics().borrow().current_leader {
            break l;
        }
        sleep(Duration::from_millis(100)).await;
    };
    let by_id = |id: NodeId| match id {
        1 => &n1,
        2 => &n2,
        3 => &n3,
        o => panic!("bad id {o}"),
    };
    let follower_id = (1..=3).find(|i| *i != leader_id).unwrap();
    let endpoints = [
        by_id(leader_id).client_endpoint.clone(),
        by_id(follower_id).client_endpoint.clone(),
    ];

    let done = Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    for w in 0..4 {
        let endpoint = endpoints[w % 2].clone();
        writers.push(tokio::spawn(async move {
            let mut kv = KvClient::connect(endpoint).await.unwrap();
            let mut i = 0;
            let mut unavailable = 0;
            while i < 60 {
                let key = format!("cm/w{w}-{i}").into_bytes();
                match kv
                    .put(pb::PutRequest { key, value: b"v".to_vec(), ..Default::default() })
                    .await
                {
                    Ok(_) => i += 1,
                    // Leadership moving on a loaded box (#44): retry.
                    // Whether the lost put applied or not, the key's real
                    // create revision is read back at the end.
                    Err(e) if e.code() == tonic::Code::Unavailable && unavailable < 20 => {
                        unavailable += 1;
                        sleep(Duration::from_millis(300)).await;
                    }
                    Err(e) => panic!("put: {e}"),
                }
            }
        }));
    }
    let mut readers = Vec::new();
    for (r, endpoint) in endpoints.iter().enumerate() {
        let endpoint = endpoint.clone();
        let done = done.clone();
        readers.push(tokio::spawn(async move {
            let mut kv = KvClient::connect(endpoint.clone()).await.unwrap();
            let mut lists = Vec::new();
            while !done.load(Ordering::Relaxed) {
                match kv
                    .range(pb::RangeRequest {
                        key: b"cm/".to_vec(),
                        range_end: b"cm0".to_vec(),
                        keys_only: true,
                        ..Default::default()
                    })
                    .await
                {
                    Ok(resp) => {
                        let resp = resp.into_inner();
                        let keys: BTreeSet<Vec<u8>> =
                            resp.kvs.into_iter().map(|kv| kv.key).collect();
                        lists.push((r, resp.header.unwrap().revision, keys));
                    }
                    Err(e) if e.code() == tonic::Code::Unavailable => {
                        sleep(Duration::from_millis(100)).await;
                    }
                    Err(e) => panic!("range via {endpoint}: {e}"),
                }
            }
            lists
        }));
    }
    for w in writers {
        w.await.unwrap();
    }
    done.store(true, Ordering::Relaxed);
    let mut lists = Vec::new();
    for r in readers {
        lists.extend(r.await.unwrap());
    }

    // Every key's real create revision, read back linearizably.
    let mut kv = KvClient::connect(endpoints[0].clone()).await.unwrap();
    let final_list = kv
        .range(pb::RangeRequest { key: b"cm/".to_vec(), range_end: b"cm0".to_vec(), ..Default::default() })
        .await
        .unwrap()
        .into_inner();
    let created: Vec<(Vec<u8>, i64)> =
        final_list.kvs.into_iter().map(|kv| (kv.key, kv.create_revision)).collect();
    assert_eq!(created.len(), 240);
    let all_keys: BTreeSet<Vec<u8>> = created.iter().map(|(k, _)| k.clone()).collect();

    for r in 0..2 {
        assert!(
            lists.iter().filter(|l| l.0 == r).count() > 5,
            "reader {r} ran too few LISTs: {}",
            lists.len()
        );
    }
    let mut bad = Vec::new();
    for (r, rev, got) in &lists {
        let want: BTreeSet<Vec<u8>> =
            created.iter().filter(|(_, c)| c <= rev).map(|(k, _)| k.clone()).collect();
        if *got != want {
            bad.push(format!(
                "{} rv={rev}: missing {}, future {}",
                ["leader", "follower"][*r],
                want.difference(got).count(),
                got.difference(&want).count()
            ));
        }
    }
    assert!(
        bad.is_empty(),
        "{} of {} LISTs inconsistent: {:?}",
        bad.len(),
        lists.len(),
        &bad[..bad.len().min(5)]
    );

    // LIST -> WATCH from rv+1 on the same member: together they hold
    // every key. Take a spread of LISTs from each member.
    for r in 0..2 {
        let mine: Vec<_> = lists.iter().filter(|l| l.0 == r).collect();
        for l in mine.iter().step_by((mine.len() / 4).max(1)) {
            let (_, rev, listed) = l;
            let expected = all_keys.len() - listed.len();
            let mut watch = WatchClient::connect(endpoints[r].clone()).await.unwrap();
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tx.send(pb::WatchRequest {
                request_union: Some(pb::watch_request::RequestUnion::CreateRequest(
                    pb::WatchCreateRequest {
                        key: b"cm/".to_vec(),
                        range_end: b"cm0".to_vec(),
                        start_revision: rev + 1,
                        ..Default::default()
                    },
                )),
            })
            .await
            .unwrap();
            let mut stream = watch.watch(ReceiverStream::new(rx)).await.unwrap().into_inner();
            let mut seen: BTreeSet<Vec<u8>> = listed.clone();
            let mut events = 0;
            while events < expected {
                let resp = tokio::time::timeout(Duration::from_secs(15), stream.next())
                    .await
                    .unwrap_or_else(|_| {
                        panic!("watch from {} got {events}/{expected} events", rev + 1)
                    })
                    .expect("watch stream ended")
                    .expect("watch error");
                assert!(!resp.canceled, "watch canceled: {}", resp.cancel_reason);
                for ev in resp.events {
                    let key = ev.kv.unwrap().key;
                    assert!(seen.insert(key.clone()), "key {key:?} both listed and watched");
                    events += 1;
                }
            }
            assert_eq!(seen, all_keys, "LIST at {rev} + WATCH from {} miss keys", rev + 1);
            drop(tx);
        }
    }
    let _ = (&n1, &n2, &n3);
}

/// fastetcd#49: a request that cannot apply, sent to the leader or to a
/// follower, gets etcd's error, and every member keeps applying. One
/// such entry used to stop the state machine on all three members.
#[tokio::test]
async fn a_refused_request_stops_no_member() {
    use pb::request_op::Request as Op;

    let p1 = pick_free_port().await;
    let p2 = pick_free_port().await;
    let p3 = pick_free_port().await;
    let mut members: BTreeMap<NodeId, String> = BTreeMap::new();
    members.insert(1, format!("http://127.0.0.1:{p1}"));
    members.insert(2, format!("http://127.0.0.1:{p2}"));
    members.insert(3, format!("http://127.0.0.1:{p3}"));
    let (n1, n2, n3) = tokio::join!(
        start_node(1, &members),
        start_node(2, &members),
        start_node(3, &members),
    );
    sleep(Duration::from_millis(150)).await;
    let mut all: BTreeMap<NodeId, openraft::BasicNode> = BTreeMap::new();
    for (id, url) in &members {
        all.insert(*id, openraft::BasicNode::new(url.clone()));
    }
    n1.raft.initialize(all).await.unwrap();
    let nodes = [&n1, &n2, &n3];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let leader_id = loop {
        if let Some(l) = n1.raft.metrics().borrow().current_leader {
            break l;
        }
        assert!(tokio::time::Instant::now() < deadline, "no leader elected in 10s");
        sleep(Duration::from_millis(100)).await;
    };
    let leader = nodes[leader_id as usize - 1];
    let follower = nodes.into_iter().find(|n| n.raft.metrics().borrow().id != leader_id).unwrap();

    let put = |key: &str, ignore_value: bool| pb::PutRequest {
        key: key.as_bytes().to_vec(),
        value: b"v".to_vec(),
        ignore_value,
        ..Default::default()
    };
    let mut via_leader = KvClient::connect(leader.client_endpoint.clone()).await.unwrap();
    via_leader.put(put("there", false)).await.unwrap();

    for n in [leader, follower] {
        let mut kv = KvClient::connect(n.client_endpoint.clone()).await.unwrap();
        // Refused by the leader before proposing.
        let s = kv.put(put("missing", true)).await.unwrap_err();
        assert_eq!((s.code(), s.message()), (tonic::Code::InvalidArgument, "etcdserver: key not found"));
        // Proposed and refused by every member's state machine: the key
        // exists when the leader checks, and the txn deletes it first.
        let s = kv
            .txn(pb::TxnRequest {
                compare: vec![],
                success: vec![
                    pb::RequestOp {
                        request: Some(Op::RequestDeleteRange(pb::DeleteRangeRequest {
                            key: b"there".to_vec(),
                            ..Default::default()
                        })),
                    },
                    pb::RequestOp { request: Some(Op::RequestPut(put("there", true))) },
                ],
                failure: vec![],
            })
            .await
            .unwrap_err();
        assert_eq!((s.code(), s.message()), (tonic::Code::InvalidArgument, "etcdserver: key not found"));
        let s = kv
            .compact(pb::CompactionRequest { revision: 999, physical: false })
            .await
            .unwrap_err();
        assert_eq!(s.code(), tonic::Code::OutOfRange, "{s:?}");
    }

    // Every member still applies: a write through the follower reaches
    // all three, and each applied every log entry the leader has.
    let mut via_follower = KvClient::connect(follower.client_endpoint.clone()).await.unwrap();
    via_follower.put(put("after", false)).await.expect("the cluster still takes writes");
    let last = leader.raft.metrics().borrow().last_log_index.unwrap();
    for n in nodes {
        let mut kv = KvClient::connect(n.client_endpoint.clone()).await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let applied = n.raft.metrics().borrow().last_applied.map(|l| l.index);
            let r = kv
                .range(pb::RangeRequest { key: b"after".to_vec(), serializable: true, ..Default::default() })
                .await
                .unwrap()
                .into_inner();
            if applied >= Some(last) && r.kvs.len() == 1 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "member {} stopped applying at {applied:?} (leader at {last})",
                n.client_endpoint
            );
            sleep(Duration::from_millis(100)).await;
        }
        let r = kv
            .range(pb::RangeRequest { key: b"there".to_vec(), serializable: true, ..Default::default() })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(r.kvs.len(), 1, "the refused txn's delete was applied on {}", n.client_endpoint);
    }
}
