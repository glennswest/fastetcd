//! fastetcd#75: on a three-member cluster, linearizable reads take the
//! leader's own read index (leadership confirmed by a quorum over the
//! `ConfirmLeader` peer RPC) instead of queueing in openraft's RaftCore,
//! and concurrent writes are proposed as batched log entries.
//!
//! Real members over the gRPC peer transport, wired as `main.rs` wires
//! them. Writers go through the leader and through a follower (which
//! forwards); readers make linearizable Ranges on both. Every read must
//! see every write acknowledged before it began. With a member older
//! than the RPC, nothing is batched, and reads still work.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openraft::{Config, Raft};
use tempfile::TempDir;
use tokio::sync::RwLock;
use tokio::time::{sleep, Instant};
use tonic::{Request, Response, Status};

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::kv_server::KvServer;
use fastetcd_proto::fastetcd_raft as rpb;
use fastetcd_proto::fastetcd_raft::raft_peer_server::{RaftPeer, RaftPeerServer};
use fastetcd_raft::kv_log_store::KvLogStore;
use fastetcd_raft::network::{GrpcNetworkFactory, PeerEndpoints, RaftPeerService};
use fastetcd_raft::types::{NodeId, TypeConfig};
use fastetcd_raft::FastetcdStateMachine;
use fastetcd_server::kv::KvService;
use fastetcd_server::ServerState;
use fastetcd_storage::mvcc::MvccStore;
use fastetcd_storage::redb_engine::RedbEngine;

struct Node {
    _dir: TempDir,
    id: NodeId,
    peer_url: String,
    client: String,
    peers: PeerEndpoints,
    raft: Raft<TypeConfig>,
    state: Arc<ServerState>,
}

/// A member older than #75: no `ConfirmLeader`, everything else real.
struct OlderPeer(RaftPeerService);

type R = Result<Response<rpb::RaftPayload>, Status>;

#[tonic::async_trait]
impl RaftPeer for OlderPeer {
    async fn append_entries(&self, r: Request<rpb::RaftPayload>) -> R {
        self.0.append_entries(r).await
    }
    async fn vote(&self, r: Request<rpb::RaftPayload>) -> R {
        self.0.vote(r).await
    }
    async fn install_snapshot(&self, r: Request<rpb::RaftPayload>) -> R {
        self.0.install_snapshot(r).await
    }
    async fn forward_write(&self, r: Request<rpb::RaftPayload>) -> R {
        self.0.forward_write(r).await
    }
    async fn forward_membership(&self, r: Request<rpb::RaftPayload>) -> R {
        self.0.forward_membership(r).await
    }
    async fn forward_read(&self, r: Request<rpb::RaftPayload>) -> R {
        self.0.forward_read(r).await
    }
    async fn auth_sync(&self, r: Request<rpb::RaftPayload>) -> R {
        self.0.auth_sync(r).await
    }
    async fn confirm_leader(&self, _: Request<rpb::RaftPayload>) -> R {
        Err(Status::unimplemented("ConfirmLeader"))
    }
}

async fn start_node(id: NodeId, older: bool) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let engine: Arc<dyn fastetcd_storage::KvStore> =
        Arc::new(RedbEngine::open(dir.path().join("data.redb")).unwrap());
    let mvcc = MvccStore::open(engine.clone()).await.unwrap();
    let sm = FastetcdStateMachine::open(mvcc.clone(), dir.path().join("snapshots"))
        .await
        .unwrap();
    mvcc.defer_apply_sync();
    let log = KvLogStore::new(engine);
    let progress = log.progress();
    let config = Arc::new(
        Config {
            // Generous: the build box runs several jobs at once (#44).
            heartbeat_interval: 100,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let peers: PeerEndpoints = Arc::new(RwLock::new(HashMap::new()));
    let raft = Raft::<TypeConfig>::new(
        id,
        config,
        GrpcNetworkFactory::new(peers.clone()),
        log,
        sm.clone(),
    )
    .await
    .unwrap();
    let forwarder = fastetcd_raft::WriteForwarder::new(peers.clone());
    let state = Arc::new(
        ServerState::new(raft.clone(), sm, 7, id, forwarder)
            .with_peer_read_index_and_batching(progress.clone()),
    );

    let peer_service = RaftPeerService::new(raft.clone(), mvcc)
        .with_log_progress(progress)
        .with_read_index(state.read_index.clone().unwrap())
        .with_proposer(state.proposer.clone().unwrap());
    let peer_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_url = format!("http://{}", peer_listener.local_addr().unwrap());
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(peer_listener);
    tokio::spawn(async move {
        let server = tonic::transport::Server::builder();
        if older {
            let mut s = server;
            s.add_service(RaftPeerServer::new(OlderPeer(peer_service)))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        } else {
            let mut s = server;
            s.add_service(RaftPeerServer::new(peer_service))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        }
    });

    let kv = KvService::new(state.clone());
    let client_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = format!("http://{}", client_listener.local_addr().unwrap());
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(KvServer::new(kv))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(client_listener))
            .await
            .unwrap();
    });

    Node { _dir: dir, id, peer_url, client, peers, raft, state }
}

async fn wait_for(what: &str, mut ok: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ok() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        sleep(Duration::from_millis(50)).await;
    }
}

/// Three members, `older` of them (ids from 3 down) older than #75.
async fn cluster(older: u64) -> Vec<Node> {
    let mut nodes = Vec::new();
    for id in 1..=3 {
        nodes.push(start_node(id, id > 3 - older).await);
    }
    for a in &nodes {
        for b in &nodes {
            if a.id != b.id {
                a.peers.write().await.insert(b.id, b.peer_url.clone());
            }
        }
    }
    let members: BTreeMap<NodeId, openraft::BasicNode> = nodes
        .iter()
        .map(|n| (n.id, openraft::BasicNode::new(n.peer_url.clone())))
        .collect();
    nodes[0].raft.initialize(members).await.unwrap();
    wait_for("a leader", || {
        nodes.iter().all(|x| x.raft.metrics().borrow().current_leader.is_some())
    })
    .await;
    let leader = leader_of(&nodes);
    leader
        .raft
        .wait(Some(Duration::from_secs(30)))
        .applied_index_at_least(Some(1), "first entry applied")
        .await
        .unwrap();
    nodes
}

fn leader_of(nodes: &[Node]) -> &Node {
    let id = nodes[0].raft.metrics().borrow().current_leader.unwrap();
    nodes.iter().find(|n| n.id == id).unwrap()
}

fn a_follower_of(nodes: &[Node]) -> &Node {
    let id = nodes[0].raft.metrics().borrow().current_leader.unwrap();
    nodes.iter().find(|n| n.id != id).unwrap()
}

/// `writers` clients each put `w<i>` = 1, 2, 3, ... for `secs`, half
/// through `a`, half through `b`, recording each acknowledged value.
/// Meanwhile a reader on each of `a` and `b` makes linearizable Ranges
/// of every `w` key and checks each value is at least what had been
/// acknowledged before the Range began. Returns (writes, reads).
async fn run_load(a: &str, b: &str, writers: usize, secs: u64) -> (u64, u64) {
    let acked: Arc<Mutex<BTreeMap<String, u64>>> = Arc::default();
    let stop = Instant::now() + Duration::from_secs(secs);
    let mut tasks = Vec::new();
    for w in 0..writers {
        let url = if w % 2 == 0 { a } else { b }.to_string();
        let acked = acked.clone();
        tasks.push(tokio::spawn(async move {
            let mut kv = KvClient::connect(url).await.unwrap();
            let key = format!("w{w:02}");
            let mut n = 0u64;
            while Instant::now() < stop {
                n += 1;
                kv.put(pb::PutRequest {
                    key: key.clone().into_bytes(),
                    value: n.to_string().into_bytes(),
                    ..Default::default()
                })
                .await
                .expect("put");
                acked.lock().unwrap().insert(key.clone(), n);
            }
            n
        }));
    }
    let mut readers = Vec::new();
    for url in [a, b] {
        let (url, acked) = (url.to_string(), acked.clone());
        readers.push(tokio::spawn(async move {
            let mut kv = KvClient::connect(url).await.unwrap();
            let mut reads = 0u64;
            while Instant::now() < stop {
                let before = acked.lock().unwrap().clone();
                let got = kv
                    .range(pb::RangeRequest {
                        key: b"w".to_vec(),
                        range_end: b"x".to_vec(),
                        ..Default::default()
                    })
                    .await
                    .expect("linearizable range")
                    .into_inner();
                let seen: BTreeMap<String, u64> = got
                    .kvs
                    .iter()
                    .map(|kv| {
                        (
                            String::from_utf8(kv.key.clone()).unwrap(),
                            String::from_utf8(kv.value.clone()).unwrap().parse().unwrap(),
                        )
                    })
                    .collect();
                for (key, n) in &before {
                    let s = seen.get(key).copied().unwrap_or(0);
                    assert!(s >= *n, "stale read: {key} = {s}, but {n} was acknowledged before the read");
                }
                reads += 1;
            }
            reads
        }));
    }
    let mut writes = 0;
    for t in tasks {
        writes += t.await.unwrap();
    }
    let mut reads = 0;
    for r in readers {
        reads += r.await.unwrap();
    }
    (writes, reads)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_are_confirmed_by_quorum_and_writes_are_batched() {
    let nodes = cluster(0).await;
    let leader = leader_of(&nodes);
    let follower = a_follower_of(&nodes);
    let (writes, reads) = run_load(&leader.client, &follower.client, 24, 4).await;

    let r = leader.state.read_index.as_ref().unwrap().stats();
    let p = leader.state.proposer.as_ref().unwrap().stats();
    let (quorum, raft) = (r.quorum.load(Ordering::Relaxed), r.raft.load(Ordering::Relaxed));
    let (batches, batched, single) = (
        p.batches.load(Ordering::Relaxed),
        p.batched.load(Ordering::Relaxed),
        p.single.load(Ordering::Relaxed),
    );
    eprintln!(
        "{writes} writes, {reads} reads; leader read barriers: {quorum} by quorum, {raft} through raft; \
         {batches} batches of {batched} proposals, {single} alone"
    );
    assert!(reads > 0 && writes > 0);
    assert!(quorum > 0, "no read used the leader's own read index");
    assert!(raft * 10 <= quorum, "most reads fell back to raft: {raft} vs {quorum}");
    assert!(batches > 0, "no proposals were batched");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_older_member_means_no_batches() {
    let nodes = cluster(1).await;
    // The leader must be a current member: an older one would not batch
    // at all, but this simulated one only lacks the RPC.
    let deadline = Instant::now() + Duration::from_secs(30);
    while leader_of(&nodes).id == 3 {
        assert!(Instant::now() < deadline, "node 3 kept the leadership");
        nodes[0].raft.trigger_elect().await.unwrap();
        sleep(Duration::from_secs(4)).await;
    }
    let leader = leader_of(&nodes);
    let follower = nodes.iter().find(|n| n.id != leader.id && n.id != 3).unwrap();
    let (writes, reads) = run_load(&leader.client, &follower.client, 16, 3).await;

    let p = leader.state.proposer.as_ref().unwrap().stats();
    let r = leader.state.read_index.as_ref().unwrap().stats();
    eprintln!(
        "{writes} writes, {reads} reads; batches {}, quorum reads {}",
        p.batches.load(Ordering::Relaxed),
        r.quorum.load(Ordering::Relaxed)
    );
    assert!(writes > 0 && reads > 0);
    assert_eq!(p.batches.load(Ordering::Relaxed), 0, "batched with an older member present");
    // The current follower and the leader are a quorum: reads stay local.
    assert!(r.quorum.load(Ordering::Relaxed) > 0);
}
