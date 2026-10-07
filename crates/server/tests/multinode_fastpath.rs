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
use fastetcd_raft::wal_log_store::{wal_dir, WalLogOptions, WalLogStore};
use fastetcd_raft::network::{GrpcNetworkFactory, PeerEndpoints, RaftPeerService};
use fastetcd_raft::types::{NodeId, TypeConfig};
use fastetcd_raft::FastetcdStateMachine;
use fastetcd_proto::etcdserverpb::lease_client::LeaseClient;
use fastetcd_proto::etcdserverpb::lease_server::LeaseServer;
use fastetcd_server::kv::KvService;
use fastetcd_server::lease::LeaseService;
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
    log: WalLogStore,
}

/// A member's raft timing: openraft's heartbeat and election range.
#[derive(Clone, Copy)]
struct Timing {
    heartbeat: u64,
    election_min: u64,
    election_max: u64,
}

/// Generous: the build box runs several jobs at once (#44).
const HARNESS_TIMING: Timing = Timing { heartbeat: 100, election_min: 1500, election_max: 3000 };

/// A member's peer service; `older` makes it a member older than #75
/// (no `ConfirmLeader`), everything else real.
struct Peer {
    inner: RaftPeerService,
    older: bool,
}

type R = Result<Response<rpb::RaftPayload>, Status>;

#[tonic::async_trait]
impl RaftPeer for Peer {
    async fn append_entries(&self, r: Request<rpb::RaftPayload>) -> R {
        self.inner.append_entries(r).await
    }
    async fn vote(&self, r: Request<rpb::RaftPayload>) -> R {
        self.inner.vote(r).await
    }
    async fn install_snapshot(&self, r: Request<rpb::RaftPayload>) -> R {
        self.inner.install_snapshot(r).await
    }
    async fn forward_write(&self, r: Request<rpb::RaftPayload>) -> R {
        self.inner.forward_write(r).await
    }
    async fn forward_membership(&self, r: Request<rpb::RaftPayload>) -> R {
        self.inner.forward_membership(r).await
    }
    async fn forward_read(&self, r: Request<rpb::RaftPayload>) -> R {
        self.inner.forward_read(r).await
    }
    async fn auth_sync(&self, r: Request<rpb::RaftPayload>) -> R {
        self.inner.auth_sync(r).await
    }
    async fn confirm_leader(&self, r: Request<rpb::RaftPayload>) -> R {
        if self.older {
            return Err(Status::unimplemented("ConfirmLeader"));
        }
        self.inner.confirm_leader(r).await
    }
    async fn lease_time_to_live(&self, r: Request<rpb::RaftPayload>) -> R {
        if self.older {
            return Err(Status::unimplemented("LeaseTimeToLive"));
        }
        self.inner.lease_time_to_live(r).await
    }
}

async fn start_node(id: NodeId, older: bool) -> Node {
    start_node_with(id, older, HARNESS_TIMING).await
}

async fn start_node_with(id: NodeId, older: bool, timing: Timing) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let engine: Arc<dyn fastetcd_storage::KvStore> =
        Arc::new(RedbEngine::open(dir.path().join("data.redb")).unwrap());
    let mvcc = MvccStore::open(engine.clone()).await.unwrap();
    let sm = FastetcdStateMachine::open(mvcc.clone(), dir.path().join("snapshots"))
        .await
        .unwrap();
    mvcc.defer_apply_sync();
    // The raft log in a WAL, as the server runs it (#85).
    let log = WalLogStore::open(&wal_dir(dir.path()), engine, WalLogOptions::default())
        .await
        .unwrap();
    let progress = log.progress();
    let log_handle = log.clone();
    fastetcd_raft::wal_log_store::spawn_checkpointer(
        log.clone(),
        sm.applied_index(),
        Default::default(),
    );
    let config = Arc::new(
        Config {
            heartbeat_interval: timing.heartbeat,
            election_timeout_min: timing.election_min,
            election_timeout_max: timing.election_max,
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
        .with_proposer(state.proposer.clone().unwrap())
        .with_lessor(state.lessor.clone());
    let peer_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_url = format!("http://{}", peer_listener.local_addr().unwrap());
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(peer_listener);
    let peer = Peer { inner: peer_service, older };
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(
                RaftPeerServer::new(peer)
                    .max_decoding_message_size(fastetcd_raft::network::PEER_MAX_DECODE_BYTES),
            )
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    let kv = KvService::new(state.clone());
    let lease = LeaseService::new(state.clone());
    fastetcd_server::lease_expiry::spawn_with_tick(state.clone(), Duration::from_millis(200));
    let client_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = format!("http://{}", client_listener.local_addr().unwrap());
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(KvServer::new(kv))
            .add_service(LeaseServer::new(lease))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(client_listener))
            .await
            .unwrap();
    });

    Node { _dir: dir, id, peer_url, client, peers, raft, state, log: log_handle }
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
    cluster_with(older, HARNESS_TIMING).await
}

async fn cluster_with(older: u64, timing: Timing) -> Vec<Node> {
    let mut nodes = Vec::new();
    for id in 1..=3 {
        nodes.push(start_node_with(id, id > 3 - older, timing).await);
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
        nodes[0].raft.trigger().elect().await.unwrap();
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

/// fastetcd#94: a member behind by more batched log than the peer port
/// takes in one message, or can append within a heartbeat, still catches
/// up. A batch is one log entry (up to 512 KiB), and openraft sends as
/// many entries as the log reader hands it (up to 300) in one
/// AppendEntries, giving it one heartbeat interval. A learner added after
/// ~24 MiB of batched writes is caught up from the log (nothing is purged
/// yet). Without the WAL reader's byte bound the leader sends that whole
/// backlog in one message, past even the 16 MiB the peer port now
/// decodes; the learner refuses it (or cannot append it in time), and
/// the leader sends it again, forever. (A learner, not a stalled voter: a voter that heard no
/// heartbeat would call an election.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_far_behind_on_big_batches_catches_up() {
    const ROUNDS: usize = 12;
    const WRITERS: usize = 500;
    let nodes = cluster(0).await;
    let leader = leader_of(&nodes);
    let proposer = leader.state.proposer.as_ref().unwrap();
    let kv = KvClient::connect(leader.client.clone()).await.unwrap();
    let puts = |prefix: String, n: usize, bytes: usize| {
        let kv = kv.clone();
        async move {
            let tasks: Vec<_> = (0..n)
                .map(|i| {
                    let mut kv = kv.clone();
                    let key = format!("{prefix}/{i:04}").into_bytes();
                    tokio::spawn(async move {
                        kv.put(pb::PutRequest { key, value: vec![b'x'; bytes], ..Default::default() })
                            .await
                            .expect("put")
                    })
                })
                .collect();
            for t in tasks {
                t.await.unwrap();
            }
        }
    };

    // Until batching is on (every member has answered ConfirmLeader).
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut warm = 0;
    while proposer.stats().batches.load(Ordering::Relaxed) == 0 {
        assert!(Instant::now() < deadline, "batching never started");
        puts(format!("warm{warm}"), 50, 1).await;
        warm += 1;
    }
    let batches_before = proposer.stats().batches.load(Ordering::Relaxed);
    for round in 0..ROUNDS {
        puts(format!("big/{round:02}"), WRITERS, 4096).await;
    }
    let batches = proposer.stats().batches.load(Ordering::Relaxed) - batches_before;
    let last = leader.raft.metrics().borrow().last_log_index.unwrap();
    eprintln!("{} puts of 4 KiB in {batches} batches; leader log at {last}", ROUNDS * WRITERS);
    assert!(batches > 0, "the writes were not batched");

    // A new member, caught up from the log.
    let learner = start_node(4, false).await;
    for n in nodes.iter() {
        n.peers.write().await.insert(4, learner.peer_url.clone());
        learner.peers.write().await.insert(n.id, n.peer_url.clone());
    }
    leader
        .raft
        .add_learner(4, openraft::BasicNode::new(learner.peer_url.clone()), false)
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let applied = learner.raft.metrics().borrow().last_applied.map(|l| l.index);
        if applied >= Some(last) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the new member never caught up: applied {applied:?}, leader at {last}"
        );
        sleep(Duration::from_millis(100)).await;
    }
    assert!(learner.raft.metrics().borrow().snapshot.is_none(), "caught up by the log, not a snapshot");
    let mut lkv = KvClient::connect(learner.client.clone()).await.unwrap();
    let got = lkv
        .range(pb::RangeRequest {
            key: b"big/".to_vec(),
            range_end: b"big0".to_vec(),
            count_only: true,
            serializable: true,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.count, (ROUNDS * WRITERS) as i64);
}

/// The highest raft term any member has seen.
fn max_term(nodes: &[Node]) -> u64 {
    nodes.iter().map(|n| n.raft.metrics().borrow().current_term).max().unwrap_or(0)
}

/// fastetcd#103: WAL fsyncs take `stall` (on every member, or with
/// `everyone` false on the leader alone) while four writes go through
/// the leader. openraft 0.9 sends heartbeats from RaftCore, which waits
/// for each append's fsync, so the leader is silent for each stall.
/// Returns (term before, highest term after, leader before, leader after).
async fn writes_through_stalls(timing: Timing, stall: Duration, everyone: bool) -> (u64, u64, NodeId, NodeId) {
    // `RUST_LOG=openraft=info` shows each member's election decisions.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let nodes = cluster_with(0, timing).await;
    let leader = leader_of(&nodes).id;
    let term = max_term(&nodes);
    for n in nodes.iter().filter(|n| everyone || n.id == leader) {
        n.log.set_sync_delay(stall);
    }
    let mut kv = KvClient::connect(nodes[(leader - 1) as usize].client.clone()).await.unwrap();
    for i in 0..4 {
        // A put may fail while leadership moves; the term says what happened.
        let put = kv.put(pb::PutRequest { key: format!("stall{i}").into_bytes(), value: b"v".to_vec(), ..Default::default() });
        let _ = tokio::time::timeout(Duration::from_secs(60), put).await;
    }
    for n in &nodes {
        n.log.set_sync_delay(Duration::ZERO);
    }
    // Let any election in progress settle before reading the outcome.
    sleep(Duration::from_secs(2)).await;
    let after = max_term(&nodes);
    let now = nodes[0].raft.metrics().borrow().current_leader.unwrap_or(0);
    (term, after, leader, now)
}

/// At the default timeouts (heartbeat 250 ms, election 1 s), a 6 s fsync
/// stall on the leader alone costs it the leadership: its followers hear
/// nothing for longer than their election timeout plus openraft's leader
/// lease (3-4 s) and elect another (3 of 3 runs on dev). (With every
/// member's disk stalled at once the outcome depends on timing: a
/// follower stuck in its own fsync does not time out. On dev it ranged
/// from no election to 40 terms of churn with no leader, so it is not
/// asserted.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stall_on_the_leader_longer_than_the_election_timeout_elects_again() {
    let timing = Timing { heartbeat: 250, election_min: 1000, election_max: 2000 };
    let (before, after, l0, l1) = writes_through_stalls(timing, Duration::from_secs(6), false).await;
    eprintln!("1 s election timeout, 6 s fsyncs on the leader: term {before} -> {after}, leader {l0} -> {l1}");
    assert!(after > before, "no election while the leader stalled 6 s at a 1 s election timeout (term {before})");
}

/// With `--election-timeout` above the stall (10 s: 30-40 s with the
/// lease), the same stall keeps the leader and the term, whether the
/// leader's disk alone or every member's stalls.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_election_timeout_above_the_stall_keeps_the_leader() {
    let timing = Timing { heartbeat: 250, election_min: 10_000, election_max: 20_000 };
    for everyone in [false, true] {
        let (before, after, l0, l1) = writes_through_stalls(timing, Duration::from_secs(6), everyone).await;
        eprintln!("10 s election timeout, 6 s fsyncs (every member: {everyone}): term {before} -> {after}, leader {l0} -> {l1}");
        assert_eq!((after, l1), (before, l0), "the leader or the term changed (every member stalled: {everyone})");
    }
}

/// fastetcd#77: once the log holds batched entries, a member older than
/// 1.10 is refused as a new member and as a voter, one not reachable yet
/// may join as a learner but not be promoted, and the leader's watch
/// names an older member (a downgrade).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_older_member_is_kept_from_a_batched_log() {
    use fastetcd_server::batch_guard::{guard_add, guard_voter, older_members};
    let nodes = cluster(0).await;
    // Concurrent puts through whoever leads (leadership may move on a
    // loaded box; a failed put is just retried) until a member has
    // applied a batch. The guards run from that member: they need its
    // mark and its peer connections, not the leadership.
    let deadline = Instant::now() + Duration::from_secs(60);
    let leader = loop {
        if let Some(n) = nodes.iter().find(|n| n.state.sm.mvcc().has_batched()) {
            break n;
        }
        assert!(Instant::now() < deadline, "60 s of load did not batch");
        let kv = KvClient::connect(leader_of(&nodes).client.clone()).await.unwrap();
        let puts: Vec<_> = (0..24)
            .map(|i| {
                let mut kv = kv.clone();
                tokio::spawn(async move {
                    let _ = kv
                        .put(pb::PutRequest { key: format!("b{i}").into_bytes(), value: b"v".to_vec(), ..Default::default() })
                        .await;
                })
            })
            .collect();
        for p in puts {
            let _ = p.await;
        }
    };

    let old = start_node(4, true).await;
    let new = start_node(5, false).await;
    leader.peers.write().await.insert(4, old.peer_url.clone());
    leader.peers.write().await.insert(5, new.peer_url.clone());
    leader.peers.write().await.insert(6, "http://127.0.0.1:1".into());
    let refused = |r: Result<(), Status>, what: &str, says: &str| match r {
        Err(s) => {
            assert_eq!(s.code(), tonic::Code::FailedPrecondition, "{what}: {s:?}");
            assert!(s.message().contains(says), "{what}: {}", s.message());
        }
        Ok(()) => panic!("{what}: allowed"),
    };
    refused(guard_add(&leader.state, 4).await, "add an older member", "older than 1.10");
    refused(guard_voter(&leader.state, 4).await, "promote an older member", "older than 1.10");
    guard_add(&leader.state, 5).await.expect("add a 1.10+ member");
    guard_voter(&leader.state, 5).await.expect("promote a 1.10+ member");
    guard_add(&leader.state, 6).await.expect("add one not started yet (the usual order)");
    refused(guard_voter(&leader.state, 6).await, "promote one that cannot be reached", "cannot be reached");

    // A member found older while the log holds batches: named by the watch.
    // (A cluster with an older member never batches, so the mark is set
    // by hand, as a downgrade after batching would find it.)
    let mixed = cluster(1).await;
    assert!(!mixed[0].state.sm.mvcc().has_batched());
    mixed[0].state.sm.mvcc().stage_batched_marker().await;
    assert_eq!(older_members(&mixed[0].state).await, vec![3], "the older member named");
    // Without batched entries an older member is no one's problem.
    assert!(!mixed[1].state.sm.mvcc().has_batched());
    guard_add(&mixed[1].state, 3).await.expect("no batches: an older member may join");
}

fn nested_txn_request() -> pb::TxnRequest {
    use pb::request_op::Request;
    let put = |k: &str, v: &str| pb::RequestOp {
        request: Some(Request::RequestPut(pb::PutRequest {
            key: k.as_bytes().to_vec(),
            value: v.as_bytes().to_vec(),
            ..Default::default()
        })),
    };
    pb::TxnRequest {
        compare: vec![],
        success: vec![
            put("n/a", "1"),
            pb::RequestOp {
                request: Some(Request::RequestTxn(pb::TxnRequest {
                    compare: vec![],
                    success: vec![put("n/b", "2")],
                    failure: vec![],
                })),
            },
        ],
        failure: vec![],
    }
}

/// A txn inside a txn, sent to a follower, is applied on every member
/// (fastetcd#56).
#[tokio::test]
async fn a_nested_txn_applies_on_every_member() {
    let nodes = cluster(0).await;
    let follower = a_follower_of(&nodes);
    let mut kv = KvClient::connect(follower.client.clone()).await.unwrap();
    let resp = kv.txn(nested_txn_request()).await.expect("nested txn").into_inner();
    assert!(resp.succeeded);
    assert!(matches!(
        &resp.responses[1].response,
        Some(pb::response_op::Response::ResponseTxn(t)) if t.succeeded && t.responses.len() == 1
    ));
    let rev = resp.header.unwrap().revision;
    for n in &nodes {
        let mut kv = KvClient::connect(n.client.clone()).await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let r = kv
                .range(pb::RangeRequest {
                    key: b"n/".to_vec(),
                    range_end: b"n0".to_vec(),
                    serializable: true,
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner();
            if r.kvs.len() == 2 {
                assert!(r.kvs.iter().all(|kv| kv.mod_revision == rev), "member {}: {r:?}", n.id);
                break;
            }
            assert!(Instant::now() < deadline, "member {} never applied the nested txn", n.id);
            sleep(Duration::from_millis(50)).await;
        }
    }
}

/// With a member that cannot decode a nested txn, one is refused before
/// it reaches the log, naming that member; a flat txn still goes through.
#[tokio::test]
async fn a_nested_txn_waits_for_every_member_to_read_it() {
    let nodes = cluster(1).await;
    // Member 1 serves it (the gate runs where the txn arrives, leader or
    // not); member 3 is the older one.
    let leader = leader_of(&nodes);
    let mut kv = KvClient::connect(nodes[0].client.clone()).await.unwrap();
    let err = kv.txn(nested_txn_request()).await.expect_err("an older member: refused");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition, "{err:?}");
    assert!(err.message().contains("member 3"), "{err:?}");
    let mut flat = nested_txn_request();
    flat.success.truncate(1);
    kv.txn(flat).await.expect("a flat txn is not gated");
    let r = kv
        .range(pb::RangeRequest { key: b"n/b".to_vec(), ..Default::default() })
        .await
        .unwrap()
        .into_inner();
    assert!(r.kvs.is_empty(), "nothing of the refused txn applied");
    // The older member keeps up with the log.
    let older = nodes.iter().find(|n| n.id == 3).unwrap();
    let applied = leader.raft.metrics().borrow().last_applied;
    older
        .raft
        .wait(Some(Duration::from_secs(30)))
        .applied_index_at_least(applied.map(|l| l.index), "the older member applies on")
        .await
        .unwrap();
}

async fn key_exists(url: &str, key: &[u8]) -> bool {
    let mut kv = KvClient::connect(url.to_string()).await.unwrap();
    !kv.range(pb::RangeRequest { key: key.to_vec(), ..Default::default() })
        .await
        .unwrap()
        .into_inner()
        .kvs
        .is_empty()
}

/// `n` keep-alives of `id` through `url`, one a second; each answered
/// with the lease's TTL.
async fn keep_alive(url: &str, id: i64, ttl: i64, n: usize) {
    let mut lc = LeaseClient::connect(url.to_string()).await.unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let mut answers = lc
        .lease_keep_alive(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    for _ in 0..n {
        tx.send(pb::LeaseKeepAliveRequest { id }).await.unwrap();
        let a = tokio_stream::StreamExt::next(&mut answers).await.unwrap().expect("keep-alive");
        assert_eq!((a.id, a.ttl), (id, ttl));
        sleep(Duration::from_secs(1)).await;
    }
}

/// Grant a lease of `ttl` through `url` and put `key` on it.
async fn lease_with_key(url: &str, ttl: i64, key: &[u8]) -> i64 {
    let mut lc = LeaseClient::connect(url.to_string()).await.unwrap();
    let id = lc.lease_grant(pb::LeaseGrantRequest { ttl, id: 0 }).await.unwrap().into_inner().id;
    let mut kv = KvClient::connect(url.to_string()).await.unwrap();
    kv.put(pb::PutRequest { key: key.to_vec(), value: b"v".to_vec(), lease: id, ..Default::default() })
        .await
        .unwrap();
    id
}

/// Keep-alives sent to a follower are renewed in the leader's RAM: no
/// log entry, TimeToLive through the follower sees them, and after a
/// leader change the new leader gives the lease a full TTL (etcd's
/// Promote), then expires it once renewals stop (fastetcd#92).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keep_alives_through_a_follower_renew_in_the_leaders_ram() {
    let nodes = cluster(0).await;
    let follower = a_follower_of(&nodes);
    let id = lease_with_key(&follower.client, 4, b"ka/k").await;
    let leader = leader_of(&nodes);
    // The gate (every member 1.23+) is asked in the background on the
    // first keep-alive; keep renewing while it answers.
    keep_alive(&follower.client, id, 4, 5).await;
    let before = leader.raft.metrics().borrow().last_log_index;
    let renewed = leader.state.lessor.stats().renewed_in_ram.load(Ordering::Relaxed);

    keep_alive(&follower.client, id, 4, 6).await;
    assert_eq!(leader.raft.metrics().borrow().last_log_index, before, "a keep-alive was logged");
    assert_eq!(leader.state.lessor.stats().renewed_in_ram.load(Ordering::Relaxed), renewed + 6);
    let mut lc = LeaseClient::connect(follower.client.clone()).await.unwrap();
    let ttl = lc
        .lease_time_to_live(pb::LeaseTimeToLiveRequest { id, keys: true })
        .await
        .unwrap()
        .into_inner();
    assert!(ttl.ttl >= 2, "the follower's TimeToLive is the leader's: {ttl:?}");
    assert_eq!(ttl.keys, vec![b"ka/k".to_vec()]);

    // A new leader: the old leader's RAM renewals are gone with it, and
    // the lease's persisted deadline passed long ago.
    let old = leader.id;
    follower.raft.trigger().elect().await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while nodes[0].raft.metrics().borrow().current_leader.is_none_or(|l| l == old) {
        assert!(Instant::now() < deadline, "no new leader");
        sleep(Duration::from_millis(100)).await;
    }
    let changed = Instant::now();
    let leader = leader_of(&nodes);
    sleep(Duration::from_secs(2)).await;
    assert!(key_exists(&leader.client, b"ka/k").await, "the new leader expired a renewed lease");
    assert!(leader.state.lessor.stats().promotions.load(Ordering::Relaxed) >= 1);
    // No more renewals: gone within TTL (4 s) after the change, plus the
    // sweep and the revoke.
    while key_exists(&leader.client, b"ka/k").await {
        assert!(changed.elapsed() < Duration::from_secs(12), "the lease never expired");
        sleep(Duration::from_millis(200)).await;
    }
    eprintln!("expired {:?} after the leader change", changed.elapsed());
}

/// With a member that does not answer as 1.23+, keep-alives go through
/// raft as before: an older leader would read only persisted deadlines.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_older_member_means_keep_alives_go_through_raft() {
    let nodes = cluster(1).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    while leader_of(&nodes).id == 3 {
        assert!(Instant::now() < deadline, "node 3 kept the leadership");
        nodes[0].raft.trigger().elect().await.unwrap();
        sleep(Duration::from_secs(4)).await;
    }
    let leader = leader_of(&nodes);
    let id = lease_with_key(&leader.client, 3, b"old/k").await;
    keep_alive(&leader.client, id, 3, 5).await;
    let before = leader.raft.metrics().borrow().last_log_index.unwrap_or(0);
    keep_alive(&leader.client, id, 3, 4).await;
    let after = leader.raft.metrics().borrow().last_log_index.unwrap_or(0);
    let stats = leader.state.lessor.stats();
    assert_eq!(stats.renewed_in_ram.load(Ordering::Relaxed), 0);
    assert!(stats.proposed.load(Ordering::Relaxed) >= 9);
    assert!(after >= before + 4, "keep-alives were not logged: {before} -> {after}");
    assert!(key_exists(&leader.client, b"old/k").await);
}
