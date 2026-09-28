//! fastetcd#32: auth state is replicated through Raft.
//!
//! Auth changes used to commit to the local engine only, and tokens were
//! a node-local set, so on a multi-member cluster RBAC was enforced on
//! some members and not others. These tests run real clusters over the
//! gRPC peer transport and check that a change made on one member is
//! enforced on every member, that a token from one member is accepted
//! by another, that a learner caught up by a raft snapshot gets the auth
//! state, and the gate in front of it all: an unreachable or older
//! member refuses auth changes, and members whose auth tables diverged
//! before replication refuse them until one is adopted.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::{Config, Raft, SnapshotPolicy};
use tempfile::TempDir;
use tokio::sync::RwLock;
use tokio::time::{sleep, Instant};
use tonic::metadata::MetadataValue;
use tonic::{Code, Request, Response, Status};

use fastetcd_proto::authpb;
use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::auth_client::AuthClient;
use fastetcd_proto::etcdserverpb::auth_server::AuthServer;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::kv_server::KvServer;
use fastetcd_proto::fastetcd_admin as apb;
use fastetcd_proto::fastetcd_admin::fastetcd_admin_client::FastetcdAdminClient;
use fastetcd_proto::fastetcd_admin::fastetcd_admin_server::FastetcdAdminServer;
use fastetcd_proto::fastetcd_raft as rpb;
use fastetcd_proto::fastetcd_raft::raft_peer_server::{RaftPeer, RaftPeerServer};
use fastetcd_raft::kv_log_store::KvLogStore;
use fastetcd_raft::network::{GrpcNetworkFactory, PeerEndpoints, RaftPeerService};
use fastetcd_raft::types::{NodeId, TypeConfig};
use fastetcd_raft::FastetcdStateMachine;
use fastetcd_server::auth::{AuthInterceptor, AuthService};
use fastetcd_server::auth_sync::AdminService;
use fastetcd_server::kv::KvService;
use fastetcd_server::ServerState;
use fastetcd_storage::mvcc::auth::AuthOp;
use fastetcd_storage::mvcc::MvccStore;
use fastetcd_storage::redb_engine::RedbEngine;

const READ: i32 = 0;

struct Node {
    _dir: TempDir,
    id: NodeId,
    peer_url: String,
    client: String,
    peers: PeerEndpoints,
    raft: Raft<TypeConfig>,
    state: Arc<ServerState>,
}

/// A member older than replicated auth: its peer service has no
/// `AuthSync` (answers `Unimplemented`), everything else is real.
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
    async fn auth_sync(&self, _: Request<rpb::RaftPayload>) -> R {
        Err(Status::unimplemented("AuthSync"))
    }
}

#[derive(Clone, Copy, Default)]
struct Opts {
    older: bool,
    snapshot_every: Option<u64>,
}

async fn start_node(id: NodeId, opts: Opts) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let engine: Arc<dyn fastetcd_storage::KvStore> =
        Arc::new(RedbEngine::open(dir.path().join("data.redb")).unwrap());
    let mvcc = MvccStore::open(engine.clone()).await.unwrap();
    let sm = FastetcdStateMachine::open(mvcc, dir.path().join("snapshots")).await.unwrap();
    let log = KvLogStore::new(engine);

    let mut config = Config {
        // Generous: the build box runs several jobs at once (#44).
        heartbeat_interval: 100,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    };
    if let Some(n) = opts.snapshot_every {
        config.snapshot_policy = SnapshotPolicy::LogsSinceLast(n);
        config.max_in_snapshot_log_to_keep = 0;
        config.purge_batch_size = 1;
    }
    let config = Arc::new(config.validate().unwrap());

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
    let state = Arc::new(ServerState::new(raft.clone(), sm, 7, id, forwarder));

    let peer_service = RaftPeerService::new(raft.clone(), state.sm.mvcc().clone());
    let peer_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_url = format!("http://{}", peer_listener.local_addr().unwrap());
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(peer_listener);
    tokio::spawn(async move {
        if opts.older {
            tonic::transport::Server::builder()
                .add_service(RaftPeerServer::new(OlderPeer(peer_service)))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        } else {
            tonic::transport::Server::builder()
                .add_service(RaftPeerServer::new(peer_service))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        }
    });

    let interceptor = AuthInterceptor::new(state.auth.clone());
    let kv = KvService::new(state.clone());
    let auth = AuthService::new(state.clone());
    let admin = AdminService::new(state.clone());
    let client_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = format!("http://{}", client_listener.local_addr().unwrap());
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(KvServer::with_interceptor(kv, interceptor.clone()))
            .add_service(FastetcdAdminServer::with_interceptor(admin, interceptor))
            .add_service(AuthServer::new(auth))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(client_listener))
            .await
            .unwrap();
    });

    Node { _dir: dir, id, peer_url, client, peers, raft, state }
}

/// Tell every node where every other node's peer port is.
async fn connect(nodes: &[&Node]) {
    for a in nodes {
        for b in nodes {
            if a.id != b.id {
                a.peers.write().await.insert(b.id, b.peer_url.clone());
            }
        }
    }
}

async fn wait_for(what: &str, mut ok: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ok() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        sleep(Duration::from_millis(50)).await;
    }
}

/// Start `n` members (ids 1..=n), form a cluster of `voters` of them
/// (the rest are never started), and wait for a leader. Returns the
/// running nodes.
async fn cluster(n: u64, voters: u64, opts: impl Fn(NodeId) -> Opts) -> Vec<Node> {
    let mut nodes = Vec::new();
    for id in 1..=n {
        nodes.push(start_node(id, opts(id)).await);
    }
    let refs: Vec<&Node> = nodes.iter().collect();
    connect(&refs).await;
    let mut members = std::collections::BTreeMap::new();
    for id in 1..=voters {
        let url = nodes
            .iter()
            .find(|x| x.id == id)
            .map(|x| x.peer_url.clone())
            // A member that is never started: an address nothing
            // listens on.
            .unwrap_or_else(|| "http://127.0.0.1:1".to_string());
        members.insert(id, openraft::BasicNode::new(url));
    }
    nodes[0].raft.initialize(members).await.unwrap();
    wait_for("a leader", || {
        nodes.iter().all(|x| x.raft.metrics().borrow().current_leader.is_some())
    })
    .await;
    // Membership committed, so a change is not refused as "already
    // undergoing a configuration change" (#44).
    let leader = nodes[0].raft.metrics().borrow().current_leader.unwrap();
    let leader = nodes.iter().find(|x| x.id == leader).unwrap();
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

fn with_token<T>(req: T, token: &str) -> Request<T> {
    let mut r = Request::new(req);
    r.metadata_mut().insert("token", MetadataValue::try_from(token).unwrap());
    r
}

async fn auth_client(n: &Node) -> AuthClient<tonic::transport::Channel> {
    AuthClient::connect(n.client.clone()).await.unwrap()
}

/// root (root role), and alice with read on `config/`, then auth on.
async fn configure_auth(n: &Node) -> Result<(), Status> {
    let mut c = auth_client(n).await;
    c.role_add(pb::AuthRoleAddRequest { name: "root".into() }).await?;
    c.user_add(pb::AuthUserAddRequest {
        name: "root".into(),
        password: "rootpw".into(),
        ..Default::default()
    })
    .await?;
    c.user_grant_role(pb::AuthUserGrantRoleRequest { user: "root".into(), role: "root".into() })
        .await?;
    c.role_add(pb::AuthRoleAddRequest { name: "config".into() }).await?;
    c.role_grant_permission(pb::AuthRoleGrantPermissionRequest {
        name: "config".into(),
        perm: Some(authpb::Permission {
            perm_type: READ,
            key: b"config/".to_vec(),
            range_end: b"config0".to_vec(),
        }),
    })
    .await?;
    c.user_add(pb::AuthUserAddRequest {
        name: "alice".into(),
        password: "pw".into(),
        ..Default::default()
    })
    .await?;
    c.user_grant_role(pb::AuthUserGrantRoleRequest { user: "alice".into(), role: "config".into() })
        .await?;
    c.auth_enable(pb::AuthEnableRequest {}).await?;
    Ok(())
}

async fn login(n: &Node, name: &str, password: &str) -> Result<String, Status> {
    auth_client(n)
        .await
        .authenticate(pb::AuthenticateRequest { name: name.into(), password: password.into() })
        .await
        .map(|r| r.into_inner().token)
}

async fn get(n: &Node, token: Option<&str>, key: &str) -> Result<pb::RangeResponse, Status> {
    let mut kv = KvClient::connect(n.client.clone()).await.unwrap();
    let req = pb::RangeRequest { key: key.as_bytes().to_vec(), serializable: true, ..Default::default() };
    let req = match token {
        Some(t) => with_token(req, t),
        None => Request::new(req),
    };
    kv.range(req).await.map(|r| r.into_inner())
}

async fn put(n: &Node, token: &str, key: &str) -> Result<(), Status> {
    let mut kv = KvClient::connect(n.client.clone()).await.unwrap();
    kv.put(with_token(
        pb::PutRequest { key: key.as_bytes().to_vec(), value: b"v".to_vec(), ..Default::default() },
        token,
    ))
    .await
    .map(|_| ())
}

async fn users(n: &Node) -> Vec<String> {
    auth_client(n)
        .await
        .user_list(pb::AuthUserListRequest {})
        .await
        .unwrap()
        .into_inner()
        .users
}

async fn wait_enabled(nodes: &[Node]) {
    for n in nodes {
        let auth = n.state.auth.clone();
        wait_for(&format!("auth on at member {}", n.id), || auth.is_enabled()).await;
    }
}

#[tokio::test]
async fn a_change_made_on_one_member_is_enforced_on_every_member() {
    let nodes = cluster(3, 3, |_| Opts::default()).await;
    // Configure through a follower: its writes are forwarded.
    let via = a_follower_of(&nodes);
    configure_auth(via).await.unwrap();
    wait_enabled(&nodes).await;

    for n in &nodes {
        let err = get(n, None, "config/a").await.unwrap_err();
        assert_eq!(err.code(), Code::Unauthenticated, "member {}: {err:?}", n.id);
        let mut u = users(n).await;
        u.sort();
        assert_eq!(u, vec!["alice", "root"], "member {}", n.id);
    }

    // A token issued by one member is accepted by every member, and
    // alice's role is enforced there.
    let issuer = leader_of(&nodes);
    let alice = login(issuer, "alice", "pw").await.unwrap();
    let root = login(issuer, "root", "rootpw").await.unwrap();
    put(issuer, &root, "config/a").await.unwrap();
    for n in &nodes {
        let auth = n.state.auth.clone();
        let t = alice.clone();
        wait_for("token replicated", || auth.user_for_token(&t).is_some()).await;
        get(n, Some(&alice), "config/a").await.unwrap();
        let err = get(n, Some(&alice), "secret/a").await.unwrap_err();
        assert_eq!(err.code(), Code::PermissionDenied, "member {}: {err:?}", n.id);
    }

    // Revoking the grant on another member takes effect everywhere.
    let other = nodes.iter().find(|n| n.id != issuer.id).unwrap();
    auth_client(other)
        .await
        .user_revoke_role(with_token(
            pb::AuthUserRevokeRoleRequest { name: "alice".into(), role: "config".into() },
            &root,
        ))
        .await
        .unwrap();
    for n in &nodes {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match get(n, Some(&alice), "config/a").await {
                Err(e) if e.code() == Code::PermissionDenied => break,
                r => assert!(Instant::now() < deadline, "member {}: still {r:?}", n.id),
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    // Deleting alice revokes her token on every member.
    auth_client(issuer)
        .await
        .user_delete(with_token(pb::AuthUserDeleteRequest { name: "alice".into() }, &root))
        .await
        .unwrap();
    for n in &nodes {
        let auth = n.state.auth.clone();
        let t = alice.clone();
        wait_for("token revoked", || auth.user_for_token(&t).is_none()).await;
    }
}

#[tokio::test]
async fn a_learner_caught_up_by_a_snapshot_gets_the_auth_state() {
    let opts = Opts { snapshot_every: Some(20), ..Default::default() };
    let nodes = cluster(1, 1, |_| opts).await;
    let leader = &nodes[0];
    configure_auth(leader).await.unwrap();
    let root = login(leader, "root", "rootpw").await.unwrap();
    for i in 0..60 {
        put(leader, &root, &format!("config/{i:03}")).await.unwrap();
    }
    wait_for("the leader to snapshot and purge its log", || {
        let m = leader.raft.metrics().borrow().clone();
        m.snapshot.is_some() && m.purged.is_some_and(|p| p.index > 10)
    })
    .await;
    let snap = leader.raft.metrics().borrow().snapshot.unwrap();

    let learner = start_node(2, opts).await;
    connect(&[leader, &learner]).await;
    leader
        .raft
        .add_learner(2, openraft::BasicNode::new(learner.peer_url.clone()), true)
        .await
        .unwrap();
    wait_for("the learner to install the snapshot", || {
        learner.raft.metrics().borrow().snapshot.is_some_and(|s| s.index >= snap.index)
    })
    .await;

    // The auth tables came in the snapshot: users, and auth on.
    assert!(learner.state.auth.is_enabled());
    let mut u = users(&learner).await;
    u.sort();
    assert_eq!(u, vec!["alice", "root"]);
    let err = get(&learner, None, "config/000").await.unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);

    // Tokens are not in snapshots (as in etcd), but one issued now
    // replicates to the learner.
    let alice = login(leader, "alice", "pw").await.unwrap();
    let auth = learner.state.auth.clone();
    let t = alice.clone();
    wait_for("token replicated to the learner", || auth.user_for_token(&t).is_some()).await;
    get(&learner, Some(&alice), "config/000").await.unwrap();
}

#[tokio::test]
async fn an_unreachable_member_refuses_auth_changes_until_it_is_back() {
    // Three voters, two running: a quorum, but member 3 cannot confirm
    // it can apply an auth entry.
    let nodes = cluster(2, 3, |_| Opts::default()).await;
    let err = configure_auth(&nodes[0]).await.unwrap_err();
    assert_eq!(err.code(), Code::Unavailable, "{err:?}");
    assert!(err.message().contains("member 3"), "{err:?}");
    // Nothing was applied anywhere.
    for n in &nodes {
        assert!(users(n).await.is_empty(), "member {}", n.id);
    }
}

#[tokio::test]
async fn an_older_member_refuses_auth_changes() {
    let nodes = cluster(3, 3, |id| Opts { older: id == 3, ..Default::default() }).await;
    let err = configure_auth(&nodes[0]).await.unwrap_err();
    assert_eq!(err.code(), Code::Unavailable, "{err:?}");
    assert!(err.message().contains("older"), "{err:?}");
    assert!(err.message().contains("member 3"), "{err:?}");

    // KV writes are unaffected.
    let mut kv = KvClient::connect(nodes[0].client.clone()).await.unwrap();
    kv.put(pb::PutRequest { key: b"k".to_vec(), value: b"v".to_vec(), ..Default::default() })
        .await
        .unwrap();
}

#[tokio::test]
async fn diverged_members_are_refused_until_one_is_adopted() {
    let nodes = cluster(3, 3, |_| Opts::default()).await;
    // What a 1.4.x member did: an auth call served by member 2 wrote
    // only its own tables.
    let (_, r) = nodes[1]
        .state
        .sm
        .mvcc()
        .apply_auth(&AuthOp::UserAdd {
            name: "mallory".into(),
            password_hash: "x".into(),
            no_password: true,
        })
        .await
        .unwrap();
    r.unwrap();

    // Auth changes are refused, on any member, naming the remedy.
    for n in &nodes {
        let err = auth_client(n)
            .await
            .role_add(pb::AuthRoleAddRequest { name: "r".into() })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition, "member {}: {err:?}", n.id);
        assert!(err.message().contains("auth adopt"), "{err:?}");
        assert!(n.state.auth_gate.diverged());
    }

    // `auth members` shows who holds what.
    let mut admin = FastetcdAdminClient::connect(nodes[0].client.clone()).await.unwrap();
    let m = admin.auth_members(apb::AuthMembersRequest {}).await.unwrap().into_inner();
    assert!(!m.identical);
    assert_eq!(m.members.len(), 3);
    let two = m.members.iter().find(|x| x.member_id == 2).unwrap();
    assert_eq!(two.users, vec!["mallory"]);
    assert_eq!(two.version, env!("CARGO_PKG_VERSION"));

    // Adopt member 1's (empty) state everywhere.
    admin.auth_adopt(apb::AuthAdoptRequest { member_id: 1 }).await.unwrap();
    for n in &nodes {
        let deadline = Instant::now() + Duration::from_secs(30);
        while !users(n).await.is_empty() {
            assert!(Instant::now() < deadline, "member {} still has mallory", n.id);
            sleep(Duration::from_millis(50)).await;
        }
    }
    let m = admin.auth_members(apb::AuthMembersRequest {}).await.unwrap().into_inner();
    assert!(m.identical, "{m:?}");

    // Replication works from here on.
    configure_auth(&nodes[2]).await.unwrap();
    wait_enabled(&nodes).await;
    assert!(!nodes[0].state.auth_gate.diverged());
}

#[tokio::test]
async fn adopt_needs_root_while_auth_is_on() {
    let nodes = cluster(1, 1, |_| Opts::default()).await;
    configure_auth(&nodes[0]).await.unwrap();
    let alice = login(&nodes[0], "alice", "pw").await.unwrap();
    let mut admin = FastetcdAdminClient::connect(nodes[0].client.clone()).await.unwrap();
    let err = admin
        .auth_adopt(with_token(apb::AuthAdoptRequest { member_id: 1 }, &alice))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err:?}");
    let err = admin.auth_members(apb::AuthMembersRequest {}).await.unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated, "{err:?}");
    let root = login(&nodes[0], "root", "rootpw").await.unwrap();
    admin
        .auth_members(with_token(apb::AuthMembersRequest {}, &root))
        .await
        .unwrap();
}
