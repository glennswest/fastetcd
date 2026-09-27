//! Peer TLS has its own identity and trust root (fastetcd#23).
//!
//! Three in-process members whose raft peer ports serve mutual TLS from
//! a peer CA, built with the same `TlsFiles` code `main.rs` uses, over
//! PEM files generated per run. They must form a cluster and replicate
//! (the raft traffic and a follower's forwarded write both cross peer
//! TLS). And the peer port must refuse a caller with no certificate,
//! and one whose certificate a *different* CA (the client CA) signed:
//! a certificate trusted by the client port is not a raft peer.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use openraft::{Config, Raft};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
};
use tempfile::TempDir;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::kv_server::KvServer;
use fastetcd_proto::fastetcd_raft as raftpb;
use fastetcd_proto::fastetcd_raft::raft_peer_client::RaftPeerClient;
use fastetcd_proto::fastetcd_raft::raft_peer_server::RaftPeerServer;
use fastetcd_raft::kv_log_store::KvLogStore;
use fastetcd_raft::network::{GrpcNetworkFactory, RaftPeerService};
use fastetcd_raft::types::{NodeId, TypeConfig};
use fastetcd_raft::FastetcdStateMachine;
use fastetcd_server::kv::KvService;
use fastetcd_server::tls::{Port, TlsFiles};
use fastetcd_server::ServerState;
use fastetcd_storage::mvcc::MvccStore;
use fastetcd_storage::redb_engine::RedbEngine;

// ---------------- certificates ----------------

struct Ca {
    cert: rcgen::Certificate,
    key: KeyPair,
}

fn ca(name: &str) -> Ca {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.distinguished_name.push(DnType::CommonName, name);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let key = KeyPair::generate().unwrap();
    let cert = params.self_signed(&key).unwrap();
    Ca { cert, key }
}

/// A leaf for 127.0.0.1, good for both ends of a TLS connection, as an
/// etcd peer certificate is. Returns (cert PEM, key PEM).
fn leaf(issuer: &Ca, cn: &str) -> (String, String) {
    let mut params =
        CertificateParams::new(vec!["127.0.0.1".to_string(), "localhost".to_string()]).unwrap();
    params.distinguished_name.push(DnType::CommonName, cn);
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let key = KeyPair::generate().unwrap();
    let cert = params.signed_by(&key, &issuer.cert, &issuer.key).unwrap();
    (cert.pem(), key.serialize_pem())
}

fn write(dir: &Path, name: &str, pem: &str) -> PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, pem).unwrap();
    p
}

/// A member's peer TLS flags: its own cert/key, the peer CA, and
/// `--peer-client-cert-auth`.
fn peer_files(dir: &Path, peer_ca: &Ca, id: NodeId) -> TlsFiles {
    let (cert, key) = leaf(peer_ca, &format!("member-{id}"));
    TlsFiles {
        cert_file: Some(write(dir, &format!("peer-{id}.crt"), &cert)),
        key_file: Some(write(dir, &format!("peer-{id}.key"), &key)),
        trusted_ca_file: Some(write(dir, "peer-ca.crt", &peer_ca.cert.pem())),
        client_cert_auth: true,
    }
}

// ---------------- members ----------------

struct Node {
    _dir: TempDir,
    client_endpoint: String,
    raft: Raft<TypeConfig>,
}

async fn start_node(
    id: NodeId,
    members: &BTreeMap<NodeId, String>,
    listener: std::net::TcpListener,
    tls: &TlsFiles,
) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let engine: Arc<dyn fastetcd_storage::KvStore> =
        Arc::new(RedbEngine::open(dir.path().join("data.redb")).unwrap());
    let mvcc = MvccStore::open(engine.clone()).await.unwrap();
    let sm = FastetcdStateMachine::open(mvcc, dir.path().join("snapshots")).await.unwrap();
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

    let mut peers = members.clone();
    peers.remove(&id);
    let peers = Arc::new(RwLock::new(peers.into_iter().collect()));
    let dial_tls = tls.peer_client_config().unwrap();
    assert!(dial_tls.is_some());
    let factory = GrpcNetworkFactory::with_tls(peers.clone(), dial_tls.clone());
    let raft = Raft::<TypeConfig>::new(id, config, factory, log, sm.clone()).await.unwrap();
    let forwarder = fastetcd_raft::WriteForwarder::with_tls(peers, dial_tls);
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

    let server_tls = tls.server_config(Port::Peer).unwrap().expect("peer TLS is on");
    listener.set_nonblocking(true).unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(
        tokio::net::TcpListener::from_std(listener).unwrap(),
    );
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .tls_config(server_tls)
            .unwrap()
            .add_service(RaftPeerServer::new(peer_service))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    // The client port stays plaintext: only the peer port is under test.
    let client = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_addr = client.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(client);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(KvServer::new(kv))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    Node {
        _dir: dir,
        client_endpoint: format!("http://{client_addr}"),
        raft,
    }
}

struct Cluster {
    _certs: TempDir,
    nodes: Vec<Node>,
    peer_urls: BTreeMap<NodeId, String>,
    peer_ca: Ca,
}

async fn tls_cluster() -> Cluster {
    let certs = tempfile::tempdir().unwrap();
    let peer_ca = ca("fastetcd peer CA");
    let listeners: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let peer_urls: BTreeMap<NodeId, String> = listeners
        .iter()
        .enumerate()
        .map(|(i, l)| (i as NodeId + 1, format!("https://{}", l.local_addr().unwrap())))
        .collect();
    let mut nodes = Vec::new();
    for (i, l) in listeners.into_iter().enumerate() {
        let id = i as NodeId + 1;
        let tls = peer_files(certs.path(), &peer_ca, id);
        nodes.push(start_node(id, &peer_urls, l, &tls).await);
    }
    Cluster {
        _certs: certs,
        nodes,
        peer_urls,
        peer_ca,
    }
}

async fn wait_for_leader(nodes: &[Node]) -> NodeId {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        assert!(tokio::time::Instant::now() < deadline, "no leader over peer TLS in 15s");
        let leaders: Vec<_> = nodes
            .iter()
            .map(|n| n.raft.metrics().borrow().current_leader)
            .collect();
        if let Some(Some(l)) = leaders.first() {
            if leaders.iter().all(|x| *x == Some(*l)) {
                return *l;
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn members_form_a_cluster_and_replicate_over_peer_mtls() {
    let c = tls_cluster().await;
    let all: BTreeMap<NodeId, openraft::BasicNode> = c
        .peer_urls
        .iter()
        .map(|(id, url)| (*id, openraft::BasicNode::new(url.clone())))
        .collect();
    c.nodes[0].raft.initialize(all).await.unwrap();
    let leader = wait_for_leader(&c.nodes).await;

    // A write to a follower is forwarded to the leader over peer TLS
    // (`WriteForwarder`), then replicated to every member over it.
    let follower = c
        .nodes
        .iter()
        .find(|n| n.raft.metrics().borrow().id != leader)
        .unwrap();
    let mut kv = KvClient::connect(follower.client_endpoint.clone()).await.unwrap();
    kv.put(pb::PutRequest {
        key: b"k".to_vec(),
        value: b"over-tls".to_vec(),
        ..Default::default()
    })
    .await
    .expect("a follower's write is forwarded over peer TLS");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    for n in &c.nodes {
        let mut kv = KvClient::connect(n.client_endpoint.clone()).await.unwrap();
        loop {
            let r = kv
                .range(pb::RangeRequest {
                    key: b"k".to_vec(),
                    serializable: true,
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner();
            if r.kvs.first().map(|kv| kv.value.as_slice()) == Some(b"over-tls".as_slice()) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{} never saw the write",
                n.client_endpoint
            );
            sleep(Duration::from_millis(100)).await;
        }
    }
}

/// Call `Vote` with an empty payload on a peer port. A caller the TLS
/// layer lets through reaches the service, which rejects the payload
/// with `InvalidArgument`; a caller refused at the handshake never gets
/// that far.
async fn vote_code(url: &str, tls: ClientTlsConfig) -> Result<tonic::Code, String> {
    let chan = match Endpoint::from_shared(url.to_string())
        .unwrap()
        .tls_config(tls)
        .unwrap()
        .connect()
        .await
    {
        Ok(c) => c,
        Err(e) => return Err(format!("connect: {e}")),
    };
    let r = RaftPeerClient::new(chan)
        .vote(raftpb::RaftPayload { data: vec![] })
        .await;
    Ok(match r {
        Ok(_) => tonic::Code::Ok,
        Err(s) => s.code(),
    })
}

#[tokio::test]
async fn the_peer_port_accepts_only_certificates_from_the_peer_ca() {
    let c = tls_cluster().await;
    let url = &c.peer_urls[&1];
    let peer_ca_cert = Certificate::from_pem(c.peer_ca.cert.pem());

    // Control: a peer-CA certificate gets through to the service.
    let (cert, key) = leaf(&c.peer_ca, "another-member");
    let ok = vote_code(
        url,
        ClientTlsConfig::new()
            .ca_certificate(peer_ca_cert.clone())
            .identity(Identity::from_pem(cert, key)),
    )
    .await;
    assert_eq!(ok, Ok(tonic::Code::InvalidArgument), "a peer-CA cert must be accepted");

    // No client certificate.
    let none = vote_code(url, ClientTlsConfig::new().ca_certificate(peer_ca_cert.clone())).await;
    assert_ne!(none, Ok(tonic::Code::InvalidArgument), "a certless caller got through");

    // A certificate from the client CA: trusted on the client port, not
    // on the raft port.
    let client_ca = ca("fastetcd client CA");
    let (cert, key) = leaf(&client_ca, "kube-apiserver");
    let other = vote_code(
        url,
        ClientTlsConfig::new()
            .ca_certificate(peer_ca_cert)
            .identity(Identity::from_pem(cert, key)),
    )
    .await;
    assert_ne!(other, Ok(tonic::Code::InvalidArgument), "a client-CA cert got through");
}

#[tokio::test]
async fn a_peer_url_whose_scheme_disagrees_with_peer_tls_is_refused_by_name() {
    let certs = tempfile::tempdir().unwrap();
    let tls = peer_files(certs.path(), &ca("peer CA"), 1)
        .peer_client_config()
        .unwrap();
    let e = fastetcd_raft::dial_peer("http://127.0.0.1:1", &tls).await.unwrap_err();
    assert!(e.to_string().contains("http://127.0.0.1:1"), "{e}");
    assert!(e.to_string().contains("peer TLS is on"), "{e}");
    let e = fastetcd_raft::dial_peer("https://127.0.0.1:1", &None).await.unwrap_err();
    assert!(e.to_string().contains("peer TLS is off"), "{e}");
}
