//! fastetcd#20: under `--client-cert-auth`, a client certificate's
//! Common Name is its etcd user, as in etcd (`AuthInfoFromTLS`), so a
//! role can scope what an mTLS client may read and write without any
//! `Authenticate` call. A `token` still wins over the certificate.
//!
//! The client port here is built the way `main.rs` builds it: TLS from
//! `TlsFiles`, services behind the auth interceptor, routes turned into
//! an axum router and back, served by tonic with `tls_config`. That path
//! must carry the verified client certificate through to the handlers.

mod common;
use common::{wait_for_leader, NopNet};

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use openraft::{Config, Raft};
use rcgen::{BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair};
use tempfile::TempDir;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity, Server};
use tonic::{Code, Request};

use fastetcd_proto::authpb;
use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::auth_client::AuthClient;
use fastetcd_proto::etcdserverpb::auth_server::AuthServer;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::kv_server::KvServer;
use fastetcd_proto::etcdserverpb::maintenance_client::MaintenanceClient;
use fastetcd_proto::etcdserverpb::maintenance_server::MaintenanceServer;
use fastetcd_proto::etcdserverpb::watch_client::WatchClient;
use fastetcd_proto::etcdserverpb::watch_server::WatchServer;
use fastetcd_raft::kv_log_store::KvLogStore;
use fastetcd_raft::types::{NodeId, TypeConfig};
use fastetcd_raft::FastetcdStateMachine;
use fastetcd_server::auth::{AuthInterceptor, AuthService};
use fastetcd_server::kv::KvService;
use fastetcd_server::maintenance::MaintenanceService;
use fastetcd_server::tls::{Port, TlsFiles};
use fastetcd_server::watch::WatchService;
use fastetcd_server::ServerState;
use fastetcd_storage::mvcc::MvccStore;
use fastetcd_storage::redb_engine::RedbEngine;

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

/// (cert PEM, key PEM) for `cn`, signed by `issuer`, for 127.0.0.1. An
/// empty `cn` leaves the Common Name out.
fn leaf(issuer: &Ca, cn: &str) -> (String, String) {
    let mut params =
        CertificateParams::new(vec!["127.0.0.1".to_string(), "localhost".to_string()]).unwrap();
    // rcgen's default name carries a CN of its own; start empty.
    params.distinguished_name = rcgen::DistinguishedName::new();
    if !cn.is_empty() {
        params.distinguished_name.push(DnType::CommonName, cn);
    }
    params.extended_key_usages =
        vec![ExtendedKeyUsagePurpose::ServerAuth, ExtendedKeyUsagePurpose::ClientAuth];
    let key = KeyPair::generate().unwrap();
    let cert = params.signed_by(&key, &issuer.cert, &issuer.key).unwrap();
    (cert.pem(), key.serialize_pem())
}

fn write(dir: &Path, name: &str, pem: &str) -> std::path::PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, pem).unwrap();
    p
}

struct Server_ {
    _dir: TempDir,
    endpoint: String,
    ca: Ca,
}

/// A single-member server whose client port serves mutual TLS, with
/// `--client-cert-auth` set as `cert_auth` says.
async fn start(cert_auth: bool) -> Server_ {
    let dir = tempfile::tempdir().unwrap();
    let engine: Arc<dyn fastetcd_storage::KvStore> =
        Arc::new(RedbEngine::open(dir.path().join("kv.redb")).unwrap());
    let mvcc = MvccStore::open(engine.clone()).await.unwrap();
    let sm = FastetcdStateMachine::open(mvcc, dir.path().join("snapshots")).await.unwrap();
    let config = Arc::new(
        Config { heartbeat_interval: 100, election_timeout_min: 200, election_timeout_max: 500, ..Default::default() }
            .validate()
            .unwrap(),
    );
    let raft = Raft::<TypeConfig>::new(1, config, NopNet, KvLogStore::new(engine), sm.clone())
        .await
        .unwrap();
    raft.initialize(BTreeSet::<NodeId>::from([1])).await.unwrap();
    wait_for_leader(&raft).await;
    let forwarder = fastetcd_raft::WriteForwarder::new(fastetcd_raft::network::empty_peers());
    let state =
        Arc::new(ServerState::new(raft, sm, 7, 1, forwarder).with_client_cert_auth(cert_auth));

    // TLS the way main.rs builds it.
    let ca = ca("client-ca");
    let (cert, key) = leaf(&ca, "server");
    let tls = TlsFiles {
        cert_file: Some(write(dir.path(), "server.crt", &cert)),
        key_file: Some(write(dir.path(), "server.key", &key)),
        trusted_ca_file: Some(write(dir.path(), "ca.crt", &ca.cert.pem())),
        client_cert_auth: true,
    }
    .server_config(Port::Client)
    .unwrap()
    .unwrap();

    let interceptor = AuthInterceptor::new(state.auth.clone()).with_client_cert_auth(cert_auth);
    let mut routes = tonic::service::Routes::builder();
    routes.add_service(KvServer::with_interceptor(KvService::new(state.clone()), interceptor.clone()));
    routes.add_service(WatchServer::with_interceptor(
        WatchService::new(state.clone()),
        interceptor.clone(),
    ));
    routes.add_service(MaintenanceServer::with_interceptor(
        MaintenanceService::new(state.clone()),
        interceptor,
    ));
    routes.add_service(AuthServer::new(AuthService::new(state.clone())));
    let app: axum::Router = routes.routes().into_axum_router();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("https://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        Server::builder()
            .accept_http1(true)
            .tls_config(tls)
            .unwrap()
            .add_routes(tonic::service::Routes::from(app))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    Server_ { _dir: dir, endpoint, ca }
}

impl Server_ {
    /// A channel presenting a client certificate for `cn`.
    async fn as_cn(&self, cn: &str) -> Channel {
        let (cert, key) = leaf(&self.ca, cn);
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(self.ca.cert.pem()))
            .identity(Identity::from_pem(cert, key))
            .domain_name("localhost");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            match Endpoint::from_shared(self.endpoint.clone()).unwrap().tls_config(tls.clone()).unwrap().connect().await {
                Ok(c) => return c,
                Err(e) => {
                    assert!(tokio::time::Instant::now() < deadline, "connect: {e}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }
}

/// As `root` (by certificate): the `config` role (readwrite on
/// `config/`), `alice` with it and no password, `root`, auth on.
async fn configure(s: &Server_) {
    let mut a = AuthClient::new(s.as_cn("root").await);
    a.role_add(pb::AuthRoleAddRequest { name: "root".into() }).await.unwrap();
    a.role_add(pb::AuthRoleAddRequest { name: "config".into() }).await.unwrap();
    a.role_grant_permission(pb::AuthRoleGrantPermissionRequest {
        name: "config".into(),
        perm: Some(authpb::Permission {
            perm_type: 2,
            key: b"config/".to_vec(),
            range_end: b"config0".to_vec(),
        }),
    })
    .await
    .unwrap();
    for (name, pw) in [("root", "rootpw"), ("alice", "")] {
        a.user_add(pb::AuthUserAddRequest {
            name: name.into(),
            password: pw.into(),
            options: Some(authpb::UserAddOptions { no_password: pw.is_empty() }),
            ..Default::default()
        })
        .await
        .unwrap();
    }
    a.user_grant_role(pb::AuthUserGrantRoleRequest { user: "root".into(), role: "root".into() })
        .await
        .unwrap();
    a.user_grant_role(pb::AuthUserGrantRoleRequest { user: "alice".into(), role: "config".into() })
        .await
        .unwrap();
    a.auth_enable(pb::AuthEnableRequest {}).await.unwrap();
}

fn put(key: &str) -> pb::PutRequest {
    pb::PutRequest { key: key.as_bytes().to_vec(), value: b"v".to_vec(), ..Default::default() }
}

fn get(key: &str) -> pb::RangeRequest {
    pb::RangeRequest { key: key.as_bytes().to_vec(), ..Default::default() }
}

#[tokio::test]
async fn a_client_certificate_cn_is_its_user_and_its_roles_apply() {
    let s = start(true).await;
    configure(&s).await;

    // root, by certificate alone: anything.
    let mut root = KvClient::new(s.as_cn("root").await);
    root.put(put("secret/a")).await.unwrap();

    // alice, by certificate alone, no Authenticate: her role's range only.
    let mut alice = KvClient::new(s.as_cn("alice").await);
    alice.put(put("config/a")).await.unwrap();
    alice.range(get("config/a")).await.unwrap();
    let err = alice.range(get("secret/a")).await.unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err:?}");
    let err = alice.put(put("secret/b")).await.unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err:?}");

    // Admin calls follow the CN too (#31): alice is not root.
    let mut m = MaintenanceClient::new(s.as_cn("alice").await);
    let err = m.hash_kv(pb::HashKvRequest { revision: 0 }).await.unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err:?}");
    MaintenanceClient::new(s.as_cn("root").await)
        .hash_kv(pb::HashKvRequest { revision: 0 })
        .await
        .unwrap();
    let mut a = AuthClient::new(s.as_cn("alice").await);
    let me = a.user_get(pb::AuthUserGetRequest { name: "alice".into() }).await.unwrap().into_inner();
    assert_eq!(me.roles, vec!["config"]);
    let err = a
        .user_grant_role(pb::AuthUserGrantRoleRequest { user: "alice".into(), role: "root".into() })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err:?}");
}

#[tokio::test]
async fn a_watch_is_scoped_by_the_certificate_user() {
    let s = start(true).await;
    configure(&s).await;
    let mut w = WatchClient::new(s.as_cn("alice").await);
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(pb::WatchRequest {
        request_union: Some(pb::watch_request::RequestUnion::CreateRequest(
            pb::WatchCreateRequest { key: b"secret/".to_vec(), range_end: b"secret0".to_vec(), ..Default::default() },
        )),
    })
    .await
    .unwrap();
    let mut stream = w.watch(tokio_stream::wrappers::ReceiverStream::new(rx)).await.unwrap().into_inner();
    let r = stream.next().await.unwrap().unwrap();
    assert!(r.created && r.canceled, "{r:?}");
    assert_eq!(r.cancel_reason, "etcdserver: permission denied");
}

#[tokio::test]
async fn a_cn_with_no_matching_user_is_refused() {
    let s = start(true).await;
    configure(&s).await;
    let mut kv = KvClient::new(s.as_cn("mallory").await);
    let err = kv.range(get("config/a")).await.unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err:?}");
    // A certificate with no Common Name identifies no one: etcd's
    // ErrUserEmpty (#105).
    let mut kv = KvClient::new(s.as_cn("").await);
    let err = kv.range(get("config/a")).await.unwrap_err();
    assert_eq!((err.code(), err.message()), (Code::InvalidArgument, "etcdserver: user name is empty"), "{err:?}");
}

#[tokio::test]
async fn a_token_wins_over_the_certificate() {
    let s = start(true).await;
    configure(&s).await;
    // Log in as root over alice's certificate: the token names root.
    let chan = s.as_cn("alice").await;
    let token = AuthClient::new(chan.clone())
        .authenticate(pb::AuthenticateRequest { name: "root".into(), password: "rootpw".into() })
        .await
        .unwrap()
        .into_inner()
        .token;
    let mut kv = KvClient::new(chan);
    let mut req = Request::new(get("secret/a"));
    req.metadata_mut().insert("token", MetadataValue::try_from(token.as_str()).unwrap());
    kv.range(req).await.unwrap();
    // An invalid token is refused, not replaced by the certificate.
    let mut req = Request::new(get("config/a"));
    req.metadata_mut().insert("token", MetadataValue::from_static("not-a-token"));
    let err = kv.range(req).await.unwrap_err();
    assert_eq!(
        (err.code(), err.message()),
        (Code::Unauthenticated, "etcdserver: invalid auth token"),
        "{err:?}"
    );
}

#[tokio::test]
async fn without_client_cert_auth_the_certificate_names_no_one() {
    let s = start(false).await;
    configure(&s).await;
    let mut kv = KvClient::new(s.as_cn("root").await);
    let err = kv.range(get("config/a")).await.unwrap_err();
    assert_eq!((err.code(), err.message()), (Code::InvalidArgument, "etcdserver: user name is empty"), "{err:?}");
}
