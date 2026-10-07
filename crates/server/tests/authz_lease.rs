//! fastetcd#47: the lease RPCs are authorized while auth is on.
//!
//! Before the fix any logged-in user could revoke any lease (deleting
//! keys it cannot write), renew it, list its keys, list every lease, and
//! attach keys to a lease whose other keys it cannot write. The rules
//! are etcd's (release-3.5 `checkLeasePuts`, `checkLeaseRenew`,
//! `checkLeaseTimeToLive`, `checkLeaseLeases`, `checkPutAuth`), each
//! over the keys attached to the lease now, root exempt.
//!
//! `alice` reads and writes `config/` only; `bob` reads `secret/` only.
//! Lease 100 carries `secret/x`, lease 200 carries `config/a`, lease 300
//! carries nothing.

mod common;
use common::start_test_server_full;

use fastetcd_proto::authpb;
use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::auth_client::AuthClient;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::lease_client::LeaseClient;
use pb::request_op::Request as Op;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

const SECRET: i64 = 100;
const CONFIG: i64 = 200;
const EMPTY: i64 = 300;

struct Env {
    h: common::TestServerHandles,
    root: String,
    alice: String,
    bob: String,
}

fn perm(perm_type: i32, key: &[u8], range_end: &[u8]) -> Option<authpb::Permission> {
    Some(authpb::Permission { perm_type, key: key.to_vec(), range_end: range_end.to_vec() })
}

async fn setup() -> Env {
    let h = start_test_server_full().await;
    let mut c = AuthClient::connect(h.endpoint.clone()).await.unwrap();
    for role in ["root", "config", "secret-reader"] {
        c.role_add(pb::AuthRoleAddRequest { name: role.into() }).await.unwrap();
    }
    // READWRITE = 2, READ = 0 (authpb.Permission.Type).
    c.role_grant_permission(pb::AuthRoleGrantPermissionRequest {
        name: "config".into(),
        perm: perm(2, b"config/", b"config0"),
    })
    .await
    .unwrap();
    c.role_grant_permission(pb::AuthRoleGrantPermissionRequest {
        name: "secret-reader".into(),
        perm: perm(0, b"secret/", b"secret0"),
    })
    .await
    .unwrap();
    for (name, role) in [("root", "root"), ("alice", "config"), ("bob", "secret-reader")] {
        c.user_add(pb::AuthUserAddRequest {
            name: name.into(),
            password: "pw".into(), // not a secret: test fixture
            ..Default::default()
        })
        .await
        .unwrap();
        c.user_grant_role(pb::AuthUserGrantRoleRequest { user: name.into(), role: role.into() })
            .await
            .unwrap();
    }
    // The leases and their keys, before auth is on.
    let mut l = LeaseClient::connect(h.endpoint.clone()).await.unwrap();
    for id in [SECRET, CONFIG, EMPTY] {
        l.lease_grant(pb::LeaseGrantRequest { ttl: 600, id }).await.unwrap();
    }
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    kv.put(put("secret/x", SECRET)).await.unwrap();
    kv.put(put("config/a", CONFIG)).await.unwrap();
    c.auth_enable(pb::AuthEnableRequest {}).await.unwrap();
    let root = login(&h.endpoint, "root").await;
    let alice = login(&h.endpoint, "alice").await;
    let bob = login(&h.endpoint, "bob").await;
    Env { h, root, alice, bob }
}

async fn login(endpoint: &str, name: &str) -> String {
    AuthClient::connect(endpoint.to_string())
        .await
        .unwrap()
        .authenticate(pb::AuthenticateRequest {
            name: name.into(),
            password: "pw".into(), // not a secret: test fixture
        })
        .await
        .unwrap()
        .into_inner()
        .token
}

fn put(key: &str, lease: i64) -> pb::PutRequest {
    pb::PutRequest { key: key.as_bytes().to_vec(), value: b"v".to_vec(), lease, ..Default::default() }
}

fn as_user<T>(req: T, token: &str) -> Request<T> {
    let mut r = Request::new(req);
    r.metadata_mut().insert("token", MetadataValue::try_from(token).unwrap());
    r
}

#[track_caller]
fn denied<T: std::fmt::Debug>(what: &str, r: Result<T, Status>) {
    match r {
        Err(s) => assert_eq!(s.code(), Code::PermissionDenied, "{what}: {s:?}"),
        Ok(v) => panic!("{what} should be denied, got {v:?}"),
    }
}

impl Env {
    async fn lease(&self) -> LeaseClient<Channel> {
        LeaseClient::connect(self.h.endpoint.clone()).await.unwrap()
    }
    async fn kv(&self) -> KvClient<Channel> {
        KvClient::connect(self.h.endpoint.clone()).await.unwrap()
    }
    async fn exists(&self, key: &str) -> bool {
        self.kv()
            .await
            .range(as_user(pb::RangeRequest { key: key.as_bytes().to_vec(), ..Default::default() }, &self.root))
            .await
            .unwrap()
            .into_inner()
            .count
            == 1
    }
    async fn keep_alive(&self, id: i64, token: &str) -> Result<pb::LeaseKeepAliveResponse, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.send(pb::LeaseKeepAliveRequest { id }).await.unwrap();
        let mut s = self
            .lease()
            .await
            .lease_keep_alive(as_user(tokio_stream::wrappers::ReceiverStream::new(rx), token))
            .await?
            .into_inner();
        s.message().await?.ok_or_else(|| Status::internal("stream ended with no answer"))
    }
    async fn ttl(&self, id: i64, keys: bool, token: &str) -> Result<pb::LeaseTimeToLiveResponse, Status> {
        self.lease()
            .await
            .lease_time_to_live(as_user(pb::LeaseTimeToLiveRequest { id, keys }, token))
            .await
            .map(|r| r.into_inner())
    }
}

#[tokio::test]
async fn revoke_needs_write_on_every_attached_key() {
    let e = setup().await;
    let mut l = e.lease().await;
    let revoke = |id| pb::LeaseRevokeRequest { id };
    denied("alice revokes the secret/ lease", l.lease_revoke(as_user(revoke(SECRET), &e.alice)).await);
    denied("bob (read only) revokes it", l.lease_revoke(as_user(revoke(SECRET), &e.bob)).await);
    assert!(e.exists("secret/x").await, "a denied revoke deleted nothing");

    l.lease_revoke(as_user(revoke(CONFIG), &e.alice)).await.expect("alice writes config/a");
    assert!(!e.exists("config/a").await);
    l.lease_revoke(as_user(revoke(EMPTY), &e.bob)).await.expect("no keys: a login is enough");
    l.lease_revoke(as_user(revoke(SECRET), &e.root)).await.expect("root");
    assert!(!e.exists("secret/x").await);
}

#[tokio::test]
async fn keep_alive_needs_write_on_every_attached_key() {
    let e = setup().await;
    denied("alice renews the secret/ lease", e.keep_alive(SECRET, &e.alice).await);
    denied("bob (read only) renews it", e.keep_alive(SECRET, &e.bob).await);
    assert_eq!(e.keep_alive(CONFIG, &e.alice).await.unwrap().ttl, 600);
    assert_eq!(e.keep_alive(EMPTY, &e.bob).await.unwrap().ttl, 600);
    assert_eq!(e.keep_alive(SECRET, &e.root).await.unwrap().ttl, 600);
}

#[tokio::test]
async fn time_to_live_with_keys_needs_read_on_every_attached_key() {
    let e = setup().await;
    // The TTL alone needs only a login.
    assert_eq!(e.ttl(SECRET, false, &e.alice).await.unwrap().granted_ttl, 600);
    denied("alice lists the secret/ lease's keys", e.ttl(SECRET, true, &e.alice).await);
    let r = e.ttl(SECRET, true, &e.bob).await.expect("bob reads secret/");
    assert_eq!(r.keys, vec![b"secret/x".to_vec()]);
    assert_eq!(e.ttl(CONFIG, true, &e.alice).await.unwrap().keys, vec![b"config/a".to_vec()]);
    assert_eq!(e.ttl(SECRET, true, &e.root).await.unwrap().keys.len(), 1);
}

#[tokio::test]
async fn leases_needs_read_on_every_key_of_every_lease() {
    let e = setup().await;
    let mut l = e.lease().await;
    denied("alice lists the leases", l.lease_leases(as_user(pb::LeaseLeasesRequest {}, &e.alice)).await);
    denied("bob lists the leases", l.lease_leases(as_user(pb::LeaseLeasesRequest {}, &e.bob)).await);
    let all = l.lease_leases(as_user(pb::LeaseLeasesRequest {}, &e.root)).await.unwrap().into_inner();
    assert_eq!(all.leases.len(), 3);
    // With only config/ keys left on leases, alice may list them.
    l.lease_revoke(as_user(pb::LeaseRevokeRequest { id: SECRET }, &e.root)).await.unwrap();
    let r = l.lease_leases(as_user(pb::LeaseLeasesRequest {}, &e.alice)).await.unwrap().into_inner();
    assert_eq!(r.leases.len(), 2);
}

#[tokio::test]
async fn a_put_naming_a_lease_needs_write_on_the_keys_already_on_it() {
    let e = setup().await;
    let mut kv = e.kv().await;
    denied(
        "alice attaches config/b to the secret/ lease",
        kv.put(as_user(put("config/b", SECRET), &e.alice)).await,
    );
    assert!(!e.exists("config/b").await);
    // In a txn too, in either branch (both are checked, as in etcd).
    let txn = |op| pb::TxnRequest {
        compare: vec![],
        success: vec![],
        failure: vec![pb::RequestOp { request: Some(Op::RequestPut(op)) }],
    };
    denied("the same put in a txn's other branch", kv.txn(as_user(txn(put("config/b", SECRET)), &e.alice)).await);
    kv.put(as_user(put("config/b", CONFIG), &e.alice)).await.expect("a lease with config/ keys");
    kv.put(as_user(put("config/c", EMPTY), &e.alice)).await.expect("a lease with no keys");
    kv.txn(as_user(txn(put("config/d", CONFIG)), &e.alice)).await.expect("txn, a config/ lease");
    kv.put(as_user(put("config/e", SECRET), &e.root)).await.expect("root");
}
