//! fastetcd#33: `Watch` is authorized like `Range`.
//!
//! Before the fix a watch create had no permission check, so any
//! authenticated user could watch any key or range (with a past
//! `start_revision` and `prev_kv`) and receive values RBAC denies to
//! `Range`. Now each create needs read on `[key, range_end)`; a denied
//! create is answered as etcd answers it (`created` + `canceled`,
//! `watch_id` -1, `etcdserver: permission denied`), nothing is replayed
//! or delivered, and the stream stays usable.
//!
//! `alice` holds one role granting `perm_type` on the `config/` prefix;
//! `root` holds the root role.

mod common;
use common::start_test_server_full;

use std::time::Duration;

use fastetcd_proto::authpb;
use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::auth_client::AuthClient;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::watch_client::WatchClient;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::{Request, Streaming};

const READ: i32 = 0;
const WRITE: i32 = 1;
const DENIED: &str = "etcdserver: permission denied";

async fn setup(perm_type: i32) -> (common::TestServerHandles, String, String) {
    let h = start_test_server_full().await;
    let mut c = AuthClient::connect(h.endpoint.clone()).await.unwrap();
    c.role_add(pb::AuthRoleAddRequest { name: "root".into() }).await.unwrap();
    c.user_add(pb::AuthUserAddRequest {
        name: "root".into(),
        password: "rootpw".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    c.user_grant_role(pb::AuthUserGrantRoleRequest { user: "root".into(), role: "root".into() })
        .await
        .unwrap();
    c.role_add(pb::AuthRoleAddRequest { name: "config".into() }).await.unwrap();
    c.role_grant_permission(pb::AuthRoleGrantPermissionRequest {
        name: "config".into(),
        perm: Some(authpb::Permission {
            perm_type,
            key: b"config/".to_vec(),
            range_end: b"config0".to_vec(),
        }),
    })
    .await
    .unwrap();
    c.user_add(pb::AuthUserAddRequest {
        name: "alice".into(),
        password: "pw".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    c.user_grant_role(pb::AuthUserGrantRoleRequest { user: "alice".into(), role: "config".into() })
        .await
        .unwrap();
    c.auth_enable(pb::AuthEnableRequest {}).await.unwrap();

    let root = token(&h.endpoint, "root", "rootpw").await;
    let alice = token(&h.endpoint, "alice", "pw").await;
    (h, root, alice)
}

async fn token(endpoint: &str, name: &str, password: &str) -> String {
    let mut c = AuthClient::connect(endpoint.to_string()).await.unwrap();
    c.authenticate(pb::AuthenticateRequest { name: name.into(), password: password.into() })
        .await
        .unwrap()
        .into_inner()
        .token
}

fn with_token<T>(req: T, token: &str) -> Request<T> {
    let mut r = Request::new(req);
    r.metadata_mut().insert("token", MetadataValue::try_from(token).unwrap());
    r
}

async fn put(endpoint: &str, token: &str, key: &str, value: &str) -> i64 {
    let mut kv = KvClient::connect(endpoint.to_string()).await.unwrap();
    kv.put(with_token(
        pb::PutRequest {
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
            ..Default::default()
        },
        token,
    ))
    .await
    .unwrap()
    .into_inner()
    .header
    .unwrap()
    .revision
}

fn create(key: &str, range_end: &str, start_revision: i64) -> pb::WatchRequest {
    pb::WatchRequest {
        request_union: Some(pb::watch_request::RequestUnion::CreateRequest(
            pb::WatchCreateRequest {
                key: key.as_bytes().to_vec(),
                range_end: range_end.as_bytes().to_vec(),
                start_revision,
                prev_kv: true,
                ..Default::default()
            },
        )),
    }
}

/// Open a watch stream as `token`; returns the request sender and the
/// response stream.
async fn open(
    endpoint: &str,
    token: &str,
) -> (mpsc::Sender<pb::WatchRequest>, Streaming<pb::WatchResponse>) {
    let mut wc = WatchClient::connect(endpoint.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel::<pb::WatchRequest>(8);
    let stream = wc
        .watch(with_token(ReceiverStream::new(rx), token))
        .await
        .unwrap()
        .into_inner();
    (tx, stream)
}

async fn next(stream: &mut Streaming<pb::WatchResponse>) -> pb::WatchResponse {
    tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("a watch response within 5s")
        .expect("stream still open")
        .expect("no stream error")
}

fn assert_refused(r: &pb::WatchResponse) {
    assert!(r.created && r.canceled, "create must be refused: {r:?}");
    assert_eq!(r.watch_id, -1, "{r:?}");
    assert_eq!(r.cancel_reason, DENIED, "{r:?}");
    assert!(r.events.is_empty(), "no events on a refused create: {r:?}");
}

/// Nothing more arrives on the stream for a while.
async fn assert_quiet(stream: &mut Streaming<pb::WatchResponse>) {
    if let Ok(msg) = tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
        panic!("nothing should arrive, got {msg:?}");
    }
}

#[tokio::test]
async fn a_watch_on_a_key_the_user_cannot_read_is_refused_and_delivers_nothing() {
    let (h, root, alice) = setup(READ).await;
    // History alice must not see, reachable only through a past start
    // revision.
    let first = put(&h.endpoint, &root, "secret/a", "s1").await;
    put(&h.endpoint, &root, "secret/a", "s2").await;

    let (tx, mut stream) = open(&h.endpoint, &alice).await;
    tx.send(create("secret/a", "", first)).await.unwrap();
    assert_refused(&next(&mut stream).await);

    put(&h.endpoint, &root, "secret/a", "s3").await;
    assert_quiet(&mut stream).await;
}

#[tokio::test]
async fn a_range_or_prefix_reaching_outside_scope_is_refused() {
    let (h, root, alice) = setup(READ).await;
    let (tx, mut stream) = open(&h.endpoint, &alice).await;

    // A prefix that contains config/ but is wider than it.
    tx.send(create("conf", "cong", 0)).await.unwrap();
    assert_refused(&next(&mut stream).await);
    // Everything from config/ upward (range_end "\0").
    tx.send(create("config/", "\0", 0)).await.unwrap();
    assert_refused(&next(&mut stream).await);
    // The whole keyspace.
    tx.send(create("\0", "\0", 0)).await.unwrap();
    assert_refused(&next(&mut stream).await);

    put(&h.endpoint, &root, "secret/x", "v").await;
    put(&h.endpoint, &root, "config/x", "v").await;
    assert_quiet(&mut stream).await;
}

#[tokio::test]
async fn a_watch_within_scope_is_allowed_and_delivers() {
    let (h, root, alice) = setup(READ).await;
    let first = put(&h.endpoint, &root, "config/a", "v1").await;

    let (tx, mut stream) = open(&h.endpoint, &alice).await;
    tx.send(create("config/", "config0", first)).await.unwrap();
    let ack = next(&mut stream).await;
    assert!(ack.created && !ack.canceled, "{ack:?}");
    assert!(ack.watch_id >= 0, "{ack:?}");

    // History replay from `first`.
    let hist = next(&mut stream).await;
    assert_eq!(hist.watch_id, ack.watch_id);
    assert_eq!(hist.events[0].kv.as_ref().unwrap().value, b"v1");

    put(&h.endpoint, &root, "config/a", "v2").await;
    let live = next(&mut stream).await;
    assert_eq!(live.watch_id, ack.watch_id);
    let e = &live.events[0];
    assert_eq!(e.kv.as_ref().unwrap().value, b"v2");
    assert_eq!(e.prev_kv.as_ref().unwrap().value, b"v1");
}

#[tokio::test]
async fn a_refused_create_leaves_the_stream_usable() {
    let (h, root, alice) = setup(READ).await;
    let (tx, mut stream) = open(&h.endpoint, &alice).await;

    tx.send(create("secret/", "secret0", 0)).await.unwrap();
    assert_refused(&next(&mut stream).await);

    tx.send(create("config/a", "", 0)).await.unwrap();
    let ack = next(&mut stream).await;
    assert!(ack.created && !ack.canceled, "{ack:?}");

    put(&h.endpoint, &root, "secret/a", "hidden").await;
    put(&h.endpoint, &root, "config/a", "shown").await;
    let live = next(&mut stream).await;
    assert_eq!(live.watch_id, ack.watch_id);
    assert_eq!(live.events.len(), 1);
    assert_eq!(live.events[0].kv.as_ref().unwrap().key, b"config/a");
    assert_quiet(&mut stream).await;
}

#[tokio::test]
async fn a_watch_needs_read_not_just_write() {
    let (h, _root, alice) = setup(WRITE).await;
    let (tx, mut stream) = open(&h.endpoint, &alice).await;
    tx.send(create("config/a", "", 0)).await.unwrap();
    assert_refused(&next(&mut stream).await);
}

#[tokio::test]
async fn root_may_watch_anything() {
    let (h, root, _alice) = setup(READ).await;
    let (tx, mut stream) = open(&h.endpoint, &root).await;
    tx.send(create("\0", "\0", 0)).await.unwrap();
    let ack = next(&mut stream).await;
    assert!(ack.created && !ack.canceled, "{ack:?}");
    put(&h.endpoint, &root, "secret/a", "v").await;
    assert_eq!(next(&mut stream).await.events.len(), 1);
}

#[tokio::test]
async fn with_auth_disabled_any_watch_is_allowed() {
    let h = start_test_server_full().await;
    let mut wc = WatchClient::connect(h.endpoint.clone()).await.unwrap();
    let (tx, rx) = mpsc::channel::<pb::WatchRequest>(8);
    let mut stream = wc.watch(ReceiverStream::new(rx)).await.unwrap().into_inner();
    tx.send(create("secret/", "secret0", 0)).await.unwrap();
    let ack = next(&mut stream).await;
    assert!(ack.created && !ack.canceled, "{ack:?}");
}

/// Wire compatibility: the third-party `etcd-client` crate, logged in
/// as alice, sees the refusal as etcd would send it.
#[tokio::test]
async fn etcd_client_sees_the_refusal() {
    let (h, root, _alice) = setup(READ).await;
    let opts = etcd_client::ConnectOptions::new().with_user("alice", "pw");
    let mut c = etcd_client::Client::connect([h.endpoint.as_str()], Some(opts))
        .await
        .unwrap();
    let mut stream = c
        .watch("secret/", Some(etcd_client::WatchOptions::new().with_prefix()))
        .await
        .unwrap();
    let resp = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .expect("a watch response within 5s")
        .unwrap()
        .expect("stream still open");
    assert!(resp.created() && resp.canceled(), "{resp:?}");
    assert_eq!(resp.watch_id(), -1);
    assert_eq!(resp.cancel_reason(), DENIED);

    put(&h.endpoint, &root, "secret/a", "v").await;
    if let Ok(Ok(Some(msg))) =
        tokio::time::timeout(Duration::from_millis(500), stream.message()).await
    {
        panic!("nothing should arrive, got {msg:?}");
    }
}
