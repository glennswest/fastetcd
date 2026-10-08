//! Auth Phase 2: per-request token enforcement.
//!
//! Verifies the AuthInterceptor wired around the non-Auth services:
//! - With auth disabled (default), every RPC works without a token.
//! - With auth enabled, an RPC without a `token` metadata field
//!   returns Unauthenticated.
//! - After Authenticate, the same RPC with the token in metadata
//!   succeeds.

mod common;
use common::start_test_server_full;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::auth_client::AuthClient;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use tonic::metadata::MetadataValue;
use tonic::Request;

async fn enable_auth_with_user(endpoint: &str, name: &str, password: &str) {
    let mut c = AuthClient::connect(endpoint.to_string()).await.unwrap();
    // Add a root user (precondition for AuthEnable) plus the test
    // user. Both get the "root" role so they bypass per-key
    // permission checks — Phase-3 enforcement has its own tests.
    c.role_add(pb::AuthRoleAddRequest {
        name: "root".to_string(),
    })
    .await
    .unwrap();
    c.user_add(pb::AuthUserAddRequest {
        name: "root".to_string(),
        password: "rootpw".to_string(),
        ..Default::default()
    })
    .await
    .unwrap();
    c.user_grant_role(pb::AuthUserGrantRoleRequest {
        user: "root".to_string(),
        role: "root".to_string(),
    })
    .await
    .unwrap();
    c.user_add(pb::AuthUserAddRequest {
        name: name.to_string(),
        password: password.to_string(),
        ..Default::default()
    })
    .await
    .unwrap();
    c.user_grant_role(pb::AuthUserGrantRoleRequest {
        user: name.to_string(),
        role: "root".to_string(),
    })
    .await
    .unwrap();
    c.auth_enable(pb::AuthEnableRequest {}).await.unwrap();
}

#[tokio::test]
async fn put_without_token_rejected_when_auth_enabled() {
    let h = start_test_server_full().await;
    enable_auth_with_user(&h.endpoint, "alice", "alice-pw").await;

    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    let err = kv
        .put(pb::PutRequest {
            key: b"foo".to_vec(),
            value: b"bar".to_vec(),
            ..Default::default()
        })
        .await
        .expect_err("should be rejected without a token");
    // etcd's ErrUserEmpty (#105).
    assert_eq!((err.code(), err.message()), (tonic::Code::InvalidArgument, "etcdserver: user name is empty"));
}

#[tokio::test]
async fn put_with_valid_token_succeeds_when_auth_enabled() {
    let h = start_test_server_full().await;
    enable_auth_with_user(&h.endpoint, "bob", "bob-pw").await;

    let mut auth_client = AuthClient::connect(h.endpoint.clone()).await.unwrap();
    let resp = auth_client
        .authenticate(pb::AuthenticateRequest {
            name: "bob".to_string(),
            password: "bob-pw".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    let token = resp.token;
    assert!(!token.is_empty());

    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    let mut req = Request::new(pb::PutRequest {
        key: b"foo".to_vec(),
        value: b"bar".to_vec(),
        ..Default::default()
    });
    req.metadata_mut()
        .insert("token", MetadataValue::try_from(&token).unwrap());
    let put_resp = kv.put(req).await.unwrap().into_inner();
    assert_eq!(put_resp.header.unwrap().revision, 1);
}

#[tokio::test]
async fn invalid_token_rejected_when_auth_enabled() {
    let h = start_test_server_full().await;
    enable_auth_with_user(&h.endpoint, "carol", "carol-pw").await;

    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    let mut req = Request::new(pb::RangeRequest {
        key: b"any".to_vec(),
        ..Default::default()
    });
    req.metadata_mut()
        .insert("token", MetadataValue::from_static("this-is-not-a-real-token"));
    let err = kv.range(req).await.expect_err("invalid token rejected");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn auth_disable_lets_unauthenticated_requests_through_again() {
    let h = start_test_server_full().await;
    enable_auth_with_user(&h.endpoint, "dan", "dan-pw").await;

    let mut auth_client = AuthClient::connect(h.endpoint.clone()).await.unwrap();
    // AuthDisable needs root while auth is on (#31); dan holds the root
    // role. Once disabled, KV should work without a token.
    let token = auth_client
        .authenticate(pb::AuthenticateRequest { name: "dan".into(), password: "dan-pw".into() })
        .await
        .unwrap()
        .into_inner()
        .token;
    let mut req = Request::new(pb::AuthDisableRequest {});
    req.metadata_mut().insert("token", MetadataValue::try_from(token.as_str()).unwrap());
    auth_client.auth_disable(req).await.unwrap();

    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    kv.put(pb::PutRequest {
        key: b"after-disable".to_vec(),
        value: b"ok".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
}

/// fastetcd#46: a token expires `--auth-token-ttl` after its last use,
/// as etcd's simple tokens do. Used within the TTL it keeps working; idle
/// past it, it is refused with etcd's `invalid auth token` (on which
/// clients authenticate again), and a new login works.
#[tokio::test]
async fn a_token_expires_its_ttl_after_its_last_use() {
    use std::time::Duration;
    let h = start_test_server_full().await;
    h.state.sm.mvcc().auth_memory().set_token_ttl(Duration::from_secs(2));
    enable_auth_with_user(&h.endpoint, "alice", "alice-pw").await; // not a secret: test fixture
    let mut auth = AuthClient::connect(h.endpoint.clone()).await.unwrap();
    let login = || pb::AuthenticateRequest { name: "alice".into(), password: "alice-pw".into() }; // not a secret: test fixture
    let token = auth.authenticate(login()).await.unwrap().into_inner().token;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    let get = |token: &str| {
        let mut r = Request::new(pb::RangeRequest { key: b"k".to_vec(), ..Default::default() });
        r.metadata_mut().insert("token", MetadataValue::try_from(token).unwrap());
        r
    };
    // Used every second for five seconds: past the TTL, still good.
    for _ in 0..5 {
        kv.range(get(&token)).await.expect("a token in use keeps working");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    // Idle for longer than the TTL: gone.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let err = kv.range(get(&token)).await.expect_err("an idle token expires");
    assert_eq!(
        (err.code(), err.message()),
        (tonic::Code::Unauthenticated, "etcdserver: invalid auth token"),
        "{err:?}"
    );
    let fresh = auth.authenticate(login()).await.unwrap().into_inner().token;
    kv.range(get(&fresh)).await.expect("a new login works");
}
