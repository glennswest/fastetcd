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
    // Add root + the user we want.
    c.user_add(pb::AuthUserAddRequest {
        name: "root".to_string(),
        password: "rootpw".to_string(),
        ..Default::default()
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
        .err()
        .expect("should be rejected without a token");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
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
    let err = kv.range(req).await.err().expect("invalid token rejected");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn auth_disable_lets_unauthenticated_requests_through_again() {
    let h = start_test_server_full().await;
    enable_auth_with_user(&h.endpoint, "dan", "dan-pw").await;

    let mut auth_client = AuthClient::connect(h.endpoint.clone()).await.unwrap();
    // While disabled, KV should work without a token.
    auth_client
        .auth_disable(pb::AuthDisableRequest {})
        .await
        .unwrap();

    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    kv.put(pb::PutRequest {
        key: b"after-disable".to_vec(),
        value: b"ok".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
}
