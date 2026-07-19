//! Auth Phase 3: per-key permission enforcement.

mod common;
use common::start_test_server_full;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::auth_client::AuthClient;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use tonic::metadata::MetadataValue;
use tonic::Request;

async fn setup(endpoint: &str) -> AuthClient<tonic::transport::Channel> {
    let mut c = AuthClient::connect(endpoint.to_string()).await.unwrap();
    // Root user + role so AuthEnable can succeed and admin ops have
    // somewhere to land. Don't grant root to the test user; we want
    // per-key enforcement to fire.
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
    c
}

async fn authenticate(endpoint: &str, name: &str, password: &str) -> String {
    let mut c = AuthClient::connect(endpoint.to_string()).await.unwrap();
    c.authenticate(pb::AuthenticateRequest {
        name: name.to_string(),
        password: password.to_string(),
    })
    .await
    .unwrap()
    .into_inner()
    .token
}

fn with_token<T>(req: T, token: &str) -> Request<T> {
    let mut r = Request::new(req);
    r.metadata_mut()
        .insert("token", MetadataValue::try_from(token).unwrap());
    r
}

#[tokio::test]
async fn user_without_perm_is_denied_write() {
    let h = start_test_server_full().await;
    let mut auth = setup(&h.endpoint).await;
    auth.user_add(pb::AuthUserAddRequest {
        name: "limited".to_string(),
        password: "pw".to_string(),
        ..Default::default()
    })
    .await
    .unwrap();
    auth.auth_enable(pb::AuthEnableRequest {}).await.unwrap();

    let tok = authenticate(&h.endpoint, "limited", "pw").await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    let err = kv
        .put(with_token(
            pb::PutRequest {
                key: b"forbidden".to_vec(),
                value: b"v".to_vec(),
                ..Default::default()
            },
            &tok,
        ))
        .await
        .expect_err("should be denied");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn user_with_matching_perm_is_allowed() {
    let h = start_test_server_full().await;
    let mut auth = setup(&h.endpoint).await;
    auth.role_add(pb::AuthRoleAddRequest {
        name: "config-writers".to_string(),
    })
    .await
    .unwrap();
    auth.role_grant_permission(pb::AuthRoleGrantPermissionRequest {
        name: "config-writers".to_string(),
        perm: Some(fastetcd_proto::authpb::Permission {
            perm_type: 1, // WRITE
            key: b"config/".to_vec(),
            range_end: b"config0".to_vec(), // exclusive upper for the "config/" prefix
        }),
    })
    .await
    .unwrap();
    auth.user_add(pb::AuthUserAddRequest {
        name: "writer".to_string(),
        password: "pw".to_string(),
        ..Default::default()
    })
    .await
    .unwrap();
    auth.user_grant_role(pb::AuthUserGrantRoleRequest {
        user: "writer".to_string(),
        role: "config-writers".to_string(),
    })
    .await
    .unwrap();
    auth.auth_enable(pb::AuthEnableRequest {}).await.unwrap();

    let tok = authenticate(&h.endpoint, "writer", "pw").await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();

    // Inside the permitted range — succeeds.
    kv.put(with_token(
        pb::PutRequest {
            key: b"config/app".to_vec(),
            value: b"hello".to_vec(),
            ..Default::default()
        },
        &tok,
    ))
    .await
    .unwrap();

    // Outside the permitted range — denied.
    let err = kv
        .put(with_token(
            pb::PutRequest {
                key: b"other/key".to_vec(),
                value: b"v".to_vec(),
                ..Default::default()
            },
            &tok,
        ))
        .await
        .expect_err("out-of-range should be denied");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn read_perm_does_not_allow_write() {
    let h = start_test_server_full().await;
    let mut auth = setup(&h.endpoint).await;
    auth.role_add(pb::AuthRoleAddRequest {
        name: "config-readers".to_string(),
    })
    .await
    .unwrap();
    auth.role_grant_permission(pb::AuthRoleGrantPermissionRequest {
        name: "config-readers".to_string(),
        perm: Some(fastetcd_proto::authpb::Permission {
            perm_type: 0, // READ
            key: b"k".to_vec(),
            range_end: Vec::new(),
        }),
    })
    .await
    .unwrap();
    auth.user_add(pb::AuthUserAddRequest {
        name: "reader".to_string(),
        password: "pw".to_string(),
        ..Default::default()
    })
    .await
    .unwrap();
    auth.user_grant_role(pb::AuthUserGrantRoleRequest {
        user: "reader".to_string(),
        role: "config-readers".to_string(),
    })
    .await
    .unwrap();
    auth.auth_enable(pb::AuthEnableRequest {}).await.unwrap();

    let tok = authenticate(&h.endpoint, "reader", "pw").await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();

    // Range allowed.
    kv.range(with_token(
        pb::RangeRequest {
            key: b"k".to_vec(),
            ..Default::default()
        },
        &tok,
    ))
    .await
    .unwrap();

    // Put denied (read-only perm).
    let err = kv
        .put(with_token(
            pb::PutRequest {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                ..Default::default()
            },
            &tok,
        ))
        .await
        .expect_err("read-only role should not write");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}
