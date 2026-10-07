//! Auth service gRPC tests.

mod common;
use common::start_test_server_full;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::auth_client::AuthClient;

#[tokio::test]
async fn user_add_get_delete() {
    let h = start_test_server_full().await;
    let mut c = AuthClient::connect(h.endpoint.clone()).await.unwrap();
    c.user_add(pb::AuthUserAddRequest {
        name: "alice".to_string(),
        password: "wonderland".to_string(), // not a secret: test fixture
        ..Default::default()
    })
    .await
    .unwrap();
    let got = c
        .user_get(pb::AuthUserGetRequest {
            name: "alice".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(got.roles.is_empty());
    let list = c
        .user_list(pb::AuthUserListRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list.users, vec!["alice".to_string()]);
    c.user_delete(pb::AuthUserDeleteRequest {
        name: "alice".to_string(),
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn role_add_grant_permission_get() {
    let h = start_test_server_full().await;
    let mut c = AuthClient::connect(h.endpoint.clone()).await.unwrap();
    c.role_add(pb::AuthRoleAddRequest {
        name: "readers".to_string(),
    })
    .await
    .unwrap();
    c.role_grant_permission(pb::AuthRoleGrantPermissionRequest {
        name: "readers".to_string(),
        perm: Some(fastetcd_proto::authpb::Permission {
            perm_type: 0, // READ
            key: b"/foo".to_vec(),
            range_end: b"/fop".to_vec(),
        }),
    })
    .await
    .unwrap();
    let got = c
        .role_get(pb::AuthRoleGetRequest {
            role: "readers".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.perm.len(), 1);
    assert_eq!(got.perm[0].perm_type, 0);
    assert_eq!(got.perm[0].key, b"/foo");
}

#[tokio::test]
async fn user_grant_role_then_get_lists_it() {
    let h = start_test_server_full().await;
    let mut c = AuthClient::connect(h.endpoint.clone()).await.unwrap();
    c.user_add(pb::AuthUserAddRequest {
        name: "bob".to_string(),
        password: "p4ss".to_string(), // not a secret: test fixture
        ..Default::default()
    })
    .await
    .unwrap();
    c.role_add(pb::AuthRoleAddRequest {
        name: "ops".to_string(),
    })
    .await
    .unwrap();
    c.user_grant_role(pb::AuthUserGrantRoleRequest {
        user: "bob".to_string(),
        role: "ops".to_string(),
    })
    .await
    .unwrap();
    let got = c
        .user_get(pb::AuthUserGetRequest {
            name: "bob".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.roles, vec!["ops".to_string()]);
}

/// Authenticate needs auth on, as in etcd: add root and enable it.
async fn enable_with_root(c: &mut AuthClient<tonic::transport::Channel>) {
    c.user_add(pb::AuthUserAddRequest {
        name: "root".to_string(),
        password: "rootpw".to_string(), // not a secret: test fixture
        ..Default::default()
    })
    .await
    .unwrap();
    c.auth_enable(pb::AuthEnableRequest {}).await.unwrap();
}

#[tokio::test]
async fn authenticate_returns_a_token_for_valid_credentials() {
    let h = start_test_server_full().await;
    let mut c = AuthClient::connect(h.endpoint.clone()).await.unwrap();
    c.user_add(pb::AuthUserAddRequest {
        name: "carol".to_string(),
        password: "open-sesame".to_string(), // not a secret: test fixture
        ..Default::default()
    })
    .await
    .unwrap();
    enable_with_root(&mut c).await;
    let resp = c
        .authenticate(pb::AuthenticateRequest {
            name: "carol".to_string(),
            password: "open-sesame".to_string(), // not a secret: test fixture
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!resp.token.is_empty());
    assert_eq!(resp.token.len(), 64); // hex of 32 random bytes
}

/// etcd's answers, which clientv3 recognises by their text (#105).
#[tokio::test]
async fn authenticate_answers_as_etcd_does() {
    let h = start_test_server_full().await;
    let mut c = AuthClient::connect(h.endpoint.clone()).await.unwrap();
    c.user_add(pb::AuthUserAddRequest {
        name: "dan".to_string(),
        password: "right-pass".to_string(), // not a secret: test fixture
        ..Default::default()
    })
    .await
    .unwrap();
    let login = |name: &str, password: &str| pb::AuthenticateRequest {
        name: name.to_string(),
        password: password.to_string(),
    };
    let err = c.authenticate(login("dan", "right-pass")).await.expect_err("auth is off"); // not a secret: test fixture
    assert_eq!(
        (err.code(), err.message()),
        (tonic::Code::FailedPrecondition, "etcdserver: authentication is not enabled")
    );
    enable_with_root(&mut c).await;
    let failed = (
        tonic::Code::InvalidArgument,
        "etcdserver: authentication failed, invalid user ID or password",
    );
    let err = c.authenticate(login("dan", "wrong-pass")).await.expect_err("wrong password"); // not a secret: test fixture
    assert_eq!((err.code(), err.message()), failed);
    let err = c.authenticate(login("nobody", "x")).await.expect_err("no such user"); // not a secret: test fixture
    assert_eq!((err.code(), err.message()), failed);
}

#[tokio::test]
async fn auth_enable_requires_root_user() {
    let h = start_test_server_full().await;
    let mut c = AuthClient::connect(h.endpoint.clone()).await.unwrap();
    let err = c
        .auth_enable(pb::AuthEnableRequest {})
        .await
        .expect_err("AuthEnable should fail without root user");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    // Add root and try again — should succeed.
    c.user_add(pb::AuthUserAddRequest {
        name: "root".to_string(),
        password: "rootpw".to_string(), // not a secret: test fixture
        ..Default::default()
    })
    .await
    .unwrap();
    c.auth_enable(pb::AuthEnableRequest {}).await.unwrap();

    // With auth on, AuthStatus needs root, as in etcd (#31).
    let token = c
        .authenticate(pb::AuthenticateRequest { name: "root".into(), password: "rootpw".into() }) // not a secret: test fixture
        .await
        .unwrap()
        .into_inner()
        .token;
    let mut req = tonic::Request::new(pb::AuthStatusRequest {});
    req.metadata_mut()
        .insert("token", tonic::metadata::MetadataValue::try_from(token.as_str()).unwrap());
    let status = c.auth_status(req).await.unwrap().into_inner();
    assert!(status.enabled);
}
