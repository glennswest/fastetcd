//! fastetcd#22: `Txn` is authorized like `Range`/`Put`/`DeleteRange`.
//!
//! Before the fix `Txn` had no authorization at all, so any
//! authenticated user could read or write any key by wrapping the
//! operation in a transaction. Each test here gives the user a role
//! scoped to `config/` and checks that a txn reaching outside it —
//! through a success op, a failure op, a compare, a range or a nested
//! txn — is refused as a whole, with nothing applied.

mod common;
use common::start_test_server_full;

use fastetcd_proto::authpb;
use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::auth_client::AuthClient;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use pb::compare::{CompareResult, CompareTarget, TargetUnion};
use pb::request_op::Request as Op;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Request;

const READ: i32 = 0;
const WRITE: i32 = 1;
const READWRITE: i32 = 2;

/// Start a server with auth enabled: `root`/`rootpw` holds the root
/// role, and `alice`/`pw` holds one role granting `perm_type` on the
/// `config/` prefix. Returns the endpoint and tokens for both users.
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

fn put(key: &str) -> pb::RequestOp {
    pb::RequestOp {
        request: Some(Op::RequestPut(pb::PutRequest {
            key: key.as_bytes().to_vec(),
            value: b"v".to_vec(),
            ..Default::default()
        })),
    }
}

fn get(key: &str) -> pb::RequestOp {
    pb::RequestOp {
        request: Some(Op::RequestRange(pb::RangeRequest {
            key: key.as_bytes().to_vec(),
            ..Default::default()
        })),
    }
}

/// `version(key) == 0`, i.e. "the key does not exist".
fn absent(key: &str) -> pb::Compare {
    pb::Compare {
        result: CompareResult::Equal as i32,
        target: CompareTarget::Version as i32,
        key: key.as_bytes().to_vec(),
        range_end: Vec::new(),
        target_union: Some(TargetUnion::Version(0)),
    }
}

/// Seed a key as root, bypassing alice's scope.
async fn seed(kv: &mut KvClient<Channel>, root: &str, key: &str) {
    kv.put(with_token(
        pb::PutRequest { key: key.as_bytes().to_vec(), value: b"seed".to_vec(), ..Default::default() },
        root,
    ))
    .await
    .unwrap();
}

/// Read a key as root. `None` if it does not exist.
async fn value_of(kv: &mut KvClient<Channel>, root: &str, key: &str) -> Option<Vec<u8>> {
    kv.range(with_token(pb::RangeRequest { key: key.as_bytes().to_vec(), ..Default::default() }, root))
        .await
        .unwrap()
        .into_inner()
        .kvs
        .pop()
        .map(|kv| kv.value)
}

async fn txn(kv: &mut KvClient<Channel>, tok: &str, t: pb::TxnRequest) -> Result<pb::TxnResponse, tonic::Status> {
    kv.txn(with_token(t, tok)).await.map(|r| r.into_inner())
}

fn assert_denied(r: Result<pb::TxnResponse, tonic::Status>) {
    match r {
        Err(s) => assert_eq!(s.code(), tonic::Code::PermissionDenied, "{s:?}"),
        Ok(resp) => panic!("txn should have been denied, got {resp:?}"),
    }
}

#[tokio::test]
async fn txn_within_scope_is_allowed() {
    let (h, _root, alice) = setup(READWRITE).await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    let resp = txn(
        &mut kv,
        &alice,
        pb::TxnRequest {
            compare: vec![absent("config/a")],
            success: vec![put("config/a"), get("config/a")],
            failure: vec![get("config/b")],
        },
    )
    .await
    .expect("in-scope txn must be allowed");
    assert!(resp.succeeded);
}

#[tokio::test]
async fn txn_success_put_outside_scope_is_denied_and_not_applied() {
    let (h, root, alice) = setup(READWRITE).await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    assert_denied(
        txn(
            &mut kv,
            &alice,
            pb::TxnRequest {
                compare: vec![],
                // The in-scope put must not land either: the whole txn fails.
                success: vec![put("config/a"), put("secret/x")],
                failure: vec![],
            },
        )
        .await,
    );
    assert_eq!(value_of(&mut kv, &root, "secret/x").await, None);
    assert_eq!(value_of(&mut kv, &root, "config/a").await, None);
}

#[tokio::test]
async fn txn_failure_branch_is_checked_even_when_not_taken() {
    let (h, root, alice) = setup(READWRITE).await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    // The compare succeeds, so the forbidden failure op would never run;
    // it is refused anyway, as in etcd, because the branch is chosen
    // after authorization.
    assert_denied(
        txn(
            &mut kv,
            &alice,
            pb::TxnRequest {
                compare: vec![absent("config/a")],
                success: vec![put("config/a")],
                failure: vec![put("secret/x")],
            },
        )
        .await,
    );
    assert_eq!(value_of(&mut kv, &root, "config/a").await, None);
}

#[tokio::test]
async fn txn_range_outside_scope_is_denied() {
    let (h, root, alice) = setup(READWRITE).await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    seed(&mut kv, &root, "secret/x").await;
    assert_denied(
        txn(
            &mut kv,
            &alice,
            pb::TxnRequest { compare: vec![], success: vec![get("secret/x")], failure: vec![] },
        )
        .await,
    );
}

#[tokio::test]
async fn txn_delete_outside_scope_is_denied_and_not_applied() {
    let (h, root, alice) = setup(READWRITE).await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    seed(&mut kv, &root, "secret/x").await;
    assert_denied(
        txn(
            &mut kv,
            &alice,
            pb::TxnRequest {
                compare: vec![],
                success: vec![pb::RequestOp {
                    request: Some(Op::RequestDeleteRange(pb::DeleteRangeRequest {
                        key: b"secret/".to_vec(),
                        range_end: b"secret0".to_vec(),
                        ..Default::default()
                    })),
                }],
                failure: vec![],
            },
        )
        .await,
    );
    assert_eq!(value_of(&mut kv, &root, "secret/x").await, Some(b"seed".to_vec()));
}

#[tokio::test]
async fn txn_compare_outside_scope_is_denied() {
    // A compare is a read: it leaks whether a key exists, and its
    // value/revision one bit at a time.
    let (h, root, alice) = setup(READWRITE).await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    seed(&mut kv, &root, "secret/x").await;
    assert_denied(
        txn(
            &mut kv,
            &alice,
            pb::TxnRequest { compare: vec![absent("secret/x")], success: vec![], failure: vec![] },
        )
        .await,
    );
}

#[tokio::test]
async fn txn_compare_needs_read_not_just_write() {
    let (h, _root, alice) = setup(WRITE).await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    assert_denied(
        txn(
            &mut kv,
            &alice,
            pb::TxnRequest {
                compare: vec![absent("config/a")],
                success: vec![put("config/a")],
                failure: vec![],
            },
        )
        .await,
    );
    // The same write without a compare is within a write-only role.
    txn(&mut kv, &alice, pb::TxnRequest { compare: vec![], success: vec![put("config/a")], failure: vec![] })
        .await
        .expect("write-only role may put in scope");
}

#[tokio::test]
async fn txn_put_in_read_only_scope_is_denied() {
    let (h, root, alice) = setup(READ).await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    assert_denied(
        txn(
            &mut kv,
            &alice,
            pb::TxnRequest { compare: vec![], success: vec![put("config/a")], failure: vec![] },
        )
        .await,
    );
    assert_eq!(value_of(&mut kv, &root, "config/a").await, None);
}

#[tokio::test]
async fn nested_txn_ops_are_authorized() {
    let (h, root, alice) = setup(READWRITE).await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    let nested = pb::RequestOp {
        request: Some(Op::RequestTxn(pb::TxnRequest {
            compare: vec![],
            success: vec![put("secret/x")],
            failure: vec![],
        })),
    };
    assert_denied(
        txn(&mut kv, &alice, pb::TxnRequest { compare: vec![], success: vec![nested], failure: vec![] })
            .await,
    );
    assert_eq!(value_of(&mut kv, &root, "secret/x").await, None);
}

#[tokio::test]
async fn prev_kv_needs_read_on_put_and_delete() {
    let (h, _root, alice) = setup(WRITE).await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();

    // Standalone put: plain write is fine; asking for the old value is a read.
    kv.put(with_token(
        pb::PutRequest { key: b"config/a".to_vec(), value: b"1".to_vec(), ..Default::default() },
        &alice,
    ))
    .await
    .expect("write-only role may put in scope");
    let err = kv
        .put(with_token(
            pb::PutRequest {
                key: b"config/a".to_vec(),
                value: b"2".to_vec(),
                prev_kv: true,
                ..Default::default()
            },
            &alice,
        ))
        .await
        .expect_err("prev_kv on a write-only role must be denied");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);

    let err = kv
        .delete_range(with_token(
            pb::DeleteRangeRequest { key: b"config/a".to_vec(), prev_kv: true, ..Default::default() },
            &alice,
        ))
        .await
        .expect_err("prev_kv delete on a write-only role must be denied");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);

    // Same rule inside a txn.
    assert_denied(
        txn(
            &mut kv,
            &alice,
            pb::TxnRequest {
                compare: vec![],
                success: vec![pb::RequestOp {
                    request: Some(Op::RequestPut(pb::PutRequest {
                        key: b"config/a".to_vec(),
                        value: b"3".to_vec(),
                        prev_kv: true,
                        ..Default::default()
                    })),
                }],
                failure: vec![],
            },
        )
        .await,
    );
}
