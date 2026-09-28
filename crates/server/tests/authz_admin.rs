//! fastetcd#31: the admin RPCs are root-only while auth is on.
//!
//! Before the fix any authenticated user could grant itself the root
//! role, disable auth, stream a snapshot of the whole keyspace or change
//! membership. The rules are etcd's (release-3.5 `needAdminPermission`,
//! `authMaintenanceServer`, `checkMembershipOperationPermission`):
//! `alice` below holds one role on `config/` and no root.

mod common;
use common::start_test_server_full;

use fastetcd_proto::authpb;
use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::auth_client::AuthClient;
use fastetcd_proto::etcdserverpb::cluster_client::ClusterClient;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::maintenance_client::MaintenanceClient;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::{Code, Request, Status};

struct Env {
    h: common::TestServerHandles,
    root: String,
    alice: String,
}

async fn setup() -> Env {
    let h = start_test_server_full().await;
    let mut c = AuthClient::connect(h.endpoint.clone()).await.unwrap();
    for role in ["root", "config", "other"] {
        c.role_add(pb::AuthRoleAddRequest { name: role.into() }).await.unwrap();
    }
    for (name, pw) in [("root", "rootpw"), ("alice", "pw"), ("bob", "pw")] {
        c.user_add(pb::AuthUserAddRequest {
            name: name.into(),
            password: pw.into(),
            ..Default::default()
        })
        .await
        .unwrap();
    }
    c.user_grant_role(pb::AuthUserGrantRoleRequest { user: "root".into(), role: "root".into() })
        .await
        .unwrap();
    c.role_grant_permission(pb::AuthRoleGrantPermissionRequest {
        name: "config".into(),
        perm: Some(authpb::Permission {
            perm_type: 2,
            key: b"config/".to_vec(),
            range_end: b"config0".to_vec(),
        }),
    })
    .await
    .unwrap();
    c.user_grant_role(pb::AuthUserGrantRoleRequest { user: "alice".into(), role: "config".into() })
        .await
        .unwrap();
    c.auth_enable(pb::AuthEnableRequest {}).await.unwrap();
    let root = login(&h.endpoint, "root", "rootpw").await;
    let alice = login(&h.endpoint, "alice", "pw").await;
    Env { h, root, alice }
}

async fn login(endpoint: &str, name: &str, password: &str) -> String {
    AuthClient::connect(endpoint.to_string())
        .await
        .unwrap()
        .authenticate(pb::AuthenticateRequest { name: name.into(), password: password.into() })
        .await
        .unwrap()
        .into_inner()
        .token
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
        Ok(v) => panic!("{what} should be denied to a non-root user, got {v:?}"),
    }
}

impl Env {
    async fn auth(&self) -> AuthClient<tonic::transport::Channel> {
        AuthClient::connect(self.h.endpoint.clone()).await.unwrap()
    }
    async fn maint(&self) -> MaintenanceClient<tonic::transport::Channel> {
        MaintenanceClient::connect(self.h.endpoint.clone()).await.unwrap()
    }
    async fn cluster(&self) -> ClusterClient<tonic::transport::Channel> {
        ClusterClient::connect(self.h.endpoint.clone()).await.unwrap()
    }
    async fn roles_of(&self, user: &str) -> Vec<String> {
        self.auth()
            .await
            .user_get(as_user(pb::AuthUserGetRequest { name: user.into() }, &self.root))
            .await
            .unwrap()
            .into_inner()
            .roles
    }
}

#[tokio::test]
async fn a_non_root_user_cannot_make_itself_root_or_change_auth() {
    let e = setup().await;
    let t = &e.alice;
    let mut a = e.auth().await;
    denied(
        "UserGrantRole root to self",
        a.user_grant_role(as_user(
            pb::AuthUserGrantRoleRequest { user: "alice".into(), role: "root".into() },
            t,
        ))
        .await,
    );
    assert_eq!(e.roles_of("alice").await, vec!["config"]);
    denied("AuthDisable", a.auth_disable(as_user(pb::AuthDisableRequest {}, t)).await);
    denied("AuthEnable", a.auth_enable(as_user(pb::AuthEnableRequest {}, t)).await);
    denied("AuthStatus", a.auth_status(as_user(pb::AuthStatusRequest {}, t)).await);
    denied(
        "UserAdd",
        a.user_add(as_user(
            pb::AuthUserAddRequest { name: "eve".into(), password: "x".into(), ..Default::default() },
            t,
        ))
        .await,
    );
    denied("UserDelete", a.user_delete(as_user(pb::AuthUserDeleteRequest { name: "bob".into() }, t)).await);
    denied(
        "UserChangePassword of root",
        a.user_change_password(as_user(
            pb::AuthUserChangePasswordRequest {
                name: "root".into(),
                password: "mine".into(),
                ..Default::default()
            },
            t,
        ))
        .await,
    );
    denied(
        "UserRevokeRole",
        a.user_revoke_role(as_user(
            pb::AuthUserRevokeRoleRequest { name: "root".into(), role: "root".into() },
            t,
        ))
        .await,
    );
    denied("UserList", a.user_list(as_user(pb::AuthUserListRequest {}, t)).await);
    denied("RoleAdd", a.role_add(as_user(pb::AuthRoleAddRequest { name: "r".into() }, t)).await);
    denied("RoleDelete", a.role_delete(as_user(pb::AuthRoleDeleteRequest { role: "config".into() }, t)).await);
    denied(
        "RoleGrantPermission to own role",
        a.role_grant_permission(as_user(
            pb::AuthRoleGrantPermissionRequest {
                name: "config".into(),
                perm: Some(authpb::Permission { perm_type: 2, key: b"\0".to_vec(), range_end: b"\0".to_vec() }),
            },
            t,
        ))
        .await,
    );
    denied(
        "RoleRevokePermission",
        a.role_revoke_permission(as_user(
            pb::AuthRoleRevokePermissionRequest {
                role: "config".into(),
                key: b"config/".to_vec(),
                range_end: b"config0".to_vec(),
            },
            t,
        ))
        .await,
    );
    denied("RoleList", a.role_list(as_user(pb::AuthRoleListRequest {}, t)).await);
    denied("UserGet of another user", a.user_get(as_user(pb::AuthUserGetRequest { name: "bob".into() }, t)).await);
    denied("RoleGet of a role not held", a.role_get(as_user(pb::AuthRoleGetRequest { role: "other".into() }, t)).await);

    // Auth is still on, and nothing changed.
    let s = a.auth_status(as_user(pb::AuthStatusRequest {}, &e.root)).await.unwrap().into_inner();
    assert!(s.enabled);
    let users = a.user_list(as_user(pb::AuthUserListRequest {}, &e.root)).await.unwrap().into_inner().users;
    assert_eq!(users, vec!["alice", "bob", "root"]);
}

#[tokio::test]
async fn a_user_may_read_itself_and_the_roles_it_holds() {
    let e = setup().await;
    let mut a = e.auth().await;
    let me = a
        .user_get(as_user(pb::AuthUserGetRequest { name: "alice".into() }, &e.alice))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(me.roles, vec!["config"]);
    let role = a
        .role_get(as_user(pb::AuthRoleGetRequest { role: "config".into() }, &e.alice))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(role.perm.len(), 1);
    // etcd does not let a non-root user change even its own password.
    denied(
        "UserChangePassword of self",
        a.user_change_password(as_user(
            pb::AuthUserChangePasswordRequest {
                name: "alice".into(),
                password: "new".into(),
                ..Default::default()
            },
            &e.alice,
        ))
        .await,
    );
}

#[tokio::test]
async fn root_and_the_root_role_may_do_everything() {
    let e = setup().await;
    let mut a = e.auth().await;
    a.user_grant_role(as_user(
        pb::AuthUserGrantRoleRequest { user: "bob".into(), role: "root".into() },
        &e.root,
    ))
    .await
    .unwrap();
    // bob is not named root, but holds the root role.
    let bob = login(&e.h.endpoint, "bob", "pw").await;
    a.role_add(as_user(pb::AuthRoleAddRequest { name: "r".into() }, &bob)).await.unwrap();
    a.user_list(as_user(pb::AuthUserListRequest {}, &bob)).await.unwrap();
    let mut m = e.maint().await;
    m.hash_kv(as_user(pb::HashKvRequest { revision: 0 }, &bob)).await.unwrap();
    m.defragment(as_user(pb::DefragmentRequest {}, &e.root)).await.unwrap();
    a.auth_disable(as_user(pb::AuthDisableRequest {}, &bob)).await.unwrap();
}

#[tokio::test]
async fn without_a_token_admin_calls_are_unauthenticated() {
    let e = setup().await;
    let mut a = e.auth().await;
    let err = a.auth_disable(pb::AuthDisableRequest {}).await.unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated, "{err:?}");
    let err = a
        .user_grant_role(as_user(
            pb::AuthUserGrantRoleRequest { user: "alice".into(), role: "root".into() },
            "not-a-token",
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated, "{err:?}");
    // Authenticate itself needs no token.
    login(&e.h.endpoint, "alice", "pw").await;
}

#[tokio::test]
async fn maintenance_admin_calls_are_root_only() {
    let e = setup().await;
    let t = &e.alice;
    let mut m = e.maint().await;
    denied("Defragment", m.defragment(as_user(pb::DefragmentRequest {}, t)).await);
    denied("Hash", m.hash(as_user(pb::HashRequest {}, t)).await);
    denied("HashKV", m.hash_kv(as_user(pb::HashKvRequest { revision: 0 }, t)).await);
    denied("MoveLeader", m.move_leader(as_user(pb::MoveLeaderRequest { target_id: 1 }, t)).await);
    denied(
        "Downgrade",
        m.downgrade(as_user(pb::DowngradeRequest { action: 0, version: "3.5.0".into() }, t)).await,
    );
    denied(
        "Alarm disarm",
        m.alarm(as_user(
            pb::AlarmRequest {
                action: pb::alarm_request::AlarmAction::Deactivate as i32,
                member_id: 0,
                alarm: pb::AlarmType::None as i32,
            },
            t,
        ))
        .await,
    );
    // The snapshot is the whole keyspace, whatever the caller may read.
    let snap = match m.snapshot(as_user(pb::SnapshotRequest {}, t)).await {
        Ok(stream) => stream.into_inner().next().await.expect("an item").map(|_| ()),
        Err(s) => Err(s),
    };
    denied("Snapshot", snap);

    // Open to any logged-in user, as in etcd.
    m.status(as_user(pb::StatusRequest {}, t)).await.unwrap();
    m.alarm(as_user(
        pb::AlarmRequest {
            action: pb::alarm_request::AlarmAction::Get as i32,
            member_id: 0,
            alarm: pb::AlarmType::None as i32,
        },
        t,
    ))
    .await
    .unwrap();
    // ... and root may take the snapshot.
    let mut s = m.snapshot(as_user(pb::SnapshotRequest {}, &e.root)).await.unwrap().into_inner();
    s.next().await.unwrap().unwrap();
}

#[tokio::test]
async fn membership_changes_are_root_only_member_list_is_not() {
    let e = setup().await;
    let t = &e.alice;
    let mut c = e.cluster().await;
    denied(
        "MemberAdd",
        c.member_add(as_user(
            pb::MemberAddRequest { peer_ur_ls: vec!["http://127.0.0.1:1".into()], is_learner: true },
            t,
        ))
        .await,
    );
    denied("MemberRemove", c.member_remove(as_user(pb::MemberRemoveRequest { id: 1 }, t)).await);
    denied(
        "MemberUpdate",
        c.member_update(as_user(
            pb::MemberUpdateRequest { id: 1, peer_ur_ls: vec!["http://127.0.0.1:2".into()] },
            t,
        ))
        .await,
    );
    denied("MemberPromote", c.member_promote(as_user(pb::MemberPromoteRequest { id: 1 }, t)).await);
    let list = c
        .member_list(as_user(pb::MemberListRequest { linearizable: false }, t))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list.members.len(), 1);
}

/// etcd lets any authenticated user compact; so does fastetcd.
#[tokio::test]
async fn compact_is_open_to_any_logged_in_user_as_in_etcd() {
    let e = setup().await;
    let mut kv = KvClient::connect(e.h.endpoint.clone()).await.unwrap();
    let rev = kv
        .put(as_user(
            pb::PutRequest { key: b"config/a".to_vec(), value: b"v".to_vec(), ..Default::default() },
            &e.alice,
        ))
        .await
        .unwrap()
        .into_inner()
        .header
        .unwrap()
        .revision;
    kv.compact(as_user(pb::CompactionRequest { revision: rev, physical: false }, &e.alice))
        .await
        .unwrap();
}

/// With auth off, everything stays open, as in etcd.
#[tokio::test]
async fn with_auth_off_admin_calls_need_no_login() {
    let h = start_test_server_full().await;
    let mut a = AuthClient::connect(h.endpoint.clone()).await.unwrap();
    a.role_add(pb::AuthRoleAddRequest { name: "r".into() }).await.unwrap();
    a.auth_status(pb::AuthStatusRequest {}).await.unwrap();
    let mut m = MaintenanceClient::connect(h.endpoint.clone()).await.unwrap();
    m.hash_kv(pb::HashKvRequest { revision: 0 }).await.unwrap();
}
