//! fastetcd#64: etcd's Lock and Election services, as etcd's v3lock and
//! v3election servers run clientv3's concurrency Mutex and Election.

mod common;
use common::start_test_server_full;

use std::time::Duration;

use fastetcd_proto::authpb;
use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::auth_client::AuthClient;
use fastetcd_proto::etcdserverpb::lease_client::LeaseClient;
use fastetcd_proto::v3electionpb as epb;
use fastetcd_proto::v3electionpb::election_client::ElectionClient;
use fastetcd_proto::v3lockpb as lpb;
use fastetcd_proto::v3lockpb::lock_client::LockClient;
use tonic::metadata::MetadataValue;
use tonic::{Code, Request};

async fn grant(endpoint: &str, ttl: i64) -> i64 {
    LeaseClient::connect(endpoint.to_string())
        .await
        .unwrap()
        .lease_grant(pb::LeaseGrantRequest { ttl, id: 0 })
        .await
        .unwrap()
        .into_inner()
        .id
}

fn lock_req(name: &str, lease: i64) -> lpb::LockRequest {
    lpb::LockRequest { name: name.as_bytes().to_vec(), lease }
}

/// A second Lock waits for the first to unlock, then holds the lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_locker_waits_for_the_first_to_unlock() {
    let h = start_test_server_full().await;
    let (la, lb) = (grant(&h.endpoint, 60).await, grant(&h.endpoint, 60).await);
    let mut c = LockClient::connect(h.endpoint.clone()).await.unwrap();
    let a = c.lock(lock_req("lk", la)).await.unwrap().into_inner();
    assert_eq!(a.key, format!("lk/{la:x}").into_bytes());
    let waiting = {
        let mut c = c.clone();
        tokio::spawn(async move { c.lock(lock_req("lk", lb)).await.map(|r| r.into_inner()) })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!waiting.is_finished(), "b holds the lock while a does");
    c.unlock(lpb::UnlockRequest { key: a.key }).await.unwrap();
    let b = tokio::time::timeout(Duration::from_secs(10), waiting).await.expect("b gets the lock").unwrap().unwrap();
    assert_eq!(b.key, format!("lk/{lb:x}").into_bytes());
}

/// The holder's lease going (expiry or revoke) frees the lock, as its key
/// goes with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_revoked_holder_frees_the_lock() {
    let h = start_test_server_full().await;
    let (la, lb) = (grant(&h.endpoint, 60).await, grant(&h.endpoint, 60).await);
    let mut c = LockClient::connect(h.endpoint.clone()).await.unwrap();
    c.lock(lock_req("rv", la)).await.unwrap();
    let waiting = {
        let mut c = c.clone();
        tokio::spawn(async move { c.lock(lock_req("rv", lb)).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!waiting.is_finished());
    LeaseClient::connect(h.endpoint.clone()).await.unwrap().lease_revoke(pb::LeaseRevokeRequest { id: la }).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), waiting).await.expect("b gets the lock").unwrap().unwrap();
}

/// Lease 0: a session lease of 60 s is granted for the lock, as
/// concurrency.NewSession does.
#[tokio::test]
async fn lock_with_no_lease_gets_a_session_lease() {
    let h = start_test_server_full().await;
    let mut c = LockClient::connect(h.endpoint.clone()).await.unwrap();
    let r = c.lock(lock_req("s", 0)).await.unwrap().into_inner();
    let hex = String::from_utf8(r.key.strip_prefix(b"s/").unwrap().to_vec()).unwrap();
    let id = i64::from_str_radix(&hex, 16).unwrap();
    let ttl = LeaseClient::connect(h.endpoint.clone())
        .await
        .unwrap()
        .lease_time_to_live(pb::LeaseTimeToLiveRequest { id, keys: true })
        .await
        .unwrap()
        .into_inner();
    assert!(ttl.ttl > 50 && ttl.granted_ttl == 60, "{ttl:?}");
    assert_eq!(ttl.keys, vec![r.key]);
}

/// The next leader value an Observe stream reports.
async fn next(o: &mut tonic::Streaming<epb::LeaderResponse>) -> String {
    let m = tokio::time::timeout(Duration::from_secs(10), o.message()).await.expect("observe").unwrap().unwrap();
    String::from_utf8(m.kv.unwrap().value).unwrap()
}

fn campaign(name: &str, lease: i64, value: &str) -> epb::CampaignRequest {
    epb::CampaignRequest { name: name.as_bytes().to_vec(), lease, value: value.as_bytes().to_vec() }
}

/// Campaign, Leader, Observe, Proclaim and Resign, in the order of a
/// leadership change.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_election_moves_to_the_next_candidate_on_resign() {
    let h = start_test_server_full().await;
    let (la, lb) = (grant(&h.endpoint, 60).await, grant(&h.endpoint, 60).await);
    let mut c = ElectionClient::connect(h.endpoint.clone()).await.unwrap();
    let leader_b = c.campaign(campaign("el", lb, "b")).await.unwrap().into_inner().leader.unwrap();
    let mut observe = c.observe(epb::LeaderRequest { name: b"el".to_vec() }).await.unwrap().into_inner();
    assert_eq!(next(&mut observe).await, "b");
    let a_campaign = {
        let mut c = c.clone();
        tokio::spawn(async move { c.campaign(campaign("el", la, "a")).await.map(|r| r.into_inner()) })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!a_campaign.is_finished(), "a is elected while b leads");
    let leader = c.leader(epb::LeaderRequest { name: b"el".to_vec() }).await.unwrap().into_inner();
    assert_eq!(leader.kv.unwrap().value, b"b");

    c.proclaim(epb::ProclaimRequest { leader: Some(leader_b.clone()), value: b"b2".to_vec() }).await.unwrap();
    assert_eq!(next(&mut observe).await, "b2");
    c.resign(epb::ResignRequest { leader: Some(leader_b.clone()) }).await.unwrap();
    let a = tokio::time::timeout(Duration::from_secs(10), a_campaign).await.expect("a is elected").unwrap().unwrap();
    assert_eq!(a.leader.unwrap().key, format!("el/{la:x}").into_bytes());
    assert_eq!(next(&mut observe).await, "a");

    // b is no longer the leader: proclaiming fails with etcd's error.
    let e = c.proclaim(epb::ProclaimRequest { leader: Some(leader_b), value: b"x".to_vec() }).await.unwrap_err();
    assert_eq!((e.code(), e.message()), (Code::Unknown, "election: not leader"));
    let e = c.leader(epb::LeaderRequest { name: b"nobody".to_vec() }).await.unwrap_err();
    assert_eq!(e.message(), "election: no leader");
    let e = c.resign(epb::ResignRequest { leader: None }).await.unwrap_err();
    assert_eq!(e.message(), "\"leader\" field must be provided");
}

/// The services act as their caller: a user who cannot write the lock's
/// prefix cannot take the lock (etcd's run with the caller's context).
#[tokio::test]
async fn a_lock_needs_write_on_its_prefix() {
    let h = start_test_server_full().await;
    let lease = grant(&h.endpoint, 60).await;
    let mut a = AuthClient::connect(h.endpoint.clone()).await.unwrap();
    for role in ["root", "reader"] {
        a.role_add(pb::AuthRoleAddRequest { name: role.into() }).await.unwrap();
    }
    a.role_grant_permission(pb::AuthRoleGrantPermissionRequest {
        name: "reader".into(),
        perm: Some(authpb::Permission { perm_type: 0, key: b"lk/".to_vec(), range_end: b"lk0".to_vec() }),
    })
    .await
    .unwrap();
    for (name, role) in [("root", "root"), ("carol", "reader")] {
        a.user_add(pb::AuthUserAddRequest {
            name: name.into(),
            password: "pw".into(), // not a secret: test fixture
            ..Default::default()
        })
        .await
        .unwrap();
        a.user_grant_role(pb::AuthUserGrantRoleRequest { user: name.into(), role: role.into() }).await.unwrap();
    }
    a.auth_enable(pb::AuthEnableRequest {}).await.unwrap();
    let token = |name: &str| {
        let ep = h.endpoint.clone();
        let name = name.to_string();
        async move {
            AuthClient::connect(ep)
                .await
                .unwrap()
                .authenticate(pb::AuthenticateRequest { name, password: "pw".into() }) // test fixture
                .await
                .unwrap()
                .into_inner()
                .token
        }
    };
    let as_user = |req: lpb::LockRequest, t: &str| {
        let mut r = Request::new(req);
        r.metadata_mut().insert("token", MetadataValue::try_from(t).unwrap());
        r
    };
    let mut c = LockClient::connect(h.endpoint.clone()).await.unwrap();
    let carol = token("carol").await;
    let e = c.lock(as_user(lock_req("lk", lease), &carol)).await.unwrap_err();
    assert_eq!(e.code(), Code::PermissionDenied, "{e:?}");
    let root = token("root").await;
    c.lock(as_user(lock_req("lk", lease), &root)).await.expect("root locks");
}
