//! fastetcd#19: a Put naming a lease that does not exist is refused,
//! with etcd's `etcdserver: requested lease not found` (NotFound), and
//! nothing is written. It used to be accepted and the key recorded the
//! dangling lease id, so a key meant to be ephemeral never expired.

mod common;
use common::start_test_server_full;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::lease_client::LeaseClient;
use pb::compare::{CompareResult, CompareTarget, TargetUnion};
use pb::request_op::Request as Op;
use tonic::transport::Channel;
use tonic::Code;

const NOT_FOUND: &str = "etcdserver: requested lease not found";

fn put(key: &str, lease: i64) -> pb::PutRequest {
    pb::PutRequest {
        key: key.as_bytes().to_vec(),
        value: b"v".to_vec(),
        lease,
        ..Default::default()
    }
}

#[track_caller]
fn refused<T: std::fmt::Debug>(r: Result<T, tonic::Status>) {
    match r {
        Err(s) => {
            assert_eq!(s.code(), Code::NotFound, "{s:?}");
            assert_eq!(s.message(), NOT_FOUND, "{s:?}");
        }
        Ok(v) => panic!("should be refused as lease not found, got {v:?}"),
    }
}

async fn exists(kv: &mut KvClient<Channel>, key: &str) -> bool {
    kv.range(pb::RangeRequest { key: key.as_bytes().to_vec(), ..Default::default() })
        .await
        .unwrap()
        .into_inner()
        .count
        > 0
}

async fn grant(endpoint: &str) -> i64 {
    LeaseClient::connect(endpoint.to_string())
        .await
        .unwrap()
        .lease_grant(pb::LeaseGrantRequest { ttl: 60, id: 0 })
        .await
        .unwrap()
        .into_inner()
        .id
}

#[tokio::test]
async fn a_put_naming_a_lease_that_does_not_exist_is_refused() {
    let h = start_test_server_full().await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    refused(kv.put(put("k", 12345)).await);
    assert!(!exists(&mut kv, "k").await, "nothing written");

    // A live lease is fine, and no lease at all is fine.
    let id = grant(&h.endpoint).await;
    kv.put(put("k", id)).await.unwrap();
    kv.put(put("plain", 0)).await.unwrap();
}

#[tokio::test]
async fn a_revoked_lease_is_refused() {
    let h = start_test_server_full().await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    let id = grant(&h.endpoint).await;
    kv.put(put("k", id)).await.unwrap();
    LeaseClient::connect(h.endpoint.clone())
        .await
        .unwrap()
        .lease_revoke(pb::LeaseRevokeRequest { id })
        .await
        .unwrap();
    refused(kv.put(put("k2", id)).await);
    assert!(!exists(&mut kv, "k2").await);
}

#[tokio::test]
async fn ignore_lease_keeps_the_key_lease_and_is_not_checked() {
    let h = start_test_server_full().await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    kv.put(put("k", 0)).await.unwrap();
    kv.put(pb::PutRequest {
        key: b"k".to_vec(),
        value: b"v2".to_vec(),
        lease: 999,
        ignore_lease: true,
        ..Default::default()
    })
    .await
    .unwrap();
    let r = kv
        .range(pb::RangeRequest { key: b"k".to_vec(), ..Default::default() })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.kvs[0].lease, 0);
    assert_eq!(r.kvs[0].value, b"v2");
}

fn absent(key: &str) -> pb::Compare {
    pb::Compare {
        result: CompareResult::Equal as i32,
        target: CompareTarget::Version as i32,
        key: key.as_bytes().to_vec(),
        target_union: Some(TargetUnion::Version(0)),
        range_end: Vec::new(),
    }
}

fn op(p: pb::PutRequest) -> pb::RequestOp {
    pb::RequestOp { request: Some(Op::RequestPut(p)) }
}

#[tokio::test]
async fn a_txn_is_refused_when_the_branch_it_takes_names_a_missing_lease() {
    let h = start_test_server_full().await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();

    // "a" is absent, so the success branch runs, and it names a lease
    // that does not exist: the whole txn is refused, nothing applied.
    refused(
        kv.txn(pb::TxnRequest {
            compare: vec![absent("a")],
            success: vec![op(put("a", 0)), op(put("b", 777))],
            failure: vec![],
        })
        .await,
    );
    assert!(!exists(&mut kv, "a").await);

    // The branch not taken is not checked, as in etcd.
    let r = kv
        .txn(pb::TxnRequest {
            compare: vec![absent("a")],
            success: vec![op(put("a", 0))],
            failure: vec![op(put("b", 777))],
        })
        .await
        .unwrap()
        .into_inner();
    assert!(r.succeeded);
    assert!(exists(&mut kv, "a").await);
}

/// Wire compatibility: the third-party `etcd-client` crate sees the
/// error etcd gives.
#[tokio::test]
async fn etcd_client_sees_lease_not_found() {
    let h = start_test_server_full().await;
    let mut c = etcd_client::Client::connect([h.endpoint.as_str()], None).await.unwrap();
    let err = c
        .put("k", "v", Some(etcd_client::PutOptions::new().with_lease(4242)))
        .await
        .unwrap_err();
    match err {
        etcd_client::Error::GRpcStatus(s) => {
            // etcd-client links its own tonic; compare the code by name.
            assert_eq!(format!("{:?}", s.code()), "NotFound", "{s:?}");
            assert_eq!(s.message(), NOT_FOUND);
        }
        other => panic!("expected a gRPC status, got {other:?}"),
    }
}
