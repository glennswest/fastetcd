//! fastetcd#49: a request that cannot apply gets etcd's error and the
//! member keeps serving. Each of these used to fail inside raft apply,
//! which stops the state machine on every member (and again on every
//! restart, replaying the entry).

mod common;
use common::start_test_server_full;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::lease_client::LeaseClient;
use pb::request_op::Request as Op;
use tonic::transport::Channel;
use tonic::Code;

const KEY_NOT_FOUND: &str = "etcdserver: key not found";
const COMPACTED: &str = "etcdserver: mvcc: required revision has been compacted";
const FUTURE_REV: &str = "etcdserver: mvcc: required revision is a future revision";

#[track_caller]
fn refused<T: std::fmt::Debug>(r: Result<T, tonic::Status>, code: Code, message: &str) {
    match r {
        Err(s) => {
            assert_eq!(s.code(), code, "{s:?}");
            assert_eq!(s.message(), message, "{s:?}");
        }
        Ok(v) => panic!("should be refused with {message:?}, got {v:?}"),
    }
}

fn put(key: &str, value: &str) -> pb::PutRequest {
    pb::PutRequest {
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
        ..Default::default()
    }
}

fn op_put(p: pb::PutRequest) -> pb::RequestOp {
    pb::RequestOp { request: Some(Op::RequestPut(p)) }
}

fn op_delete(key: &str) -> pb::RequestOp {
    pb::RequestOp {
        request: Some(Op::RequestDeleteRange(pb::DeleteRangeRequest {
            key: key.as_bytes().to_vec(),
            ..Default::default()
        })),
    }
}

async fn revision(kv: &mut KvClient<Channel>) -> i64 {
    kv.range(pb::RangeRequest { key: b"x".to_vec(), ..Default::default() })
        .await
        .unwrap()
        .into_inner()
        .header
        .unwrap()
        .revision
}

async fn value(kv: &mut KvClient<Channel>, key: &str) -> Option<Vec<u8>> {
    kv.range(pb::RangeRequest { key: key.as_bytes().to_vec(), ..Default::default() })
        .await
        .unwrap()
        .into_inner()
        .kvs
        .pop()
        .map(|kv| kv.value)
}

#[tokio::test]
async fn ignore_value_or_lease_on_a_missing_key_is_key_not_found() {
    let h = start_test_server_full().await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    kv.put(put("there", "v")).await.unwrap();
    let before = revision(&mut kv).await;

    for (ignore_value, ignore_lease) in [(true, false), (false, true), (true, true)] {
        let p = pb::PutRequest { ignore_value, ignore_lease, ..put("missing", "") };
        refused(kv.put(p.clone()).await, Code::InvalidArgument, KEY_NOT_FOUND);
        refused(
            kv.txn(pb::TxnRequest {
                compare: vec![],
                success: vec![op_put(put("other", "o")), op_put(p)],
                failure: vec![],
            })
            .await,
            Code::InvalidArgument,
            KEY_NOT_FOUND,
        );
    }
    // Refused by the state machine itself, not the leader's precheck: the
    // key exists when the precheck looks, and the txn deletes it first.
    refused(
        kv.txn(pb::TxnRequest {
            compare: vec![],
            success: vec![
                op_delete("there"),
                op_put(pb::PutRequest { ignore_value: true, ..put("there", "") }),
            ],
            failure: vec![],
        })
        .await,
        Code::InvalidArgument,
        KEY_NOT_FOUND,
    );
    assert_eq!(revision(&mut kv).await, before, "a refused request changed the store");
    assert_eq!(value(&mut kv, "there").await.as_deref(), Some(&b"v"[..]));
    assert_eq!(value(&mut kv, "other").await, None);

    // Still serving; ignore_value on a key that exists keeps its value.
    kv.put(pb::PutRequest { ignore_value: true, ..put("there", "") }).await.unwrap();
    kv.put(put("after", "a")).await.unwrap();
    assert_eq!(value(&mut kv, "there").await.as_deref(), Some(&b"v"[..]));
    assert_eq!(value(&mut kv, "after").await.as_deref(), Some(&b"a"[..]));
}

#[tokio::test]
async fn compact_out_of_range_is_refused_as_etcd_does() {
    let h = start_test_server_full().await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    for i in 0..5 {
        kv.put(put("k", &i.to_string())).await.unwrap();
    }
    let compact = |revision| pb::CompactionRequest { revision, physical: false };
    refused(kv.compact(compact(99)).await, Code::OutOfRange, FUTURE_REV);
    kv.compact(compact(4)).await.unwrap();
    refused(kv.compact(compact(2)).await, Code::OutOfRange, COMPACTED);
    // A Txn range at a compacted revision.
    refused(
        kv.txn(pb::TxnRequest {
            compare: vec![],
            success: vec![
                op_put(put("t", "t")),
                pb::RequestOp {
                    request: Some(Op::RequestRange(pb::RangeRequest {
                        key: b"k".to_vec(),
                        revision: 2,
                        ..Default::default()
                    })),
                },
            ],
            failure: vec![],
        })
        .await,
        Code::OutOfRange,
        COMPACTED,
    );
    assert_eq!(value(&mut kv, "t").await, None);
    kv.put(put("after", "a")).await.unwrap();
    assert_eq!(revision(&mut kv).await, 6);
}

#[tokio::test]
async fn keep_alive_of_a_missing_lease_answers_ttl_0() {
    let h = start_test_server_full().await;
    let mut lease = LeaseClient::connect(h.endpoint.clone()).await.unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let mut stream = lease
        .lease_keep_alive(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    tx.send(pb::LeaseKeepAliveRequest { id: 4242 }).await.unwrap();
    let r = stream.message().await.unwrap().expect("an answer");
    assert_eq!((r.id, r.ttl), (4242, 0), "etcd answers TTL 0 for a lease that is gone");

    // The same stream still renews a live lease.
    let id = lease
        .lease_grant(pb::LeaseGrantRequest { ttl: 60, id: 0 })
        .await
        .unwrap()
        .into_inner()
        .id;
    tx.send(pb::LeaseKeepAliveRequest { id }).await.unwrap();
    let r = stream.message().await.unwrap().expect("an answer");
    assert_eq!((r.id, r.ttl), (id, 60));
}

#[tokio::test]
async fn a_lease_grant_with_ttl_0_gets_the_minimum_ttl() {
    let h = start_test_server_full().await;
    let mut lease = LeaseClient::connect(h.endpoint.clone()).await.unwrap();
    for ttl in [0, -5] {
        let r = lease.lease_grant(pb::LeaseGrantRequest { ttl, id: 0 }).await.unwrap().into_inner();
        assert_eq!(r.ttl, fastetcd_server::lease::MIN_LEASE_TTL_SECS, "TTL {ttl}");
        assert!(r.id != 0);
    }
}

/// Wire compatibility: the third-party `etcd-client` crate sees etcd's
/// error.
#[tokio::test]
async fn etcd_client_sees_key_not_found() {
    let h = start_test_server_full().await;
    let mut c = etcd_client::Client::connect([h.endpoint.as_str()], None).await.unwrap();
    let err = c
        .put("missing", "", Some(etcd_client::PutOptions::new().with_ignore_value()))
        .await
        .unwrap_err();
    match err {
        etcd_client::Error::GRpcStatus(s) => {
            assert_eq!(format!("{:?}", s.code()), "InvalidArgument", "{s:?}");
            assert_eq!(s.message(), KEY_NOT_FOUND);
        }
        other => panic!("expected a gRPC status, got {other:?}"),
    }
    c.put("k", "v", None).await.unwrap();
}
