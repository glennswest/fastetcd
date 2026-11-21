//! A lease TTL too large for a monotonic deadline (fastetcd#218).
//!
//! LeaseGrant used to accept any positive TTL, and the leader's lessor
//! added it to an `Instant` unchecked: a TTL of `i64::MAX` s panicked the
//! expiry sweeper (and, through promotion, poisoned the RAM deadlines)
//! for good, so no lease expired again. Now a grant above etcd's maximum
//! is refused as etcd refuses it, and a lease an older member persisted
//! with such a TTL is held at that maximum while every other lease
//! expires as before.

mod common;
use common::{start_test_server_full, start_test_server_full_with_expiry_ticker};

use std::time::Duration;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::lease_client::LeaseClient;
use fastetcd_raft::lessor::{Renewal, MAX_LEASE_TTL_SECS};
use fastetcd_raft::{FastetcdLogEntry, FastetcdLogResponse};
use tokio::time::{sleep, Instant};

/// A TTL is counted from its grant: allow the test's own running time.
const SLACK_SECS: i64 = 120;

/// Grant through raft directly, as a member without the check did.
async fn grant_logged(h: &common::TestServerHandles, ttl_secs: i64) -> i64 {
    let now_unix = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
    let w = h
        .state
        .raft
        .client_write(FastetcdLogEntry::LeaseGrant { id: 0, ttl_secs, now_unix })
        .await
        .expect("grant");
    match w.data {
        FastetcdLogResponse::LeaseGrant(g) => g.id,
        other => panic!("grant answered {other:?}"),
    }
}

async fn alive(kv: &mut KvClient<tonic::transport::Channel>, key: &[u8]) -> bool {
    !kv.range(pb::RangeRequest { key: key.to_vec(), ..Default::default() })
        .await
        .unwrap()
        .into_inner()
        .kvs
        .is_empty()
}

#[tokio::test]
async fn a_grant_above_etcds_maximum_ttl_is_refused_as_etcd_refuses_it() {
    let h = start_test_server_full().await;
    let mut lc = LeaseClient::connect(h.endpoint.clone()).await.unwrap();
    for ttl in [MAX_LEASE_TTL_SECS + 1, i64::MAX] {
        let err = lc.lease_grant(pb::LeaseGrantRequest { ttl, id: 0 }).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::OutOfRange, "ttl {ttl}: {err:?}");
        assert_eq!(err.message(), "etcdserver: too large lease TTL", "ttl {ttl}");
    }
    let max = lc.lease_grant(pb::LeaseGrantRequest { ttl: MAX_LEASE_TTL_SECS, id: 0 }).await.unwrap().into_inner();
    assert_eq!(max.ttl, MAX_LEASE_TTL_SECS);
    let ttl = lc
        .lease_time_to_live(pb::LeaseTimeToLiveRequest { id: max.id, keys: false })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ttl.granted_ttl, MAX_LEASE_TTL_SECS);
    assert!(ttl.ttl >= MAX_LEASE_TTL_SECS - SLACK_SECS, "{ttl:?}");
}

#[tokio::test]
async fn promoting_a_persisted_oversized_lease_neither_panics_nor_expires_it() {
    // No sweeper: this test drives the lessor itself, so the oversized
    // lease is in the store when the leader first promotes.
    let h = start_test_server_full().await;
    let huge = grant_logged(&h, i64::MAX).await;
    let short = grant_logged(&h, 1).await;

    // Promotion (first lessor call of the term) and TimeToLive.
    let t = h.state.lessor.time_to_live(huge, false).await.unwrap().expect("huge lease");
    assert_eq!(t.granted_ttl_secs, i64::MAX);
    assert!(t.remaining_ttl_secs >= MAX_LEASE_TTL_SECS - SLACK_SECS, "{t:?}");

    // A keep-alive of it renews (in RAM) without panicking.
    match h.state.lessor.renew(huge).await {
        Renewal::Renewed(r) => assert_eq!(r.granted_ttl_secs, i64::MAX),
        other => panic!("renewing the huge lease: {other:?}"),
    }

    // The short lease expires; the huge one does not.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let expired = h.state.lessor.expired().await.unwrap();
        assert!(!expired.contains(&huge), "the huge lease expired");
        if expired.contains(&short) {
            break;
        }
        assert!(Instant::now() < deadline, "the 1 s lease never expired");
        sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn ordinary_leases_keep_expiring_beside_an_oversized_one() {
    let h = start_test_server_full_with_expiry_ticker().await;
    let mut lc = LeaseClient::connect(h.endpoint.clone()).await.unwrap();
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    let huge = grant_logged(&h, i64::MAX).await;
    let put = |key: &'static [u8], lease| pb::PutRequest {
        key: key.to_vec(),
        value: b"v".to_vec(),
        lease,
        ..Default::default()
    };
    kv.put(put(b"huge", huge)).await.unwrap();

    // Several rounds: each sweep has seen the huge lease, and the sweeper
    // is still alive for the next short one.
    for round in 0..3 {
        let short = lc.lease_grant(pb::LeaseGrantRequest { ttl: 2, id: 0 }).await.unwrap().into_inner().id;
        kv.put(put(b"short", short)).await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        while alive(&mut kv, b"short").await {
            assert!(Instant::now() < deadline, "round {round}: the 2 s lease never expired");
            sleep(Duration::from_millis(200)).await;
        }
        assert!(alive(&mut kv, b"huge").await, "round {round}: the huge lease's key went");
    }
    let t = lc
        .lease_time_to_live(pb::LeaseTimeToLiveRequest { id: huge, keys: false })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(t.granted_ttl, i64::MAX);
    assert!(t.ttl >= MAX_LEASE_TTL_SECS - SLACK_SECS, "{t:?}");
}
