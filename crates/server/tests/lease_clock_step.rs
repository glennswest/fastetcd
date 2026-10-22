//! Lease deadlines do not follow the wall clock (fastetcd#162).
//!
//! An X9 blade has no RTC battery: it boots at 1970 and NTP steps the
//! clock to now. A grant or keep-alive entry carries the proposer's wall
//! clock, so entries proposed before and after such a step disagree by
//! decades. Proposed here with a clock of 1970 (as before a forward
//! step) and of year 3000 (as before a backward one), each lease must
//! live its TTL and no more, as on etcd, whose lessor uses only
//! monotonic time.

mod common;
use common::start_test_server_full_with_expiry_ticker;

use std::time::Duration;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::lease_client::LeaseClient;
use fastetcd_raft::{FastetcdLogEntry, FastetcdLogResponse};
use tokio::time::{sleep, Instant};

const TTL: i64 = 3;
/// 3000-01-01T00:00:00Z.
const YEAR_3000: i64 = 32_503_680_000;

async fn grant(h: &common::TestServerHandles, now_unix: i64) -> i64 {
    let w = h
        .state
        .raft
        .client_write(FastetcdLogEntry::LeaseGrant { id: 0, ttl_secs: TTL, now_unix })
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
async fn a_stepped_wall_clock_neither_expires_nor_immortalises_a_lease() {
    let h = start_test_server_full_with_expiry_ticker().await;
    let mut lc = LeaseClient::connect(h.endpoint.clone()).await.unwrap();
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    // The leader has promoted its leases for this term before the grants
    // below, as a long-running leader has.
    let warm = lc.lease_grant(pb::LeaseGrantRequest { ttl: 60, id: 0 }).await.unwrap().into_inner().id;
    lc.lease_time_to_live(pb::LeaseTimeToLiveRequest { id: warm, keys: false }).await.unwrap();

    let past = grant(&h, 0).await; // persisted deadline: 1970
    let future = grant(&h, YEAR_3000).await; // persisted deadline: year 3000
    let logged = lc.lease_grant(pb::LeaseGrantRequest { ttl: TTL, id: 0 }).await.unwrap().into_inner().id;
    let started = Instant::now();
    for (key, id) in [(&b"past"[..], past), (b"future", future), (b"logged", logged)] {
        kv.put(pb::PutRequest { key: key.to_vec(), value: b"v".to_vec(), lease: id, ..Default::default() })
            .await
            .unwrap();
    }
    for id in [past, future] {
        let t = lc.lease_time_to_live(pb::LeaseTimeToLiveRequest { id, keys: false }).await.unwrap().into_inner();
        assert!((1..=TTL).contains(&t.ttl), "lease {id}: TimeToLive {} of a {TTL} s lease", t.ttl);
    }

    // A keep-alive logged through Raft (as while the 1.23 gate is closed)
    // with a 1970 clock renews `logged` from when it is applied.
    sleep(Duration::from_secs(2)).await;
    h.state
        .raft
        .client_write(FastetcdLogEntry::LeaseKeepAlive { id: logged, now_unix: 0 })
        .await
        .expect("keep-alive");
    let renewed = Instant::now();

    // Alive for their TTL ...
    sleep(Duration::from_millis(500)).await;
    assert!(alive(&mut kv, b"past").await, "a lease granted with a 1970 clock expired at once");
    assert!(alive(&mut kv, b"future").await);
    assert!(alive(&mut kv, b"logged").await);

    // ... and gone soon after it: the sweeper ticks every 200 ms.
    let deadline = started + Duration::from_secs(TTL as u64 + 3);
    while alive(&mut kv, b"past").await || alive(&mut kv, b"future").await {
        assert!(Instant::now() < deadline, "a lease outlived its TTL (year-3000 clock: immortal)");
        sleep(Duration::from_millis(100)).await;
    }
    assert!(
        alive(&mut kv, b"logged").await || renewed.elapsed() >= Duration::from_secs(TTL as u64),
        "the logged keep-alive (1970 clock) did not renew the lease"
    );
    let deadline = renewed + Duration::from_secs(TTL as u64 + 3);
    while alive(&mut kv, b"logged").await {
        assert!(Instant::now() < deadline, "the renewed lease outlived its TTL");
        sleep(Duration::from_millis(100)).await;
    }
}
