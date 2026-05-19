//! End-to-end test: a lease granted with a 1-second TTL is
//! auto-revoked by the leader-side ticker, and its attached keys are
//! cascade-deleted.

mod common;
use common::start_test_server_full_with_expiry_ticker;

use std::time::Duration;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::lease_client::LeaseClient;

#[tokio::test]
async fn expired_lease_is_auto_revoked_and_keys_cascade_delete() {
    let h = start_test_server_full_with_expiry_ticker().await;
    let mut lc = LeaseClient::connect(h.endpoint.clone()).await.unwrap();
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();

    // 1-second TTL.
    let grant = lc
        .lease_grant(pb::LeaseGrantRequest { ttl: 1, id: 0 })
        .await
        .unwrap()
        .into_inner();
    let id = grant.id;

    // Attach two keys.
    kv.put(pb::PutRequest {
        key: b"k1".to_vec(),
        value: b"v".to_vec(),
        lease: id,
        ..Default::default()
    })
    .await
    .unwrap();
    kv.put(pb::PutRequest {
        key: b"k2".to_vec(),
        value: b"v".to_vec(),
        lease: id,
        ..Default::default()
    })
    .await
    .unwrap();

    // Wait through (TTL + tick) + buffer.
    tokio::time::sleep(Duration::from_millis(2500)).await;

    // Lease should be gone.
    let leases = lc
        .lease_leases(pb::LeaseLeasesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(
        leases.leases.is_empty(),
        "lease should have been auto-revoked"
    );

    // Attached keys should be cascade-deleted.
    let range = kv
        .range(pb::RangeRequest {
            key: b"k1".to_vec(),
            range_end: b"k3".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        range.kvs.len(),
        0,
        "attached keys must be cascade-deleted by expiry"
    );
}
