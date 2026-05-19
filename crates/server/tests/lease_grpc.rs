//! Lease service gRPC tests.

mod common;
use common::start_test_server_full;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::lease_client::LeaseClient;

#[tokio::test]
async fn grant_revoke_round_trips() {
    let h = start_test_server_full().await;
    let mut lc = LeaseClient::connect(h.endpoint.clone()).await.unwrap();
    let grant = lc
        .lease_grant(pb::LeaseGrantRequest { ttl: 60, id: 0 })
        .await
        .unwrap()
        .into_inner();
    assert!(grant.id > 0);
    assert_eq!(grant.ttl, 60);

    let leases = lc
        .lease_leases(pb::LeaseLeasesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(leases.leases.len(), 1);
    assert_eq!(leases.leases[0].id, grant.id);

    lc.lease_revoke(pb::LeaseRevokeRequest { id: grant.id })
        .await
        .unwrap();
    let leases_after = lc
        .lease_leases(pb::LeaseLeasesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(leases_after.leases.is_empty());
}

#[tokio::test]
async fn revoke_cascade_deletes_attached_keys() {
    let h = start_test_server_full().await;
    let mut lc = LeaseClient::connect(h.endpoint.clone()).await.unwrap();
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();

    let grant = lc
        .lease_grant(pb::LeaseGrantRequest { ttl: 60, id: 0 })
        .await
        .unwrap()
        .into_inner();
    let id = grant.id;

    for k in [b"a", b"b", b"c"] {
        kv.put(pb::PutRequest {
            key: k.to_vec(),
            value: b"v".to_vec(),
            lease: id,
            ..Default::default()
        })
        .await
        .unwrap();
    }
    let range = kv
        .range(pb::RangeRequest {
            key: b"a".to_vec(),
            range_end: b"z".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(range.kvs.len(), 3);

    lc.lease_revoke(pb::LeaseRevokeRequest { id })
        .await
        .unwrap();
    let range_after = kv
        .range(pb::RangeRequest {
            key: b"a".to_vec(),
            range_end: b"z".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(range_after.kvs.len(), 0, "lease revoke must cascade-delete attached keys");
}

#[tokio::test]
async fn time_to_live_reports_attached_keys() {
    let h = start_test_server_full().await;
    let mut lc = LeaseClient::connect(h.endpoint.clone()).await.unwrap();
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();

    let grant = lc
        .lease_grant(pb::LeaseGrantRequest { ttl: 60, id: 0 })
        .await
        .unwrap()
        .into_inner();
    let id = grant.id;
    kv.put(pb::PutRequest {
        key: b"attached".to_vec(),
        value: b"v".to_vec(),
        lease: id,
        ..Default::default()
    })
    .await
    .unwrap();

    let ttl = lc
        .lease_time_to_live(pb::LeaseTimeToLiveRequest { id, keys: true })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ttl.id, id);
    assert_eq!(ttl.granted_ttl, 60);
    assert!(ttl.ttl > 0);
    assert_eq!(ttl.keys, vec![b"attached".to_vec()]);
}
