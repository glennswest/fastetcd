//! End-to-end tests for Cluster.MemberList and Maintenance.Status /
//! Hash / HashKV via real tonic clients.

mod common;
use common::start_test_server_full;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::cluster_client::ClusterClient;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::maintenance_client::MaintenanceClient;

#[tokio::test]
async fn member_list_returns_self() {
    let h = start_test_server_full().await;
    let mut cluster = ClusterClient::connect(h.endpoint.clone()).await.unwrap();
    let resp = cluster
        .member_list(pb::MemberListRequest { linearizable: false })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.members.len(), 1);
    let m = &resp.members[0];
    assert_eq!(m.name, "test-node");
    assert!(!m.is_learner);
    assert_eq!(m.id, 1);
    assert_eq!(m.peer_ur_ls, vec!["http://test-peer:0".to_string()]);
}

#[tokio::test]
async fn status_returns_real_values() {
    let h = start_test_server_full().await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    let mut mnt = MaintenanceClient::connect(h.endpoint.clone()).await.unwrap();

    // Drive revision forward.
    kv.put(pb::PutRequest {
        key: b"a".to_vec(),
        value: b"1".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();

    let resp = mnt
        .status(pb::StatusRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(resp.header.is_some());
    let header = resp.header.unwrap();
    assert_eq!(header.revision, 1);
    assert_eq!(resp.leader, 1); // single-node, leader is self
    assert!(resp.raft_term >= 1);
    assert!(resp.raft_applied_index >= 1);
    assert!(resp.db_size > 0);
    assert_eq!(resp.version, "3.6.0");
}

#[tokio::test]
async fn hash_kv_is_deterministic_across_calls_at_same_state() {
    let h = start_test_server_full().await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    let mut mnt = MaintenanceClient::connect(h.endpoint.clone()).await.unwrap();

    kv.put(pb::PutRequest {
        key: b"a".to_vec(),
        value: b"1".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    kv.put(pb::PutRequest {
        key: b"b".to_vec(),
        value: b"2".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();

    let r1 = mnt
        .hash_kv(pb::HashKvRequest { revision: 0 })
        .await
        .unwrap()
        .into_inner();
    let r2 = mnt
        .hash_kv(pb::HashKvRequest { revision: 0 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r1.hash, r2.hash);
    assert_eq!(r1.hash_revision, r2.hash_revision);
}

#[tokio::test]
async fn snapshot_streams_bytes() {
    use futures::StreamExt;
    let h = start_test_server_full().await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    let mut mnt = MaintenanceClient::connect(h.endpoint.clone()).await.unwrap();
    kv.put(pb::PutRequest {
        key: b"a".to_vec(),
        value: vec![0u8; 4096],
        ..Default::default()
    })
    .await
    .unwrap();
    let stream = mnt
        .snapshot(pb::SnapshotRequest {})
        .await
        .unwrap()
        .into_inner();
    let chunks: Vec<_> = stream.collect().await;
    assert!(!chunks.is_empty(), "expected at least one snapshot chunk");
    let total: usize = chunks
        .iter()
        .map(|r| r.as_ref().unwrap().blob.len())
        .sum();
    assert!(total > 0, "expected nonzero snapshot bytes");
}

#[tokio::test]
async fn defragment_succeeds_after_writes() {
    let h = start_test_server_full().await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    let mut mnt = MaintenanceClient::connect(h.endpoint.clone()).await.unwrap();
    for i in 0..50u32 {
        kv.put(pb::PutRequest {
            key: format!("k-{i}").into_bytes(),
            value: vec![b'x'; 256],
            ..Default::default()
        })
        .await
        .unwrap();
    }
    // Re-overwrite to create garbage that compaction can reclaim.
    for i in 0..50u32 {
        kv.put(pb::PutRequest {
            key: format!("k-{i}").into_bytes(),
            value: vec![b'y'; 256],
            ..Default::default()
        })
        .await
        .unwrap();
    }
    let resp = mnt
        .defragment(pb::DefragmentRequest {})
        .await
        .expect("defragment should succeed")
        .into_inner();
    assert!(resp.header.is_some());

    // Data should still be readable after defragment.
    let r = kv
        .range(pb::RangeRequest {
            key: b"k-0".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.kvs.len(), 1);
    assert_eq!(r.kvs[0].value, vec![b'y'; 256]);
}

#[tokio::test]
async fn member_add_as_learner_appears_in_member_list() {
    // The test harness is single-node with no live peer reachable at
    // the URL we pass — so a voting MemberAdd would block on
    // change_membership waiting for the new voter to catch up. As a
    // learner, the add proceeds without requiring liveness.
    let h = start_test_server_full().await;
    let mut cluster = ClusterClient::connect(h.endpoint.clone()).await.unwrap();
    let resp = cluster
        .member_add(pb::MemberAddRequest {
            peer_ur_ls: vec!["http://127.0.0.1:65535".to_string()],
            is_learner: true,
        })
        .await
        .expect("MemberAdd should succeed")
        .into_inner();
    let added = resp.member.expect("member field set");
    assert!(added.is_learner);
    assert_eq!(added.peer_ur_ls, vec!["http://127.0.0.1:65535".to_string()]);

    let listed = cluster
        .member_list(pb::MemberListRequest { linearizable: false })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.members.len(), 2, "expected self + new learner");
    assert!(listed.members.iter().any(|m| m.id == added.id && m.is_learner));
}
