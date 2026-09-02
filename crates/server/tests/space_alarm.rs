//! The NOSPACE alarm must stop the store from filling its volume
//! *without* stopping the operations that empty it (fastetcd#14).
//!
//! The failure this guards against: a full data volume wedged the store
//! in both directions — every read failed at the linearizable barrier
//! behind a pending snapshot write, and every write failed at the same
//! place, so the one recovery a client could attempt (deleting keys so
//! the next snapshot fits) was refused for the same reason. The store
//! must instead refuse writes *early*, while reads, deletes, compaction
//! and defragment still work.

mod common;

use std::time::Duration;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::lease_client::LeaseClient;
use fastetcd_proto::etcdserverpb::maintenance_client::MaintenanceClient;
use fastetcd_server::space::{SpaceConfig, ERR_NO_SPACE};

use common::{start_test_server_full, start_test_server_with_space};

/// A guard whose capacity is one byte: any store at all is over every
/// mark, so the alarm is raised on the first sample.
fn always_full() -> SpaceConfig {
    SpaceConfig {
        quota_backend_bytes: 1,
        high_water_percent: 100,
        alarm_percent: 1,
        clear_percent: 0,
        interval: Duration::from_secs(3600), // no background ticker in tests
        ..SpaceConfig::default()
    }
}

async fn put(c: &mut KvClient<tonic::transport::Channel>, k: &str, v: &str) -> Result<(), tonic::Status> {
    c.put(pb::PutRequest {
        key: k.as_bytes().to_vec(),
        value: v.as_bytes().to_vec(),
        ..Default::default()
    })
    .await
    .map(|_| ())
}

#[tokio::test]
async fn under_the_alarm_writes_are_refused_but_recovery_still_works() {
    let h = start_test_server_with_space(Some(always_full())).await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();

    // Seed data *before* the alarm is raised, so there is something to
    // read back and delete afterwards.
    assert!(h.state.space.check_write().is_ok(), "not alarmed yet");
    put(&mut kv, "keep", "v").await.unwrap();
    put(&mut kv, "drop", "v").await.unwrap();

    // Sampling the (over-quota) store raises the alarm.
    let stats = h.state.space.clone().refresh(&h.state).await;
    assert!(stats.nospace, "a store over its quota must raise NOSPACE");

    // Writes are refused, with etcd's error.
    let err = put(&mut kv, "new", "v").await.expect_err("put must be refused");
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    assert_eq!(err.message(), ERR_NO_SPACE);

    // A txn that writes is refused too — including one hiding its put
    // in the failure branch.
    let err = kv
        .txn(pb::TxnRequest {
            compare: vec![],
            success: vec![],
            failure: vec![pb::RequestOp {
                response: None,
                request: Some(pb::request_op::Request::RequestPut(pb::PutRequest {
                    key: b"sneaky".to_vec(),
                    value: b"v".to_vec(),
                    ..Default::default()
                })),
            }],
        })
        .await
        .expect_err("a txn containing a put must be refused");
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);

    // A lease grant is capped the same way etcd caps it.
    let mut lease = LeaseClient::connect(h.endpoint.clone()).await.unwrap();
    let err = lease
        .lease_grant(pb::LeaseGrantRequest { ttl: 60, id: 0 })
        .await
        .expect_err("lease grant must be refused");
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);

    // --- and now everything recovery needs, which must still work ---

    // Reads.
    let got = kv
        .range(pb::RangeRequest {
            key: b"keep".to_vec(),
            ..Default::default()
        })
        .await
        .expect("a read must still be served under NOSPACE")
        .into_inner();
    assert_eq!(got.kvs.len(), 1);

    // A read-only txn.
    kv.txn(pb::TxnRequest {
        compare: vec![],
        success: vec![pb::RequestOp {
            response: None,
            request: Some(pb::request_op::Request::RequestRange(pb::RangeRequest {
                key: b"keep".to_vec(),
                ..Default::default()
            })),
        }],
        failure: vec![],
    })
    .await
    .expect("a read-only txn must still be served under NOSPACE");

    // Deletes — the client's own way out.
    let deleted = kv
        .delete_range(pb::DeleteRangeRequest {
            key: b"drop".to_vec(),
            ..Default::default()
        })
        .await
        .expect("a delete must still be accepted under NOSPACE")
        .into_inner();
    assert_eq!(deleted.deleted, 1);

    // A delete-only txn.
    kv.txn(pb::TxnRequest {
        compare: vec![],
        success: vec![pb::RequestOp {
            response: None,
            request: Some(pb::request_op::Request::RequestDeleteRange(
                pb::DeleteRangeRequest {
                    key: b"keep".to_vec(),
                    ..Default::default()
                },
            )),
        }],
        failure: vec![],
    })
    .await
    .expect("a delete-only txn must still be accepted under NOSPACE");

    // Compaction, which is what actually bounds history.
    let rev = h.state.sm.mvcc().current_revision().await;
    kv.compact(pb::CompactionRequest {
        revision: rev,
        physical: false,
    })
    .await
    .expect("compaction must still be accepted under NOSPACE");

    // Defragment, which is what actually returns space to the volume.
    let mut maint = MaintenanceClient::connect(h.endpoint.clone()).await.unwrap();
    maint
        .defragment(pb::DefragmentRequest {})
        .await
        .expect("defragment must still work under NOSPACE");
}

#[tokio::test]
async fn the_alarm_is_listed_and_can_be_disarmed() {
    let h = start_test_server_with_space(Some(always_full())).await;
    let mut maint = MaintenanceClient::connect(h.endpoint.clone()).await.unwrap();

    let listed = maint
        .alarm(pb::AlarmRequest {
            action: pb::alarm_request::AlarmAction::Get as i32,
            member_id: 0,
            alarm: pb::AlarmType::None as i32,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.alarms.len(), 1, "NOSPACE must be listed");
    assert_eq!(listed.alarms[0].alarm, pb::AlarmType::Nospace as i32);
    assert_eq!(listed.alarms[0].member_id, h.state.member_id);

    // Status reports it under `errors`, the way etcdctl renders alarms.
    let status = maint
        .status(pb::StatusRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status.errors, vec!["NOSPACE".to_string()]);
    assert!(status.db_size_quota > 0, "the effective quota is reported");

    // Disarm clears it, and a write goes through again — until the next
    // sample re-raises it, which is exactly etcd's behavior.
    maint
        .alarm(pb::AlarmRequest {
            action: pb::alarm_request::AlarmAction::Deactivate as i32,
            member_id: 0,
            alarm: pb::AlarmType::Nospace as i32,
        })
        .await
        .unwrap();
    assert!(!h.state.space.nospace(), "disarm must clear the alarm");
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    put(&mut kv, "after-disarm", "v")
        .await
        .expect("a write must be accepted once the alarm is disarmed");
}

#[tokio::test]
async fn a_store_with_room_raises_no_alarm_and_reports_its_capacity() {
    // The default test server runs with a disabled guard; a live guard
    // with no configured quota measures the real filesystem, which has
    // room, so nothing should alarm.
    let h = start_test_server_with_space(Some(SpaceConfig {
        quota_backend_bytes: 0,
        ..SpaceConfig::default()
    }))
    .await;
    let stats = h.state.space.clone().refresh(&h.state).await;
    assert!(!stats.nospace, "a store with room must not alarm");
    assert!(
        stats.capacity_bytes > stats.used_bytes(),
        "capacity {} should exceed usage {}",
        stats.capacity_bytes,
        stats.used_bytes()
    );

    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    put(&mut kv, "k", "v").await.expect("writes are unaffected");
}

#[tokio::test]
async fn without_a_space_guard_nothing_changes() {
    // Every deployment that doesn't configure space management must see
    // exactly the old behavior.
    let h = start_test_server_full().await;
    assert!(!h.state.space.is_enabled());
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    put(&mut kv, "k", "v").await.unwrap();

    let mut maint = MaintenanceClient::connect(h.endpoint.clone()).await.unwrap();
    let listed = maint
        .alarm(pb::AlarmRequest {
            action: pb::alarm_request::AlarmAction::Get as i32,
            member_id: 0,
            alarm: pb::AlarmType::None as i32,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(listed.alarms.is_empty());
}
