//! fastetcd#202: `Downgrade ENABLE` sets a replicated downgrade target,
//! `Status.downgrade_info` reports it, the version gates it predates are
//! held, CANCEL clears it, and the leader clears it once every member runs
//! it. The maintenance service runs as a newer fastetcd than this binary
//! (`ServerState::with_this_version`): a target must be older than the
//! member and no older than 1.45, the version that reads its entry.

mod common;
use common::start_test_server_full;

use std::sync::Arc;
use std::time::Duration;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::maintenance_server::Maintenance as _;
use fastetcd_raft::version_gate::{GateError, MembersAtLeast};
use fastetcd_server::maintenance::MaintenanceService;
use fastetcd_server::ServerState;
use tonic::{Code, Request};

fn as_version(state: &Arc<ServerState>, version: &str) -> Arc<ServerState> {
    Arc::new((**state).clone().with_this_version(version))
}

async fn downgrade(state: &Arc<ServerState>, action: i32, version: &str) -> Result<String, tonic::Status> {
    MaintenanceService::new(state.clone())
        .downgrade(Request::new(pb::DowngradeRequest { action, version: version.to_string() }))
        .await
        .map(|r| r.into_inner().version)
}

async fn info(state: &Arc<ServerState>) -> pb::DowngradeInfo {
    MaintenanceService::new(state.clone())
        .status(Request::new(pb::StatusRequest {}))
        .await
        .unwrap()
        .into_inner()
        .downgrade_info
        .expect("Status answers downgrade_info")
}

const VALIDATE: i32 = pb::downgrade_request::DowngradeAction::Validate as i32;
const ENABLE: i32 = pb::downgrade_request::DowngradeAction::Enable as i32;
const CANCEL: i32 = pb::downgrade_request::DowngradeAction::Cancel as i32;

#[tokio::test]
async fn enable_sets_a_replicated_target_that_holds_the_gates_and_cancel_clears_it() {
    let h = start_test_server_full().await;
    let newer = as_version(&h.state, "1.46.0");
    assert_eq!(info(&newer).await, pb::DowngradeInfo { enabled: false, target_version: String::new() });

    // Refused: older than the entry's version, and nothing to cancel.
    let e = downgrade(&newer, ENABLE, "1.44.7").await.unwrap_err();
    assert_eq!(e.code(), Code::FailedPrecondition, "{}", e.message());
    assert!(e.message().contains("1.45.0"), "{}", e.message());
    let e = downgrade(&newer, CANCEL, "").await.unwrap_err();
    assert_eq!((e.code(), e.message()), (Code::FailedPrecondition, "etcdserver: no inflight downgrade job"));

    // ENABLE.
    assert_eq!(downgrade(&newer, ENABLE, "1.45").await.unwrap(), "1.46.0");
    assert_eq!(info(&newer).await, pb::DowngradeInfo { enabled: true, target_version: "1.45.0".into() });
    let e = downgrade(&newer, ENABLE, "1.45.1").await.unwrap_err();
    assert_eq!((e.code(), e.message()), (Code::FailedPrecondition, "etcdserver: cluster has a downgrade job in progress"));

    // A gate the target predates is held; one it reads is not.
    let mvcc = h.state.sm.mvcc().clone();
    let held = MembersAtLeast::new((1, 46, 0)).held_by(mvcc.clone());
    assert_eq!(
        held.check(&h.state.raft, &h.state.forwarder).await,
        Err(GateError::Held { target: "1.45.0".into() })
    );
    assert!(!held.passed(&h.state.raft));
    let open = MembersAtLeast::new((1, 44, 0)).held_by(mvcc.clone());
    assert_eq!(open.check(&h.state.raft, &h.state.forwarder).await, Ok(()));

    // The entry is a floor: VALIDATE below 1.45 is refused from now on.
    let e = downgrade(&newer, VALIDATE, "1.44.7").await.unwrap_err();
    assert!(e.message().contains("below 1.45.0"), "{}", e.message());

    // CANCEL.
    assert_eq!(downgrade(&newer, CANCEL, "").await.unwrap(), "1.46.0");
    assert_eq!(info(&newer).await, pb::DowngradeInfo { enabled: false, target_version: String::new() });
    assert_eq!(held.check(&h.state.raft, &h.state.forwarder).await, Ok(()), "no longer held");
}

/// A sole member that runs the target: the leader clears it by itself.
#[tokio::test]
async fn the_leader_clears_a_target_every_member_runs() {
    let h = start_test_server_full().await;
    let newer = as_version(&h.state, "1.46.0");
    downgrade(&newer, ENABLE, "1.45.0").await.unwrap();
    // Not finished while this member answers as 1.46.
    fastetcd_server::downgrade::follow(newer.clone());
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert!(info(&newer).await.enabled, "cleared while a member runs 1.46");
    // Restarted onto the target (here, answering as it).
    let at_target = as_version(&h.state, "1.45.2");
    fastetcd_server::downgrade::follow(at_target.clone());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while info(&at_target).await.enabled {
        assert!(tokio::time::Instant::now() < deadline, "the finished downgrade was never cleared");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
