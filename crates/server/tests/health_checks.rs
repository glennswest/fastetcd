//! /health, /livez and /readyz check the member as etcd's do (fastetcd#69):
//! healthy, NOSPACE, CORRUPT, no leader; `exclude`, `serializable`,
//! `verbose`, the per-check routes.

mod common;
use common::{start_test_server_full, start_test_server_with_space, NopNet};

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use fastetcd_raft::kv_log_store::KvLogStore;
use fastetcd_raft::types::{NodeId, TypeConfig};
use fastetcd_raft::FastetcdStateMachine;
use fastetcd_server::recovery::{RecoveryAlarm, RecoveryRecord};
use fastetcd_server::space::SpaceConfig;
use fastetcd_server::ServerState;
use fastetcd_storage::mvcc::MvccStore;
use fastetcd_storage::redb_engine::RedbEngine;
use fastetcd_storage::{KvStore, WriteBatch, WriteOptions};
use openraft::{Config, Raft};

/// Serve `state`'s health routes on a free port.
async fn serve(state: Arc<ServerState>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, fastetcd_server::health::router(state)).await.unwrap();
    });
    format!("http://{addr}")
}

async fn get(base: &str, path: &str) -> (u16, String) {
    let r = reqwest::get(format!("{base}{path}")).await.unwrap();
    (r.status().as_u16(), r.text().await.unwrap())
}

#[tokio::test]
async fn a_healthy_member() {
    let h = start_test_server_full().await;
    let base = serve(h.state.clone()).await;
    assert_eq!(get(&base, "/health").await, (200, r#"{"health":"true","reason":""}"#.into()));
    assert_eq!(get(&base, "/health?serializable=true").await.0, 200);
    assert_eq!(get(&base, "/livez").await, (200, "ok\n".into()));
    assert_eq!(get(&base, "/readyz").await, (200, "ok\n".into()));
    let (code, body) = get(&base, "/readyz?verbose").await;
    assert_eq!(code, 200);
    assert_eq!(
        body,
        "[+]data_corruption ok\n[+]serializable_read ok\n[+]linearizable_read ok\n[+]non_learner ok\nok\n"
    );
    assert_eq!(get(&base, "/readyz/linearizable_read").await, (200, "ok\n".into()));
    assert_eq!(get(&base, "/livez/serializable_read").await, (200, "ok\n".into()));
    assert_eq!(get(&base, "/readyz/nonsense").await.0, 404);
    assert_eq!(h.state.traffic.health_success.get(), 2);
}

#[tokio::test]
async fn nospace_fails_health_unless_excluded() {
    let h = start_test_server_with_space(Some(SpaceConfig {
        quota_backend_bytes: 1,
        high_water_percent: 100,
        alarm_percent: 1,
        clear_percent: 0,
        interval: Duration::from_secs(3600),
        ..SpaceConfig::default()
    }))
    .await;
    assert!(h.state.space.clone().refresh(&h.state).await.nospace);
    let base = serve(h.state.clone()).await;
    assert_eq!(
        get(&base, "/health").await,
        (503, "{\"health\":\"false\",\"reason\":\"ALARM NOSPACE\"}\n".into())
    );
    assert_eq!(get(&base, "/health?exclude=NOSPACE").await.0, 200);
    // etcd's /readyz checks CORRUPT only; NOSPACE leaves the member ready.
    assert_eq!(get(&base, "/readyz").await.0, 200);
    assert_eq!(h.state.traffic.health_failures.get(), 1);
}

#[tokio::test]
async fn corrupt_fails_health_and_readyz_until_excluded() {
    let h = start_test_server_full().await;
    // A recovery from backup on record (#37): the CORRUPT alarm.
    let engine = h.state.sm.mvcc().engine().clone();
    let record = RecoveryRecord {
        recovered_unix_ms: 1,
        reason: "test".into(),
        corrupt_file: "x".into(),
        backup_file: "y".into(),
        backup_created_unix_ms: 1,
        backup_revision: 1,
    };
    let mut b = WriteBatch::new();
    b.put("node_meta", b"recovered_from_backup", &bincode::serialize(&record).unwrap());
    engine.commit(b, WriteOptions { sync: true }).await.unwrap();
    let mut state = (*h.state).clone();
    state.recovery = Arc::new(RecoveryAlarm::load(&engine).await.unwrap());
    assert!(state.recovery.active().is_some());
    let base = serve(Arc::new(state)).await;
    let (code, body) = get(&base, "/health").await;
    assert_eq!((code, body.as_str()), (503, "{\"health\":\"false\",\"reason\":\"ALARM CORRUPT\"}\n"));
    assert_eq!(get(&base, "/health?exclude=CORRUPT").await.0, 200);
    let (code, body) = get(&base, "/readyz").await;
    assert_eq!(code, 503);
    assert!(body.contains("[-]data_corruption failed: alarm activated: CORRUPT\n"), "{body}");
    assert!(body.contains("[+]linearizable_read ok\n"), "{body}");
    assert_eq!(get(&base, "/readyz?exclude=data_corruption").await.0, 200);
    assert_eq!(get(&base, "/livez").await.0, 200, "liveness does not look at alarms");
}

/// A member of a two-voter cluster whose other voter never appears: no
/// leader, ever.
async fn leaderless() -> (tempfile::TempDir, Arc<ServerState>) {
    let dir = tempfile::tempdir().unwrap();
    let engine: Arc<dyn KvStore> = Arc::new(RedbEngine::open(dir.path().join("kv.redb")).unwrap());
    let mvcc = MvccStore::open(engine.clone()).await.unwrap();
    let sm = FastetcdStateMachine::open(mvcc, dir.path().join("snapshots")).await.unwrap();
    let config = Arc::new(
        Config { heartbeat_interval: 50, election_timeout_min: 100, election_timeout_max: 200, ..Default::default() }
            .validate()
            .unwrap(),
    );
    let raft = Raft::<TypeConfig>::new(1, config, NopNet, KvLogStore::new(engine), sm.clone())
        .await
        .unwrap();
    let members: BTreeSet<NodeId> = [1, 2].into_iter().collect();
    raft.initialize(members).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(raft.metrics().borrow().current_leader.is_none());
    let forwarder = fastetcd_raft::WriteForwarder::new(fastetcd_raft::network::empty_peers());
    (dir, Arc::new(ServerState::new(raft, sm, 7, 1, forwarder)))
}

#[tokio::test]
async fn no_leader_fails_health_and_readyz_not_livez() {
    let (_d, state) = leaderless().await;
    let base = serve(state).await;
    assert_eq!(
        get(&base, "/health").await,
        (503, "{\"health\":\"false\",\"reason\":\"RAFT NO LEADER\"}\n".into())
    );
    // Serializable: this member's own state, no leader needed.
    assert_eq!(get(&base, "/health?serializable=true").await.0, 200);
    // A liveness probe must not restart members during a partition.
    assert_eq!(get(&base, "/livez").await, (200, "ok\n".into()));
    let (code, body) = get(&base, "/readyz").await;
    assert_eq!(code, 503);
    assert!(body.contains("[-]linearizable_read failed: "), "{body}");
    assert!(body.contains("[+]serializable_read ok\n"), "{body}");
}
