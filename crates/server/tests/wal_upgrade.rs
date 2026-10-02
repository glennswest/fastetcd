//! Upgrading a data directory whose raft log is in the data file
//! (every version before #85) to the WAL, with the real binary.
//!
//! A node built the way 1.11 ran (the log in redb's `raft_log`) writes
//! some keys and stops. `fastetcd` then starts on that directory: the
//! log moves into `wal/`, the keys are all there, writes work, and the
//! data file's own copy of the log is gone.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use openraft::{BasicNode, Config, Raft};

use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::{PutRequest, RangeRequest};
use fastetcd_raft::kv_log_store::KvLogStore;
use fastetcd_raft::types::{FastetcdLogEntry, TypeConfig};
use fastetcd_raft::{empty_peers, FastetcdStateMachine, GrpcNetworkFactory};
use fastetcd_storage::mvcc::{Mutation, MvccStore};
use fastetcd_storage::redb_engine::RedbEngine;
use fastetcd_storage::KvStore;

const KEYS: usize = 50;

async fn pick_free_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    port
}

/// Write `KEYS` keys through a node whose raft log is in redb.
async fn legacy_node(dir: &Path) {
    let engine: Arc<dyn KvStore> = Arc::new(RedbEngine::open(dir.join("fastetcd.redb")).unwrap());
    let mvcc = MvccStore::open(engine.clone()).await.unwrap();
    let sm = FastetcdStateMachine::open(mvcc, dir.join("snapshots")).await.unwrap();
    let log = KvLogStore::new(engine);
    let config = Arc::new(
        Config {
            heartbeat_interval: 100,
            election_timeout_min: 300,
            election_timeout_max: 600,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let raft = Raft::<TypeConfig>::new(1, config, GrpcNetworkFactory::new(empty_peers()), log, sm)
        .await
        .unwrap();
    raft.initialize(BTreeMap::from([(1u64, BasicNode::new("http://127.0.0.1:1"))]))
        .await
        .unwrap();
    raft.wait(Some(Duration::from_secs(10)))
        .state(openraft::ServerState::Leader, "leader")
        .await
        .unwrap();
    for i in 0..KEYS {
        raft.client_write(FastetcdLogEntry::Apply {
            mutations: vec![Mutation::Put {
                key: format!("k{i:03}").into_bytes(),
                value: format!("v{i}").into_bytes(),
                lease: 0,
                prev_kv: false,
                ignore_value: false,
                ignore_lease: false,
            }],
        })
        .await
        .unwrap();
    }
    raft.shutdown().await.unwrap();
}

async fn start(dir: &Path) -> (tokio::process::Child, KvClient<tonic::transport::Channel>) {
    let client_port = pick_free_port().await;
    let peer_port = pick_free_port().await;
    let child = tokio::process::Command::new(env!("CARGO_BIN_EXE_fastetcd"))
        .arg("--data-dir")
        .arg(dir)
        .arg("--node-id")
        .arg("1")
        .arg("--listen-client-urls")
        .arg(format!("http://127.0.0.1:{client_port}"))
        .arg("--listen-peer-urls")
        .arg(format!("http://127.0.0.1:{peer_port}"))
        .arg("--listen-metrics-url")
        .arg("")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn fastetcd");
    let url = format!("http://127.0.0.1:{client_port}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(mut kv) = KvClient::connect(url.clone()).await {
            if kv.range(RangeRequest { key: b"x".to_vec(), ..Default::default() }).await.is_ok() {
                return (child, kv);
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "fastetcd did not come up");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_data_dir_with_its_log_in_redb_upgrades_to_the_wal() {
    let dir = tempfile::tempdir().unwrap();
    legacy_node(dir.path()).await;
    // The redb file may still be held for a moment by the stopped node.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!dir.path().join("wal").exists());

    let (mut child, mut kv) = start(dir.path()).await;
    assert!(dir.path().join("wal").is_dir(), "the log moved into wal/");
    let all = kv
        .range(RangeRequest { key: b"k".to_vec(), range_end: b"l".to_vec(), ..Default::default() })
        .await
        .expect("range")
        .into_inner();
    assert_eq!(all.count, KEYS as i64, "every key written before the upgrade is there");
    kv.put(PutRequest { key: b"after".to_vec(), value: b"1".to_vec(), ..Default::default() })
        .await
        .expect("a write after the upgrade");
    child.kill().await.unwrap();
    child.wait().await.unwrap();

    // The data file no longer holds a log; the WAL does.
    let engine = RedbEngine::open(dir.path().join("fastetcd.redb")).unwrap();
    let snap = engine.snapshot().await.unwrap();
    assert!(snap.last("raft_log").await.unwrap().is_none(), "redb's raft_log was cleared");

    // And it starts again from the WAL alone.
    drop(snap);
    drop(engine);
    let (_child, mut kv) = start(dir.path()).await;
    let after = kv
        .range(RangeRequest { key: b"after".to_vec(), ..Default::default() })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(after.count, 1);
}
