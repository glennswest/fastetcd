//! A start logs how long each of its phases took (fastetcd#165).

use std::time::Duration;

mod spawned;

use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::{PutRequest, RangeRequest};

const PHASES: [&str; 8] = [
    "data_file_open",
    "index_load",
    "state_machine_open",
    "upgrade_backup",
    "wal_open",
    "format_recovery",
    "raft_start",
    "listening",
];

async fn start_and_read_log(dir: &std::path::Path) -> (spawned::Spawned, String) {
    let member = spawned::spawn_fastetcd(&[std::ffi::OsStr::new("--data-dir"), dir.as_os_str()]).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(mut kv) = KvClient::connect(member.client_url()).await {
            if kv.range(RangeRequest { key: b"x".to_vec(), ..Default::default() }).await.is_ok() {
                kv.put(PutRequest { key: b"k".to_vec(), value: b"v".to_vec(), ..Default::default() })
                    .await
                    .unwrap();
                break;
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "fastetcd did not come up:\n{}", member.log_tail());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // raft_ready is logged from a task: give it a moment.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let log = std::fs::read_to_string(&member.log).unwrap_or_default();
        if log.contains("raft_ready") || tokio::time::Instant::now() > deadline {
            return (member, log);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn check_phases(log: &str, tail: &str) {
    let mut at = 0;
    for phase in PHASES {
        let Some(found) = log[at..].find(&format!("\"{phase}\"")).or_else(|| log[at..].find(phase)) else {
            panic!("startup phase {phase} not logged after the previous one:\n{tail}");
        };
        at += found + phase.len();
    }
    assert!(log[at..].contains("startup: listening"), "no summary line after the phases:\n{tail}");
    let summary = log.lines().find(|l| l.contains("startup: listening")).unwrap();
    for phase in PHASES {
        assert!(summary.contains(&format!("{phase}=")), "summary lacks {phase}: {summary}");
    }
    assert!(log.contains("raft_ready"), "no raft_ready line:\n{tail}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_start_logs_each_phase_and_when_raft_is_ready() {
    let dir = tempfile::tempdir().unwrap();
    // A new store, then a restart on it (a WAL and an index to load).
    for _ in 0..2 {
        let (mut member, log) = start_and_read_log(dir.path()).await;
        check_phases(&log, &member.log_tail());
        member.child.kill().await.unwrap();
        member.child.wait().await.unwrap();
    }
}
