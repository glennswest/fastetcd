//! SIGTERM stops a member cleanly (fastetcd#161).
//!
//! A kubelet, systemd or podman stops a member with SIGTERM. Only SIGINT
//! was handled, so every stop was a kill: the write-behind layers lost
//! from RAM and redb's file left marked dirty, so the next start walked
//! the whole file to repair it and replayed the WAL tail. Run the real
//! binary: the start after a SIGKILL reports the unclean stop (the
//! control, so the check below can see one) and, since fastetcd#176's
//! quick-repair commits, opens without walking the file; after SIGTERM
//! the member exits 0, the file opens clean and holds every acknowledged
//! write.

use std::path::Path;
use std::time::Duration;

mod spawned;

use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::{PutRequest, RangeRequest};
use fastetcd_storage::mvcc::MvccStore;
use fastetcd_storage::redb_engine::RedbEngine;

const KEYS: i64 = 100;

async fn start(dir: &Path) -> (spawned::Spawned, KvClient<tonic::transport::Channel>) {
    let member = spawned::spawn_fastetcd(&[std::ffi::OsStr::new("--data-dir"), dir.as_os_str()]).await;
    let url = member.client_url();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(mut kv) = KvClient::connect(url.clone()).await {
            if kv.range(RangeRequest { key: b"x".to_vec(), ..Default::default() }).await.is_ok() {
                return (member, kv);
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "fastetcd did not come up:\n{}", member.log_tail());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn put_keys(kv: &mut KvClient<tonic::transport::Channel>, round: i64) {
    for i in 0..KEYS {
        kv.put(PutRequest {
            key: format!("r{round}/k{i:03}").into_bytes(),
            value: format!("v{i}").into_bytes(),
            ..Default::default()
        })
        .await
        .expect("put");
    }
}

/// Open the data file as the next start would: whether redb repaired
/// it, and the revision it holds. Dropping the engine closes it cleanly.
async fn open_file(dir: &Path) -> (bool, i64) {
    let engine = RedbEngine::open(dir.join("fastetcd.redb")).unwrap();
    let repaired = engine.unclean_at_open();
    let rev = MvccStore::open(std::sync::Arc::new(engine)).await.unwrap().current_revision().await;
    (repaired, rev)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_leaves_nothing_to_repair_or_replay() {
    let dir = tempfile::tempdir().unwrap();

    // Control: a kill leaves the file not closed cleanly. Killed after a
    // checkpoint (every 100 ms), as a member running for more than a
    // moment always is.
    let (mut member, mut kv) = start(dir.path()).await;
    put_keys(&mut kv, 1).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    member.child.kill().await.unwrap();
    member.child.wait().await.unwrap();

    // SIGTERM: a clean stop.
    let (mut member, mut kv) = start(dir.path()).await;
    let log = std::fs::read_to_string(&member.log).unwrap_or_default();
    assert!(
        log.contains("data file was not closed cleanly"),
        "the start after SIGKILL reported no unclean stop: the check cannot see one:\n{}",
        member.log_tail()
    );
    assert!(
        !log.contains("redb is repairing it"),
        "the start after SIGKILL walked the whole file: the last checkpoint was not a \
         quick-repair commit (fastetcd#176):\n{}",
        member.log_tail()
    );
    put_keys(&mut kv, 2).await;
    let acknowledged = kv
        .range(RangeRequest { key: b"r".to_vec(), range_end: b"s".to_vec(), count_only: true, ..Default::default() })
        .await
        .expect("range")
        .into_inner()
        .header
        .unwrap()
        .revision;
    let pid = member.child.id().expect("pid").to_string();
    assert!(std::process::Command::new("kill").args(["-TERM", &pid]).status().unwrap().success());
    let status = tokio::time::timeout(Duration::from_secs(20), member.child.wait())
        .await
        .unwrap_or_else(|_| panic!("fastetcd did not stop within 20 s of SIGTERM:\n{}", member.log_tail()))
        .unwrap();
    let log = std::fs::read_to_string(&member.log).unwrap_or_default();
    assert!(status.success(), "fastetcd exited {status} on SIGTERM:\n{}", member.log_tail());
    assert!(log.contains("SIGTERM received"), "no SIGTERM line:\n{}", member.log_tail());
    assert!(log.contains("applied state is durable; stopped"), "no clean-stop line:\n{}", member.log_tail());

    let (repaired, on_disk) = open_file(dir.path()).await;
    assert!(!repaired, "after SIGTERM the file was still not closed cleanly:\n{}", member.log_tail());
    assert_eq!(on_disk, acknowledged, "every acknowledged write is in the data file: nothing left to replay");
    assert_eq!(acknowledged, 2 * KEYS);

    // And the member starts again with everything.
    let (member, mut kv) = start(dir.path()).await;
    let all = kv
        .range(RangeRequest { key: b"r".to_vec(), range_end: b"s".to_vec(), count_only: true, ..Default::default() })
        .await
        .expect("range")
        .into_inner();
    assert_eq!(all.count, 2 * KEYS);
    let log = std::fs::read_to_string(&member.log).unwrap_or_default();
    assert!(!log.contains("not closed cleanly"), "the start after SIGTERM repaired:\n{}", member.log_tail());
}
