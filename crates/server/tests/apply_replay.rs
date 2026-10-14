//! Applies are not fsync'd on their own; a crash replays them from the
//! raft log (fastetcd#71), which since #85 is the WAL in `wal/`.
//!
//! The state machine commits each apply without an fsync: the next
//! background checkpoint of the data file persists it. Kill the real
//! binary with SIGKILL straight after a run of acknowledged puts, check
//! the data file really did lose the last applies (so the test
//! exercises the replay), then restart it and read every key back.

use std::path::Path;
use std::time::Duration;

mod spawned;

use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::{PutRequest, RangeRequest};
use fastetcd_storage::mvcc::MvccStore;
use fastetcd_storage::redb_engine::RedbEngine;

const KEYS: i64 = 100;

/// Start `fastetcd` on `dir` and connect a KV client to it.
async fn start(dir: &Path) -> (spawned::Spawned, KvClient<tonic::transport::Channel>) {
    // Ports it cannot lose, and its output kept for a failure (#63).
    let member = spawned::spawn_fastetcd(&[std::ffi::OsStr::new("--data-dir"), dir.as_os_str()]).await;
    let url = member.client_url();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(mut kv) = KvClient::connect(url.clone()).await {
            // Up, and a leader elected: a linearizable read succeeds.
            if kv.range(RangeRequest { key: b"x".to_vec(), ..Default::default() }).await.is_ok() {
                return (member, kv);
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "fastetcd did not come up:\n{}", member.log_tail());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acknowledged_puts_survive_sigkill_through_log_replay() {
    let dir = tempfile::tempdir().unwrap();

    let (mut child, mut kv) = start(dir.path()).await;
    for i in 0..KEYS {
        kv.put(PutRequest {
            key: format!("k{i:03}").into_bytes(),
            value: format!("v{i}").into_bytes(),
            ..Default::default()
        })
        .await
        .expect("put");
    }
    // SIGKILL: no shutdown flush, no destructor.
    child.child.kill().await.unwrap();
    child.child.wait().await.unwrap();

    // What reached the data file: the last applies were not followed by
    // a checkpoint, so they are not in it; they are in the WAL.
    let on_disk = {
        let engine = RedbEngine::open(dir.path().join("fastetcd.redb")).unwrap();
        MvccStore::open(std::sync::Arc::new(engine)).await.unwrap().current_revision().await
    };
    eprintln!("revision on disk after SIGKILL: {on_disk} (acknowledged: {KEYS})");
    assert!(
        on_disk < KEYS,
        "every apply was on disk ({on_disk}); this test no longer exercises the replay"
    );

    // Restart: the WAL replays the lost applies.
    let (_child, mut kv) = start(dir.path()).await;
    let all = kv
        .range(RangeRequest {
            key: b"k".to_vec(),
            range_end: b"l".to_vec(),
            ..Default::default()
        })
        .await
        .expect("range")
        .into_inner();
    assert_eq!(all.count, KEYS, "every acknowledged put is there after the restart");
    for (i, item) in all.kvs.iter().enumerate() {
        assert_eq!(item.key, format!("k{i:03}").into_bytes());
        assert_eq!(item.value, format!("v{i}").into_bytes());
        assert_eq!(item.mod_revision, i as i64 + 1, "same revision as before the crash");
    }
    assert!(all.header.unwrap().revision >= KEYS);
}
