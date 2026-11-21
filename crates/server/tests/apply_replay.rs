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
    // The store starts at revision 1 (#160): the last put is KEYS + 1.
    assert!(
        on_disk <= KEYS,
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
        assert_eq!(item.mod_revision, i as i64 + 2, "same revision as before the crash");
    }
    assert!(all.header.unwrap().revision > KEYS);
}

/// An offline backup of a member killed before its checkpoint carries
/// every acknowledged write (fastetcd#210): `fastetcd backup` replays the
/// WAL into the data file before copying it, so a restore, which moves
/// the WAL aside, loses none of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_offline_backup_after_sigkill_holds_every_acknowledged_put() {
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
    child.child.kill().await.unwrap();
    child.child.wait().await.unwrap();
    let on_disk = |path: std::path::PathBuf| async move {
        let engine = RedbEngine::open(path).unwrap();
        MvccStore::open(std::sync::Arc::new(engine)).await.unwrap().current_revision().await
    };
    let before = on_disk(dir.path().join("fastetcd.redb")).await;
    eprintln!("revision on disk after SIGKILL: {before} (acknowledged: {})", KEYS + 1);
    assert!(before <= KEYS, "every apply was on disk ({before}); this test no longer exercises the WAL");

    // Offline backup, then restore it into a new data dir.
    let backup = dir.path().join("offline.redb");
    let fastetcd = env!("CARGO_BIN_EXE_fastetcd");
    let out = std::process::Command::new(fastetcd)
        .arg("--data-dir")
        .arg(dir.path())
        .arg("backup")
        .arg("--out")
        .arg(&backup)
        .output()
        .unwrap();
    let said = String::from_utf8_lossy(&out.stdout).into_owned() + &String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "backup failed: {said}");
    eprintln!("{said}");
    assert_eq!(on_disk(backup.clone()).await, KEYS + 1, "the backup is at the last acknowledged put");

    let restored = tempfile::tempdir().unwrap();
    let out = std::process::Command::new(fastetcd)
        .arg("--data-dir")
        .arg(restored.path())
        .arg("restore")
        .arg(&backup)
        .output()
        .unwrap();
    assert!(out.status.success(), "restore failed: {}", String::from_utf8_lossy(&out.stderr));
    let (_child, mut kv) = start(restored.path()).await;
    let all = kv
        .range(RangeRequest { key: b"k".to_vec(), range_end: b"l".to_vec(), ..Default::default() })
        .await
        .expect("range")
        .into_inner();
    assert_eq!(all.count, KEYS, "every acknowledged put is in the restored member");
    let last = all.kvs.last().unwrap();
    assert_eq!((last.key.as_slice(), last.value.as_slice()), (format!("k{:03}", KEYS - 1).as_bytes(), format!("v{}", KEYS - 1).as_bytes()));
}
