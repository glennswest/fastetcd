//! fastetcd#231 on the real binary: the safety backup taken before a new
//! version starts holds every acknowledged write, including the ones a
//! kill left only in the raft WAL, and `fastetcd restore` of it brings
//! them all back.

use std::ffi::OsStr;
use std::sync::Arc;
use std::time::Duration;

mod spawned;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_storage::mvcc::MvccStore;
use fastetcd_storage::redb_engine::RedbEngine;

const KEYS: usize = 20;

/// Checkpoints that never come while the test runs: the data file stays
/// behind the WAL.
const NO_CHECKPOINT: [&str; 4] =
    ["--wal-checkpoint-interval-ms", "600000", "--wal-checkpoint-entries", "1000000"];

fn args<'a>(dir: &'a std::path::Path, extra: &'a [&'a str]) -> Vec<&'a OsStr> {
    let mut v = vec![OsStr::new("--data-dir"), dir.as_os_str()];
    v.extend(extra.iter().map(OsStr::new));
    v
}

/// A client once the member serves a linearizable read.
async fn serving(member: &spawned::Spawned) -> KvClient<tonic::transport::Channel> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(mut kv) = KvClient::connect(member.client_url()).await {
            if kv.range(pb::RangeRequest { key: b"x".to_vec(), ..Default::default() }).await.is_ok() {
                return kv;
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "no read in 30 s:\n{}", member.log_tail());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn data_file_revision(dir: &std::path::Path) -> i64 {
    let engine = Arc::new(RedbEngine::open(dir.join("fastetcd.redb")).unwrap());
    MvccStore::open(engine).await.unwrap().current_revision().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_upgrade_backup_keeps_writes_held_only_in_the_wal() {
    let data = tempfile::tempdir().unwrap();
    let backups = tempfile::tempdir().unwrap();
    let restored = tempfile::tempdir().unwrap();

    // 1. Acknowledged puts, then a kill before any checkpoint.
    let last = {
        let mut member = spawned::spawn_fastetcd(&args(data.path(), &NO_CHECKPOINT)).await;
        let mut kv = serving(&member).await;
        let mut last = 0;
        for i in 0..KEYS {
            let r = kv
                .put(pb::PutRequest { key: format!("k{i:02}").into_bytes(), value: b"v".to_vec(), ..Default::default() })
                .await
                .unwrap()
                .into_inner();
            last = r.header.unwrap().revision;
        }
        member.child.kill().await.unwrap();
        last
    };
    let on_disk = data_file_revision(data.path()).await;
    assert!(on_disk < last, "the data file has every write (rev {on_disk} of {last}): nothing to prove");

    // 2. As if an older version had last opened it.
    {
        let engine = Arc::new(RedbEngine::open(data.path().join("fastetcd.redb")).unwrap());
        MvccStore::open(engine).await.unwrap().write_open_version("0.0.1-test").await.unwrap();
    }

    // 3. The "new version" starts: it takes the safety backup first.
    {
        let dir = backups.path().to_str().unwrap();
        let extra = ["--upgrade-backup", "true", "--upgrade-backup-dir", dir];
        let mut member = spawned::spawn_fastetcd(&args(data.path(), &extra)).await;
        serving(&member).await;
        member.child.kill().await.unwrap();
    }
    let backup = std::fs::read_dir(backups.path())
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|x| x == "redb"))
        .expect("no safety backup taken");
    let name = backup.file_name().unwrap().to_string_lossy().into_owned();
    assert!(name.starts_with("fastetcd-0.0.1-test-to-"), "{name}");
    assert!(backup.with_extension("wal").is_dir(), "no WAL copy beside {name}");

    // 4. Restore it into a new data directory and read every key.
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_fastetcd"))
        .arg("--data-dir")
        .arg(restored.path())
        .arg("restore")
        .arg(&backup)
        .output()
        .unwrap();
    assert!(out.status.success(), "restore: {}", String::from_utf8_lossy(&out.stderr));
    let printed = String::from_utf8_lossy(&out.stdout);
    assert!(printed.contains(".wal"), "restore did not put the WAL back: {printed}");
    let member = spawned::spawn_fastetcd(&args(restored.path(), &[])).await;
    let mut kv = serving(&member).await;
    let got = kv
        .range(pb::RangeRequest { key: b"k".to_vec(), range_end: b"l".to_vec(), ..Default::default() })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.kvs.len(), KEYS, "acknowledged writes lost by the backup:\n{}", member.log_tail());
    assert!(got.header.unwrap().revision >= last);
}
