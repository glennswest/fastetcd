//! A live snapshot restores (fastetcd#61), with the real binary.
//!
//! `Maintenance.Snapshot` (`etcdctl snapshot save`, `fastetcd-ctl
//! snapshot-save`) used to stream the raft snapshot body, which nothing
//! could restore. It now streams a backup file of every table. A member
//! writes keys, a lease with a key on it and a user, a snapshot is taken
//! over gRPC right after the last write (still in the write-behind layer,
//! not yet in the data file), `fastetcd restore` writes it into a new
//! data dir, and a member started there has all of it.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use tokio_stream::StreamExt;
use tonic::transport::Channel;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::auth_client::AuthClient;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::lease_client::LeaseClient;
use fastetcd_proto::etcdserverpb::maintenance_client::MaintenanceClient;

const KEYS: usize = 30;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

struct Member {
    child: tokio::process::Child,
    channel: Channel,
}

/// Start the real binary on `dir`. Its ports are picked free and could
/// be taken before it binds them (#63), so a child that exits early is
/// started again on new ports; its stderr is kept for the failure.
async fn start(dir: &Path) -> Member {
    let log = dir.with_extension("log");
    for _attempt in 0..3 {
        let (client, peer) = (free_port(), free_port());
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_fastetcd"))
            .arg("--data-dir")
            .arg(dir)
            .args(["--node-id", "1", "--listen-metrics-url", ""])
            .arg(format!("--listen-client-urls=http://127.0.0.1:{client}"))
            .arg(format!("--listen-peer-urls=http://127.0.0.1:{peer}"))
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(&log).unwrap())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn fastetcd");
        let url = format!("http://127.0.0.1:{client}");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        while tokio::time::Instant::now() < deadline {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            if let Ok(channel) = Channel::from_shared(url.clone()).unwrap().connect().await {
                let mut kv = KvClient::new(channel.clone());
                if kv.range(pb::RangeRequest { key: b"x".to_vec(), ..Default::default() }).await.is_ok() {
                    return Member { child, channel };
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let _ = child.kill().await;
    }
    panic!(
        "fastetcd did not come up on {}; its log:\n{}",
        dir.display(),
        std::fs::read_to_string(&log).unwrap_or_default()
    );
}

async fn snapshot_to(channel: &Channel, path: &Path) -> u64 {
    let mut m = MaintenanceClient::new(channel.clone());
    let mut stream = m.snapshot(pb::SnapshotRequest {}).await.unwrap().into_inner();
    let mut bytes = Vec::new();
    let mut last_remaining = None;
    while let Some(msg) = stream.next().await {
        let msg = msg.expect("snapshot chunk");
        bytes.extend_from_slice(&msg.blob);
        last_remaining = Some(msg.remaining_bytes);
    }
    assert_eq!(last_remaining, Some(0), "the last chunk says nothing remains");
    std::fs::write(path, &bytes).unwrap();
    bytes.len() as u64
}

fn restore(data_dir: &Path, file: &Path) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_fastetcd"))
        .arg("--data-dir")
        .arg(data_dir)
        .arg("restore")
        .arg(file)
        .output()
        .expect("run fastetcd restore")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_snapshot_restores_into_a_new_data_dir() {
    let tmp = tempfile::tempdir().unwrap();
    let (a, b): (PathBuf, PathBuf) = (tmp.path().join("a"), tmp.path().join("b"));
    let file = tmp.path().join("snap.db");

    let mut m = start(&a).await;
    let mut kv = KvClient::new(m.channel.clone());
    for i in 0..KEYS {
        kv.put(pb::PutRequest {
            key: format!("k{i:03}").into_bytes(),
            value: format!("v{i}").into_bytes(),
            ..Default::default()
        })
        .await
        .unwrap();
    }
    let lease = LeaseClient::new(m.channel.clone())
        .lease_grant(pb::LeaseGrantRequest { ttl: 600, id: 0 })
        .await
        .unwrap()
        .into_inner()
        .id;
    kv.put(pb::PutRequest { key: b"leased".to_vec(), value: b"l".to_vec(), lease, ..Default::default() })
        .await
        .unwrap();
    AuthClient::new(m.channel.clone())
        .user_add(pb::AuthUserAddRequest {
            name: "alice".into(),
            password: "pw".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    // The last write, then the snapshot at once: the write is still in
    // the write-behind layer's RAM, not in the data file.
    let last_rev = kv
        .put(pb::PutRequest { key: b"last".to_vec(), value: b"z".to_vec(), ..Default::default() })
        .await
        .unwrap()
        .into_inner()
        .header
        .unwrap()
        .revision;
    let size = snapshot_to(&m.channel, &file).await;

    let header = fastetcd_server::backup::verify(&file).expect("a checksummed backup file");
    assert_eq!(header.revision, last_rev, "the snapshot holds the last write");
    assert_eq!(header.node_id, 1);
    eprintln!("snapshot: {size} bytes at revision {}", header.revision);
    m.child.kill().await.unwrap();

    // One flipped byte is refused by its checksum, and nothing written.
    let mut bad = std::fs::read(&file).unwrap();
    let mid = bad.len() / 2;
    bad[mid] ^= 0x40;
    let bad_file = tmp.path().join("bad.db");
    std::fs::write(&bad_file, bad).unwrap();
    let c = tmp.path().join("c");
    let out = restore(&c, &bad_file);
    assert!(!out.status.success(), "a damaged snapshot was restored");
    assert!(String::from_utf8_lossy(&out.stderr).contains("checksum"), "{out:?}");
    assert!(!c.join("fastetcd.redb").exists());

    let out = restore(&b, &file);
    assert!(
        out.status.success(),
        "fastetcd restore failed: {}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let m = start(&b).await;
    let mut kv = KvClient::new(m.channel.clone());
    let all = kv
        .range(pb::RangeRequest { key: b"k".to_vec(), range_end: b"l".to_vec(), ..Default::default() })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(all.count, KEYS as i64);
    let r = kv
        .range(pb::RangeRequest { key: b"last".to_vec(), ..Default::default() })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.kvs.len(), 1, "the write made just before the snapshot");
    assert!(r.header.unwrap().revision >= last_rev);
    let leased = kv
        .range(pb::RangeRequest { key: b"leased".to_vec(), ..Default::default() })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(leased.kvs[0].lease, lease);
    let ttl = LeaseClient::new(m.channel.clone())
        .lease_time_to_live(pb::LeaseTimeToLiveRequest { id: lease, keys: true })
        .await
        .unwrap()
        .into_inner();
    assert!(ttl.ttl > 0, "the lease came back: {ttl:?}");
    assert_eq!(ttl.keys, vec![b"leased".to_vec()]);
    let users = AuthClient::new(m.channel.clone())
        .user_list(pb::AuthUserListRequest {})
        .await
        .unwrap()
        .into_inner()
        .users;
    assert!(users.contains(&"alice".to_string()), "{users:?}");
    // And it takes writes.
    kv.put(pb::PutRequest { key: b"after".to_vec(), value: b"1".to_vec(), ..Default::default() })
        .await
        .unwrap();
}

#[test]
fn a_damaged_snapshot_or_an_old_format_one_is_refused_clearly() {
    let tmp = tempfile::tempdir().unwrap();
    // What 1.14.1 and earlier streamed: a raft snapshot body, which is
    // neither a backup nor a data file.
    let old = tmp.path().join("old.db");
    std::fs::write(&old, vec![7u8; 697]).unwrap();
    let out = restore(&tmp.path().join("d1"), &old);
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("fastetcd#61") && err.contains("fastetcd-migrate"), "{err}");
    assert!(!tmp.path().join("d1").join("fastetcd.redb").exists());
}
