//! What etcd's robustness suite needs from a fastetcd member (#195),
//! run on the real binary with etcd's flags: the `ready to serve client
//! requests` line it waits for, `--snapshot-catchup-entries` and
//! `--experimental-watch-progress-notify-interval`, and
//! `fastetcd persisted-requests` read from a running member whose log
//! snapshots are purging (`--wal-keep-purged`).

use std::ffi::OsStr;
use std::time::Duration;

use base64::Engine;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

mod spawned;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::lease_client::LeaseClient;
use fastetcd_proto::etcdserverpb::watch_client::WatchClient;

const KEYS: usize = 60;

fn b64(s: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(s)
}

fn put(key: &str, lease: i64) -> pb::PutRequest {
    pb::PutRequest { key: key.as_bytes().to_vec(), value: b"v".to_vec(), lease, ..Default::default() }
}

fn range_op(key: &str) -> pb::RequestOp {
    pb::RequestOp {
        request: Some(pb::request_op::Request::RequestRange(pb::RangeRequest {
            key: key.as_bytes().to_vec(),
            ..Default::default()
        })),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_running_member_serves_the_robustness_suite() {
    let dir = tempfile::tempdir().unwrap();
    let member = spawned::spawn_fastetcd(&[
        OsStr::new("--data-dir"),
        dir.path().as_os_str(),
        OsStr::new("--wal-keep-purged"),
        OsStr::new("--snapshot-count=20"),
        OsStr::new("--snapshot-catchup-entries=0"),
        OsStr::new("--experimental-watch-progress-notify-interval=100ms"),
    ])
    .await;

    // etcd's ready line, once the member serves with a leader.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !std::fs::read_to_string(&member.log).unwrap_or_default().contains("ready to serve client requests") {
        assert!(tokio::time::Instant::now() < deadline, "no ready line:\n{}", member.log_tail());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let url = member.client_url();
    let mut kv = KvClient::connect(url.clone()).await.unwrap();
    let mut lease = LeaseClient::connect(url.clone()).await.unwrap();

    // A progress notification within a second: the interval is 100 ms,
    // not the default 10 s.
    let mut watch = WatchClient::connect(url.clone()).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    tx.send(pb::WatchRequest {
        request_union: Some(pb::watch_request::RequestUnion::CreateRequest(pb::WatchCreateRequest {
            key: b"progress".to_vec(),
            progress_notify: true,
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let mut stream = watch.watch(ReceiverStream::new(rx)).await.unwrap().into_inner();
    assert!(stream.next().await.unwrap().unwrap().created);
    let notify = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("no progress notification within 1 s of a 100 ms interval")
        .unwrap()
        .unwrap();
    assert!(notify.events.is_empty() && !notify.created && !notify.canceled, "{notify:?}");

    // Traffic: puts (enough for snapshots every 20), a lease, a txn, a
    // read-only txn, a delete, a compaction.
    let mut first_rev = 0;
    for i in 0..KEYS {
        let rev = kv.put(put(&format!("k{i:03}"), 0)).await.unwrap().into_inner().header.unwrap().revision;
        if i == 0 {
            first_rev = rev;
        }
    }
    let granted = lease.lease_grant(pb::LeaseGrantRequest { ttl: 60, id: 0 }).await.unwrap().into_inner().id;
    kv.put(put("leased", granted)).await.unwrap();
    let txn = kv
        .txn(pb::TxnRequest {
            compare: vec![pb::Compare {
                key: b"k000".to_vec(),
                result: pb::compare::CompareResult::Equal as i32,
                target: pb::compare::CompareTarget::Mod as i32,
                target_union: Some(pb::compare::TargetUnion::ModRevision(first_rev)),
                ..Default::default()
            }],
            success: vec![pb::RequestOp { request: Some(pb::request_op::Request::RequestPut(put("t", 0))) }],
            failure: vec![range_op("t")],
        })
        .await
        .unwrap()
        .into_inner();
    assert!(txn.succeeded);
    kv.txn(pb::TxnRequest { success: vec![range_op("k000")], ..Default::default() }).await.unwrap();
    kv.delete_range(pb::DeleteRangeRequest { key: b"k001".to_vec(), ..Default::default() }).await.unwrap();
    kv.compact(pb::CompactionRequest { revision: first_rev, physical: false }).await.unwrap();

    // Read while the member runs, as the suite does.
    let out = tokio::process::Command::new(env!("CARGO_BIN_EXE_fastetcd"))
        .arg("--data-dir")
        .arg(dir.path())
        .arg("persisted-requests")
        .output()
        .await
        .unwrap();
    assert!(out.status.success(), "persisted-requests failed:\n{}", String::from_utf8_lossy(&out.stderr));
    let requests: Vec<Value> = String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();

    let kind = |r: &Value| r.as_object().unwrap().keys().next().unwrap().clone();
    let puts: Vec<&Value> = requests.iter().filter(|r| kind(r) == "put").collect();
    assert_eq!(puts.len(), KEYS + 1, "{requests:?}");
    assert_eq!(puts[0]["put"]["key"], b64(b"k000"), "from the first write");
    assert_eq!(puts[KEYS]["put"]["key"], b64(b"leased"));
    assert_eq!(puts[KEYS]["put"]["lease"], granted);
    let grant_at = requests.iter().position(|r| kind(r) == "lease_grant").unwrap();
    assert_eq!(requests[grant_at]["lease_grant"]["id"], granted, "the id the client was given");
    assert!(grant_at < requests.iter().position(|r| r == puts[KEYS]).unwrap());
    let txns: Vec<&Value> = requests.iter().filter(|r| kind(r) == "txn").collect();
    assert_eq!(txns.len(), 1, "the read-only txn is left out: {txns:?}");
    assert_eq!(txns[0]["txn"]["compare"][0]["target"], "mod");
    assert_eq!(txns[0]["txn"]["compare"][0]["value"], first_rev);
    assert_eq!(requests.iter().filter(|r| kind(r) == "delete").count(), 1);
    assert_eq!(requests.last().unwrap()["compact"]["revision"], first_rev);
    drop(member);
}
