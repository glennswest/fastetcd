//! fastetcd#220 on the real binary: a lone member switches to counting
//! puts by itself, a put after a compaction gets etcd's version, and the
//! rule holds across a restart (marker persisted, log replayed).

use std::ffi::OsStr;
use std::time::Duration;

mod spawned;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;

const SWITCHED: &str = "a put's version counts puts, as etcd's";

async fn wait_for_line(member: &spawned::Spawned, line: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !std::fs::read_to_string(&member.log).unwrap_or_default().contains(line) {
        assert!(tokio::time::Instant::now() < deadline, "no {line:?} line:\n{}", member.log_tail());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn put(kv: &mut KvClient<tonic::transport::Channel>, v: &str) -> i64 {
    let r = kv
        .put(pb::PutRequest { key: b"k".to_vec(), value: v.into(), ..Default::default() })
        .await
        .unwrap()
        .into_inner();
    r.header.unwrap().revision
}

async fn version(kv: &mut KvClient<tonic::transport::Channel>) -> i64 {
    let r = kv.range(pb::RangeRequest { key: b"k".to_vec(), ..Default::default() }).await.unwrap().into_inner();
    r.kvs[0].version
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_versions_a_put_after_a_compaction_gets_etcds_version() {
    let dir = tempfile::tempdir().unwrap();
    let args = [OsStr::new("--data-dir"), dir.path().as_os_str()];
    {
        let member = spawned::spawn_fastetcd(&args).await;
        wait_for_line(&member, SWITCHED).await;
        let mut kv = KvClient::connect(member.client_url()).await.unwrap();
        let mut revs = Vec::new();
        for v in ["1", "2", "3"] {
            revs.push(put(&mut kv, v).await);
        }
        assert_eq!(version(&mut kv).await, 3);
        kv.compact(pb::CompactionRequest { revision: revs[1], physical: true }).await.unwrap();
        let rev = put(&mut kv, "4").await;
        assert_eq!(version(&mut kv).await, 4);
        // Compare(Version) sees it.
        let txn = kv
            .txn(pb::TxnRequest {
                compare: vec![pb::Compare {
                    key: b"k".to_vec(),
                    result: pb::compare::CompareResult::Equal as i32,
                    target: pb::compare::CompareTarget::Version as i32,
                    target_union: Some(pb::compare::TargetUnion::Version(4)),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert!(txn.succeeded, "Compare(Version == 4) failed");
        kv.compact(pb::CompactionRequest { revision: rev, physical: true }).await.unwrap();
    }
    // Restarted: the WAL replays the same versions and the rule holds.
    let member = spawned::spawn_fastetcd(&args).await;
    let mut kv = KvClient::connect(member.client_url()).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match kv.range(pb::RangeRequest { key: b"k".to_vec(), ..Default::default() }).await {
            Ok(r) => {
                assert_eq!(r.into_inner().kvs[0].version, 4, "after the restart");
                break;
            }
            Err(e) => {
                assert!(tokio::time::Instant::now() < deadline, "no read after the restart: {e}\n{}", member.log_tail());
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    put(&mut kv, "5").await;
    assert_eq!(version(&mut kv).await, 5, "{}", member.log_tail());
}
