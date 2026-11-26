//! fastetcd#160's live proof on the real binary: the pod's log says
//! `this store starts at revision 1` when a first write moves a new
//! store (not only when the leader's background proposer does), and
//! again after a restart, so the current container's log always shows it.

use std::ffi::OsStr;
use std::time::Duration;

mod spawned;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;

const LINE: &str = "this store starts at revision 1, as etcd's (fastetcd#160)";

async fn wait_for_line(member: &spawned::Spawned, line: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !std::fs::read_to_string(&member.log).unwrap_or_default().contains(line) {
        assert!(tokio::time::Instant::now() < deadline, "no {line:?} line:\n{}", member.log_tail());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A put, retried until the lone member has elected itself.
async fn first_put(member: &spawned::Spawned) -> i64 {
    let mut kv = KvClient::connect(member.client_url()).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match kv.put(pb::PutRequest { key: b"k".to_vec(), value: b"v".to_vec(), ..Default::default() }).await {
            Ok(r) => return r.into_inner().header.unwrap().revision,
            Err(e) => {
                assert!(tokio::time::Instant::now() < deadline, "no put: {e}\n{}", member.log_tail());
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_start_revision_is_in_the_log_after_a_first_write_and_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let args = [OsStr::new("--data-dir"), dir.path().as_os_str()];
    {
        let mut member = spawned::spawn_fastetcd(&args).await;
        assert_eq!(first_put(&member).await, 2, "the first write of a new store, as etcd's");
        wait_for_line(&member, LINE).await;
        member.child.kill().await.unwrap();
    }
    let member = spawned::spawn_fastetcd(&args).await;
    wait_for_line(&member, LINE).await;
}
