//! fastetcd#211's live proof on the real binary: a member logs the
//! addresses raft dials, from the replicated membership, once it has one
//! and again after a restart, so the current container's log always shows
//! it.

use std::ffi::OsStr;
use std::time::Duration;

mod spawned;

const LINE: &str = "routes follow the replicated membership (fastetcd#211)";

async fn wait_for_line(member: &spawned::Spawned) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let text = std::fs::read_to_string(&member.log).unwrap_or_default();
        if let Some(line) = text.lines().find(|l| l.contains(LINE)) {
            return line.to_string();
        }
        assert!(tokio::time::Instant::now() < deadline, "no {LINE:?} line:\n{}", member.log_tail());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_member_logs_the_membership_it_dials_at_start_and_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let args = [OsStr::new("--data-dir"), dir.path().as_os_str()];
    {
        let mut member = spawned::spawn_fastetcd(&args).await;
        let line = wait_for_line(&member).await;
        assert!(line.contains("members="), "no members field: {line}");
        member.child.kill().await.unwrap();
    }
    let member = spawned::spawn_fastetcd(&args).await;
    let line = wait_for_line(&member).await;
    assert!(line.contains("members="), "no members field: {line}");
}
