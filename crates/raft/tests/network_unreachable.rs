//! fastetcd#226: a member that cannot be reached is reported to openraft
//! as `Unreachable`, so replication to it backs off instead of retrying
//! about every millisecond. (A member stopped behind a connection already
//! made is `tests/replication_backoff.sh`.)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use fastetcd_raft::network::{unreachable_backoff, GrpcNetworkFactory};
use fastetcd_raft::types::TypeConfig;
use openraft::error::RPCError;
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::AppendEntriesRequest;
use tokio::sync::RwLock;

fn heartbeat() -> AppendEntriesRequest<TypeConfig> {
    AppendEntriesRequest { vote: Default::default(), prev_log_id: None, entries: vec![], leader_commit: None }
}

/// A local port with nothing listening on it.
fn closed_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

#[tokio::test]
async fn a_member_nothing_listens_for_is_unreachable() {
    let mut factory = GrpcNetworkFactory::new(Arc::new(RwLock::new(HashMap::new())));
    let url = format!("http://127.0.0.1:{}", closed_port());
    let mut net = factory.new_client(3, &openraft::BasicNode::new(&url)).await;
    let option = RPCOption::new(Duration::from_secs(2));
    for attempt in 0..2 {
        // The first dials; the second goes through the same handle.
        match net.append_entries(heartbeat(), option.clone()).await {
            Err(RPCError::Unreachable(_)) => {}
            other => panic!("attempt {attempt}: want Unreachable, got {other:?}"),
        }
    }
    let waits: Vec<u64> = unreachable_backoff().take(6).map(|d| d.as_millis() as u64).collect();
    assert_eq!(waits, [100, 200, 400, 500, 500, 500]);
}
