//! Shared state passed into each gRPC service.

use openraft::Raft;
use tonic::Status;

use crate::auth::AuthState;
use fastetcd_raft::{
    FastetcdLogEntry, FastetcdLogResponse, FastetcdStateMachine, TypeConfig, WriteForwarder,
};

/// Bundle of handles every gRPC service needs: the Raft node (for
/// proposing writes), the state machine (for direct reads and
/// snapshots), and stable identifiers (`cluster_id`, `member_id`)
/// that go into every `ResponseHeader`.
#[derive(Clone)]
pub struct ServerState {
    pub raft: Raft<TypeConfig>,
    pub sm: FastetcdStateMachine,
    pub cluster_id: u64,
    pub member_id: u64,
    pub auth: AuthState,
    pub forwarder: WriteForwarder,
}

impl ServerState {
    pub fn new(
        raft: Raft<TypeConfig>,
        sm: FastetcdStateMachine,
        cluster_id: u64,
        member_id: u64,
        auth: AuthState,
        forwarder: WriteForwarder,
    ) -> Self {
        Self {
            raft,
            sm,
            cluster_id,
            member_id,
            auth,
            forwarder,
        }
    }

    /// Fetch the current raft term — used for `ResponseHeader.raft_term`.
    pub async fn current_term(&self) -> u64 {
        self.raft.metrics().borrow().current_term
    }

    /// Propose a write. If this node isn't the raft leader, openraft's
    /// `client_write` fails with `ForwardToLeader` — rather than
    /// surfacing that to the caller (who has no way to act on it; see
    /// #4), hand the same entry off to the leader over the peer
    /// channel via `forwarder` and return its result as if it had
    /// been applied locally.
    pub async fn propose(
        &self,
        entry: FastetcdLogEntry,
    ) -> Result<FastetcdLogResponse, Status> {
        match self.raft.client_write(entry.clone()).await {
            Ok(w) => Ok(w.data),
            Err(e) => {
                if let Some(fwd) = e.forward_to_leader::<openraft::BasicNode>() {
                    if let Some(leader_id) = fwd.leader_id {
                        return self.forwarder.forward(leader_id, &entry).await.map_err(|msg| {
                            Status::unavailable(format!(
                                "forwarded write to leader {leader_id}: {msg}"
                            ))
                        });
                    }
                }
                Err(Status::unavailable(format!("raft client_write: {e}")))
            }
        }
    }
}

/// Build a `ResponseHeader` with the current cluster/member/raft_term
/// and the given revision.
pub async fn response_header(
    state: &ServerState,
    revision: i64,
) -> fastetcd_proto::etcdserverpb::ResponseHeader {
    fastetcd_proto::etcdserverpb::ResponseHeader {
        cluster_id: state.cluster_id,
        member_id: state.member_id,
        revision,
        raft_term: state.current_term().await,
    }
}
