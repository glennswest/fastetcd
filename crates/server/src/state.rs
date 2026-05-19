//! Shared state passed into each gRPC service.

use openraft::Raft;

use crate::auth::AuthState;
use fastetcd_raft::{FastetcdStateMachine, TypeConfig};

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
}

impl ServerState {
    pub fn new(
        raft: Raft<TypeConfig>,
        sm: FastetcdStateMachine,
        cluster_id: u64,
        member_id: u64,
        auth: AuthState,
    ) -> Self {
        Self {
            raft,
            sm,
            cluster_id,
            member_id,
            auth,
        }
    }

    /// Fetch the current raft term — used for `ResponseHeader.raft_term`.
    pub async fn current_term(&self) -> u64 {
        self.raft.metrics().borrow().current_term
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
