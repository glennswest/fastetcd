//! Shared state passed into each gRPC service.

use openraft::Raft;
use tonic::Status;

use crate::auth::AuthState;
use fastetcd_raft::types::MembershipChange;
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

    /// Perform a linearizable read barrier before a local range read.
    ///
    /// etcd's default read is linearizable: it must never return state
    /// older than a write that completed before the read began. On the
    /// leader, `ensure_linearizable` confirms leadership via a heartbeat
    /// quorum and waits for the state machine to catch up to the read
    /// index. On a follower it returns `ForwardToLeader`, so we hand the
    /// whole range to the leader, which does the barrier and reads its
    /// own state machine (#10).
    ///
    /// Returns `Ok(None)` when the caller should read locally (this node
    /// is the leader and the barrier passed), or `Ok(Some(result))` when
    /// the leader already produced the result via forwarding.
    pub async fn linearize_read(
        &self,
        read: &fastetcd_raft::ForwardedRead,
    ) -> Result<Option<fastetcd_storage::mvcc::RangeResult>, Status> {
        match self.raft.ensure_linearizable().await {
            Ok(_) => Ok(None),
            Err(e) => {
                if let Some(fwd) = e.forward_to_leader::<openraft::BasicNode>() {
                    if let Some(leader_id) = fwd.leader_id {
                        return self
                            .forwarder
                            .forward_read(leader_id, read)
                            .await
                            .map(Some)
                            .map_err(|msg| {
                                Status::unavailable(format!(
                                    "forwarded linearizable read to leader {leader_id}: {msg}"
                                ))
                            });
                    }
                }
                Err(Status::unavailable(format!(
                    "linearizable read barrier: {e}"
                )))
            }
        }
    }

    /// Add a learner, forwarding to the leader if this node isn't it.
    pub async fn propose_add_learner(
        &self,
        node_id: fastetcd_raft::NodeId,
        addr: &str,
    ) -> Result<(), Status> {
        match self
            .raft
            .add_learner(node_id, openraft::BasicNode::new(addr), false)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                self.forward_membership(
                    &e,
                    MembershipChange::AddLearner {
                        node_id,
                        addr: addr.to_string(),
                    },
                )
                .await
                .unwrap_or_else(|| {
                    Err(Status::unavailable(format!("raft add_learner: {e}")))
                })
            }
        }
    }

    /// Replace the voter set, forwarding to the leader if this node
    /// isn't it.
    pub async fn propose_set_voters(
        &self,
        voters: std::collections::BTreeSet<fastetcd_raft::NodeId>,
    ) -> Result<(), Status> {
        match self.raft.change_membership(voters.clone(), false).await {
            Ok(_) => Ok(()),
            Err(e) => {
                self.forward_membership(
                    &e,
                    MembershipChange::SetVoters {
                        voters: voters.into_iter().collect(),
                    },
                )
                .await
                .unwrap_or_else(|| {
                    Err(Status::unavailable(format!("raft change_membership: {e}")))
                })
            }
        }
    }

    /// If `err` is a `ForwardToLeader` naming a leader, send `change`
    /// there and return the outcome. `None` means the error wasn't a
    /// forwardable one (or no leader is known yet), so the caller
    /// should surface its own error.
    ///
    /// etcd forwards membership changes transparently, so `etcdctl
    /// member remove` works against any endpoint; returning
    /// ForwardToLeader to the client instead is the #7 compat gap.
    async fn forward_membership<E>(
        &self,
        err: &openraft::error::RaftError<fastetcd_raft::NodeId, E>,
        change: MembershipChange,
    ) -> Option<Result<(), Status>>
    where
        E: std::error::Error
            + openraft::TryAsRef<
                openraft::error::ForwardToLeader<fastetcd_raft::NodeId, openraft::BasicNode>,
            >,
    {
        let fwd = err.forward_to_leader::<openraft::BasicNode>()?;
        let leader_id = fwd.leader_id?;
        Some(
            self.forwarder
                .forward_membership(leader_id, &change)
                .await
                .map_err(|msg| {
                    Status::unavailable(format!(
                        "forwarded membership change to leader {leader_id}: {msg}"
                    ))
                }),
        )
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
