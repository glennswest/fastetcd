//! Keep members older than 1.10 away from a log that holds batched
//! entries (fastetcd#77).
//!
//! From 1.10 a leader proposes `FastetcdLogEntry::Batch`, which an older
//! member cannot decode: its AppendEntries (or its own log replay, after a
//! downgrade) fails at the first one and it stops. The proposer batches
//! only once every member answers the 1.10 `ConfirmLeader` RPC, but that
//! does not cover a member added, or downgraded, after the cluster has
//! batched. The store records that it has (`MvccStore::has_batched`, set
//! by the first applied batch and carried by snapshots); with it set:
//!
//! - `MemberAdd` asks the new member `ConfirmLeader` first and refuses one
//!   that answers as older. One that does not answer yet is added: the
//!   usual order is `member add`, then start the member.
//! - `MemberPromote`, and `MemberAdd` of a voter that answers, need the
//!   member to answer as 1.10 or later: a voter that cannot read the log
//!   costs the cluster its quorum.
//! - The leader asks every member every 30 s; an older one (a downgrade)
//!   is logged as an error, naming it, and counted in
//!   `fastetcd_members_unable_to_read_batches`.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use fastetcd_raft::network::ConfirmError;
use fastetcd_raft::types::NodeId;
use tonic::Status;

use crate::state::ServerState;

const PROBE: Duration = Duration::from_secs(3);

/// What a member answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberVersion {
    /// It answered `ConfirmLeader`: 1.10 or later, with its version.
    Reads(String),
    /// It answered `Unimplemented`: older than 1.10.
    Older,
    /// It did not answer.
    Unreachable(String),
}

pub async fn member_version(state: &ServerState, id: NodeId) -> MemberVersion {
    match state.forwarder.confirm_leader(id, 0, PROBE).await {
        Ok(r) => MemberVersion::Reads(r.version),
        Err(ConfirmError::Older) => MemberVersion::Older,
        Err(ConfirmError::Unreachable(e)) => MemberVersion::Unreachable(e),
    }
}

fn older(id: NodeId) -> Status {
    Status::failed_precondition(format!(
        "member {id:x} runs a fastetcd older than 1.10 (it does not answer ConfirmLeader): this \
         cluster's log holds batched entries it cannot read, so it would stop at the first one \
         (fastetcd#77). Upgrade it to 1.10 or later first."
    ))
}

/// MemberAdd: refuse a member that answers as older, once the log has
/// batched. One that does not answer yet is allowed (it is usually started
/// after `member add`), with a warning.
pub async fn guard_add(state: &ServerState, id: NodeId) -> Result<(), Status> {
    if !state.sm.mvcc().has_batched() {
        return Ok(());
    }
    match member_version(state, id).await {
        MemberVersion::Older => Err(older(id)),
        MemberVersion::Reads(_) => Ok(()),
        MemberVersion::Unreachable(e) => {
            tracing::warn!(
                member = format_args!("{id:x}"),
                error = %e,
                "adding a member this cluster cannot reach yet: its version is unknown, and this \
                 log holds batched entries a member older than 1.10 cannot read (#77); it is \
                 checked again before it is promoted, and every 30 s"
            );
            Ok(())
        }
    }
}

/// MemberPromote (and MemberAdd of a voter): the member must answer as
/// 1.10 or later once the log has batched.
pub async fn guard_voter(state: &ServerState, id: NodeId) -> Result<(), Status> {
    if !state.sm.mvcc().has_batched() {
        return Ok(());
    }
    match member_version(state, id).await {
        MemberVersion::Reads(_) => Ok(()),
        MemberVersion::Older => Err(older(id)),
        MemberVersion::Unreachable(e) => Err(Status::failed_precondition(format!(
            "member {id:x} cannot be reached to confirm it runs fastetcd 1.10 or later ({e}): this \
             cluster's log holds batched entries an older member cannot read (#77). Promote it \
             once it is up."
        ))),
    }
}

/// The members (but this one) that answer as older than 1.10.
pub async fn older_members(state: &ServerState) -> Vec<NodeId> {
    let ids: Vec<NodeId> = {
        let m = state.raft.metrics().borrow().clone();
        m.membership_config.membership().nodes().map(|(id, _)| *id).filter(|id| *id != m.id).collect()
    };
    let mut out = Vec::new();
    for id in ids {
        if member_version(state, id).await == MemberVersion::Older {
            out.push(id);
        }
    }
    out
}

/// On the leader, every 30 s once the log has batched: report members
/// that cannot read it (a downgrade, or one added while unreachable).
pub fn spawn_watch(state: Arc<ServerState>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let leading = state.raft.metrics().borrow().current_leader == Some(state.member_id);
            if !leading || !state.sm.mvcc().has_batched() {
                state.older_members.store(0, Ordering::Relaxed);
                continue;
            }
            let older = older_members(&state).await;
            for id in &older {
                tracing::error!(
                    member = format_args!("{id:x}"),
                    "member runs a fastetcd older than 1.10 but this cluster's log holds batched \
                     entries: it cannot read them and stops at the first one, and a voter that \
                     stops costs the quorum (#77). Upgrade it, or remove it and add it back empty."
                );
            }
            state.older_members.store(older.len() as u64, Ordering::Relaxed);
        }
    });
}
