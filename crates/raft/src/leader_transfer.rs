//! MoveLeader: hand leadership to another voter (fastetcd#57).
//!
//! openraft 0.9 has no leader transfer, and its voters refuse every vote
//! request while the leader's lease is live, so the target cannot simply
//! elect. The vendored openraft takes `Trigger::accept_leader_transfer`
//! (`vendor/openraft/FASTETCD.md`): a member whose vote is still the
//! leader's stops refusing votes by that vote's lease, and the target, if
//! its log holds the leader's last log id, elects. The rest is here, as
//! etcd's `MoveLeader` and openraft 0.10's `transfer_leader` do it:
//!
//! 1. only the leader moves leadership, only to a voter;
//! 2. hold new proposals and wait until the target's log matches the
//!    leader's last log id (so the target can win, and nothing the old
//!    leader acknowledged is missing from the new one's log);
//! 3. `TransferLeader` to every other voter, and accept it locally; a
//!    quorum (per joint config) with the target must accept, else stop
//!    before the target is asked (a member older than 1.36 answers
//!    `Unimplemented` and keeps its lease, so it does not count);
//! 4. `TransferLeader` to the target, which elects; wait until it leads.
//!
//! The election is an ordinary one: it is granted only by members whose
//! logs are no newer than the target's, and the waiver covers only the
//! transferring leader's vote, for twice the lease.

use std::collections::BTreeSet;
use std::time::Duration;

use openraft::{LogId, Raft, ServerState, Vote};
use tokio::time::Instant;

use crate::network::{ConfirmError, WriteForwarder};
use crate::proposer::Proposer;
use crate::types::{NodeId, TransferLeaderRequest, TypeConfig};

/// How long one member gets to answer `TransferLeader`.
const ASK: Duration = Duration::from_secs(2);

/// Why MoveLeader did not move leadership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferError {
    /// This member is not the leader (etcd's `ErrNotLeader`).
    NotLeader,
    /// The target is not a voter (etcd's `ErrBadLeaderTransferee`).
    BadTransferee(String),
    /// Too few members accepted the transfer for the target to win.
    NoQuorum(String),
    /// The target did not catch up, or did not become leader, in time
    /// (etcd's `ErrTimeoutLeaderTransfer`).
    TimedOut(String),
    /// openraft stopped.
    Fatal(String),
}

impl std::fmt::Display for TransferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransferError::NotLeader => write!(f, "not leader"),
            TransferError::BadTransferee(m)
            | TransferError::NoQuorum(m)
            | TransferError::TimedOut(m)
            | TransferError::Fatal(m) => write!(f, "{m}"),
        }
    }
}

fn fatal(e: impl std::fmt::Display) -> TransferError {
    TransferError::Fatal(e.to_string())
}

/// This member's vote and last log id, as RaftCore last published them.
fn vote_and_last(raft: &Raft<TypeConfig>) -> (Vote<NodeId>, Option<LogId<NodeId>>) {
    let vote = raft.server_metrics().borrow().vote;
    let last = raft.data_metrics().borrow().last_log;
    (vote, last)
}

/// The log id `target` is known to hold, from the leader's replication.
fn matched(raft: &Raft<TypeConfig>, target: NodeId) -> Option<LogId<NodeId>> {
    let m = raft.metrics().borrow().clone();
    m.replication.as_ref().and_then(|r| r.get(&target).cloned()).flatten()
}

/// Move leadership from this member to `target` within `timeout`.
pub async fn move_leader(
    raft: &Raft<TypeConfig>,
    forwarder: &WriteForwarder,
    proposer: Option<&Proposer>,
    target: NodeId,
    timeout: Duration,
) -> Result<(), TransferError> {
    let deadline = Instant::now() + timeout;
    let m = raft.metrics().borrow().clone();
    let me = m.id;
    if m.state != ServerState::Leader || m.current_leader != Some(me) {
        return Err(TransferError::NotLeader);
    }
    if target == me {
        return Ok(());
    }
    let membership = m.membership_config.membership().clone();
    let voters: BTreeSet<NodeId> = membership.voter_ids().collect();
    if !voters.contains(&target) {
        return Err(TransferError::BadTransferee(format!("member {target:x} is not a voter")));
    }

    tracing::info!(to = format_args!("{target:x}"), "leader transfer starting");
    let _hold = match proposer {
        Some(p) => Some(p.hold().await),
        None => None,
    };

    // Wait for the target to hold everything this leader has appended;
    // look again once it does, for what was in flight when proposals
    // were held.
    let _ = raft.trigger().heartbeat().await;
    let (vote, last) = loop {
        let (vote, last) = vote_and_last(raft);
        if !vote.is_committed() || vote.leader_id().voted_for() != Some(me) {
            return Err(TransferError::NotLeader);
        }
        while matched(raft, target) < last {
            if Instant::now() >= deadline {
                return Err(TransferError::TimedOut(format!(
                    "member {target:x} did not catch up with the leader's log ({last:?}) in time"
                )));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // Let RaftCore publish what was in flight, then look again.
        tokio::time::sleep(Duration::from_millis(20)).await;
        if vote_and_last(raft) == (vote, last) {
            break (vote, last);
        }
    };

    let req = TransferLeaderRequest { leader_vote: vote, to: target, last_log_id: last };

    // Every voter but the target first, so their leases are waived before
    // the target asks for their votes.
    let mut accepted: BTreeSet<NodeId> = BTreeSet::from([target]);
    let mut refused: Vec<String> = Vec::new();
    if accept(raft, req.clone()).await.map_err(fatal)? {
        accepted.insert(me);
    } else {
        return Err(TransferError::NotLeader);
    }
    let others: Vec<NodeId> = voters.iter().copied().filter(|v| *v != me && *v != target).collect();
    let mut asks = tokio::task::JoinSet::new();
    for v in others {
        let (forwarder, req) = (forwarder.clone(), req.clone());
        asks.spawn(async move { (v, forwarder.transfer_leader(v, &req, ASK).await) });
    }
    while let Some(joined) = asks.join_next().await {
        let (v, answer) = joined.map_err(fatal)?;
        match answer {
            Ok(true) => {
                accepted.insert(v);
            }
            Ok(false) => refused.push(format!("{v:x}: its vote is not this leader's")),
            Err(ConfirmError::Older) => refused.push(format!("{v:x}: older than fastetcd 1.36")),
            Err(ConfirmError::Unreachable(e)) => refused.push(format!("{v:x}: {e}")),
        }
    }
    let quorum = membership
        .get_joint_config()
        .iter()
        .all(|config| config.iter().filter(|v| accepted.contains(v)).count() * 2 > config.len());
    if !quorum {
        return Err(TransferError::NoQuorum(format!(
            "too few members accepted the leader transfer for {target:x} to be elected: {}",
            refused.join("; ")
        )));
    }

    let remaining = deadline.saturating_duration_since(Instant::now());
    match forwarder.transfer_leader(target, &req, ASK.min(remaining)).await {
        Ok(true) => {}
        Ok(false) => return Err(TransferError::NotLeader),
        Err(ConfirmError::Older) => {
            return Err(TransferError::BadTransferee(format!(
                "member {target:x} is older than fastetcd 1.36 and cannot take leadership"
            )))
        }
        Err(ConfirmError::Unreachable(e)) => {
            return Err(TransferError::TimedOut(format!("member {target:x} did not answer: {e}")))
        }
    }

    loop {
        let m = raft.metrics().borrow().clone();
        if m.current_leader == Some(target) {
            tracing::info!(
                to = format_args!("{target:x}"),
                term = m.current_term,
                "leader transfer done"
            );
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(TransferError::TimedOut(format!(
                "member {target:x} did not become leader in time (leader now {:?})",
                m.current_leader
            )));
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Accept a leader transfer on this member: `Ok(true)` if its vote is
/// the request's leader vote (it then waives that vote's lease, and the
/// target elects). A member whose vote moved on ignores it.
pub async fn accept(
    raft: &Raft<TypeConfig>,
    req: TransferLeaderRequest,
) -> Result<bool, openraft::error::Fatal<NodeId>> {
    let leader_vote = req.leader_vote;
    let same = raft.with_raft_state(move |st| *st.vote_ref() == leader_vote).await?;
    if same {
        raft.trigger()
            .accept_leader_transfer(req.leader_vote, req.to, req.last_log_id)
            .await?;
    }
    Ok(same)
}
