//! The read barrier in front of a linearizable read (fastetcd#10, #71).
//!
//! openraft's `ensure_linearizable` is a message to RaftCore, which
//! handles one message at a time and, for a client write, awaits the
//! log append's fsync before taking the next. Under write load a read
//! therefore queued behind every write ahead of it: 150 ms to 7 s at
//! ~58 writes/s on one member, with serializable reads at 0.2 ms
//! (fastetcd#71).
//!
//! When this node is the leader and the *only* voter, no other node can
//! commit anything or become leader, so leadership needs no heartbeat
//! round, and the read index is known locally: everything durably in
//! the log, and everything openraft has marked committed. The read
//! waits until the state machine has applied that far — etcd's
//! ReadIndex: wait for applied >= the commit index seen when the read
//! arrived — and never enters RaftCore's queue.
//!
//! The sole-voter test reads openraft's metrics, which RaftCore
//! publishes between messages and so may be a little behind. Behind is
//! safe in every direction but one: a membership change appended but
//! not yet shown. The log store raises its membership mark *before*
//! writing such an entry, and the local path is taken only when the
//! metrics show a membership at least that new.
//!
//! With more voters (fastetcd#75) the leader still needs a quorum to
//! confirm no newer leader exists, but it asks them itself, over the
//! `ConfirmLeader` peer RPC, instead of through RaftCore. Each member
//! answers with the term of the vote its log store last saved. openraft
//! saves a vote before granting it or acting on it (`SaveVote` runs
//! before the vote's `Respond` in RaftCore's command queue), so a
//! member answering term <= the leader's has granted no vote of a newer
//! term. A newer leader needs a quorum of such votes; any two quorums
//! share a member; so if a quorum (in every config of a joint
//! membership) answers term <= ours after the read arrived, no newer
//! leader had been elected when it arrived, and nothing it commits can
//! precede the read. The read index is the larger of the committed
//! index and the first index of the leader's own term (its blank
//! entry, which follows everything earlier leaders committed), taken
//! after the read arrived; then the read waits for the state machine to
//! apply that far. This is etcd's ReadIndex. Reads that arrive while a
//! round is out join the next one, so a burst of reads costs one round.
//!
//! Anything else — a follower, a pending membership change, a member
//! that does not answer or is older than the RPC, a round with no
//! quorum — goes through `ensure_linearizable`.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{CheckIsLeaderError, RaftError};
use openraft::{BasicNode, Raft, ServerState};
use tokio::sync::watch;

use crate::kv_log_store::LogProgress;
use crate::network::WriteForwarder;
use crate::state_machine::next_index;
use crate::types::{NodeId, TypeConfig};

/// Error of a read barrier: the same as `Raft::ensure_linearizable`'s,
/// so callers keep handling `ForwardToLeader` as they did.
pub type ReadBarrierError = RaftError<NodeId, CheckIsLeaderError<NodeId, BasicNode>>;

/// How long the local path waits for the state machine before checking
/// again that it still applies. A sole-voter leader applies what it
/// committed in well under this; it bounds a wait that membership
/// changed under.
const RECHECK: Duration = Duration::from_millis(500);
/// How long a leader waits for each member's `ConfirmLeader` answer.
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(1);
/// How long a confirmed read waits for the state machine before it
/// gives up on the local path.
const APPLY_TIMEOUT: Duration = Duration::from_secs(10);

/// Confirmation rounds of a multi-voter leader: one at a time; a read
/// uses the first round that started after it arrived.
#[derive(Default)]
struct Rounds {
    /// Rounds started so far.
    started: AtomicU64,
    /// The last round's number and read index (`None`: not confirmed).
    /// Held for the whole of a round, so rounds do not overlap.
    last: tokio::sync::Mutex<Option<(u64, Option<u64>)>>,
}

/// Read barriers served, by path (for `/metrics` and tests).
#[derive(Default, Debug)]
pub struct ReadIndexStats {
    /// Sole voter, from local state.
    pub sole_voter: AtomicU64,
    /// Leader confirmed by a quorum over `ConfirmLeader`.
    pub quorum: AtomicU64,
    /// Through openraft's `ensure_linearizable`.
    pub raft: AtomicU64,
}

/// The local handles a leader serves its read index from.
#[derive(Clone)]
pub struct LocalReadIndex {
    progress: LogProgress,
    applied: watch::Receiver<u64>,
    /// Reaches the other voters; `None` serves only a sole voter.
    peers: Option<WriteForwarder>,
    rounds: Arc<Rounds>,
    stats: Arc<ReadIndexStats>,
}

impl LocalReadIndex {
    /// `progress` from the node's `KvLogStore::progress`, `applied` from
    /// its `FastetcdStateMachine::applied_index`.
    pub fn new(progress: LogProgress, applied: watch::Receiver<u64>) -> Self {
        Self { progress, applied, peers: None, rounds: Arc::default(), stats: Arc::default() }
    }

    /// Barriers served so far, by path.
    pub fn stats(&self) -> &ReadIndexStats {
        &self.stats
    }

    /// Also serve a leader with other voters, confirming leadership over
    /// `peers` (fastetcd#75).
    pub fn with_peers(mut self, peers: WriteForwarder) -> Self {
        self.peers = Some(peers);
        self
    }

    /// A confirmed read index (`index + 1`) for a leader with other
    /// voters, from the first round that started after this call.
    async fn multi_voter_read_index(&self, raft: &Raft<TypeConfig>) -> Option<u64> {
        let peers = self.peers.as_ref()?;
        // A follower does not queue for a round only to find out.
        {
            let m = raft.metrics();
            let m = m.borrow();
            if m.state != ServerState::Leader || m.current_leader != Some(m.id) {
                return None;
            }
        }
        let arrival = self.rounds.started.load(Ordering::Acquire);
        let mut last = self.rounds.last.lock().await;
        if let Some((round, result)) = *last {
            if round > arrival {
                return result;
            }
        }
        let round = self.rounds.started.fetch_add(1, Ordering::AcqRel) + 1;
        let result = self.confirm_round(raft, peers).await;
        *last = Some((round, result));
        result
    }

    /// One round: read index from local state, then a quorum of voters
    /// confirms no newer term has been voted for.
    async fn confirm_round(&self, raft: &Raft<TypeConfig>, peers: &WriteForwarder) -> Option<u64> {
        let (me, term, configs) = {
            let m = raft.metrics();
            let m = m.borrow();
            if m.running_state.is_err()
                || m.state != ServerState::Leader
                || m.current_leader != Some(m.id)
            {
                return None;
            }
            let shown = next_index(m.membership_config.log_id().as_ref());
            if shown < self.progress.membership.load(Ordering::Acquire) {
                return None;
            }
            let configs: Vec<BTreeSet<NodeId>> =
                m.membership_config.membership().get_joint_config().clone();
            (m.id, m.current_term, configs)
        };
        // The vote this node saved is the one it leads with.
        if self.progress.saved_vote_term() != term {
            return None;
        }
        let start = self.progress.term_start(term)?;
        let index = self.progress.committed.load(Ordering::Acquire).max(start);

        let mut confirmed: BTreeSet<NodeId> = BTreeSet::from([me]);
        let quorum = |c: &BTreeSet<NodeId>| {
            configs.iter().all(|cfg| cfg.iter().filter(|id| c.contains(id)).count() * 2 > cfg.len())
        };
        if !quorum(&confirmed) {
            let others: BTreeSet<NodeId> =
                configs.iter().flatten().copied().filter(|id| *id != me).collect();
            let mut asks = tokio::task::JoinSet::new();
            for id in others {
                let peers = peers.clone();
                asks.spawn(async move { (id, peers.confirm_leader(id, term, CONFIRM_TIMEOUT).await) });
            }
            while let Some(done) = asks.join_next().await {
                let Ok((id, answer)) = done else { continue };
                if answer.is_ok_and(|a| a.saved_term <= term) {
                    confirmed.insert(id);
                    if quorum(&confirmed) {
                        break;
                    }
                }
            }
            asks.abort_all();
            if !quorum(&confirmed) {
                return None;
            }
        }
        // Still the term this round confirmed.
        (self.progress.saved_vote_term() == term).then_some(index)
    }

    /// The read index (`index + 1`) if this node is the leader and sole
    /// voter as of `raft`'s metrics, with no membership change pending
    /// that they don't show yet.
    fn sole_voter_read_index(&self, raft: &Raft<TypeConfig>) -> Option<u64> {
        let m = raft.metrics();
        let m = m.borrow();
        if m.running_state.is_err()
            || m.state != ServerState::Leader
            || m.current_leader != Some(m.id)
        {
            return None;
        }
        let shown = next_index(m.membership_config.log_id().as_ref());
        if shown < self.progress.membership.load(Ordering::Acquire) {
            return None;
        }
        let configs = m.membership_config.membership().get_joint_config();
        let sole = configs.len() == 1
            && configs[0].len() == 1
            && configs[0].contains(&m.id);
        if !sole {
            return None;
        }
        let committed = self.progress.committed.load(Ordering::Acquire);
        let durable = self.progress.last_durable.load(Ordering::Acquire);
        Some(committed.max(durable))
    }
}

/// Wait until a linearizable read may be served from this node's state
/// machine. `Err` with `ForwardToLeader` means this node is not the
/// leader; any other `Err` means leadership could not be confirmed.
pub async fn read_barrier(
    raft: &Raft<TypeConfig>,
    local: Option<&LocalReadIndex>,
) -> Result<(), ReadBarrierError> {
    if let Some(local) = local {
        let mut applied = local.applied.clone();
        while let Some(index) = local.sole_voter_read_index(raft) {
            match tokio::time::timeout(RECHECK, applied.wait_for(|a| *a >= index)).await {
                Ok(Ok(_)) => {
                    local.stats.sole_voter.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                // The state machine is gone: let openraft say why.
                Ok(Err(_)) => break,
                // Still waiting: check the node is still a sole voter.
                Err(_) => continue,
            }
        }
        if let Some(index) = local.multi_voter_read_index(raft).await {
            // Confirmed: serving at `index` is linearizable even if
            // leadership moves on while the state machine catches up.
            if let Ok(Ok(_)) =
                tokio::time::timeout(APPLY_TIMEOUT, applied.wait_for(|a| *a >= index)).await
            {
                local.stats.quorum.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
        }
        local.stats.raft.fetch_add(1, Ordering::Relaxed);
    }
    raft.ensure_linearizable().await.map(|_| ())
}
