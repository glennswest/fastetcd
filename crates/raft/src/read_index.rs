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
//! metrics show a membership at least that new. Anything else — a
//! follower, two voters, a joint config, a pending membership change,
//! no local progress handles — goes through `ensure_linearizable`.

use std::sync::atomic::Ordering;
use std::time::Duration;

use openraft::error::{CheckIsLeaderError, RaftError};
use openraft::{BasicNode, Raft, ServerState};
use tokio::sync::watch;

use crate::kv_log_store::LogProgress;
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

/// The local handles a sole-voter leader serves its read index from.
#[derive(Clone)]
pub struct LocalReadIndex {
    progress: LogProgress,
    applied: watch::Receiver<u64>,
}

impl LocalReadIndex {
    /// `progress` from the node's `KvLogStore::progress`, `applied` from
    /// its `FastetcdStateMachine::applied_index`.
    pub fn new(progress: LogProgress, applied: watch::Receiver<u64>) -> Self {
        Self { progress, applied }
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
                Ok(Ok(_)) => return Ok(()),
                // The state machine is gone: let openraft say why.
                Ok(Err(_)) => break,
                // Still waiting: check the node is still a sole voter.
                Err(_) => continue,
            }
        }
    }
    raft.ensure_linearizable().await.map(|_| ())
}
