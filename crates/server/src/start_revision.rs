//! A new store starts at revision 1, as an empty etcd store does, so
//! its first write is revision 2 (fastetcd#160; the owner's decision on
//! #195).
//!
//! The move is a replicated `StartRevision` log entry, so every member
//! (and every replay of the log) moves at the same point of the
//! history: applied on a store at revision 0 it sets revision 1;
//! applied on a store that has a revision it changes nothing. A store
//! that already has revisions therefore keeps its numbering, and one
//! with none (made before 1.41 and never written) moves to 1.
//!
//! Before a proposal that can create a revision is made on a store at
//! revision 0, [`before_proposal`] proposes `StartRevision` and waits
//! for it, so the first write lands at 2. Any member does it (a
//! follower's proposal is forwarded to the leader like a write). A
//! leader of an empty store also proposes it by itself ([`follow`]), so
//! an empty cluster reports revision 1 before any write, as etcd's.
//!
//! A member older than 1.41 cannot decode the entry, so it is not
//! proposed while a member answers with an older version: the write
//! goes ahead and the cluster keeps 0-based numbering (its first write
//! is revision 1, as before). A member that does not answer is not
//! waited for: a new cluster's members come up one by one, and its
//! first write must not wait for the last of them. Such a member, if it
//! turns out to be older than 1.41, stops at the entry until it is
//! upgraded, as with any entry it cannot read (03-deploy).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use fastetcd_raft::version_gate::GateError;
use fastetcd_raft::{FastetcdLogEntry, FastetcdLogResponse};
use tonic::Status;

use crate::state::ServerState;

/// The first version that decodes a `StartRevision` entry.
pub const START_REVISION_SINCE: (u64, u64, u64) = (1, 41, 0);

/// Whether applying `entry` can create a revision: a write, a txn, a
/// lease revoke (it deletes the lease's keys), or a batch of them.
pub fn creates_revision(entry: &FastetcdLogEntry) -> bool {
    matches!(
        entry,
        FastetcdLogEntry::Apply { .. }
            | FastetcdLogEntry::Txn { .. }
            | FastetcdLogEntry::LeaseRevoke { .. }
            | FastetcdLogEntry::Batch(_)
    )
}

/// Called by [`ServerState::propose`] before it proposes `entry`: on a
/// store at revision 0, move it to revision 1 first (see the module
/// docs). Errors only when the `StartRevision` proposal itself fails
/// (no leader, no quorum): the write would have failed the same way.
pub async fn before_proposal(state: &ServerState, entry: &FastetcdLogEntry) -> Result<(), Status> {
    if !creates_revision(entry) || state.sm.mvcc().current_revision().await != 0 {
        return Ok(());
    }
    if !may_propose(state, false).await {
        return Ok(());
    }
    propose(state).await.map(|_| ())
}

/// Whether the entry may be proposed now. An older member closes it.
/// A member that does not answer closes it only when `strict` (the
/// background proposer, which can wait for it); a write does not wait.
async fn may_propose(state: &ServerState, strict: bool) -> bool {
    match state.start_revision_gate.check(&state.raft, &state.forwarder).await {
        Ok(()) => true,
        Err(GateError::Older { member, version }) => {
            tracing::debug!(
                member = format!("{member:x}"),
                version = version.as_deref().unwrap_or("older than 1.10"),
                "store keeps 0-based revisions: a member is older than 1.41 (fastetcd#160)"
            );
            false
        }
        Err(GateError::Unreachable { member, error }) => {
            tracing::debug!(member = format!("{member:x}"), error, "a member did not answer its version");
            !strict
        }
    }
}

/// Propose `StartRevision`. Boxed: it goes through
/// [`ServerState::propose`], which calls [`before_proposal`].
fn propose(state: &ServerState) -> Pin<Box<dyn Future<Output = Result<FastetcdLogResponse, Status>> + Send + '_>> {
    Box::pin(state.propose(FastetcdLogEntry::StartRevision))
}

/// While running: a leader of a store at revision 0 proposes
/// `StartRevision` once every member answers as 1.41 or later, so an
/// empty cluster reports revision 1. Ends once the store has a
/// revision.
pub fn follow(state: Arc<ServerState>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(2));
        loop {
            tick.tick().await;
            if state.sm.mvcc().current_revision().await != 0 {
                return;
            }
            if !fastetcd_raft::precheck::is_leader(&state.raft) || !may_propose(&state, true).await {
                continue;
            }
            match propose(&state).await {
                Ok(_) => tracing::info!("this store starts at revision 1, as etcd's (fastetcd#160)"),
                Err(e) => tracing::debug!(error = %e, "proposing the start revision"),
            }
        }
    });
}
