//! A put's version counts puts, as etcd's, whatever a compaction has
//! dropped (fastetcd#220).
//!
//! Before 1.44 a put's version was the number of revisions the key's
//! open generation still held, and compaction drops those below the
//! compact revision, so every put after a compaction stored a version too
//! low (etcd keeps a per-generation put counter that compaction does not
//! touch). From a `PutVersions` log entry on, a put's version is the
//! key's live record's version + 1 (1 for a put that creates the key).
//!
//! The rule changes at that replicated entry, so every member, and every
//! replay of the log, stores the same version for the same put: a member
//! on the old rule and one on the new would disagree. Snapshots carry the
//! marker the entry sets. A member older than 1.44 cannot decode the
//! entry, so the leader proposes it only once every member answers as
//! 1.44 or later ([`follow`]); until then puts keep the old count.
//!
//! Keys stored with a too-low version keep it until they are deleted:
//! their next put continues from the version stored (03-deploy).

use std::sync::Arc;
use std::time::Duration;

use fastetcd_raft::version_gate::GateError;
use fastetcd_raft::FastetcdLogEntry;

use crate::state::ServerState;

/// The first version that decodes a `PutVersions` entry.
pub const PUT_VERSIONS_SINCE: (u64, u64, u64) = (1, 44, 0);

/// While running: a leader of a store whose log has not held
/// `PutVersions` proposes it once every member, voters and learners,
/// answers as 1.44 or later. A member that does not answer keeps it
/// waiting (it could be older). Ends once the entry has applied here.
pub fn follow(state: Arc<ServerState>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(2));
        loop {
            tick.tick().await;
            if state.sm.mvcc().has_put_versions() {
                return;
            }
            if !fastetcd_raft::precheck::is_leader(&state.raft) {
                continue;
            }
            match state.put_versions_gate.check(&state.raft, &state.forwarder).await {
                Ok(()) => {}
                Err(GateError::Older { member, version }) => {
                    tracing::debug!(
                        member = format!("{member:x}"),
                        version = version.as_deref().unwrap_or("older than 1.10"),
                        "puts keep the old version count: a member is older than 1.44 (fastetcd#220)"
                    );
                    continue;
                }
                Err(GateError::Unreachable { member, error }) => {
                    tracing::debug!(member = format!("{member:x}"), error, "a member did not answer its version");
                    continue;
                }
                Err(GateError::Held { target }) => {
                    tracing::debug!(target_version = %target, "puts keep the old version count: a downgrade is in progress (fastetcd#202)");
                    continue;
                }
            }
            match state.propose(FastetcdLogEntry::PutVersions).await {
                Ok(_) => tracing::info!("a put's version counts puts, as etcd's (fastetcd#220)"),
                Err(e) => tracing::debug!(error = %e, "proposing put versions"),
            }
        }
    });
}
