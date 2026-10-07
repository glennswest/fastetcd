//! A txn inside a txn is proposed only once every member can read it
//! (fastetcd#56).
//!
//! A nested txn is a new variant of the raft entry's `TxnOp`, which a
//! member older than 1.22 cannot decode: its AppendEntries (or its own
//! log replay) fails at that entry and it stops, and a voter that stops
//! costs the quorum. So before a txn that nests is proposed, every other
//! member (voters and learners) is asked `ConfirmLeader`, which answers
//! with its version; all at 1.22 or later opens the gate for that
//! membership, and a membership change closes it again. Flat txns never
//! wait on it.
//!
//! Left open: a member older than 1.22 added after a nested txn was
//! applied cannot read the log either; `MemberAdd` refuses only members
//! older than 1.10 (#77). Do not add one (03-deploy).

use std::sync::atomic::Ordering;

use tonic::Status;

use crate::batch_guard::{member_version, MemberVersion};
use crate::state::ServerState;

/// The first version that decodes a nested txn.
pub const NESTED_TXN_SINCE: (u64, u64, u64) = (1, 22, 0);

/// `major.minor.patch` of a version string (`1.22.0`, `1.22.0-rc1`).
pub fn parse_version(v: &str) -> Option<(u64, u64, u64)> {
    let mut parts = v.split(|c: char| !c.is_ascii_digit()).filter(|p| !p.is_empty());
    Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
}

/// The current membership's key: its log index + 1 (0: none yet).
fn membership_key(state: &ServerState) -> u64 {
    let m = state.raft.metrics().borrow().clone();
    m.membership_config.log_id().map(|l| l.index + 1).unwrap_or(0)
}

/// Ok once every member runs 1.22 or later. An older member is refused
/// with FailedPrecondition naming it; one that does not answer, with
/// Unavailable (its version is unknown, and it may be older).
pub async fn require_nested_txn(state: &ServerState) -> Result<(), Status> {
    let key = membership_key(state) + 1;
    if state.nested_txn_checked.load(Ordering::Acquire) == key {
        return Ok(());
    }
    let others: Vec<_> = {
        let m = state.raft.metrics().borrow().clone();
        m.membership_config.membership().nodes().map(|(id, _)| *id).filter(|id| *id != m.id).collect()
    };
    let (a, b, c) = NESTED_TXN_SINCE;
    for id in others {
        match member_version(state, id).await {
            MemberVersion::Reads(v) if parse_version(&v).is_some_and(|got| got >= NESTED_TXN_SINCE) => {}
            MemberVersion::Reads(v) => {
                return Err(Status::failed_precondition(format!(
                    "a txn inside a txn needs every member at fastetcd {a}.{b}.{c} or later, and \
                     member {id:x} runs {v}: it could not read the entry and would stop \
                     (fastetcd#56). Upgrade it first."
                )))
            }
            MemberVersion::Older => {
                return Err(Status::failed_precondition(format!(
                    "a txn inside a txn needs every member at fastetcd {a}.{b}.{c} or later, and \
                     member {id:x} runs a fastetcd older than 1.10 (fastetcd#56). Upgrade it first."
                )))
            }
            MemberVersion::Unreachable(e) => {
                return Err(Status::unavailable(format!(
                    "a txn inside a txn needs every member at fastetcd {a}.{b}.{c} or later, and \
                     member {id:x} cannot be asked its version ({e}) (fastetcd#56)"
                )))
            }
        }
    }
    if membership_key(state) + 1 == key {
        state.nested_txn_checked.store(key, Ordering::Release);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_parse_and_order() {
        assert_eq!(parse_version("1.22.0"), Some((1, 22, 0)));
        assert_eq!(parse_version("1.22.1-rc1"), Some((1, 22, 1)));
        assert_eq!(parse_version("junk"), None);
        assert!(parse_version("1.21.9").unwrap() < NESTED_TXN_SINCE);
        assert!(parse_version("1.22.0").unwrap() >= NESTED_TXN_SINCE);
        assert!(parse_version("2.0.0").unwrap() >= NESTED_TXN_SINCE);
    }
}
