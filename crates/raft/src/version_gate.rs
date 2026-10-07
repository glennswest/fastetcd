//! "Every member runs at least version X", asked over `ConfirmLeader`,
//! whose answer carries the member's version (#75). Used where a feature
//! is safe only once no member is older: a nested txn in the log (#56),
//! keep-alives renewed in the leader's RAM (#92).
//!
//! A pass is remembered for the membership it was checked against (the
//! log index of the newest membership entry); a membership change asks
//! again.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use openraft::Raft;

use crate::network::{ConfirmError, WriteForwarder};
use crate::types::{NodeId, TypeConfig};

const PROBE: Duration = Duration::from_secs(3);

/// `major.minor.patch` of a version string (`1.22.0`, `1.22.0-rc1`).
pub fn parse_version(v: &str) -> Option<(u64, u64, u64)> {
    let mut parts = v.split(|c: char| !c.is_ascii_digit()).filter(|p| !p.is_empty());
    Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
}

/// Why the gate is closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateError {
    /// The member answered with an older version (`None`: older than
    /// `ConfirmLeader` itself, 1.10).
    Older { member: NodeId, version: Option<String> },
    /// The member did not answer.
    Unreachable { member: NodeId, error: String },
}

/// The current membership's key: its log index + 1 (0: none yet).
pub fn membership_key(raft: &Raft<TypeConfig>) -> u64 {
    let m = raft.metrics().borrow().clone();
    m.membership_config.log_id().map(|l| l.index + 1).unwrap_or(0)
}

/// Passes once every other member (voters and learners) answers with
/// `min` or later.
#[derive(Debug, Default)]
pub struct MembersAtLeast {
    min: (u64, u64, u64),
    /// Membership key + 1 the gate last passed for; 0 = never.
    passed: AtomicU64,
}

impl MembersAtLeast {
    pub const fn new(min: (u64, u64, u64)) -> Self {
        Self { min, passed: AtomicU64::new(0) }
    }

    pub fn min(&self) -> (u64, u64, u64) {
        self.min
    }

    /// Passed for the current membership already: no RPC.
    pub fn passed(&self, raft: &Raft<TypeConfig>) -> bool {
        self.passed.load(Ordering::Acquire) == membership_key(raft) + 1
    }

    /// Ask every other member, unless passed for this membership.
    pub async fn check(
        &self,
        raft: &Raft<TypeConfig>,
        forwarder: &WriteForwarder,
    ) -> Result<(), GateError> {
        let key = membership_key(raft) + 1;
        if self.passed.load(Ordering::Acquire) == key {
            return Ok(());
        }
        let others: Vec<NodeId> = {
            let m = raft.metrics().borrow().clone();
            m.membership_config.membership().nodes().map(|(id, _)| *id).filter(|id| *id != m.id).collect()
        };
        for member in others {
            match forwarder.confirm_leader(member, 0, PROBE).await {
                Ok(r) if parse_version(&r.version).is_some_and(|v| v >= self.min) => {}
                Ok(r) => return Err(GateError::Older { member, version: Some(r.version) }),
                Err(ConfirmError::Older) => return Err(GateError::Older { member, version: None }),
                Err(ConfirmError::Unreachable(error)) => {
                    return Err(GateError::Unreachable { member, error })
                }
            }
        }
        if membership_key(raft) + 1 == key {
            self.passed.store(key, Ordering::Release);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_parse_and_order() {
        assert_eq!(parse_version("1.22.0"), Some((1, 22, 0)));
        assert_eq!(parse_version("1.22.1-rc1"), Some((1, 22, 1)));
        assert_eq!(parse_version("junk"), None);
        assert!(parse_version("1.21.9").unwrap() < (1, 22, 0));
        assert!(parse_version("2.0.0").unwrap() >= (1, 22, 0));
    }
}
