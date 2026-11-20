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

/// A semver version, ordered by semver precedence (2.0.0, §11): a
/// pre-release is older than its release (`1.22.0-rc1` < `1.22.0`), and
/// build metadata (`+...`) is ignored (#203).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version {
    /// `major.minor.patch`.
    pub core: (u64, u64, u64),
    /// The pre-release identifiers; empty for a release.
    pub pre: Vec<PreId>,
}

/// One pre-release identifier. Numeric ones are lower than alphanumeric
/// ones and compare as numbers; alphanumeric ones compare in ASCII order
/// (the derived order).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum PreId {
    Num(u64),
    Alpha(String),
}

impl Version {
    /// The release `major.minor.patch`, no pre-release.
    pub fn release(core: (u64, u64, u64)) -> Self {
        Self { core, pre: Vec::new() }
    }

    pub fn is_prerelease(&self) -> bool {
        !self.pre.is_empty()
    }
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        self.core.cmp(&other.core).then_with(|| match (self.pre.is_empty(), other.pre.is_empty()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => self.pre.cmp(&other.pre),
        })
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (a, b, c) = self.core;
        write!(f, "{a}.{b}.{c}")?;
        for (i, id) in self.pre.iter().enumerate() {
            f.write_str(if i == 0 { "-" } else { "." })?;
            match id {
                PreId::Num(n) => write!(f, "{n}")?,
                PreId::Alpha(s) => f.write_str(s)?,
            }
        }
        Ok(())
    }
}

/// A version string: `major.minor.patch`, an optional leading `v`, an
/// optional `-pre.release` and `+build` (`1.22.0`, `v1.22.0-rc.1+abc`).
pub fn parse_version(v: &str) -> Option<Version> {
    let v = v.trim();
    let v = v.strip_prefix('v').unwrap_or(v);
    let (v, build) = match v.split_once('+') {
        Some((v, b)) => (v, Some(b)),
        None => (v, None),
    };
    let ident_ok = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-');
    if let Some(b) = build {
        if !b.split('.').all(ident_ok) {
            return None;
        }
    }
    let (core, pre) = match v.split_once('-') {
        Some((c, p)) => (c, Some(p)),
        None => (v, None),
    };
    let num = |s: &str| -> Option<u64> {
        if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        s.parse().ok()
    };
    let parts: Vec<&str> = core.split('.').collect();
    let [a, b, c] = parts.as_slice() else { return None };
    let core = (num(a)?, num(b)?, num(c)?);
    let mut ids = Vec::new();
    if let Some(p) = pre {
        for id in p.split('.') {
            if !ident_ok(id) {
                return None;
            }
            ids.push(match num(id) {
                Some(n) => PreId::Num(n),
                None => PreId::Alpha(id.to_string()),
            });
        }
    }
    Some(Version { core, pre: ids })
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
                Ok(r) if parse_version(&r.version).is_some_and(|v| v >= Version::release(self.min)) => {}
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
        let v = |s: &str| parse_version(s).unwrap_or_else(|| panic!("{s:?}"));
        assert_eq!(v("1.22.0"), Version::release((1, 22, 0)));
        assert_eq!(v("1.22.1-rc1").core, (1, 22, 1));
        assert!(v("1.22.1-rc1").is_prerelease());
        assert_eq!(v("v1.22.0+abc.1"), Version::release((1, 22, 0)));
        for bad in ["junk", "", "1.22", "1.22.0.1", "1.22.0-", "1.22.0-rc..1", "1.22.0+", "1.x.0", "1.22.0-rc_1"] {
            assert_eq!(parse_version(bad), None, "{bad:?}");
        }
        assert!(v("1.21.9") < Version::release((1, 22, 0)));
        assert!(v("2.0.0") >= Version::release((1, 22, 0)));
    }

    #[test]
    fn prereleases_order_per_semver() {
        // semver 2.0.0 §11's own example, lowest first.
        let order = [
            "1.0.0-alpha",
            "1.0.0-alpha.1",
            "1.0.0-alpha.beta",
            "1.0.0-beta",
            "1.0.0-beta.2",
            "1.0.0-beta.11",
            "1.0.0-rc.1",
            "1.0.0",
            "1.0.1-rc1",
            "1.0.1",
        ];
        for w in order.windows(2) {
            let (a, b) = (parse_version(w[0]).unwrap(), parse_version(w[1]).unwrap());
            assert!(a < b, "{} < {}", w[0], w[1]);
        }
        assert_eq!(parse_version("1.0.0+x").unwrap().cmp(&parse_version("1.0.0+y").unwrap()), std::cmp::Ordering::Equal);
        assert_eq!(parse_version("1.0.0-rc.1").unwrap().to_string(), "1.0.0-rc.1");
    }

    #[test]
    fn a_prerelease_of_the_minimum_is_not_at_least_the_minimum() {
        let min = Version::release((1, 22, 0));
        assert!(parse_version("1.22.0-rc1").unwrap() < min);
        assert!(parse_version("1.22.0").unwrap() >= min);
        assert!(parse_version("1.22.1-rc1").unwrap() >= min);
    }
}
