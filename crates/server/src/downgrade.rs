//! `Maintenance/Downgrade` in fastetcd's terms (fastetcd#192).
//!
//! etcd's downgrade lowers a cluster version every member applies.
//! fastetcd has none: each feature an older member cannot read is gated
//! on every member's version instead (03-deploy, Downgrades), and a
//! downgrade is a rolling restart onto the older binary. So:
//!
//! - **VALIDATE `<version>`** answers whether this cluster may go back to
//!   that *fastetcd* version (`1.33`, `1.33.0`, `v1.33.0`; an etcd version
//!   such as `3.5` is refused as one). The target is checked against the
//!   floors below: every floor above it that applies refuses it,
//!   `FailedPrecondition` naming each and the lowest version allowed. A
//!   pass answers with this member's fastetcd version. It reads this
//!   member's applied state (the floors that depend on it only ever turn
//!   on), changes nothing, and needs root while auth is on, as in etcd.
//! - **ENABLE / CANCEL** are not served: they answer as any unknown action
//!   does (`InvalidArgument`, `unknown downgrade action N`). The gap is
//!   fastetcd#202.

use fastetcd_proto::etcdserverpb as pb;
use tonic::Status;

use crate::state::ServerState;

/// This binary's fastetcd version.
pub const THIS_VERSION: &str = env!("CARGO_PKG_VERSION");

/// A version a cluster cannot go back below once it has used a feature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Floor {
    /// The first version that reads the feature.
    pub since: (u64, u64, u64),
    /// What an older member could not read, and why it applies.
    pub why: &'static str,
}

/// What this member's state says the cluster has used.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Used {
    /// The log has held a batched entry (`fastetcd_log_has_batched`).
    pub batched: bool,
    /// The cluster id is replicated (`fastetcd_cluster_id_replicated`).
    pub cluster_id_replicated: bool,
}

impl Used {
    pub fn of(state: &ServerState) -> Self {
        let mvcc = state.sm.mvcc();
        Self { batched: mvcc.has_batched(), cluster_id_replicated: mvcc.cluster_id().is_some() }
    }
}

/// The floors that apply to a cluster that has used `used`, lowest
/// first. A member running this binary has started on 1.12 or later,
/// and nothing records whether a nested txn was applied (#134), so
/// those floors always apply.
pub fn floors(used: Used) -> Vec<Floor> {
    let mut out = Vec::new();
    if used.batched {
        out.push(Floor {
            since: (1, 10, 0),
            why: "this cluster's log has held batched entries (fastetcd_log_has_batched = 1), \
                  which an older member cannot decode (#77)",
        });
    }
    out.push(Floor {
        since: (1, 12, 0),
        why: "a member that has started on 1.12 or later keeps its raft log in wal/, not in the \
              data file, and an older binary finds no raft log (#85)",
    });
    out.push(Floor {
        since: (1, 22, 0),
        why: "a txn inside a txn may have been applied (fastetcd does not record whether one has, \
              #134), and an older member cannot decode one (#56)",
    });
    out.push(Floor {
        since: (1, 23, 0),
        why: "keep-alives are renewed in the leader's RAM while every member runs 1.23 or later, \
              and an older member that became leader would expire every lease kept alive that \
              way (#92)",
    });
    if used.cluster_id_replicated {
        out.push(Floor {
            since: (1, 33, 0),
            why: "the cluster id is replicated in a log entry an older member cannot decode, and \
                  fastetcd does not track when a snapshot and a log purge have passed it on every \
                  member (#38)",
        });
    }
    out
}

fn show((a, b, c): (u64, u64, u64)) -> String {
    format!("{a}.{b}.{c}")
}

/// A target version: `X.Y` (as `X.Y.0`) or `X.Y.Z`, with an optional
/// leading `v` and an optional pre-release suffix on the patch.
pub fn parse_target(v: &str) -> Option<(u64, u64, u64)> {
    let v = v.trim();
    let v = v.strip_prefix('v').unwrap_or(v);
    let core = v.split(['-', '+']).next()?;
    let parts: Vec<&str> = core.split('.').collect();
    let num = |s: &str| -> Option<u64> {
        if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        s.parse().ok()
    };
    match parts.as_slice() {
        [a, b] => Some((num(a)?, num(b)?, 0)),
        [a, b, c] => Some((num(a)?, num(b)?, num(c)?)),
        _ => None,
    }
}

/// VALIDATE: may a cluster that has used `used`, answered by a member
/// running `this`, go back to `target`? `Ok` carries `this`.
pub fn validate(this: &str, target: &str, used: Used) -> Result<String, Status> {
    let own = crate::version_gate::parse_version(this)
        .ok_or_else(|| Status::internal(format!("this member's version {this:?} does not parse")))?;
    let Some(want) = parse_target(target) else {
        return Err(Status::invalid_argument(format!(
            "invalid downgrade target version {target:?}: give a fastetcd version, such as 1.33 \
             or 1.33.0 (this member runs {this})"
        )));
    };
    if want.0 == 3 {
        return Err(Status::invalid_argument(format!(
            "invalid downgrade target version {target:?}: that is an etcd version, and fastetcd \
             has no etcd cluster version to lower; give the fastetcd version to go back to, such \
             as 1.33 (this member runs {this}; docs/03-deploy.md, Downgrades)"
        )));
    }
    if want >= own {
        return Err(Status::invalid_argument(format!(
            "invalid downgrade target version {target:?}: not older than this member's fastetcd \
             {this}"
        )));
    }
    let refusing: Vec<Floor> = floors(used).into_iter().filter(|f| want < f.since).collect();
    let Some(lowest) = refusing.iter().map(|f| f.since).max() else {
        return Ok(this.to_string());
    };
    let reasons: Vec<String> =
        refusing.iter().map(|f| format!("below {}: {}", show(f.since), f.why)).collect();
    Err(Status::failed_precondition(format!(
        "cannot downgrade this cluster to fastetcd {}: {}. The lowest version it may go back to \
         is {} (docs/03-deploy.md, Downgrades)",
        show(want),
        reasons.join("; "),
        show(lowest)
    )))
}

/// The `Maintenance/Downgrade` handler's work, after the auth check.
pub fn serve(state: &ServerState, req: &pb::DowngradeRequest) -> Result<String, Status> {
    use pb::downgrade_request::DowngradeAction;
    match DowngradeAction::try_from(req.action) {
        Ok(DowngradeAction::Validate) => validate(THIS_VERSION, &req.version, Used::of(state)),
        _ => Err(Status::invalid_argument(format!("unknown downgrade action {}", req.action))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    const ALL: Used = Used { batched: true, cluster_id_replicated: true };

    #[test]
    fn targets_parse() {
        assert_eq!(parse_target("1.33"), Some((1, 33, 0)));
        assert_eq!(parse_target("1.33.2"), Some((1, 33, 2)));
        assert_eq!(parse_target("v1.33.2"), Some((1, 33, 2)));
        assert_eq!(parse_target(" 1.33.2-rc1 "), Some((1, 33, 2)));
        assert_eq!(parse_target("3.5"), Some((3, 5, 0)));
        for bad in ["", "1", "1.x", "1.2.3.4", "junk", "1..2", "-1.2"] {
            assert_eq!(parse_target(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn a_target_above_every_floor_passes_with_this_version() {
        assert_eq!(validate("1.39.0", "1.33", ALL).unwrap(), "1.39.0");
        assert_eq!(validate("1.39.0", "1.38.0", ALL).unwrap(), "1.39.0");
        assert_eq!(validate("1.39.0", "1.23.0", Used::default()).unwrap(), "1.39.0");
    }

    #[test]
    fn the_floors_above_the_target_refuse_it_and_name_the_lowest_allowed() {
        let err = validate("1.39.0", "1.32.3", ALL).unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(err.message().contains("below 1.33.0"), "{}", err.message());
        assert!(err.message().contains("lowest version it may go back to is 1.33.0"), "{}", err.message());
        assert!(!err.message().contains("below 1.23.0"), "{}", err.message());

        let err = validate("1.39.0", "1.9", ALL).unwrap_err();
        for f in ["1.10.0", "1.12.0", "1.22.0", "1.23.0", "1.33.0"] {
            assert!(err.message().contains(&format!("below {f}")), "{f}: {}", err.message());
        }
    }

    #[test]
    fn floors_follow_what_the_cluster_used() {
        // Without a replicated cluster id, 1.23 is the floor.
        let err = validate("1.39.0", "1.22.9", Used::default()).unwrap_err();
        assert!(err.message().contains("may go back to is 1.23.0"), "{}", err.message());
        assert!(!err.message().contains("below 1.33.0"), "{}", err.message());
        // Batched only shows below 1.10, and only once used.
        let err = validate("1.39.0", "1.9.0", Used::default()).unwrap_err();
        assert!(!err.message().contains("below 1.10.0"), "{}", err.message());
        let err = validate("1.39.0", "1.9.0", Used { batched: true, ..Used::default() }).unwrap_err();
        assert!(err.message().contains("below 1.10.0"), "{}", err.message());
    }

    #[test]
    fn bad_targets_are_invalid_arguments() {
        for (target, says) in [
            ("3.5", "etcd version"),
            ("3.5.0", "etcd version"),
            ("1.39.0", "not older"),
            ("1.40", "not older"),
            ("2.0.0", "not older"),
            ("junk", "give a fastetcd version"),
            ("", "give a fastetcd version"),
        ] {
            let err = validate("1.39.0", target, ALL).unwrap_err();
            assert_eq!(err.code(), Code::InvalidArgument, "{target:?}");
            assert!(err.message().contains(says), "{target:?}: {}", err.message());
        }
    }
}
