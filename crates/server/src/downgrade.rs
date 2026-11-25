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
//! - **ENABLE `<version>`** (fastetcd#202) sets that version as the
//!   cluster's downgrade target, after VALIDATE passes for it: a
//!   replicated `DowngradeTarget` log entry, kept with the data and in
//!   snapshots. While it is set, no version gate turns on a feature the
//!   target could not read (`MembersAtLeast::held_by`), so nothing new
//!   lands between ENABLE and the members' restarts onto the target.
//!   The entry is one only 1.45 or later decodes, so a target older than
//!   1.45 is refused, and it is proposed only once every member runs
//!   1.45 or later. One target at a time (etcd's `cluster has a downgrade
//!   job in progress`). It answers with this member's version.
//! - **CANCEL** clears the target (etcd's `no inflight downgrade job` when
//!   none is set).
//! - The leader clears the target by itself once every member runs it
//!   ([`follow`]), as etcd ends a downgrade when its members all run the
//!   target version. `Status.downgrade_info` reports it on every member.

use std::sync::Arc;
use std::time::Duration;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_raft::version_gate::GateError;
use fastetcd_raft::FastetcdLogEntry;
use tonic::Status;

use crate::state::ServerState;
use crate::version_gate::{parse_version, Version};

/// This binary's fastetcd version.
pub const THIS_VERSION: &str = env!("CARGO_PKG_VERSION");

/// The first version that decodes a `DowngradeTarget` entry: the oldest
/// target ENABLE accepts, and every member must run it or later.
pub const DOWNGRADE_TARGET_SINCE: (u64, u64, u64) = (1, 45, 0);

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
    /// The log has held a `StartRevision` entry (fastetcd#160).
    pub start_revision: bool,
    /// The log has held a `PutVersions` entry (fastetcd#220).
    pub put_versions: bool,
    /// The log has held a `DowngradeTarget` entry (fastetcd#202).
    pub downgrade_target: bool,
}

impl Used {
    pub fn of(state: &ServerState) -> Self {
        let mvcc = state.sm.mvcc();
        Self {
            batched: mvcc.has_batched(),
            cluster_id_replicated: mvcc.cluster_id().is_some(),
            start_revision: mvcc.has_start_revision(),
            put_versions: mvcc.has_put_versions(),
            downgrade_target: mvcc.has_downgrade_target_entry(),
        }
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
    if used.start_revision {
        out.push(Floor {
            since: (1, 41, 0),
            why: "this cluster's log has held a StartRevision entry (its store starts at revision \
                  1, as etcd's), which an older member cannot decode (#160)",
        });
    }
    if used.downgrade_target {
        out.push(Floor {
            since: DOWNGRADE_TARGET_SINCE,
            why: "this cluster's log has held a DowngradeTarget entry (Downgrade ENABLE or CANCEL), \
                  which an older member cannot decode (#202)",
        });
    }
    if used.put_versions {
        out.push(Floor {
            since: (1, 44, 0),
            why: "this cluster's log has held a PutVersions entry (a put's version counts puts, as \
                  etcd's), which an older member cannot decode, and would store other versions \
                  for the same puts (#220)",
        });
    }
    out
}

fn show((a, b, c): (u64, u64, u64)) -> String {
    format!("{a}.{b}.{c}")
}

/// A target version: `X.Y` (as `X.Y.0`) or `X.Y.Z`, with an optional
/// leading `v` and an optional pre-release (and build) suffix on the
/// patch. A pre-release keeps its place in semver order: `1.33.0-rc1` is
/// older than `1.33.0` (#203).
pub fn parse_target(v: &str) -> Option<Version> {
    let v = v.trim();
    let v = v.strip_prefix('v').unwrap_or(v);
    if !v.contains(['-', '+']) && v.split('.').count() == 2 {
        return parse_version(&format!("{v}.0"));
    }
    parse_version(v)
}

/// VALIDATE: may a cluster that has used `used`, answered by a member
/// running `this`, go back to `target`? `Ok` carries `this`.
pub fn validate(this: &str, target: &str, used: Used) -> Result<String, Status> {
    let own = parse_version(this)
        .ok_or_else(|| Status::internal(format!("this member's version {this:?} does not parse")))?;
    let Some(want) = parse_target(target) else {
        return Err(Status::invalid_argument(format!(
            "invalid downgrade target version {target:?}: give a fastetcd version, such as 1.33 \
             or 1.33.0 (this member runs {this})"
        )));
    };
    if want.core.0 == 3 {
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
    let refusing: Vec<Floor> =
        floors(used).into_iter().filter(|f| want < Version::release(f.since)).collect();
    let Some(lowest) = refusing.iter().map(|f| f.since).max() else {
        return Ok(this.to_string());
    };
    let reasons: Vec<String> =
        refusing.iter().map(|f| format!("below {}: {}", show(f.since), f.why)).collect();
    Err(Status::failed_precondition(format!(
        "cannot downgrade this cluster to fastetcd {}: {}. The lowest version it may go back to \
         is {} (docs/03-deploy.md, Downgrades)",
        want,
        reasons.join("; "),
        show(lowest)
    )))
}

/// ENABLE's checks before anything is proposed: VALIDATE passes for
/// `target`, it is no older than [`DOWNGRADE_TARGET_SINCE`], and no other
/// target is set (`current`). `Ok` carries the target as recorded (its
/// version text, without a leading `v`).
pub fn check_enable(this: &str, target: &str, used: Used, current: Option<&str>) -> Result<String, Status> {
    if current.is_some() {
        return Err(Status::failed_precondition(crate::etcd_errors::DOWNGRADE_IN_PROCESS));
    }
    validate(this, target, used)?;
    let want = parse_target(target).expect("validate parsed it");
    if want < Version::release(DOWNGRADE_TARGET_SINCE) {
        return Err(Status::failed_precondition(format!(
            "cannot enable a downgrade to fastetcd {want}: the downgrade target is a log entry only \
             fastetcd {} or later can read, so it can be no older than that (#202). A downgrade to \
             {want} is VALIDATE, then a rolling restart onto it (docs/03-deploy.md, Downgrades)",
            show(DOWNGRADE_TARGET_SINCE)
        )));
    }
    Ok(want.to_string())
}

/// Whether a downgrade to `target` has finished: every member, this one
/// included, runs a fastetcd no newer than the target's release (its
/// major and minor; a patch of it counts as it).
pub fn finished(target: &str, versions: &[String]) -> bool {
    let Some(t) = parse_version(target) else {
        return false;
    };
    !versions.is_empty()
        && versions.iter().all(|v| parse_version(v).is_some_and(|v| (v.core.0, v.core.1) <= (t.core.0, t.core.1)))
}

/// The `Maintenance/Downgrade` handler's work, after the auth check.
pub async fn serve(state: &ServerState, req: &pb::DowngradeRequest) -> Result<String, Status> {
    use pb::downgrade_request::DowngradeAction;
    let this = state.this_version.as_str();
    match DowngradeAction::try_from(req.action) {
        Ok(DowngradeAction::Validate) => validate(this, &req.version, Used::of(state)),
        Ok(DowngradeAction::Enable) => {
            let current = state.sm.mvcc().downgrade_target();
            let target = check_enable(this, &req.version, Used::of(state), current.as_deref())?;
            require_every_member_reads_it(state).await?;
            state.propose(FastetcdLogEntry::DowngradeTarget(Some(target.clone()))).await?;
            tracing::warn!(
                target_version = %target,
                "downgrade enabled: no version gate turns on a feature {target} could not read until it \
                 is cancelled or every member runs it"
            );
            Ok(this.to_string())
        }
        Ok(DowngradeAction::Cancel) => {
            let Some(target) = state.sm.mvcc().downgrade_target() else {
                return Err(Status::failed_precondition(crate::etcd_errors::NO_INFLIGHT_DOWNGRADE));
            };
            state.propose(FastetcdLogEntry::DowngradeTarget(None)).await?;
            tracing::warn!(target_version = %target, "downgrade cancelled");
            Ok(this.to_string())
        }
        Err(_) => Err(Status::invalid_argument(format!("unknown downgrade action {}", req.action))),
    }
}

/// Every member must decode the entry before it is proposed: one that
/// could not would stop at it.
async fn require_every_member_reads_it(state: &ServerState) -> Result<(), Status> {
    let need = format!(
        "enabling a downgrade needs every member at fastetcd {} or later",
        show(DOWNGRADE_TARGET_SINCE)
    );
    match state.downgrade_gate.check(&state.raft, &state.forwarder).await {
        Ok(()) => Ok(()),
        Err(GateError::Older { member, version }) => Err(Status::failed_precondition(format!(
            "{need}, and member {member:x} runs {}: it could not read the entry (#202)",
            version.as_deref().unwrap_or("a fastetcd older than 1.10")
        ))),
        Err(GateError::Unreachable { member, error }) => Err(Status::unavailable(format!(
            "{need}, and member {member:x} cannot be asked its version ({error})"
        ))),
        Err(GateError::Held { target }) => Err(Status::failed_precondition(format!(
            "{need}; a downgrade to {target} is in progress"
        ))),
    }
}

/// `Status.downgrade_info`: the cluster's downgrade target, if one is set.
pub fn info(state: &ServerState) -> pb::DowngradeInfo {
    match state.sm.mvcc().downgrade_target() {
        Some(target) => pb::DowngradeInfo { enabled: true, target_version: target },
        None => pb::DowngradeInfo { enabled: false, target_version: String::new() },
    }
}

/// While running: a leader with a downgrade target set clears it once
/// every member runs it ([`finished`]), as etcd ends a finished downgrade.
pub fn follow(state: Arc<ServerState>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        loop {
            tick.tick().await;
            let Some(target) = state.sm.mvcc().downgrade_target() else {
                continue;
            };
            if !fastetcd_raft::precheck::is_leader(&state.raft) {
                continue;
            }
            let Some(versions) = member_versions(&state).await else {
                continue;
            };
            if !finished(&target, &versions) {
                continue;
            }
            match state.propose(FastetcdLogEntry::DowngradeTarget(None)).await {
                Ok(_) => tracing::warn!(
                    target_version = %target,
                    "downgrade finished: every member runs {target}; target cleared"
                ),
                Err(e) => tracing::debug!(error = %e, "clearing a finished downgrade"),
            }
        }
    });
}

/// This member's version and every other member's, as `ConfirmLeader`
/// answers it; `None` if one does not answer it.
async fn member_versions(state: &ServerState) -> Option<Vec<String>> {
    let others: Vec<u64> = {
        let m = state.raft.metrics().borrow().clone();
        m.membership_config.membership().nodes().map(|(id, _)| *id).filter(|id| *id != m.id).collect()
    };
    let mut out = vec![state.this_version.clone()];
    for member in others {
        let r = state.forwarder.confirm_leader(member, 0, Duration::from_secs(2)).await.ok()?;
        out.push(r.version);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    /// Every floor below 1.41 (these tests answer as 1.39).
    const ALL: Used = Used {
        batched: true,
        cluster_id_replicated: true,
        start_revision: false,
        put_versions: false,
        downgrade_target: false,
    };

    #[test]
    fn targets_parse() {
        let rel = |c| Some(Version::release(c));
        assert_eq!(parse_target("1.33"), rel((1, 33, 0)));
        assert_eq!(parse_target("1.33.2"), rel((1, 33, 2)));
        assert_eq!(parse_target("v1.33.2"), rel((1, 33, 2)));
        assert_eq!(parse_target(" 1.33.2-rc1 ").map(|v| v.to_string()), Some("1.33.2-rc1".into()));
        assert_eq!(parse_target("3.5"), rel((3, 5, 0)));
        for bad in ["", "1", "1.x", "1.2.3.4", "junk", "1..2", "-1.2", "1.33-rc1", "1.33.0-"] {
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
    fn a_prerelease_is_older_than_its_release() {
        // 1.33.0-rc1 predates 1.33.0, so the 1.33.0 floor refuses it (#203).
        let err = validate("1.39.0", "1.33.0-rc1", ALL).unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(err.message().contains("to fastetcd 1.33.0-rc1"), "{}", err.message());
        assert!(err.message().contains("below 1.33.0"), "{}", err.message());
        assert!(err.message().contains("may go back to is 1.33.0"), "{}", err.message());
        let err = validate("1.39.0", "v1.23.0-rc.2", Used::default()).unwrap_err();
        assert!(err.message().contains("may go back to is 1.23.0"), "{}", err.message());
        // A pre-release after a floor's release passes.
        assert_eq!(validate("1.39.0", "1.33.1-rc1", ALL).unwrap(), "1.39.0");
        // Against this member: its own release is newer than its pre-release,
        // and its pre-release older than its release.
        let err = validate("1.40.0-rc1", "1.40.0", ALL).unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(err.message().contains("not older"), "{}", err.message());
        assert_eq!(validate("1.40.0", "1.40.0-rc1", ALL).unwrap(), "1.40.0");
        assert_eq!(validate("1.40.0-rc.2", "1.40.0-rc.1", ALL).unwrap(), "1.40.0-rc.2");
        let err = validate("1.40.0-rc.2", "1.40.0-rc.10", ALL).unwrap_err();
        assert!(err.message().contains("not older"), "{}", err.message());
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

    /// fastetcd#160: once the log has held a StartRevision entry, 1.41
    /// is the floor; before, 1.40 passes.
    #[test]
    fn a_start_revision_entry_sets_a_1_41_floor() {
        assert_eq!(validate("1.41.0", "1.40.1", ALL).unwrap(), "1.41.0");
        let used = Used { start_revision: true, ..ALL };
        let err = validate("1.41.0", "1.40.1", used).unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(err.message().contains("below 1.41.0"), "{}", err.message());
        assert!(err.message().contains("StartRevision"), "{}", err.message());
        assert!(err.message().contains("may go back to is 1.41.0"), "{}", err.message());
    }

    /// fastetcd#220: once the log has held a PutVersions entry, 1.44 is
    /// the floor; before, 1.43 passes.
    #[test]
    fn a_put_versions_entry_sets_a_1_44_floor() {
        assert_eq!(validate("1.44.0", "1.43.5", ALL).unwrap(), "1.44.0");
        let used = Used { put_versions: true, start_revision: true, ..ALL };
        let err = validate("1.44.0", "1.43.5", used).unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(err.message().contains("below 1.44.0"), "{}", err.message());
        assert!(err.message().contains("PutVersions"), "{}", err.message());
        assert!(err.message().contains("may go back to is 1.44.0"), "{}", err.message());
    }

    /// fastetcd#202: ENABLE takes a target VALIDATE passes and no older
    /// than the version that reads its entry, one at a time.
    #[test]
    fn enable_checks_its_target() {
        let none = Used::default();
        assert_eq!(check_enable("1.46.0", "1.45", none, None).unwrap(), "1.45.0");
        assert_eq!(check_enable("1.46.0", "v1.45.2", none, None).unwrap(), "1.45.2");
        let err = check_enable("1.46.0", "1.44.7", none, None).unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(err.message().contains("no older than that"), "{}", err.message());
        assert!(err.message().contains("1.45.0"), "{}", err.message());
        // VALIDATE's refusals come first.
        assert_eq!(check_enable("1.45.0", "1.45.0", none, None).unwrap_err().code(), Code::InvalidArgument);
        assert_eq!(check_enable("1.46.0", "3.5", none, None).unwrap_err().code(), Code::InvalidArgument);
        let err = check_enable("1.46.0", "1.45", none, Some("1.45.0")).unwrap_err();
        assert_eq!((err.code(), err.message()), (Code::FailedPrecondition, crate::etcd_errors::DOWNGRADE_IN_PROCESS));
    }

    /// A downgrade has finished once every member runs the target's
    /// release (a patch of it counts as it), or older.
    #[test]
    fn a_downgrade_finishes_when_every_member_runs_the_target() {
        let v = |s: &[&str]| s.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        assert!(finished("1.45.0", &v(&["1.45.0", "1.45.3"])));
        assert!(!finished("1.45.0", &v(&["1.45.0", "1.46.0"])));
        assert!(!finished("1.45.0", &v(&["1.45.0", "junk"])));
        assert!(!finished("1.45.0", &[]));
        assert!(!finished("junk", &v(&["1.45.0"])));
    }

    /// Once the log has held a DowngradeTarget entry, 1.45 is the floor.
    #[test]
    fn a_downgrade_target_entry_sets_a_1_45_floor() {
        let used = Used { downgrade_target: true, ..ALL };
        let err = validate("1.46.0", "1.44.7", used).unwrap_err();
        assert!(err.message().contains("below 1.45.0"), "{}", err.message());
        assert!(err.message().contains("DowngradeTarget"), "{}", err.message());
        assert_eq!(validate("1.46.0", "1.45.0", used).unwrap(), "1.46.0");
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
