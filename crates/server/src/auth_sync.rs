//! Replicated auth (fastetcd#32): when an auth change may be proposed,
//! proposing one, and the `FastetcdAdmin` service that shows and
//! converges members' auth state.
//!
//! Auth changes are raft log entries (`FastetcdLogEntry::Auth`), applied
//! by every member. Two things must hold before one is proposed:
//!
//! - **Every member can decode it.** A member older than replicated auth
//!   cannot: its AppendEntries fails and it stops receiving the log, and
//!   with two such members out of three the cluster has no quorum. So
//!   each member is asked over the peer port (`AuthSync`); one that
//!   answers `Unimplemented` is older, and one that cannot be reached
//!   cannot be confirmed. Either way the change is refused with
//!   `Unavailable` and a message naming the member.
//! - **Every member starts from the same auth tables.** Before
//!   replication each member kept whatever auth calls it served, so
//!   members upgraded from 1.4.x may differ. Entries applied to
//!   different tables give different results, so the digests are
//!   compared first. If they differ the change is refused with
//!   `FailedPrecondition`, `fastetcd_auth_diverged` is 1, and the
//!   operator chooses a member to adopt from (`fastetcd-ctl auth
//!   members`, then `fastetcd-ctl auth adopt <member>`). Nothing is
//!   chosen automatically (owner's decision, 2026-09-28).
//!
//! Once every current member is confirmed, later changes skip the survey
//! until the membership changes: the entries themselves, and raft
//! snapshots (which now carry the auth tables), keep members identical.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fastetcd_proto::fastetcd_admin as apb;
use fastetcd_proto::fastetcd_admin::fastetcd_admin_server::FastetcdAdmin;
use fastetcd_raft::types::{AuthSyncRequest, AuthSyncResponse, NodeId};
use fastetcd_raft::{auth_status, AuthSyncError, FastetcdLogEntry, FastetcdLogResponse};
use fastetcd_storage::mvcc::auth::{AuthApplyError, AuthOp};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::auth::AuthState;
use crate::authz::{require_root, UserIdentity};
use crate::state::ServerState;

/// How long the serving member waits to apply an auth entry the leader
/// has committed, before answering.
const APPLY_WAIT: Duration = Duration::from_secs(10);

/// How long members' auth digests may disagree before it is divergence
/// rather than a member that has not applied the last entry yet.
const CONVERGE: Duration = Duration::from_secs(3);

#[derive(Default)]
pub struct AuthGate {
    /// Members confirmed to run replicated auth and to hold the same
    /// auth tables as this member.
    confirmed: Mutex<BTreeSet<NodeId>>,
    /// Set when the last survey found different tables; with the number
    /// of adoptions applied at the time, since an adopt (from any member)
    /// makes that verdict stale.
    diverged: AtomicBool,
    diverged_at: AtomicU64,
}

/// One member's answer to the survey.
struct Surveyed {
    id: NodeId,
    status: Result<AuthSyncResponse, AuthSyncError>,
}

fn members(state: &ServerState) -> BTreeSet<NodeId> {
    let mut ids: BTreeSet<NodeId> = state
        .raft
        .metrics()
        .borrow()
        .membership_config
        .membership()
        .nodes()
        .map(|(id, _)| *id)
        .collect();
    ids.insert(state.member_id);
    ids
}

async fn survey(state: &ServerState, ids: &BTreeSet<NodeId>) -> Vec<Surveyed> {
    let mut out = Vec::with_capacity(ids.len());
    for &id in ids {
        let status = if id == state.member_id {
            state
                .sm
                .mvcc()
                .auth_tables()
                .await
                .map(|t| auth_status(&t))
                .map_err(|e| AuthSyncError::Unreachable(format!("read auth tables: {e}")))
        } else {
            state.forwarder.auth_sync(id, &AuthSyncRequest::Status).await
        };
        out.push(Surveyed { id, status });
    }
    out
}

fn digest_of(s: &Surveyed) -> Option<&str> {
    match &s.status {
        Ok(AuthSyncResponse::Status { digest, .. }) => Some(digest),
        _ => None,
    }
}

/// Refuse unless every member runs replicated auth.
fn require_all_upgraded(surveyed: &[Surveyed]) -> Result<(), Status> {
    for s in surveyed {
        match &s.status {
            Ok(_) => {}
            Err(AuthSyncError::Older) => {
                return Err(Status::unavailable(format!(
                    "auth changes are refused until every member runs fastetcd {} or later: \
                     member {:x} runs an older version, which cannot apply a replicated auth \
                     change (fastetcd#32)",
                    env!("CARGO_PKG_VERSION"),
                    s.id
                )))
            }
            Err(AuthSyncError::Unreachable(e)) => {
                return Err(Status::unavailable(format!(
                    "cannot confirm member {:x} runs a fastetcd with replicated auth ({e}); \
                     auth changes need every member reachable once after an upgrade or a \
                     membership change (fastetcd#32)",
                    s.id
                )))
            }
        }
    }
    Ok(())
}

impl AuthGate {
    /// Whether the last survey found members with different auth tables,
    /// and no adopt has applied here since.
    pub fn diverged(&self, auth: &AuthState) -> bool {
        self.diverged.load(Ordering::Relaxed)
            && self.diverged_at.load(Ordering::Relaxed) == auth.adoptions()
    }

    /// Forget which members are confirmed, so the next change surveys
    /// again. Called after an adopt.
    pub async fn reset(&self) {
        self.confirmed.lock().await.clear();
    }

    /// `Ok` when every member runs replicated auth, whatever its tables
    /// hold. Enough for `Authenticate`: each member validates the token
    /// entry against its own tables and adds it only if the user and
    /// password hash match there, so a login cannot change what differs.
    /// It must stay possible while members differ, or with auth on no
    /// one could get the root token that `auth adopt` needs.
    pub async fn check_upgraded(&self, state: &ServerState) -> Result<(), Status> {
        let ids = members(state);
        if ids.is_subset(&*self.confirmed.lock().await) {
            return Ok(());
        }
        require_all_upgraded(&survey(state, &ids).await)
    }

    /// `Ok` when an auth change may be proposed: every member runs
    /// replicated auth and holds the same auth tables as this one.
    pub async fn check(&self, state: &ServerState) -> Result<(), Status> {
        let ids = members(state);
        let mut confirmed = self.confirmed.lock().await;
        if ids.is_subset(&confirmed) {
            return Ok(());
        }
        // A member that has not applied the last auth entry yet (it learns
        // of the commit a heartbeat later) answers the digest before it.
        // That is lag, not divergence: survey again until the digests
        // agree or `CONVERGE` has passed. Tables that differ because
        // members kept their own before 1.5 never agree (fastetcd#65).
        let deadline = tokio::time::Instant::now() + CONVERGE;
        let surveyed = loop {
            let surveyed = survey(state, &ids).await;
            require_all_upgraded(&surveyed)?;
            let first = surveyed.first().and_then(digest_of);
            if surveyed.iter().all(|s| digest_of(s) == first) {
                *confirmed = ids;
                self.diverged.store(false, Ordering::Relaxed);
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                break surveyed;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        self.diverged_at.store(state.auth.adoptions(), Ordering::Relaxed);
        self.diverged.store(true, Ordering::Relaxed);
        let listing: Vec<String> = surveyed
            .iter()
            .map(|s| match &s.status {
                Ok(AuthSyncResponse::Status { digest, users, roles, enabled, .. }) => format!(
                    "member {:x}: {} users, {} roles, auth {} (digest {})",
                    s.id,
                    users.len(),
                    roles.len(),
                    if *enabled { "on" } else { "off" },
                    &digest[..12.min(digest.len())]
                ),
                _ => format!("member {:x}: ?", s.id),
            })
            .collect();
        tracing::warn!(
            target: "fastetcd::auth",
            members = %listing.join("; "),
            "members hold different auth state; auth changes are refused until one is \
             adopted with `fastetcd-ctl auth adopt <member>`"
        );
        Err(Status::failed_precondition(format!(
            "auth changes are refused: members hold different auth state (they kept whatever \
             auth calls each served before auth was replicated). {}. Inspect them with \
             `fastetcd-ctl auth members`, then replicate the right one to all with \
             `fastetcd-ctl auth adopt <member>` (fastetcd#32)",
            listing.join("; ")
        )))
    }
}

/// etcd answers every auth refusal at apply `FailedPrecondition` (user or
/// role not found or already existing, no root user), except
/// `ErrAuthOldRevision`, `InvalidArgument` (fastetcd#105).
fn apply_error(e: AuthApplyError) -> Status {
    match e {
        AuthApplyError::NotFound(m)
        | AuthApplyError::AlreadyExists(m)
        | AuthApplyError::FailedPrecondition(m) => {
            if m == crate::etcd_errors::AUTH_OLD_REVISION {
                crate::etcd_errors::auth_old_revision()
            } else {
                Status::failed_precondition(m)
            }
        }
    }
}

/// Propose an auth change through raft (after `AuthGate::check`) and
/// wait until this member has applied it too, so it is in effect here
/// when the call returns: a token works on the member that issued it,
/// a `UserGet` sees the new role. Returns the revision for the header.
pub async fn propose_auth(state: &ServerState, op: AuthOp) -> Result<i64, Status> {
    let resp = state.propose(FastetcdLogEntry::Auth(op)).await?;
    let FastetcdLogResponse::Auth { revision, log_index, result } = resp else {
        return Err(Status::internal(format!("auth entry got a non-auth response: {resp:?}")));
    };
    state
        .raft
        .wait(Some(APPLY_WAIT))
        .applied_index_at_least(Some(log_index), "auth entry applied locally")
        .await
        .map_err(|e| {
            Status::unavailable(format!(
                "auth change committed, but this member has not applied it yet: {e}"
            ))
        })?;
    result.map_err(apply_error)?;
    Ok(revision)
}

fn member_pb(s: &Surveyed) -> apb::AuthMember {
    let mut m = apb::AuthMember { member_id: s.id, ..Default::default() };
    match &s.status {
        Ok(AuthSyncResponse::Status { version, digest, users, roles, enabled }) => {
            m.version = version.clone();
            m.digest = digest.clone();
            m.users = users.clone();
            m.roles = roles.clone();
            m.enabled = *enabled;
        }
        Ok(AuthSyncResponse::Export(_)) => m.error = "unexpected export".into(),
        Err(AuthSyncError::Older) => {
            m.error = "runs a fastetcd older than replicated auth".into()
        }
        Err(AuthSyncError::Unreachable(e)) => m.error = format!("unreachable: {e}"),
    }
    m
}

/// `FastetcdAdmin` on the client port.
#[derive(Clone)]
pub struct AdminService {
    state: Arc<ServerState>,
}

impl AdminService {
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }

    async fn root<T>(&self, req: &Request<T>) -> Result<(), Status> {
        let user = req.extensions().get::<UserIdentity>().cloned();
        require_root(self.state.sm.mvcc().engine(), &self.state.auth, user.as_ref()).await
    }
}

#[tonic::async_trait]
impl FastetcdAdmin for AdminService {
    async fn auth_members(
        &self,
        req: Request<apb::AuthMembersRequest>,
    ) -> Result<Response<apb::AuthMembersResponse>, Status> {
        self.root(&req).await?;
        let surveyed = survey(&self.state, &members(&self.state)).await;
        let first = surveyed.first().and_then(digest_of);
        let identical = first.is_some() && surveyed.iter().all(|s| digest_of(s) == first);
        Ok(Response::new(apb::AuthMembersResponse {
            members: surveyed.iter().map(member_pb).collect(),
            identical,
        }))
    }

    async fn auth_adopt(
        &self,
        req: Request<apb::AuthAdoptRequest>,
    ) -> Result<Response<apb::AuthAdoptResponse>, Status> {
        self.root(&req).await?;
        let source = req.into_inner().member_id;
        let state = &self.state;
        let ids = members(state);
        if !ids.contains(&source) {
            return Err(Status::not_found(format!("no member {source:x} in this cluster")));
        }
        // Every member must be able to apply the adopt entry; whether
        // their tables match is exactly what this is here to fix.
        require_all_upgraded(&survey(state, &ids).await)?;
        let tables = if source == state.member_id {
            state
                .sm
                .mvcc()
                .auth_tables()
                .await
                .map_err(|e| Status::internal(format!("read auth tables: {e}")))?
        } else {
            match state.forwarder.auth_sync(source, &AuthSyncRequest::Export).await {
                Ok(AuthSyncResponse::Export(t)) => t,
                Ok(other) => {
                    return Err(Status::internal(format!("member {source:x} answered {other:?}")))
                }
                Err(e) => {
                    return Err(Status::unavailable(format!(
                        "cannot read member {source:x}'s auth tables: {e:?}"
                    )))
                }
            }
        };
        let adopted = Surveyed { id: source, status: Ok(auth_status(&tables)) };
        tracing::warn!(
            target: "fastetcd::auth",
            member = %format!("{source:x}"),
            digest = %tables.digest(),
            "adopting this member's auth tables on every member"
        );
        propose_auth(state, AuthOp::Adopt { tables }).await?;
        state.auth_gate.reset().await;
        Ok(Response::new(apb::AuthAdoptResponse { adopted: Some(member_pb(&adopted)) }))
    }
}
