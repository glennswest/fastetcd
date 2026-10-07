//! Implementation of the etcd `Lease` gRPC service.
//!
//! Grant and Revoke go through Raft (`FastetcdLogEntry::Lease*`). A
//! keep-alive is renewed in the leader's RAM by its lessor
//! (`fastetcd_raft::lessor`, fastetcd#92): a follower forwards it, and
//! while any member is older than 1.23 it is proposed through Raft as
//! before. TimeToLive is answered by the leader. Expiry is the leader's
//! sweeper (`lease_expiry`), which proposes the revoke.

use std::pin::Pin;
use std::sync::Arc;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::lease_server::Lease;
use fastetcd_raft::lessor::Renewal;
use fastetcd_raft::network::TtlForwardError;
use fastetcd_raft::{FastetcdLogEntry, FastetcdLogResponse};
use fastetcd_storage::mvcc::Refusal;
use tokio::sync::mpsc;

/// etcd's minimum lease TTL with its default timing (heartbeat 100 ms,
/// election 1 s: ceil(1.5 x election timeout) = 2 s). A grant asking
/// for TTL <= 0 gets this.
pub const MIN_LEASE_TTL_SECS: i64 = 2;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

use crate::authz::{authorize_keys, RequiredPerm, UserIdentity};
use crate::state::{response_header, ServerState};

#[derive(Clone)]
pub struct LeaseService {
    state: Arc<ServerState>,
}

impl LeaseService {
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }

    async fn propose(&self, entry: FastetcdLogEntry) -> Result<FastetcdLogResponse, Status> {
        self.state.propose(entry).await
    }
}

/// With auth on, require `perm` on every key attached to lease `id` now
/// (fastetcd#47): etcd's `checkLeasePuts` (revoke, write), `checkLeaseRenew`
/// (keep-alive, write) and `checkLeaseTimeToLive` (read). Root is exempt;
/// a lease with no keys, or none at all, needs only the login the
/// interceptor already checked. Checked against this member's applied
/// state at the API layer, as the KV checks are; etcd checks a revoke
/// at apply.
pub(crate) async fn authorize_lease(
    state: &ServerState,
    user: Option<&UserIdentity>,
    perm: RequiredPerm,
    id: i64,
) -> Result<(), Status> {
    if !state.auth.is_enabled() || id == 0 {
        return Ok(());
    }
    let keys = state
        .sm
        .mvcc()
        .lease_attached_keys(id)
        .await
        .map_err(|e| Status::internal(format!("lease keys: {e}")))?;
    authorize_keys(state.sm.mvcc().engine(), &state.auth, user, perm, &keys).await
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// TimeToLive as the leader knows it (#92): keep-alives live in the
/// leader's RAM, so a follower asks the leader. A leader older than 1.23
/// logs every keep-alive, so then the follower's own table is right.
async fn time_to_live(
    state: &ServerState,
    id: i64,
    keys: bool,
) -> Result<Option<fastetcd_storage::mvcc::LeaseTtlResult>, Status> {
    let internal = |e: fastetcd_storage::mvcc::MvccError| Status::internal(format!("lease_ttl: {e}"));
    let m = state.raft.metrics().borrow().clone();
    match m.current_leader {
        Some(leader) if leader != m.id => {
            match state.forwarder.lease_time_to_live(leader, id, keys).await {
                Ok(t) => Ok(t),
                Err(TtlForwardError::Older) => state.lessor.time_to_live(id, keys).await.map_err(internal),
                Err(TtlForwardError::Failed(e)) => Err(Status::unavailable(format!(
                    "lease TimeToLive from leader {leader:x}: {e}"
                ))),
            }
        }
        _ => state.lessor.time_to_live(id, keys).await.map_err(internal),
    }
}

#[tonic::async_trait]
impl Lease for LeaseService {
    async fn lease_grant(
        &self,
        request: Request<pb::LeaseGrantRequest>,
    ) -> Result<Response<pb::LeaseGrantResponse>, Status> {
        let req = request.into_inner();
        // etcd caps lease grants under a NOSPACE alarm alongside puts —
        // a new lease is a new keyspace to fill (fastetcd#14). Revoke
        // and keep-alive stay available.
        self.state.space.check_write()?;
        // etcd grants a TTL <= 0 at its minimum lease TTL rather than
        // refusing it; the state machine refuses one that reaches it
        // (#49), so it is raised here, before proposing.
        let ttl_secs = if req.ttl <= 0 { MIN_LEASE_TTL_SECS } else { req.ttl };
        let resp = self
            .propose(FastetcdLogEntry::LeaseGrant {
                id: req.id,
                ttl_secs,
                now_unix: now_unix(),
            })
            .await?;
        let grant = match resp {
            FastetcdLogResponse::LeaseGrant(g) => g,
            other => return Err(Status::internal(format!("unexpected response: {other:?}"))),
        };
        let header = response_header(&self.state, grant.revision).await;
        Ok(Response::new(pb::LeaseGrantResponse {
            header: Some(header),
            id: grant.id,
            ttl: grant.ttl_secs,
            error: String::new(),
        }))
    }

    async fn lease_revoke(
        &self,
        request: Request<pb::LeaseRevokeRequest>,
    ) -> Result<Response<pb::LeaseRevokeResponse>, Status> {
        let user = request.extensions().get::<UserIdentity>().cloned();
        let req = request.into_inner();
        // Revoking deletes every attached key (etcd `checkLeasePuts`).
        authorize_lease(&self.state, user.as_ref(), RequiredPerm::Write, req.id).await?;
        let resp = self
            .propose(FastetcdLogEntry::LeaseRevoke { id: req.id })
            .await?;
        let revoke = match resp {
            FastetcdLogResponse::LeaseRevoke(r) => r,
            other => return Err(Status::internal(format!("unexpected response: {other:?}"))),
        };
        let header = response_header(&self.state, revoke.revision).await;
        Ok(Response::new(pb::LeaseRevokeResponse {
            header: Some(header),
        }))
    }

    type LeaseKeepAliveStream =
        Pin<Box<dyn Stream<Item = Result<pb::LeaseKeepAliveResponse, Status>> + Send>>;

    async fn lease_keep_alive(
        &self,
        request: Request<Streaming<pb::LeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>, Status> {
        let state = self.state.clone();
        let user = request.extensions().get::<UserIdentity>().cloned();
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<pb::LeaseKeepAliveResponse, Status>>(8);

        tokio::spawn(async move {
            while let Some(req) = inbound.next().await {
                let Ok(req) = req else {
                    break;
                };
                // A renewal keeps every attached key alive (etcd
                // `checkLeaseRenew`); denied, the stream ends with the
                // error, as etcd's does.
                if let Err(status) =
                    authorize_lease(&state, user.as_ref(), RequiredPerm::Write, req.id).await
                {
                    let _ = tx.send(Err(status)).await;
                    break;
                }
                // The leader renews in RAM (#92); a follower forwards, and
                // the leader renews it there.
                let renewed = match state.lessor.renew(req.id).await {
                    Renewal::Renewed(t) => Ok(FastetcdLogResponse::LeaseKeepAlive(t)),
                    Renewal::NotFound => {
                        Err(Status::not_found(Refusal::LeaseNotFound.message()))
                    }
                    Renewal::Propose => {
                        state
                            .propose(FastetcdLogEntry::LeaseKeepAlive {
                                id: req.id,
                                now_unix: now_unix(),
                            })
                            .await
                    }
                };
                let res = match renewed {
                    Ok(data) => data,
                    // etcd answers a keep-alive of a lease that does not
                    // exist (expired, revoked) with TTL 0, not an error;
                    // clientv3 takes that as the lease being gone (#49).
                    Err(status)
                        if status.code() == tonic::Code::NotFound
                            && status.message() == Refusal::LeaseNotFound.message() =>
                    {
                        let revision = state.sm.mvcc().current_revision().await;
                        let resp = pb::LeaseKeepAliveResponse {
                            header: Some(response_header(&state, revision).await),
                            id: req.id,
                            ttl: 0,
                        };
                        if tx.send(Ok(resp)).await.is_err() {
                            break;
                        }
                        continue;
                    }
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        continue;
                    }
                };
                let ttl = match res {
                    FastetcdLogResponse::LeaseKeepAlive(t) => t,
                    _ => continue,
                };
                let revision = state.sm.mvcc().current_revision().await;
                let header = response_header(&state, revision).await;
                let resp = pb::LeaseKeepAliveResponse {
                    header: Some(header),
                    id: ttl.id,
                    ttl: ttl.granted_ttl_secs,
                };
                if tx.send(Ok(resp)).await.is_err() {
                    break;
                }
            }
        });

        let stream: Self::LeaseKeepAliveStream = Box::pin(ReceiverStream::new(rx));
        Ok(Response::new(stream))
    }

    async fn lease_time_to_live(
        &self,
        request: Request<pb::LeaseTimeToLiveRequest>,
    ) -> Result<Response<pb::LeaseTimeToLiveResponse>, Status> {
        let user = request.extensions().get::<UserIdentity>().cloned();
        let req = request.into_inner();
        // Listing the keys needs read on each of them; the TTL alone
        // needs only a login (etcd: checked "only if Keys is true").
        if req.keys {
            authorize_lease(&self.state, user.as_ref(), RequiredPerm::Read, req.id).await?;
        }
        let ttl = time_to_live(&self.state, req.id, req.keys).await?;

        let revision = self.state.sm.mvcc().current_revision().await;
        let header = response_header(&self.state, revision).await;
        let resp = match ttl {
            Some(t) => pb::LeaseTimeToLiveResponse {
                header: Some(header),
                id: t.id,
                ttl: t.remaining_ttl_secs,
                granted_ttl: t.granted_ttl_secs,
                keys: t.keys,
            },
            None => pb::LeaseTimeToLiveResponse {
                header: Some(header),
                id: req.id,
                ttl: -1, // etcd's convention for "lease not found"
                granted_ttl: 0,
                keys: Vec::new(),
            },
        };
        Ok(Response::new(resp))
    }

    async fn lease_leases(
        &self,
        request: Request<pb::LeaseLeasesRequest>,
    ) -> Result<Response<pb::LeaseLeasesResponse>, Status> {
        // Read on every key of every lease (etcd `checkLeaseLeases`).
        if self.state.auth.is_enabled() {
            let user = request.extensions().get::<UserIdentity>().cloned();
            let keys = self
                .state
                .sm
                .mvcc()
                .all_lease_attached_keys()
                .await
                .map_err(|e| Status::internal(format!("lease keys: {e}")))?;
            authorize_keys(
                self.state.sm.mvcc().engine(),
                &self.state.auth,
                user.as_ref(),
                RequiredPerm::Read,
                &keys,
            )
            .await?;
        }
        let ids = self
            .state
            .sm
            .mvcc()
            .lease_list()
            .await
            .map_err(|e| Status::internal(format!("lease_list: {e}")))?;
        let revision = self.state.sm.mvcc().current_revision().await;
        let header = response_header(&self.state, revision).await;
        let leases = ids
            .into_iter()
            .map(|id| pb::LeaseStatus { id })
            .collect();
        Ok(Response::new(pb::LeaseLeasesResponse {
            header: Some(header),
            leases,
        }))
    }
}
