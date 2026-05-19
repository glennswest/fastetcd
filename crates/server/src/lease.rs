//! Implementation of the etcd `Lease` gRPC service.
//!
//! Phase 1: Grant / Revoke / KeepAlive / TimeToLive / Leases all
//! route through Raft via `FastetcdLogEntry::Lease*` (mutations) or
//! direct `MvccStore` reads (TimeToLive / Leases).
//!
//! **Known gap:** there is no background ticker that auto-revokes
//! expired leases. The TTL machinery is correct (deadline is
//! persisted; TimeToLive reports remaining seconds) but expired
//! leases stay attached until a client explicitly revokes them. A
//! follow-up commit will add a leader-side ticker that proposes
//! `LeaseRevoke` entries for any lease whose deadline is past.

use std::pin::Pin;
use std::sync::Arc;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::lease_server::Lease;
use fastetcd_raft::{FastetcdLogEntry, FastetcdLogResponse};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

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
        match self.state.raft.client_write(entry).await {
            Ok(w) => Ok(w.data),
            Err(e) => Err(Status::unavailable(format!("raft client_write: {e}"))),
        }
    }
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[tonic::async_trait]
impl Lease for LeaseService {
    async fn lease_grant(
        &self,
        request: Request<pb::LeaseGrantRequest>,
    ) -> Result<Response<pb::LeaseGrantResponse>, Status> {
        let req = request.into_inner();
        let resp = self
            .propose(FastetcdLogEntry::LeaseGrant {
                id: req.id,
                ttl_secs: req.ttl,
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
        let req = request.into_inner();
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
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<pb::LeaseKeepAliveResponse, Status>>(8);

        tokio::spawn(async move {
            while let Some(req) = inbound.next().await {
                let Ok(req) = req else {
                    break;
                };
                let res = match state
                    .raft
                    .client_write(FastetcdLogEntry::LeaseKeepAlive {
                        id: req.id,
                        now_unix: now_unix(),
                    })
                    .await
                {
                    Ok(w) => w.data,
                    Err(e) => {
                        let _ = tx
                            .send(Err(Status::unavailable(format!(
                                "raft client_write: {e}"
                            ))))
                            .await;
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
        let req = request.into_inner();
        let ttl = self
            .state
            .sm
            .mvcc()
            .lease_ttl(req.id, req.keys, now_unix())
            .await
            .map_err(|e| Status::internal(format!("lease_ttl: {e}")))?;

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
        _request: Request<pb::LeaseLeasesRequest>,
    ) -> Result<Response<pb::LeaseLeasesResponse>, Status> {
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
