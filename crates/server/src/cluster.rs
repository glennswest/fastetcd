//! Implementation of the etcd `Cluster` gRPC service.
//!
//! Single-node fastetcd: `MemberList` returns just this node.
//! `MemberAdd` / `MemberRemove` / `MemberUpdate` / `MemberPromote`
//! return `Status::unimplemented` until peer transport (task #13)
//! lands and openraft membership-change RPCs are wired up.

use std::sync::Arc;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::cluster_server::Cluster;
use tonic::{Request, Response, Status};

use crate::state::{response_header, ServerState};

#[derive(Clone)]
pub struct ClusterService {
    state: Arc<ServerState>,
    self_name: String,
    self_peer_urls: Vec<String>,
    self_client_urls: Vec<String>,
}

impl ClusterService {
    pub fn new(
        state: Arc<ServerState>,
        self_name: String,
        self_peer_urls: Vec<String>,
        self_client_urls: Vec<String>,
    ) -> Self {
        Self {
            state,
            self_name,
            self_peer_urls,
            self_client_urls,
        }
    }

    fn self_member(&self) -> pb::Member {
        pb::Member {
            id: self.state.member_id,
            name: self.self_name.clone(),
            peer_ur_ls: self.self_peer_urls.clone(),
            client_ur_ls: self.self_client_urls.clone(),
            is_learner: false,
        }
    }
}

#[tonic::async_trait]
impl Cluster for ClusterService {
    async fn member_list(
        &self,
        _request: Request<pb::MemberListRequest>,
    ) -> Result<Response<pb::MemberListResponse>, Status> {
        let revision = self.state.sm.mvcc().current_revision().await;
        let header = response_header(&self.state, revision).await;
        Ok(Response::new(pb::MemberListResponse {
            header: Some(header),
            members: vec![self.self_member()],
        }))
    }

    async fn member_add(
        &self,
        _request: Request<pb::MemberAddRequest>,
    ) -> Result<Response<pb::MemberAddResponse>, Status> {
        Err(Status::unimplemented(
            "MemberAdd requires peer transport (task #13)",
        ))
    }

    async fn member_remove(
        &self,
        _request: Request<pb::MemberRemoveRequest>,
    ) -> Result<Response<pb::MemberRemoveResponse>, Status> {
        Err(Status::unimplemented(
            "MemberRemove requires peer transport (task #13)",
        ))
    }

    async fn member_update(
        &self,
        _request: Request<pb::MemberUpdateRequest>,
    ) -> Result<Response<pb::MemberUpdateResponse>, Status> {
        Err(Status::unimplemented(
            "MemberUpdate requires peer transport (task #13)",
        ))
    }

    async fn member_promote(
        &self,
        _request: Request<pb::MemberPromoteRequest>,
    ) -> Result<Response<pb::MemberPromoteResponse>, Status> {
        Err(Status::unimplemented(
            "MemberPromote requires peer transport (task #13)",
        ))
    }
}
