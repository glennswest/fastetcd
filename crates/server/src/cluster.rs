//! Implementation of the etcd `Cluster` gRPC service — multi-node.
//!
//! Backed by openraft's `add_learner` / `change_membership` APIs.
//! Holds a shared `PeerEndpoints` map (the same one
//! `GrpcNetworkFactory` dials through) so peers added at runtime are
//! immediately reachable.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::cluster_server::Cluster;
use fastetcd_raft::network::PeerEndpoints;
use fastetcd_raft::types::NodeId;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use crate::state::{response_header, ServerState};

/// Member metadata that `MemberList` returns — name + URLs etc.
/// openraft only persists the address (peer URL) per node; we keep
/// the rest in a separate map.
#[derive(Clone, Default)]
pub struct MemberInfo {
    pub name: String,
    pub peer_urls: Vec<String>,
    pub client_urls: Vec<String>,
    pub is_learner: bool,
}

pub type MemberDirectory = Arc<RwLock<BTreeMap<NodeId, MemberInfo>>>;

#[derive(Clone)]
pub struct ClusterService {
    state: Arc<ServerState>,
    peers: PeerEndpoints,
    directory: MemberDirectory,
    self_node_id: NodeId,
}

impl ClusterService {
    /// The caller is responsible for seeding `directory` with the
    /// initial member set (including self) before constructing the
    /// service. `new` does not touch the directory itself so it can
    /// be called from either sync or async contexts.
    pub fn new(
        state: Arc<ServerState>,
        self_node_id: NodeId,
        peers: PeerEndpoints,
        directory: MemberDirectory,
    ) -> Self {
        Self {
            state,
            peers,
            directory,
            self_node_id,
        }
    }

    /// Seed the directory with this node's own member info. Helper
    /// for bootstrap; callers can also populate the directory
    /// directly.
    pub async fn seed_self(
        directory: &MemberDirectory,
        self_node_id: NodeId,
        self_name: String,
        self_peer_urls: Vec<String>,
        self_client_urls: Vec<String>,
    ) {
        let mut dir = directory.write().await;
        dir.insert(
            self_node_id,
            MemberInfo {
                name: self_name,
                peer_urls: self_peer_urls,
                client_urls: self_client_urls,
                is_learner: false,
            },
        );
    }

    /// Derive a stable NodeId from a member's peer URL (used when the
    /// caller did not supply one). FNV-1a over the URL bytes, masked
    /// to 63 bits to avoid openraft's sentinel values.
    fn derive_node_id_from_url(url: &str) -> NodeId {
        let mut hash: u64 = 0xcbf29ce484222325;
        for b in url.as_bytes() {
            hash ^= *b as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        (hash & 0x7FFF_FFFF_FFFF_FFFF).max(1)
    }
}

fn members_to_pb(dir: &BTreeMap<NodeId, MemberInfo>) -> Vec<pb::Member> {
    dir.iter()
        .map(|(id, info)| pb::Member {
            id: *id,
            name: info.name.clone(),
            peer_ur_ls: info.peer_urls.clone(),
            client_ur_ls: info.client_urls.clone(),
            is_learner: info.is_learner,
        })
        .collect()
}

#[tonic::async_trait]
impl Cluster for ClusterService {
    async fn member_list(
        &self,
        _request: Request<pb::MemberListRequest>,
    ) -> Result<Response<pb::MemberListResponse>, Status> {
        let revision = self.state.sm.mvcc().current_revision().await;
        let header = response_header(&self.state, revision).await;
        let dir = self.directory.read().await;
        Ok(Response::new(pb::MemberListResponse {
            header: Some(header),
            members: members_to_pb(&dir),
        }))
    }

    async fn member_add(
        &self,
        request: Request<pb::MemberAddRequest>,
    ) -> Result<Response<pb::MemberAddResponse>, Status> {
        let req = request.into_inner();
        let first_url = req
            .peer_ur_ls
            .first()
            .cloned()
            .ok_or_else(|| Status::invalid_argument("MemberAddRequest.peer_urls empty"))?;
        let new_id = Self::derive_node_id_from_url(&first_url);

        // Register peer endpoint BEFORE add_learner so the leader's
        // first AppendEntries to the new node has a route.
        {
            let mut peers = self.peers.write().await;
            peers.insert(new_id, first_url.clone());
        }

        // add_learner; non-blocking so we don't wait for catch-up.
        self.state
            .raft
            .add_learner(new_id, openraft::BasicNode::new(&first_url), false)
            .await
            .map_err(|e| Status::unavailable(format!("raft add_learner: {e}")))?;

        if !req.is_learner {
            // Promote to voter via change_membership.
            let current_voters = current_voter_set(&self.state.raft).await;
            let mut new_voters: BTreeSet<NodeId> = current_voters;
            new_voters.insert(new_id);
            self.state
                .raft
                .change_membership(new_voters, false)
                .await
                .map_err(|e| Status::unavailable(format!("raft change_membership: {e}")))?;
        }

        // Update the directory.
        let info = MemberInfo {
            name: String::new(),
            peer_urls: req.peer_ur_ls.clone(),
            client_urls: Vec::new(),
            is_learner: req.is_learner,
        };
        {
            let mut dir = self.directory.write().await;
            dir.insert(new_id, info.clone());
        }

        let revision = self.state.sm.mvcc().current_revision().await;
        let header = response_header(&self.state, revision).await;
        let dir = self.directory.read().await;
        Ok(Response::new(pb::MemberAddResponse {
            header: Some(header),
            member: Some(pb::Member {
                id: new_id,
                name: info.name,
                peer_ur_ls: info.peer_urls,
                client_ur_ls: info.client_urls,
                is_learner: info.is_learner,
            }),
            members: members_to_pb(&dir),
        }))
    }

    async fn member_remove(
        &self,
        request: Request<pb::MemberRemoveRequest>,
    ) -> Result<Response<pb::MemberRemoveResponse>, Status> {
        let req = request.into_inner();
        if req.id == self.self_node_id {
            return Err(Status::invalid_argument(
                "cannot MemberRemove self; use MoveLeader first or remove via another node",
            ));
        }
        // Build the new voter set minus the target.
        let mut voters = current_voter_set(&self.state.raft).await;
        let was_present = voters.remove(&req.id);
        if was_present {
            self.state
                .raft
                .change_membership(voters, false)
                .await
                .map_err(|e| Status::unavailable(format!("raft change_membership: {e}")))?;
        }
        // Drop peer + directory entries.
        {
            let mut peers = self.peers.write().await;
            peers.remove(&req.id);
        }
        {
            let mut dir = self.directory.write().await;
            dir.remove(&req.id);
        }
        let revision = self.state.sm.mvcc().current_revision().await;
        let header = response_header(&self.state, revision).await;
        let dir = self.directory.read().await;
        Ok(Response::new(pb::MemberRemoveResponse {
            header: Some(header),
            members: members_to_pb(&dir),
        }))
    }

    async fn member_update(
        &self,
        request: Request<pb::MemberUpdateRequest>,
    ) -> Result<Response<pb::MemberUpdateResponse>, Status> {
        let req = request.into_inner();
        let first_url = req
            .peer_ur_ls
            .first()
            .cloned()
            .ok_or_else(|| Status::invalid_argument("MemberUpdateRequest.peer_urls empty"))?;
        // Update peer URL map.
        {
            let mut peers = self.peers.write().await;
            peers.insert(req.id, first_url.clone());
        }
        {
            let mut dir = self.directory.write().await;
            if let Some(info) = dir.get_mut(&req.id) {
                info.peer_urls = req.peer_ur_ls.clone();
            }
        }
        let revision = self.state.sm.mvcc().current_revision().await;
        let header = response_header(&self.state, revision).await;
        let dir = self.directory.read().await;
        Ok(Response::new(pb::MemberUpdateResponse {
            header: Some(header),
            members: members_to_pb(&dir),
        }))
    }

    async fn member_promote(
        &self,
        request: Request<pb::MemberPromoteRequest>,
    ) -> Result<Response<pb::MemberPromoteResponse>, Status> {
        let req = request.into_inner();
        let mut voters = current_voter_set(&self.state.raft).await;
        if !voters.insert(req.id) {
            return Err(Status::failed_precondition(format!(
                "member {} is already a voter",
                req.id
            )));
        }
        self.state
            .raft
            .change_membership(voters, false)
            .await
            .map_err(|e| Status::unavailable(format!("raft change_membership: {e}")))?;
        {
            let mut dir = self.directory.write().await;
            if let Some(info) = dir.get_mut(&req.id) {
                info.is_learner = false;
            }
        }
        let revision = self.state.sm.mvcc().current_revision().await;
        let header = response_header(&self.state, revision).await;
        let dir = self.directory.read().await;
        Ok(Response::new(pb::MemberPromoteResponse {
            header: Some(header),
            members: members_to_pb(&dir),
        }))
    }
}

async fn current_voter_set(
    raft: &openraft::Raft<fastetcd_raft::types::TypeConfig>,
) -> BTreeSet<NodeId> {
    let m = raft.metrics().borrow().membership_config.clone();
    m.voter_ids().collect()
}
