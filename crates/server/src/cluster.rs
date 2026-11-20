//! Implementation of the etcd `Cluster` gRPC service — multi-node.
//!
//! Backed by openraft's `add_learner` / `change_membership` APIs: every
//! change goes through raft (forwarded to the leader from a follower),
//! so the committed membership is the record of who the members are and
//! where they are. [`follow_membership`] keeps each member's peer routes
//! (the `PeerEndpoints` map `GrpcNetworkFactory` and the forwarder dial
//! through) and its member directory in step with it, so a change made
//! through any member, or a restart with an old `--initial-cluster`,
//! sees the same members at the same addresses (#208).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::cluster_server::Cluster;
use fastetcd_raft::network::PeerEndpoints;
use fastetcd_raft::types::{MembershipChange, NodeId, TypeConfig};
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
        // Root only while auth is on, as in etcd (checkMembershipOperationPermission, #31).
        crate::authz::require_admin(&self.state, &request).await?;
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

        // A member that cannot read this log's batched entries is refused
        // before anything changes (#77).
        if let Err(e) = crate::batch_guard::guard_add(&self.state, new_id).await {
            self.peers.write().await.remove(&new_id);
            return Err(e);
        }
        if !req.is_learner && self.state.sm.mvcc().has_batched() {
            // A voter must be known to read the log; one not reachable yet
            // is added as a learner and promoted later (#77).
            if let crate::batch_guard::MemberVersion::Unreachable(_) =
                crate::batch_guard::member_version(&self.state, new_id).await
            {
                self.peers.write().await.remove(&new_id);
                return Err(Status::failed_precondition(format!(
                    "member {new_id:x} cannot be reached to confirm it runs fastetcd 1.10 or \
                     later, and this cluster's log holds batched entries an older member cannot \
                     read (#77): add it as a learner (`member add --learner`), start it, then \
                     promote it"
                )));
            }
        }

        // add_learner; non-blocking so we don't wait for catch-up.
        // Forwards to the leader if this node isn't it (#7).
        self.state.propose_add_learner(new_id, &first_url).await?;

        if !req.is_learner {
            // Promote to voter via change_membership.
            let current_voters = current_voter_set(&self.state.raft).await;
            let mut new_voters: BTreeSet<NodeId> = current_voters;
            new_voters.insert(new_id);
            self.state.propose_set_voters(new_voters).await?;
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
        // Root only while auth is on, as in etcd (checkMembershipOperationPermission, #31).
        crate::authz::require_admin(&self.state, &request).await?;
        let req = request.into_inner();
        if req.id == self.self_node_id {
            return Err(Status::invalid_argument(
                "cannot MemberRemove self; use MoveLeader first or remove via another node",
            ));
        }
        if !membership_nodes(&self.state.raft).contains_key(&req.id) {
            return Err(member_not_found());
        }
        // A voter or a learner: either way out of the raft membership
        // (a learner used to be dropped only from local maps, #208).
        self.state.propose_membership(MembershipChange::RemoveNode { node_id: req.id }).await?;
        // Drop peer + directory entries here at once; every member drops
        // them as the change reaches it (follow_membership).
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
        // Root only while auth is on, as in etcd (checkMembershipOperationPermission, #31).
        crate::authz::require_admin(&self.state, &request).await?;
        let req = request.into_inner();
        let first_url = req
            .peer_ur_ls
            .first()
            .cloned()
            .ok_or_else(|| Status::invalid_argument("MemberUpdateRequest.peer_urls empty"))?;
        let nodes = membership_nodes(&self.state.raft);
        if !nodes.contains_key(&req.id) {
            return Err(member_not_found());
        }
        if nodes.iter().any(|(id, addr)| *id != req.id && req.peer_ur_ls.contains(addr)) {
            return Err(Status::failed_precondition("etcdserver: Peer URLs already exists"));
        }
        // The address is part of the raft membership: a change made only
        // here was not seen by the leader, was lost on restart, and could
        // leave the member unreachable (#208). Raft keeps one address
        // per member, the first URL.
        self.state
            .propose_membership(MembershipChange::SetNodeAddr { node_id: req.id, addr: first_url.clone() })
            .await?;
        // Update peer URL map here at once; every member follows the
        // committed membership (follow_membership).
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
        // Root only while auth is on, as in etcd (checkMembershipOperationPermission, #31).
        crate::authz::require_admin(&self.state, &request).await?;
        let req = request.into_inner();
        // A voter that cannot read this log's batched entries would stop
        // at the first and cost the quorum (#77).
        crate::batch_guard::guard_voter(&self.state, req.id).await?;
        let mut voters = current_voter_set(&self.state.raft).await;
        if !voters.insert(req.id) {
            return Err(Status::failed_precondition(format!(
                "member {} is already a voter",
                req.id
            )));
        }
        self.state.propose_set_voters(voters).await?;
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

/// etcd's `ErrGRPCMemberNotFound`.
fn member_not_found() -> Status {
    Status::not_found("etcdserver: member not found")
}

/// Every member of the raft membership (voters and learners) and its
/// address, as this member knows it.
fn membership_nodes(raft: &openraft::Raft<TypeConfig>) -> BTreeMap<NodeId, String> {
    let m = raft.metrics().borrow().membership_config.clone();
    m.membership().nodes().map(|(id, node)| (*id, node.addr.clone())).collect()
}

/// A member as the raft membership has it: its address and whether it
/// votes.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Membered {
    addr: String,
    voter: bool,
}

fn membership_of(raft: &openraft::Raft<TypeConfig>) -> BTreeMap<NodeId, Membered> {
    let m = raft.metrics().borrow().membership_config.clone();
    let voters: BTreeSet<NodeId> = m.voter_ids().collect();
    m.membership()
        .nodes()
        .map(|(id, node)| (*id, Membered { addr: node.addr.clone(), voter: voters.contains(id) }))
        .collect()
}

/// Bring `peers` and `directory` in line with `now` (the membership):
/// every member's address and learner flag; members that are gone
/// dropped. `before` is the membership last applied, or `None` the
/// first time, when anything not in it goes (an `--initial-cluster`
/// member removed before this start). Only members that left since
/// `before` are dropped later: MemberAdd routes a member just before
/// it joins the membership.
async fn apply_membership(
    peers: &PeerEndpoints,
    directory: &MemberDirectory,
    self_id: NodeId,
    before: Option<&BTreeMap<NodeId, Membered>>,
    now: &BTreeMap<NodeId, Membered>,
) {
    let gone: Vec<NodeId> = match before {
        Some(b) => b.keys().filter(|id| !now.contains_key(id)).copied().collect(),
        None => {
            let mut ids: BTreeSet<NodeId> = peers.read().await.keys().copied().collect();
            ids.extend(directory.read().await.keys().copied());
            ids.into_iter().filter(|id| !now.contains_key(id)).collect()
        }
    };
    {
        let mut p = peers.write().await;
        for (id, m) in now {
            if *id != self_id && p.get(id) != Some(&m.addr) {
                tracing::info!(member = format!("{id:x}"), addr = %m.addr, "peer route from the raft membership");
                p.insert(*id, m.addr.clone());
            }
        }
        for id in &gone {
            if p.remove(id).is_some() {
                tracing::info!(member = format!("{id:x}"), "member left the raft membership: route dropped");
            }
        }
    }
    let mut dir = directory.write().await;
    for (id, m) in now {
        let info = dir.entry(*id).or_default();
        if info.peer_urls.first() != Some(&m.addr) {
            info.peer_urls = vec![m.addr.clone()];
        }
        info.is_learner = !m.voter;
    }
    for id in &gone {
        if *id != self_id {
            dir.remove(id);
        }
    }
}

/// While running: keep this member's peer routes and member directory
/// in line with the raft membership as it changes (#208). Nothing is
/// done while the membership is empty (a new member not yet joined).
pub fn follow_membership(
    raft: openraft::Raft<TypeConfig>,
    peers: PeerEndpoints,
    directory: MemberDirectory,
    self_id: NodeId,
) {
    tokio::spawn(async move {
        let mut metrics = raft.metrics();
        let mut applied: Option<BTreeMap<NodeId, Membered>> = None;
        loop {
            metrics.borrow_and_update();
            let now = membership_of(&raft);
            if !now.is_empty() && applied.as_ref() != Some(&now) {
                apply_membership(&peers, &directory, self_id, applied.as_ref(), &now).await;
                applied = Some(now);
            }
            if metrics.changed().await.is_err() {
                return;
            }
        }
    });
}

async fn current_voter_set(
    raft: &openraft::Raft<fastetcd_raft::types::TypeConfig>,
) -> BTreeSet<NodeId> {
    let m = raft.metrics().borrow().membership_config.clone();
    m.voter_ids().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(addr: &str, voter: bool) -> Membered {
        Membered { addr: addr.to_string(), voter }
    }

    #[tokio::test]
    async fn routes_and_directory_follow_the_membership() {
        let peers = fastetcd_raft::network::empty_peers();
        let directory: MemberDirectory = Arc::new(RwLock::new(BTreeMap::new()));
        // As seeded from --initial-cluster: 2 and 9, 9 removed before
        // this start; self is 1.
        peers.write().await.extend([(2, "http://old-2".to_string()), (9, "http://9".to_string())]);
        ClusterService::seed_self(&directory, 1, "one".into(), vec!["http://1".into()], vec![]).await;
        directory.write().await.insert(9, MemberInfo { name: "nine".into(), ..Default::default() });

        let first = BTreeMap::from([(1, m("http://1", true)), (2, m("http://2", true)), (3, m("http://3", false))]);
        apply_membership(&peers, &directory, 1, None, &first).await;
        {
            let p = peers.read().await;
            assert_eq!(p.get(&2).map(String::as_str), Some("http://2"), "the address moves");
            assert_eq!(p.get(&3).map(String::as_str), Some("http://3"));
            assert!(!p.contains_key(&9) && !p.contains_key(&1), "gone, and never self");
            let d = directory.read().await;
            assert_eq!(d.keys().copied().collect::<Vec<_>>(), vec![1, 2, 3]);
            assert!(d[&3].is_learner && !d[&2].is_learner);
            assert_eq!(d[&1].name, "one", "self keeps its own info");
        }

        // MemberAdd routes 7 before it is in the membership; a later
        // change that removes 3 and promotes nothing must keep that.
        peers.write().await.insert(7, "http://7".into());
        let second = BTreeMap::from([(1, m("http://1", true)), (2, m("http://2", true))]);
        apply_membership(&peers, &directory, 1, Some(&first), &second).await;
        let p = peers.read().await;
        assert!(!p.contains_key(&3), "a removed learner's route goes");
        assert!(p.contains_key(&7), "a member being added keeps its route");
        assert!(!directory.read().await.contains_key(&3));
    }
}
