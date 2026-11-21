//! Implementation of the etcd `Cluster` gRPC service — multi-node.
//!
//! Backed by openraft's `add_learner` / `change_membership` APIs.
//! Holds the shared `PeerEndpoints` map the forwarder dials through
//! and the member directory MemberList answers from; both follow the
//! applied raft membership ([`follow_membership`], fastetcd#211). Raft
//! itself dials the address in the membership.

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
        // Build the new voter set minus the target.
        let mut voters = current_voter_set(&self.state.raft).await;
        let was_present = voters.remove(&req.id);
        if was_present {
            self.state.propose_set_voters(voters).await?;
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
        // Root only while auth is on, as in etcd (checkMembershipOperationPermission, #31).
        crate::authz::require_admin(&self.state, &request).await?;
        let req = request.into_inner();
        let first_url = req
            .peer_ur_ls
            .first()
            .cloned()
            .ok_or_else(|| Status::invalid_argument("MemberUpdateRequest.peer_urls empty"))?;
        // The address is the replicated membership's (#211): raft dials
        // it, and every member's routes and MemberList follow it.
        let known = self.state.raft.metrics().borrow().membership_config.membership().get_node(&req.id).is_some();
        if !known {
            return Err(Status::not_found(crate::etcd_errors::MEMBER_NOT_FOUND));
        }
        self.state.propose_set_node_addr(req.id, &first_url).await?;
        if req.id != self.self_node_id {
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

async fn current_voter_set(
    raft: &openraft::Raft<fastetcd_raft::types::TypeConfig>,
) -> BTreeSet<NodeId> {
    let m = raft.metrics().borrow().membership_config.clone();
    m.voter_ids().collect()
}

/// Make this member's routes and member directory agree with the raft
/// membership `nodes` (with `voters` among them), fastetcd#211. Before,
/// both were seeded from `--initial-cluster` and changed only by the
/// Cluster RPCs this member served, so a member added through another
/// one, or before a restart, had no route here and was missing from
/// MemberList.
///
/// - every member but `self_id` with an address is routed at it;
/// - every member has a directory row: its learner flag follows the
///   voter set, and its peer URL the replicated address (a row this
///   member has not seen gets an empty name, as etcd lists a member that
///   has not started);
/// - an id in `gone` (in the membership before, not now) loses its route
///   and its row. An id never in the membership keeps its route:
///   MemberAdd routes the new member before proposing it.
///
/// An empty membership (a member waiting to be joined) changes nothing.
pub fn reconcile(
    peers: &mut std::collections::HashMap<NodeId, String>,
    dir: &mut BTreeMap<NodeId, MemberInfo>,
    nodes: &BTreeMap<NodeId, openraft::BasicNode>,
    voters: &BTreeSet<NodeId>,
    gone: &BTreeSet<NodeId>,
    self_id: NodeId,
) {
    if nodes.is_empty() {
        return;
    }
    for id in gone {
        if !nodes.contains_key(id) {
            peers.remove(id);
            dir.remove(id);
        }
    }
    for (id, node) in nodes {
        if *id != self_id && !node.addr.is_empty() {
            peers.insert(*id, node.addr.clone());
        }
        let row = dir.entry(*id).or_insert_with(|| MemberInfo {
            peer_urls: if node.addr.is_empty() { Vec::new() } else { vec![node.addr.clone()] },
            ..MemberInfo::default()
        });
        row.is_learner = !voters.contains(id);
        if !node.addr.is_empty() && row.peer_urls.first() != Some(&node.addr) {
            row.peer_urls = vec![node.addr.clone()];
        }
    }
}

/// Keep `peers` and `directory` in step with the raft membership
/// (fastetcd#211): once now, from the membership this member applied
/// before it started (which may differ from `--initial-cluster`), and
/// again on every change, until raft shuts down.
pub fn follow_membership(
    raft: openraft::Raft<fastetcd_raft::types::TypeConfig>,
    peers: PeerEndpoints,
    directory: MemberDirectory,
    self_id: NodeId,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut rx = raft.metrics();
        // At start, what the seeds hold beyond the membership is gone.
        let mut known: BTreeSet<NodeId> = peers.read().await.keys().copied().collect();
        known.extend(directory.read().await.keys().copied());
        let mut seen = None;
        loop {
            let m = rx.borrow_and_update().membership_config.clone();
            if seen.as_ref() != Some(&m) {
                let nodes: BTreeMap<NodeId, openraft::BasicNode> =
                    m.membership().nodes().map(|(id, n)| (*id, n.clone())).collect();
                let voters: BTreeSet<NodeId> = m.voter_ids().collect();
                let members: BTreeSet<NodeId> = nodes.keys().copied().collect();
                let gone: BTreeSet<NodeId> = known.difference(&members).copied().collect();
                {
                    let mut p = peers.write().await;
                    let mut d = directory.write().await;
                    reconcile(&mut p, &mut d, &nodes, &voters, &gone, self_id);
                }
                if !nodes.is_empty() {
                    known = members;
                }
                seen = Some(m);
            }
            if rx.changed().await.is_err() {
                return;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn nodes(list: &[(NodeId, &str)]) -> BTreeMap<NodeId, openraft::BasicNode> {
        list.iter().map(|(id, a)| (*id, openraft::BasicNode::new(*a))).collect()
    }
    fn set(ids: &[NodeId]) -> BTreeSet<NodeId> {
        ids.iter().copied().collect()
    }
    fn seeded() -> (HashMap<NodeId, String>, BTreeMap<NodeId, MemberInfo>) {
        let peers: HashMap<NodeId, String> =
            [(2, "http://b:2380".to_string()), (3, "http://c:2380".to_string())].into_iter().collect();
        let mut dir = BTreeMap::new();
        for (id, name, url) in [(1, "a", "http://a:2380"), (2, "b", "http://b:2380"), (3, "c", "http://c:2380")] {
            dir.insert(
                id,
                MemberInfo { name: name.into(), peer_urls: vec![url.into()], client_urls: Vec::new(), is_learner: false },
            );
        }
        (peers, dir)
    }

    /// A member added through another member is routed and listed here.
    #[test]
    fn a_member_added_elsewhere_gets_a_route_and_a_row() {
        let (mut peers, mut dir) = seeded();
        let m = nodes(&[(1, "http://a:2380"), (2, "http://b:2380"), (3, "http://c:2380"), (9, "http://d:2380")]);
        reconcile(&mut peers, &mut dir, &m, &set(&[1, 2, 3]), &BTreeSet::new(), 1);
        assert_eq!(peers.get(&9).map(String::as_str), Some("http://d:2380"));
        assert!(!peers.contains_key(&1), "no route to itself");
        let row = &dir[&9];
        assert!(row.is_learner);
        assert_eq!(row.peer_urls, vec!["http://d:2380".to_string()]);
        assert_eq!(row.name, "");
        // Its promotion turns the flag off; the seeded names stay.
        reconcile(&mut peers, &mut dir, &m, &set(&[1, 2, 3, 9]), &BTreeSet::new(), 1);
        assert!(!dir[&9].is_learner);
        assert_eq!(dir[&2].name, "b");
    }

    /// A member removed elsewhere loses its route and row; an address
    /// moved by MemberUpdate elsewhere moves here.
    #[test]
    fn removals_and_moved_addresses_follow_the_membership() {
        let (mut peers, mut dir) = seeded();
        let m = nodes(&[(1, "http://a:2380"), (2, "http://b2:2380")]);
        reconcile(&mut peers, &mut dir, &m, &set(&[1, 2]), &set(&[1, 2, 3]), 1);
        assert!(!peers.contains_key(&3));
        assert!(!dir.contains_key(&3));
        assert_eq!(peers[&2], "http://b2:2380");
        assert_eq!(dir[&2].peer_urls, vec!["http://b2:2380".to_string()]);
    }

    /// A route MemberAdd made before proposing (never in the
    /// membership) is kept; an empty membership changes nothing.
    #[test]
    fn a_pending_add_and_an_unjoined_member_keep_their_routes() {
        let (mut peers, mut dir) = seeded();
        peers.insert(7, "http://new:2380".into());
        let m = nodes(&[(1, "http://a:2380"), (2, "http://b:2380"), (3, "http://c:2380")]);
        reconcile(&mut peers, &mut dir, &m, &set(&[1, 2, 3]), &set(&[1, 2, 3]), 1);
        assert_eq!(peers.get(&7).map(String::as_str), Some("http://new:2380"));

        let (mut peers, mut dir) = seeded();
        let before = (peers.clone(), dir.len());
        reconcile(&mut peers, &mut dir, &BTreeMap::new(), &BTreeSet::new(), &set(&[2, 3]), 1);
        assert_eq!((peers, dir.len()), before);
    }

    /// A member with no address in the membership (bootstrapped before
    /// #4) keeps its seeded route.
    #[test]
    fn an_empty_address_keeps_the_seeded_route() {
        let (mut peers, mut dir) = seeded();
        let m = nodes(&[(1, ""), (2, ""), (3, "")]);
        reconcile(&mut peers, &mut dir, &m, &set(&[1, 2, 3]), &BTreeSet::new(), 1);
        assert_eq!(peers[&2], "http://b:2380");
        assert_eq!(dir[&3].peer_urls, vec!["http://c:2380".to_string()]);
    }
}
