//! Raft peer transport: gRPC `RaftNetworkFactory` + `RaftNetwork`
//! implementation, plus the server-side `RaftPeerService`.
//!
//! Payloads are bincode-serialized openraft request/response structs
//! wrapped in `fastetcd.raft.RaftPayload`. Channels are kept open
//! across RPCs (one tonic `Channel` per peer).

use std::collections::HashMap;
use std::sync::Arc;

use fastetcd_proto::fastetcd_raft as pb;
use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::Raft;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::types::{NodeId, TypeConfig};

/// Map of `NodeId -> base URL` used by the network factory to dial
/// peers. URLs are `http://host:port` matching tonic's expected form.
pub type PeerEndpoints = Arc<RwLock<HashMap<NodeId, String>>>;

/// Construct an empty peer endpoints map. Bootstrap code populates it
/// before starting the raft loop (the local node is *not* registered).
pub fn empty_peers() -> PeerEndpoints {
    Arc::new(RwLock::new(HashMap::new()))
}

#[derive(Clone)]
pub struct GrpcNetworkFactory {
    peers: PeerEndpoints,
}

impl GrpcNetworkFactory {
    pub fn new(peers: PeerEndpoints) -> Self {
        Self { peers }
    }
}

impl openraft::network::RaftNetworkFactory<TypeConfig> for GrpcNetworkFactory {
    type Network = GrpcNetwork;

    async fn new_client(
        &mut self,
        target: NodeId,
        _node: &openraft::BasicNode,
    ) -> Self::Network {
        GrpcNetwork {
            target,
            peers: self.peers.clone(),
            client: tokio::sync::Mutex::new(None),
        }
    }
}

/// Per-peer network handle. Lazily dials the first time it's used and
/// caches the tonic `Channel`; reconnect is a fresh dial on the next
/// call after an error.
pub struct GrpcNetwork {
    target: NodeId,
    peers: PeerEndpoints,
    client: tokio::sync::Mutex<Option<pb::raft_peer_client::RaftPeerClient<Channel>>>,
}

impl GrpcNetwork {
    async fn client(
        &self,
    ) -> Result<
        pb::raft_peer_client::RaftPeerClient<Channel>,
        RPCError<NodeId, openraft::BasicNode, RaftError<NodeId>>,
    > {
        let mut guard = self.client.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.clone());
        }
        let peers = self.peers.read().await;
        let url = peers.get(&self.target).cloned().ok_or_else(|| {
            RPCError::Network(NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("no peer URL for node {}", self.target),
            )))
        })?;
        drop(peers);
        let chan = Channel::from_shared(url.clone())
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
            .connect()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let cli = pb::raft_peer_client::RaftPeerClient::new(chan);
        *guard = Some(cli.clone());
        Ok(cli)
    }
}

impl openraft::network::RaftNetwork<TypeConfig> for GrpcNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, openraft::BasicNode, RaftError<NodeId>>>
    {
        let data = bincode::serialize(&rpc)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let mut cli = self.client().await?;
        let resp = cli
            .append_entries(Request::new(pb::RaftPayload { data }))
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
            .into_inner();
        bincode::deserialize(&resp.data)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, openraft::BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let data = bincode::serialize(&rpc).map_err(|e| {
            RPCError::Network(NetworkError::new(&e))
        })?;
        let mut cli = self
            .client()
            .await
            // The error type for install_snapshot is different; remap.
            .map_err(|e| match e {
                RPCError::Network(n) => RPCError::Network(n),
                RPCError::Timeout(t) => RPCError::Timeout(t),
                RPCError::Unreachable(u) => RPCError::Unreachable(u),
                RPCError::PayloadTooLarge(p) => RPCError::PayloadTooLarge(p),
                RPCError::RemoteError(_) => RPCError::Network(NetworkError::new(
                    &std::io::Error::other("remote raft error"),
                )),
            })?;
        let resp = cli
            .install_snapshot(Request::new(pb::RaftPayload { data }))
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
            .into_inner();
        bincode::deserialize(&resp.data)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, openraft::BasicNode, RaftError<NodeId>>>
    {
        let data = bincode::serialize(&rpc)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let mut cli = self.client().await?;
        let resp = cli
            .vote(Request::new(pb::RaftPayload { data }))
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
            .into_inner();
        bincode::deserialize(&resp.data)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }
}

/// Client for `RaftPeer.ForwardWrite` — hands a client write off to
/// another node over the same peer-address map `GrpcNetworkFactory`
/// already resolves correctly for AppendEntries/Vote/InstallSnapshot.
/// Used when a node isn't the raft leader: rather than requiring a
/// separate exchange of client URLs between members, it forwards the
/// write over the peer (raft) connection that's already known to work.
#[derive(Clone)]
pub struct WriteForwarder {
    peers: PeerEndpoints,
    clients: Arc<RwLock<HashMap<NodeId, pb::raft_peer_client::RaftPeerClient<Channel>>>>,
}

impl WriteForwarder {
    pub fn new(peers: PeerEndpoints) -> Self {
        Self {
            peers,
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn client(
        &self,
        target: NodeId,
    ) -> Result<pb::raft_peer_client::RaftPeerClient<Channel>, String> {
        if let Some(c) = self.clients.read().await.get(&target) {
            return Ok(c.clone());
        }
        let url = self
            .peers
            .read()
            .await
            .get(&target)
            .cloned()
            .ok_or_else(|| format!("no peer URL for node {target}"))?;
        let chan = Channel::from_shared(url)
            .map_err(|e| e.to_string())?
            .connect()
            .await
            .map_err(|e| e.to_string())?;
        let cli = pb::raft_peer_client::RaftPeerClient::new(chan);
        self.clients.write().await.insert(target, cli.clone());
        Ok(cli)
    }

    /// Forward `entry` to `target`'s `ForwardWrite` RPC and return its
    /// decoded response. The `Err` string covers both local failure to
    /// reach `target` and a `client_write` error the remote node hit
    /// applying the entry (e.g. it lost leadership mid-flight).
    pub async fn forward(
        &self,
        target: NodeId,
        entry: &crate::types::FastetcdLogEntry,
    ) -> Result<crate::types::FastetcdLogResponse, String> {
        let data = bincode::serialize(entry).map_err(|e| e.to_string())?;
        let cli_result = async {
            let mut cli = self.client(target).await?;
            cli.forward_write(Request::new(pb::RaftPayload { data }))
                .await
                .map_err(|e| e.to_string())
        }
        .await;
        let resp = match cli_result {
            Ok(r) => r.into_inner(),
            Err(e) => {
                // Drop the cached channel so the next attempt redials
                // instead of reusing a possibly-dead connection.
                self.clients.write().await.remove(&target);
                return Err(e);
            }
        };
        let result: Result<crate::types::FastetcdLogResponse, String> =
            bincode::deserialize(&resp.data).map_err(|e| e.to_string())?;
        result
    }

    /// Forward a linearizable Range to `target`'s `ForwardRead` RPC and
    /// return the leader's `RangeResult` (#10). Same transport and
    /// failure handling as [`forward`](Self::forward).
    pub async fn forward_read(
        &self,
        target: NodeId,
        read: &crate::types::ForwardedRead,
    ) -> Result<fastetcd_storage::mvcc::RangeResult, String> {
        let data = bincode::serialize(read).map_err(|e| e.to_string())?;
        let cli_result = async {
            let mut cli = self.client(target).await?;
            cli.forward_read(Request::new(pb::RaftPayload { data }))
                .await
                .map_err(|e| e.to_string())
        }
        .await;
        let resp = match cli_result {
            Ok(r) => r.into_inner(),
            Err(e) => {
                self.clients.write().await.remove(&target);
                return Err(e);
            }
        };
        let result: Result<fastetcd_storage::mvcc::RangeResult, String> =
            bincode::deserialize(&resp.data).map_err(|e| e.to_string())?;
        result
    }

    /// Forward a cluster-membership change to `target`'s
    /// `ForwardMembership` RPC. Same transport and failure handling as
    /// [`forward`](Self::forward); only a leader can apply one, so a
    /// follower handling `MemberAdd`/`MemberRemove` sends it here (#7).
    pub async fn forward_membership(
        &self,
        target: NodeId,
        change: &crate::types::MembershipChange,
    ) -> Result<(), String> {
        let data = bincode::serialize(change).map_err(|e| e.to_string())?;
        let cli_result = async {
            let mut cli = self.client(target).await?;
            cli.forward_membership(Request::new(pb::RaftPayload { data }))
                .await
                .map_err(|e| e.to_string())
        }
        .await;
        let resp = match cli_result {
            Ok(r) => r.into_inner(),
            Err(e) => {
                self.clients.write().await.remove(&target);
                return Err(e);
            }
        };
        let result: Result<(), String> =
            bincode::deserialize(&resp.data).map_err(|e| e.to_string())?;
        result
    }
}

/// Server-side handler for inbound peer RPCs. Holds a clone of the
/// local `Raft<TypeConfig>` and dispatches each bincode-decoded
/// request into the appropriate openraft method. Also holds the
/// `MvccStore` so it can serve a forwarded linearizable read (#10)
/// against the leader's own state machine.
#[derive(Clone)]
pub struct RaftPeerService {
    raft: Raft<TypeConfig>,
    mvcc: fastetcd_storage::mvcc::MvccStore,
}

impl RaftPeerService {
    pub fn new(raft: Raft<TypeConfig>, mvcc: fastetcd_storage::mvcc::MvccStore) -> Self {
        Self { raft, mvcc }
    }
}

#[tonic::async_trait]
impl pb::raft_peer_server::RaftPeer for RaftPeerService {
    async fn append_entries(
        &self,
        request: Request<pb::RaftPayload>,
    ) -> Result<Response<pb::RaftPayload>, Status> {
        let req: AppendEntriesRequest<TypeConfig> =
            bincode::deserialize(&request.into_inner().data)
                .map_err(|e| Status::invalid_argument(format!("decode AppendEntries: {e}")))?;
        let resp = self
            .raft
            .append_entries(req)
            .await
            .map_err(|e| Status::internal(format!("raft.append_entries: {e}")))?;
        let data = bincode::serialize(&resp)
            .map_err(|e| Status::internal(format!("encode response: {e}")))?;
        Ok(Response::new(pb::RaftPayload { data }))
    }

    async fn install_snapshot(
        &self,
        request: Request<pb::RaftPayload>,
    ) -> Result<Response<pb::RaftPayload>, Status> {
        let req: InstallSnapshotRequest<TypeConfig> =
            bincode::deserialize(&request.into_inner().data)
                .map_err(|e| Status::invalid_argument(format!("decode InstallSnapshot: {e}")))?;
        let resp = self
            .raft
            .install_snapshot(req)
            .await
            .map_err(|e| Status::internal(format!("raft.install_snapshot: {e}")))?;
        let data = bincode::serialize(&resp)
            .map_err(|e| Status::internal(format!("encode response: {e}")))?;
        Ok(Response::new(pb::RaftPayload { data }))
    }

    async fn vote(
        &self,
        request: Request<pb::RaftPayload>,
    ) -> Result<Response<pb::RaftPayload>, Status> {
        let req: VoteRequest<NodeId> = bincode::deserialize(&request.into_inner().data)
            .map_err(|e| Status::invalid_argument(format!("decode Vote: {e}")))?;
        let resp = self
            .raft
            .vote(req)
            .await
            .map_err(|e| Status::internal(format!("raft.vote: {e}")))?;
        let data = bincode::serialize(&resp)
            .map_err(|e| Status::internal(format!("encode response: {e}")))?;
        Ok(Response::new(pb::RaftPayload { data }))
    }

    async fn forward_write(
        &self,
        request: Request<pb::RaftPayload>,
    ) -> Result<Response<pb::RaftPayload>, Status> {
        let entry: crate::types::FastetcdLogEntry =
            bincode::deserialize(&request.into_inner().data)
                .map_err(|e| Status::invalid_argument(format!("decode ForwardWrite: {e}")))?;
        let result: Result<crate::types::FastetcdLogResponse, String> =
            match self.raft.client_write(entry).await {
                Ok(resp) => Ok(resp.data),
                // Stringify rather than propagate ForwardToLeader
                // further — a forwarding hop that itself needs
                // forwarding means leadership just changed again;
                // the original caller gets a plain error and, same
                // as any raft client, retries.
                Err(e) => Err(e.to_string()),
            };
        let data = bincode::serialize(&result)
            .map_err(|e| Status::internal(format!("encode response: {e}")))?;
        Ok(Response::new(pb::RaftPayload { data }))
    }

    async fn forward_read(
        &self,
        request: Request<pb::RaftPayload>,
    ) -> Result<Response<pb::RaftPayload>, Status> {
        let read: crate::types::ForwardedRead =
            bincode::deserialize(&request.into_inner().data)
                .map_err(|e| Status::invalid_argument(format!("decode ForwardRead: {e}")))?;

        // Confirm leadership + wait for the state machine to reach the
        // read index, so the read is linearizable. If leadership moved
        // on, stringify the error (as forward_write does) and let the
        // original caller retry against the new leader.
        let result: Result<fastetcd_storage::mvcc::RangeResult, String> = async {
            self.raft
                .ensure_linearizable()
                .await
                .map_err(|e| e.to_string())?;
            self.mvcc
                .range(
                    &read.key,
                    &read.range_end,
                    read.limit as usize,
                    read.revision,
                    read.keys_only,
                    read.count_only,
                )
                .await
                .map_err(|e| e.to_string())
        }
        .await;
        let data = bincode::serialize(&result)
            .map_err(|e| Status::internal(format!("encode response: {e}")))?;
        Ok(Response::new(pb::RaftPayload { data }))
    }

    async fn forward_membership(
        &self,
        request: Request<pb::RaftPayload>,
    ) -> Result<Response<pb::RaftPayload>, Status> {
        let change: crate::types::MembershipChange =
            bincode::deserialize(&request.into_inner().data)
                .map_err(|e| Status::invalid_argument(format!("decode ForwardMembership: {e}")))?;

        // As in forward_write: a hop that itself needs forwarding means
        // leadership changed again, so stringify rather than propagate
        // ForwardToLeader and let the caller retry.
        let result: Result<(), String> = match change {
            crate::types::MembershipChange::AddLearner { node_id, addr } => self
                .raft
                .add_learner(node_id, openraft::BasicNode::new(&addr), false)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string()),
            crate::types::MembershipChange::SetVoters { voters } => self
                .raft
                .change_membership(voters.into_iter().collect::<std::collections::BTreeSet<_>>(), false)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string()),
        };
        let data = bincode::serialize(&result)
            .map_err(|e| Status::internal(format!("encode response: {e}")))?;
        Ok(Response::new(pb::RaftPayload { data }))
    }
}
