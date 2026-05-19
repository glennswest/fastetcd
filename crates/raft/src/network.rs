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
                    &std::io::Error::new(std::io::ErrorKind::Other, "remote raft error"),
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

/// Server-side handler for inbound peer RPCs. Holds a clone of the
/// local `Raft<TypeConfig>` and dispatches each bincode-decoded
/// request into the appropriate openraft method.
#[derive(Clone)]
pub struct RaftPeerService {
    raft: Raft<TypeConfig>,
}

impl RaftPeerService {
    pub fn new(raft: Raft<TypeConfig>) -> Self {
        Self { raft }
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
}
