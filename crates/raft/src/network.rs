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
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Response, Status};

use crate::types::{NodeId, TypeConfig};
use fastetcd_storage::mvcc::auth::AuthTables;

/// Map of `NodeId -> base URL` used by the network factory to dial
/// peers. URLs are `http://host:port` (or `https://` with peer TLS)
/// matching tonic's expected form.
pub type PeerEndpoints = Arc<RwLock<HashMap<NodeId, String>>>;

/// Construct an empty peer endpoints map. Bootstrap code populates it
/// before starting the raft loop (the local node is *not* registered).
pub fn empty_peers() -> PeerEndpoints {
    Arc::new(RwLock::new(HashMap::new()))
}

/// TLS for outbound peer connections: the peer CA to verify the other
/// member's certificate against, and the peer certificate this member
/// presents as its client certificate. `None` is plaintext.
pub type PeerTls = Option<ClientTlsConfig>;

/// Dial a peer. The URL's scheme must agree with whether peer TLS is
/// on: an `https://` peer without TLS configured, or an `http://` peer
/// with it, is refused with an error naming the URL, rather than
/// quietly falling back to plaintext or failing an opaque handshake
/// (fastetcd#23).
pub async fn dial_peer(url: &str, tls: &PeerTls) -> Result<Channel, std::io::Error> {
    let https = url.starts_with("https://");
    let endpoint = Endpoint::from_shared(url.to_string()).map_err(std::io::Error::other)?;
    let endpoint = match (tls, https) {
        (Some(tls), true) => endpoint.tls_config(tls.clone()).map_err(std::io::Error::other)?,
        (None, false) => endpoint,
        (Some(_), false) => {
            return Err(std::io::Error::other(format!(
                "peer URL {url} is not https:// but peer TLS is on (--peer-cert-file)"
            )))
        }
        (None, true) => {
            return Err(std::io::Error::other(format!(
                "peer URL {url} is https:// but peer TLS is off (set --peer-cert-file, \
                 --peer-key-file and --peer-trusted-ca-file)"
            )))
        }
    };
    endpoint.connect().await.map_err(std::io::Error::other)
}

/// Most bytes the peer port decodes in one message (fastetcd#94).
/// tonic's default is 4 MiB, and an AppendEntries holding one client
/// write near the client port's own 4 MiB limit, plus the RPC's framing,
/// could exceed it, so the follower refused it and was sent it again,
/// forever. The WAL reader keeps AppendEntries near 1 MiB
/// (`wal_log_store::REPLICATION_BYTES`) unless one entry is larger.
pub const PEER_MAX_DECODE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct GrpcNetworkFactory {
    peers: PeerEndpoints,
    tls: PeerTls,
}

impl GrpcNetworkFactory {
    /// Plaintext peer connections.
    pub fn new(peers: PeerEndpoints) -> Self {
        Self::with_tls(peers, None)
    }

    pub fn with_tls(peers: PeerEndpoints, tls: PeerTls) -> Self {
        Self { peers, tls }
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
            tls: self.tls.clone(),
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
    tls: PeerTls,
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
        let chan = dial_peer(&url, &self.tls)
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
/// This member's answer to an `AuthSync` status request.
pub fn auth_status(tables: &AuthTables) -> crate::types::AuthSyncResponse {
    let names = |rows: &[(Vec<u8>, Vec<u8>)]| -> Vec<String> {
        rows.iter().map(|(k, _)| String::from_utf8_lossy(k).into_owned()).collect()
    };
    crate::types::AuthSyncResponse::Status {
        version: env!("CARGO_PKG_VERSION").to_string(),
        digest: tables.digest(),
        users: names(&tables.users),
        roles: names(&tables.roles),
        enabled: tables.enabled(),
    }
}

/// Why a member's `AuthSync` status could not be had.
#[derive(Debug, Clone)]
pub enum AuthSyncError {
    /// The member answered `Unimplemented`: it runs a fastetcd older
    /// than replicated auth and cannot decode an auth log entry.
    Older,
    /// The member could not be reached, or the call failed.
    Unreachable(String),
}

/// Why a member's `ConfirmLeader` answer could not be had (#75).
#[derive(Debug, Clone)]
pub enum ConfirmError {
    /// The member answered `Unimplemented`: it is older than the RPC
    /// and cannot decode a batched log entry.
    Older,
    /// Unreachable, timed out, or the call failed.
    Unreachable(String),
}

#[derive(Clone)]
pub struct WriteForwarder {
    peers: PeerEndpoints,
    tls: PeerTls,
    clients: Arc<RwLock<HashMap<NodeId, pb::raft_peer_client::RaftPeerClient<Channel>>>>,
}

impl WriteForwarder {
    /// Plaintext peer connections.
    pub fn new(peers: PeerEndpoints) -> Self {
        Self::with_tls(peers, None)
    }

    pub fn with_tls(peers: PeerEndpoints, tls: PeerTls) -> Self {
        Self {
            peers,
            tls,
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
        let chan = dial_peer(&url, &self.tls).await.map_err(|e| e.to_string())?;
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

    /// Ask `target` for its auth status or tables (fastetcd#32).
    pub async fn auth_sync(
        &self,
        target: NodeId,
        req: &crate::types::AuthSyncRequest,
    ) -> Result<crate::types::AuthSyncResponse, AuthSyncError> {
        let data =
            bincode::serialize(req).map_err(|e| AuthSyncError::Unreachable(e.to_string()))?;
        let mut cli = self
            .client(target)
            .await
            .map_err(AuthSyncError::Unreachable)?;
        let resp = match cli.auth_sync(Request::new(pb::RaftPayload { data })).await {
            Ok(r) => r.into_inner(),
            Err(s) if s.code() == tonic::Code::Unimplemented => return Err(AuthSyncError::Older),
            Err(s) => {
                self.clients.write().await.remove(&target);
                return Err(AuthSyncError::Unreachable(s.message().to_string()));
            }
        };
        bincode::deserialize(&resp.data).map_err(|e| AuthSyncError::Unreachable(e.to_string()))
    }

    /// Ask `target` for the vote term it last saved (fastetcd#75), within
    /// `timeout`. `term` is the caller's term, for the record.
    pub async fn confirm_leader(
        &self,
        target: NodeId,
        term: u64,
        timeout: std::time::Duration,
    ) -> Result<crate::types::ConfirmLeaderResponse, ConfirmError> {
        let data = bincode::serialize(&crate::types::ConfirmLeaderRequest { term })
            .map_err(|e| ConfirmError::Unreachable(e.to_string()))?;
        let call = async {
            let mut cli = self.client(target).await.map_err(ConfirmError::Unreachable)?;
            match cli.confirm_leader(Request::new(pb::RaftPayload { data })).await {
                Ok(r) => Ok(r.into_inner()),
                Err(s) if s.code() == tonic::Code::Unimplemented => Err(ConfirmError::Older),
                Err(s) => {
                    self.clients.write().await.remove(&target);
                    Err(ConfirmError::Unreachable(s.message().to_string()))
                }
            }
        };
        let resp = tokio::time::timeout(timeout, call)
            .await
            .map_err(|_| ConfirmError::Unreachable(format!("no answer in {timeout:?}")))??;
        bincode::deserialize(&resp.data).map_err(|e| ConfirmError::Unreachable(e.to_string()))
    }

    /// Forward a linearizable Range to `target`'s `ForwardRead` RPC and
    /// return the leader's `RangeResult` (#10) with the revision the
    /// leader read at (#50; `None` from a leader older than that, see
    /// [`decode_forwarded_read`]). Same transport and failure handling
    /// as [`forward`](Self::forward).
    pub async fn forward_read(
        &self,
        target: NodeId,
        read: &crate::types::ForwardedRead,
    ) -> Result<(fastetcd_storage::mvcc::RangeResult, Option<i64>), String> {
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
        let (result, revision) = decode_forwarded_read(&resp.data).map_err(|e| e.to_string())?;
        result.map(|r| (r, revision))
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
    /// Answers `ConfirmLeader` (#75); without it the RPC is
    /// `Unimplemented`, as from an older member.
    progress: Option<crate::kv_log_store::LogProgress>,
    /// Forwarded writes are batched with local ones (#75).
    proposer: Option<crate::proposer::Proposer>,
    /// The read barrier for forwarded reads and the lease precheck.
    read_index: Option<crate::read_index::LocalReadIndex>,
}

impl RaftPeerService {
    pub fn new(raft: Raft<TypeConfig>, mvcc: fastetcd_storage::mvcc::MvccStore) -> Self {
        Self { raft, mvcc, progress: None, proposer: None, read_index: None }
    }

    /// Answer `ConfirmLeader` from this node's log store (#75).
    pub fn with_log_progress(mut self, progress: crate::kv_log_store::LogProgress) -> Self {
        self.progress = Some(progress);
        self
    }

    /// Serve forwarded linearizable reads behind `read_index` (#75).
    pub fn with_read_index(mut self, read_index: crate::read_index::LocalReadIndex) -> Self {
        self.read_index = Some(read_index);
        self
    }

    /// Propose forwarded writes through `proposer` (#75).
    pub fn with_proposer(mut self, proposer: crate::proposer::Proposer) -> Self {
        self.proposer = Some(proposer);
        self
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
        // The leader refuses a put naming a lease that does not exist
        // (#19), and a request the state machine would refuse (#49),
        // before proposing it. A node that is not the leader skips the
        // check: `client_write` refuses it below anyway.
        if crate::precheck::is_leader(&self.raft) {
            if let Err(e) = crate::precheck::check(&self.raft, self.read_index.as_ref(), &self.mvcc, &entry).await {
                let result: Result<crate::types::FastetcdLogResponse, String> = match e {
                    // Answered as the apply would have: the caller turns
                    // it into the client's error. (A follower older than
                    // 1.14 cannot decode it and reports the forward as
                    // failed; the request is refused either way.)
                    crate::precheck::PrecheckError::Refused(refusal) => {
                        let revision = self.mvcc.current_revision().await;
                        Ok(crate::types::FastetcdLogResponse::Refused { revision, refusal })
                    }
                    e => Err(e.to_string()),
                };
                let data = bincode::serialize(&result)
                    .map_err(|e| Status::internal(format!("encode response: {e}")))?;
                return Ok(Response::new(pb::RaftPayload { data }));
            }
        }
        let result: Result<crate::types::FastetcdLogResponse, String> = match &self.proposer {
            Some(p) => p.propose(entry).await.map_err(|e| e.to_string()),
            None => match self.raft.client_write(entry).await {
                Ok(resp) => Ok(resp.data),
                // Stringify rather than propagate ForwardToLeader
                // further — a forwarding hop that itself needs
                // forwarding means leadership just changed again;
                // the original caller gets a plain error and, same
                // as any raft client, retries.
                Err(e) => Err(e.to_string()),
            },
        };
        let data = bincode::serialize(&result)
            .map_err(|e| Status::internal(format!("encode response: {e}")))?;
        Ok(Response::new(pb::RaftPayload { data }))
    }

    async fn confirm_leader(
        &self,
        request: Request<pb::RaftPayload>,
    ) -> Result<Response<pb::RaftPayload>, Status> {
        let Some(progress) = &self.progress else {
            return Err(Status::unimplemented("ConfirmLeader is not served by this node"));
        };
        let _req: crate::types::ConfirmLeaderRequest =
            bincode::deserialize(&request.into_inner().data)
                .map_err(|e| Status::invalid_argument(format!("decode ConfirmLeader: {e}")))?;
        let resp = crate::types::ConfirmLeaderResponse {
            saved_term: progress.saved_vote_term(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        };
        let data = bincode::serialize(&resp)
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
        let result: Result<(fastetcd_storage::mvcc::RangeResult, i64), String> = async {
            crate::read_index::read_barrier(&self.raft, self.read_index.as_ref())
                .await
                .map_err(|e| e.to_string())?;
            self.mvcc
                .range_with_revision(
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
        let data = encode_forwarded_read(result)
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

    async fn auth_sync(
        &self,
        request: Request<pb::RaftPayload>,
    ) -> Result<Response<pb::RaftPayload>, Status> {
        let req: crate::types::AuthSyncRequest = bincode::deserialize(&request.into_inner().data)
            .map_err(|e| Status::invalid_argument(format!("decode AuthSync: {e}")))?;
        let tables = self
            .mvcc
            .auth_tables()
            .await
            .map_err(|e| Status::internal(format!("read auth tables: {e}")))?;
        let resp = match req {
            crate::types::AuthSyncRequest::Status => auth_status(&tables),
            crate::types::AuthSyncRequest::Export => crate::types::AuthSyncResponse::Export(tables),
        };
        let data = bincode::serialize(&resp)
            .map_err(|e| Status::internal(format!("encode response: {e}")))?;
        Ok(Response::new(pb::RaftPayload { data }))
    }
}

type ForwardedReadResult = Result<fastetcd_storage::mvcc::RangeResult, String>;

/// Encode a leader's `ForwardRead` reply: the result as releases before
/// fastetcd#50 sent it, then the revision the read was taken at. The
/// revision goes *after* so a follower older than #50 still decodes the
/// reply (bincode 1 ignores trailing bytes) and just doesn't use it.
pub fn encode_forwarded_read(
    result: Result<(fastetcd_storage::mvcc::RangeResult, i64), String>,
) -> bincode::Result<Vec<u8>> {
    let (result, revision): (ForwardedReadResult, i64) = match result {
        Ok((r, rev)) => (Ok(r), rev),
        Err(e) => (Err(e), 0),
    };
    bincode::serialize(&(result, revision))
}

/// Decode a `ForwardRead` reply. The revision is `None` when the leader
/// is older than fastetcd#50 and sent the result alone.
pub fn decode_forwarded_read(
    data: &[u8],
) -> bincode::Result<(ForwardedReadResult, Option<i64>)> {
    match bincode::deserialize::<(ForwardedReadResult, i64)>(data) {
        Ok((result, revision)) => Ok((result, Some(revision))),
        Err(_) => bincode::deserialize::<ForwardedReadResult>(data).map(|r| (r, None)),
    }
}

#[cfg(test)]
mod forwarded_read_tests {
    use super::*;
    use fastetcd_storage::mvcc::{KvRecord, RangeResult};

    fn sample() -> RangeResult {
        RangeResult {
            kvs: vec![KvRecord {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                create_revision: 3,
                mod_revision: 4,
                version: 2,
                lease: 0,
                deleted: false,
            }],
            more: true,
            count: 7,
        }
    }

    #[test]
    fn new_leader_to_new_follower_carries_the_revision() {
        let data = encode_forwarded_read(Ok((sample(), 42))).unwrap();
        let (r, rev) = decode_forwarded_read(&data).unwrap();
        let r = r.unwrap();
        assert_eq!((r.kvs.len(), r.more, r.count, rev), (1, true, 7, Some(42)));
        let data = encode_forwarded_read(Err("not leader".into())).unwrap();
        let (r, _) = decode_forwarded_read(&data).unwrap();
        assert_eq!(r.unwrap_err(), "not leader");
    }

    /// A follower older than #50 decodes exactly what it did before.
    #[test]
    fn new_leader_to_old_follower_still_decodes() {
        let data = encode_forwarded_read(Ok((sample(), 42))).unwrap();
        let old: ForwardedReadResult = bincode::deserialize(&data).unwrap();
        assert_eq!(old.unwrap().count, 7);
        let data = encode_forwarded_read(Err("e".into())).unwrap();
        let old: ForwardedReadResult = bincode::deserialize(&data).unwrap();
        assert_eq!(old.unwrap_err(), "e");
    }

    /// A leader older than #50 sends the result alone: no revision.
    #[test]
    fn old_leader_to_new_follower_has_no_revision() {
        let old: ForwardedReadResult = Ok(sample());
        let (r, rev) = decode_forwarded_read(&bincode::serialize(&old).unwrap()).unwrap();
        assert_eq!((r.unwrap().count, rev), (7, None));
        let old: ForwardedReadResult = Err("e".into());
        let (r, rev) = decode_forwarded_read(&bincode::serialize(&old).unwrap()).unwrap();
        assert_eq!((r.unwrap_err(), rev), ("e".to_string(), None));
    }
}
