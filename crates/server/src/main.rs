use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use openraft::{Config, Raft};
use tonic::transport::Server;

use fastetcd_proto::etcdserverpb::kv_server::KvServer;
use fastetcd_raft::log_store::MemLogStore;
use fastetcd_raft::types::{NodeId, TypeConfig};
use fastetcd_raft::FastetcdStateMachine;
use fastetcd_server::kv::KvService;
use fastetcd_server::ServerState;
use fastetcd_storage::mvcc::MvccStore;
use fastetcd_storage::redb_engine::RedbEngine;

/// fastetcd — a Rust implementation of the etcd v3 wire protocol.
///
/// Flags mirror etcd's where possible so existing client configurations
/// work unmodified.
#[derive(Debug, Parser)]
#[command(name = "fastetcd", version, about)]
struct Args {
    /// Human-readable node name. Must be unique within the cluster.
    #[arg(long, env = "FASTETCD_NAME", default_value = "default")]
    name: String,

    /// Numeric node ID. Must be unique within the cluster. Auto-derived
    /// from `name` if not supplied (FNV-1a hash of `name` truncated to
    /// 63 bits to avoid collisions with `0` / openraft sentinel values).
    #[arg(long, env = "FASTETCD_NODE_ID")]
    node_id: Option<u64>,

    /// Cluster ID. Stable across restarts; clients see it in the
    /// response header. Defaults to `1` for single-node tests.
    #[arg(long, env = "FASTETCD_CLUSTER_ID", default_value_t = 1)]
    cluster_id: u64,

    /// Directory holding the storage and Raft log.
    #[arg(long, env = "FASTETCD_DATA_DIR", default_value = "default.fastetcd")]
    data_dir: PathBuf,

    /// Address to listen on for client gRPC traffic. Currently a
    /// single socket address; comma-separated lists like etcd will be
    /// added later.
    #[arg(
        long,
        env = "FASTETCD_LISTEN_CLIENT_URL",
        default_value = "127.0.0.1:2379"
    )]
    listen_client_url: String,

    /// Address to listen on for peer Raft traffic. Currently a single
    /// socket address. Used once the peer transport (task #13) lands.
    #[arg(
        long,
        env = "FASTETCD_LISTEN_PEER_URL",
        default_value = "127.0.0.1:2380"
    )]
    listen_peer_url: String,
}

fn derive_node_id(name: &str) -> NodeId {
    // FNV-1a 64-bit, then mask to 63 bits so the result is never zero
    // and avoids the openraft sentinel.
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in name.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash & 0x7FFF_FFFF_FFFF_FFFF).max(1)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let node_id = args.node_id.unwrap_or_else(|| derive_node_id(&args.name));

    tracing::info!(
        name = %args.name,
        node_id,
        cluster_id = args.cluster_id,
        data_dir = %args.data_dir.display(),
        listen_client = %args.listen_client_url,
        listen_peer = %args.listen_peer_url,
        "fastetcd starting"
    );

    std::fs::create_dir_all(&args.data_dir)?;
    let engine = Arc::new(RedbEngine::open(args.data_dir.join("fastetcd.redb"))?);
    let mvcc = MvccStore::open(engine).await?;
    let sm = FastetcdStateMachine::new(mvcc);
    let log = MemLogStore::new();

    let config = Arc::new(
        Config {
            heartbeat_interval: 100,
            election_timeout_min: 200,
            election_timeout_max: 500,
            ..Default::default()
        }
        .validate()
        .map_err(|e| anyhow::anyhow!("raft config validate: {e}"))?,
    );

    let raft = Raft::<TypeConfig>::new(
        node_id,
        config,
        SingleNodeNet,
        log.clone(),
        sm.clone(),
    )
    .await?;

    // Bootstrap as a one-member cluster. Multi-node bootstrap arrives
    // with task #13 (peer transport).
    let mut members: BTreeSet<NodeId> = BTreeSet::new();
    members.insert(node_id);
    if let Err(e) = raft.initialize(members).await {
        tracing::warn!("raft initialize: {e} — assuming already initialized");
    }

    let server_state = Arc::new(ServerState::new(
        raft,
        sm,
        args.cluster_id,
        node_id,
        log,
    ));

    let listen: std::net::SocketAddr = args.listen_client_url.parse()?;
    tracing::info!(%listen, "serving KV gRPC");

    let kv = KvService::new(server_state);
    Server::builder()
        .add_service(KvServer::new(kv))
        .serve(listen)
        .await?;

    Ok(())
}

/// Placeholder network for single-node operation. Errors on any peer
/// message — a single-node cluster never sends them.
#[derive(Clone)]
struct SingleNodeNet;

impl openraft::network::RaftNetworkFactory<TypeConfig> for SingleNodeNet {
    type Network = SingleNodeConn;
    async fn new_client(
        &mut self,
        _target: NodeId,
        _node: &openraft::BasicNode,
    ) -> Self::Network {
        SingleNodeConn
    }
}

struct SingleNodeConn;

impl openraft::network::RaftNetwork<TypeConfig> for SingleNodeConn {
    async fn append_entries(
        &mut self,
        _rpc: openraft::raft::AppendEntriesRequest<TypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::AppendEntriesResponse<NodeId>,
        openraft::error::RPCError<
            NodeId,
            openraft::BasicNode,
            openraft::error::RaftError<NodeId>,
        >,
    > {
        Err(no_peer_err())
    }

    async fn install_snapshot(
        &mut self,
        _rpc: openraft::raft::InstallSnapshotRequest<TypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<NodeId>,
        openraft::error::RPCError<
            NodeId,
            openraft::BasicNode,
            openraft::error::RaftError<NodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        Err(openraft::error::RPCError::Network(
            openraft::error::NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::Other,
                "single-node: no peer network",
            )),
        ))
    }

    async fn vote(
        &mut self,
        _rpc: openraft::raft::VoteRequest<NodeId>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::VoteResponse<NodeId>,
        openraft::error::RPCError<
            NodeId,
            openraft::BasicNode,
            openraft::error::RaftError<NodeId>,
        >,
    > {
        Err(no_peer_err())
    }
}

fn no_peer_err() -> openraft::error::RPCError<
    NodeId,
    openraft::BasicNode,
    openraft::error::RaftError<NodeId>,
> {
    openraft::error::RPCError::Network(openraft::error::NetworkError::new(&std::io::Error::new(
        std::io::ErrorKind::Other,
        "single-node: no peer network",
    )))
}
