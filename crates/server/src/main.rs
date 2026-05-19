use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use openraft::{Config, Raft};
use tokio::sync::RwLock;
use tonic::transport::Server;

use fastetcd_proto::etcdserverpb::cluster_server::ClusterServer;
use fastetcd_proto::etcdserverpb::kv_server::KvServer;
use fastetcd_proto::etcdserverpb::lease_server::LeaseServer;
use fastetcd_proto::etcdserverpb::maintenance_server::MaintenanceServer;
use fastetcd_proto::etcdserverpb::watch_server::WatchServer;
use fastetcd_proto::fastetcd_raft::raft_peer_server::RaftPeerServer;
use fastetcd_raft::kv_log_store::KvLogStore;
use fastetcd_raft::network::{GrpcNetworkFactory, RaftPeerService};
use fastetcd_raft::types::{NodeId, TypeConfig};
use fastetcd_raft::FastetcdStateMachine;
use fastetcd_server::cluster::ClusterService;
use fastetcd_server::kv::KvService;
use fastetcd_server::lease::LeaseService;
use fastetcd_server::maintenance::MaintenanceService;
use fastetcd_server::watch::WatchService;
use fastetcd_server::ServerState;
use fastetcd_storage::mvcc::MvccStore;
use fastetcd_storage::redb_engine::RedbEngine;

/// fastetcd — a Rust implementation of the etcd v3 wire protocol.
#[derive(Debug, Parser)]
#[command(name = "fastetcd", version, about)]
struct Args {
    #[arg(long, env = "FASTETCD_NAME", default_value = "default")]
    name: String,

    /// Numeric node ID; auto-derived from `name` if omitted.
    #[arg(long, env = "FASTETCD_NODE_ID")]
    node_id: Option<u64>,

    #[arg(long, env = "FASTETCD_CLUSTER_ID", default_value_t = 1)]
    cluster_id: u64,

    #[arg(long, env = "FASTETCD_DATA_DIR", default_value = "default.fastetcd")]
    data_dir: PathBuf,

    /// Listen address for client gRPC (KV / Watch / Lease / Cluster /
    /// Maintenance). Defaults to 127.0.0.1:2379.
    #[arg(
        long,
        env = "FASTETCD_LISTEN_CLIENT_URL",
        default_value = "127.0.0.1:2379"
    )]
    listen_client_url: String,

    /// Listen address for peer Raft traffic (AppendEntries / Vote /
    /// InstallSnapshot). Defaults to 127.0.0.1:2380.
    #[arg(
        long,
        env = "FASTETCD_LISTEN_PEER_URL",
        default_value = "127.0.0.1:2380"
    )]
    listen_peer_url: String,

    /// Initial cluster membership, in etcd's `name=URL[,name=URL]`
    /// format. Each URL must be reachable from this node. Empty
    /// means single-node bootstrap (cluster of one).
    #[arg(long, env = "FASTETCD_INITIAL_CLUSTER", default_value = "")]
    initial_cluster: String,

    /// `new` to bootstrap a fresh cluster; `existing` to join one
    /// that's already initialized (skip `raft.initialize`).
    #[arg(long, env = "FASTETCD_INITIAL_CLUSTER_STATE", default_value = "new")]
    initial_cluster_state: String,
}

fn derive_node_id(name: &str) -> NodeId {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in name.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash & 0x7FFF_FFFF_FFFF_FFFF).max(1)
}

/// Parse an `initial_cluster` string of the form
/// `n1=http://h1:2380,n2=http://h2:2380`.
fn parse_initial_cluster(s: &str) -> anyhow::Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    if s.trim().is_empty() {
        return Ok(out);
    }
    for entry in s.split(',') {
        let mut it = entry.splitn(2, '=');
        let name = it
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing name in initial-cluster entry"))?;
        let url = it
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing URL in initial-cluster entry for {name}"))?;
        out.insert(name.trim().to_string(), url.trim().to_string());
    }
    Ok(out)
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

    let initial_cluster = parse_initial_cluster(&args.initial_cluster)?;
    let is_bootstrap = args.initial_cluster_state.eq_ignore_ascii_case("new");

    tracing::info!(
        name = %args.name,
        node_id,
        cluster_id = args.cluster_id,
        data_dir = %args.data_dir.display(),
        listen_client = %args.listen_client_url,
        listen_peer = %args.listen_peer_url,
        cluster_state = %args.initial_cluster_state,
        peers = ?initial_cluster.keys().collect::<Vec<_>>(),
        "fastetcd starting"
    );

    std::fs::create_dir_all(&args.data_dir)?;
    let engine: Arc<dyn fastetcd_storage::KvStore> =
        Arc::new(RedbEngine::open(args.data_dir.join("fastetcd.redb"))?);
    let mvcc = MvccStore::open(engine.clone()).await?;
    let sm = FastetcdStateMachine::new(mvcc);
    let log = KvLogStore::new(engine);

    let config = Arc::new(
        Config {
            heartbeat_interval: 250,
            election_timeout_min: 1000,
            election_timeout_max: 2000,
            ..Default::default()
        }
        .validate()
        .map_err(|e| anyhow::anyhow!("raft config validate: {e}"))?,
    );

    // Build a NodeId map for the initial cluster (excluding self).
    let mut peers_map: BTreeMap<NodeId, String> = BTreeMap::new();
    let mut all_members: BTreeSet<NodeId> = BTreeSet::new();
    all_members.insert(node_id);
    for (name, url) in &initial_cluster {
        let nid = derive_node_id(name);
        all_members.insert(nid);
        if nid != node_id {
            peers_map.insert(nid, url.clone());
        }
    }
    let peers = Arc::new(RwLock::new(peers_map.into_iter().collect()));

    // Seed the cluster directory with peers we know at boot.
    let directory: fastetcd_server::cluster::MemberDirectory =
        Arc::new(tokio::sync::RwLock::new(std::collections::BTreeMap::new()));
    {
        let mut dir = directory.write().await;
        for (name, url) in &initial_cluster {
            let nid = derive_node_id(name);
            dir.insert(
                nid,
                fastetcd_server::cluster::MemberInfo {
                    name: name.clone(),
                    peer_urls: vec![url.clone()],
                    client_urls: Vec::new(),
                    is_learner: false,
                },
            );
        }
    }

    let factory = GrpcNetworkFactory::new(peers.clone());
    let raft = Raft::<TypeConfig>::new(node_id, config, factory, log, sm.clone()).await?;

    // Bootstrap: only the `new` state initializes; `existing` waits
    // for an external add-learner call.
    if is_bootstrap {
        if let Err(e) = raft.initialize(all_members).await {
            tracing::warn!("raft initialize: {e} — assuming already initialized");
        }
    } else {
        tracing::info!("cluster_state=existing — skipping raft.initialize; waiting to be joined");
    }

    let server_state = Arc::new(ServerState::new(raft.clone(), sm, args.cluster_id, node_id));

    // Spawn the lease auto-expiry ticker — leader-only, no-op on followers.
    fastetcd_server::lease_expiry::spawn(server_state.clone());

    // Build peer URLs / client URLs for Member representation.
    let peer_urls = vec![format!("http://{}", args.listen_peer_url)];
    let client_urls = vec![format!("http://{}", args.listen_client_url)];

    let kv = KvService::new(server_state.clone());
    ClusterService::seed_self(
        &directory,
        node_id,
        args.name.clone(),
        peer_urls,
        client_urls,
    )
    .await;
    let cluster = ClusterService::new(
        server_state.clone(),
        node_id,
        peers.clone(),
        directory.clone(),
    );
    let maintenance = MaintenanceService::new(server_state.clone());
    let watch = WatchService::new(server_state.clone());
    let lease = LeaseService::new(server_state);

    let peer_service = RaftPeerService::new(raft);

    let client_listen: std::net::SocketAddr = args.listen_client_url.parse()?;
    let peer_listen: std::net::SocketAddr = args.listen_peer_url.parse()?;

    // Spawn the peer server on its own port.
    let peer_handle = {
        tokio::spawn(async move {
            tracing::info!(%peer_listen, "serving RaftPeer gRPC");
            Server::builder()
                .add_service(RaftPeerServer::new(peer_service))
                .serve(peer_listen)
                .await
        })
    };

    // Client services on the client port.
    let client_handle = {
        tokio::spawn(async move {
            tracing::info!(%client_listen, "serving KV / Cluster / Maintenance / Watch / Lease gRPC");
            Server::builder()
                .add_service(KvServer::new(kv))
                .add_service(ClusterServer::new(cluster))
                .add_service(MaintenanceServer::new(maintenance))
                .add_service(WatchServer::new(watch))
                .add_service(LeaseServer::new(lease))
                .serve(client_listen)
                .await
        })
    };

    tokio::select! {
        r = peer_handle => {
            if let Ok(Err(e)) = r {
                anyhow::bail!("peer server exited: {e}");
            }
        }
        r = client_handle => {
            if let Ok(Err(e)) = r {
                anyhow::bail!("client server exited: {e}");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("ctrl-c received, shutting down");
        }
    }

    Ok(())
}
