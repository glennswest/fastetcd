use clap::Parser;

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

    /// Directory holding the storage and Raft log.
    #[arg(long, env = "FASTETCD_DATA_DIR", default_value = "default.fastetcd")]
    data_dir: String,

    /// URLs to listen on for client gRPC traffic. Comma-separated.
    #[arg(
        long,
        env = "FASTETCD_LISTEN_CLIENT_URLS",
        default_value = "http://127.0.0.1:2379"
    )]
    listen_client_urls: String,

    /// URLs to listen on for peer Raft traffic. Comma-separated.
    #[arg(
        long,
        env = "FASTETCD_LISTEN_PEER_URLS",
        default_value = "http://127.0.0.1:2380"
    )]
    listen_peer_urls: String,

    /// URLs other peers should use to reach this node. Comma-separated.
    #[arg(long, env = "FASTETCD_INITIAL_ADVERTISE_PEER_URLS", default_value = "")]
    initial_advertise_peer_urls: String,

    /// URLs clients should use to reach this node. Comma-separated.
    #[arg(long, env = "FASTETCD_ADVERTISE_CLIENT_URLS", default_value = "")]
    advertise_client_urls: String,

    /// Initial cluster membership in `name=url[,name=url]` form.
    #[arg(long, env = "FASTETCD_INITIAL_CLUSTER", default_value = "")]
    initial_cluster: String,

    /// Cluster state: `new` for bootstrap, `existing` to join.
    #[arg(long, env = "FASTETCD_INITIAL_CLUSTER_STATE", default_value = "new")]
    initial_cluster_state: String,
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

    tracing::info!(
        name = %args.name,
        data_dir = %args.data_dir,
        listen_client = %args.listen_client_urls,
        listen_peer = %args.listen_peer_urls,
        cluster_state = %args.initial_cluster_state,
        "fastetcd starting (skeleton — services not yet wired)"
    );

    // Real wiring lands as tasks #3–#8 land:
    //   1. Open storage at data_dir
    //   2. Open / init Raft log
    //   3. Start Raft node with peer transport
    //   4. Start gRPC client server with KV / Watch / Lease / Cluster / Maintenance
    //   5. Wait for shutdown signal

    tracing::warn!("no services started — this binary is a skeleton");
    Ok(())
}
