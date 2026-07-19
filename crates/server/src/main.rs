use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use openraft::{Config, Raft};
use tokio::sync::RwLock;
use tonic::transport::{Identity, Server, ServerTlsConfig};

use fastetcd_proto::etcdserverpb::auth_server::AuthServer;
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
use fastetcd_server::auth::{AuthInterceptor, AuthService, AuthState};
use fastetcd_server::cluster::ClusterService;
use fastetcd_server::kv::KvService;
use fastetcd_server::lease::LeaseService;
use fastetcd_server::maintenance::MaintenanceService;
use fastetcd_server::watch::WatchService;
use fastetcd_server::ServerState;
use fastetcd_storage::mvcc::MvccStore;
use fastetcd_storage::redb_engine::RedbEngine;

/// fastetcd — a Rust implementation of the etcd v3 wire protocol.
///
/// With no subcommand, runs the server. Subcommands operate on the data
/// directory offline (the server must be stopped): `backup`, `restore`,
/// `fsck`.
#[derive(Debug, Parser)]
#[command(name = "fastetcd", version, about)]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,

    #[arg(long, env = "FASTETCD_NAME", default_value = "default")]
    name: String,

    /// Numeric node ID; auto-derived from `name` if omitted.
    #[arg(long, env = "FASTETCD_NODE_ID")]
    node_id: Option<u64>,

    #[arg(long, env = "FASTETCD_CLUSTER_ID", default_value_t = 1)]
    cluster_id: u64,

    #[arg(long, env = "FASTETCD_DATA_DIR", default_value = "default.fastetcd")]
    data_dir: PathBuf,

    /// Comma-separated list of client gRPC URLs (KV / Watch /
    /// Lease / Cluster / Maintenance). Matches etcd's
    /// `--listen-client-urls`. fastetcd binds to the first entry;
    /// the rest are accepted for compatibility. Defaults to
    /// `http://127.0.0.1:2379`.
    #[arg(
        long = "listen-client-urls",
        alias = "listen-client-url",
        env = "FASTETCD_LISTEN_CLIENT_URLS",
        default_value = "http://127.0.0.1:2379"
    )]
    listen_client_urls: String,

    /// Comma-separated list of peer Raft URLs. Matches etcd's
    /// `--listen-peer-urls`. fastetcd binds to the first entry.
    /// Defaults to `http://127.0.0.1:2380`.
    #[arg(
        long = "listen-peer-urls",
        alias = "listen-peer-url",
        env = "FASTETCD_LISTEN_PEER_URLS",
        default_value = "http://127.0.0.1:2380"
    )]
    listen_peer_urls: String,

    /// Advertised peer URLs that other members will use to reach
    /// this node. Accepted for etcd compatibility; fastetcd
    /// currently uses `listen_peer_urls` for advertising too.
    #[arg(long = "initial-advertise-peer-urls", env = "FASTETCD_INITIAL_ADVERTISE_PEER_URLS")]
    initial_advertise_peer_urls: Option<String>,

    /// Advertised client URLs. Accepted for etcd compatibility;
    /// reported back in Member.client_urls when no other source
    /// is available.
    #[arg(long = "advertise-client-urls", env = "FASTETCD_ADVERTISE_CLIENT_URLS")]
    advertise_client_urls: Option<String>,

    /// Initial cluster membership, in etcd's `name=URL[,name=URL]`
    /// format. Each URL must be reachable from this node. Empty
    /// means single-node bootstrap (cluster of one).
    #[arg(long, env = "FASTETCD_INITIAL_CLUSTER", default_value = "")]
    initial_cluster: String,

    /// `new` to bootstrap a fresh cluster; `existing` to join one
    /// that's already initialized (skip `raft.initialize`).
    #[arg(long, env = "FASTETCD_INITIAL_CLUSTER_STATE", default_value = "new")]
    initial_cluster_state: String,

    /// Recovery: rebuild raft membership as a single-node cluster of
    /// this member, preserving the existing MVCC data, then continue.
    /// The etcd-parity escape hatch for a data directory whose
    /// membership is lost or wrong (e.g. fastetcd#11). Use on exactly
    /// one surviving member, then re-add the others with `member add`.
    #[arg(long, env = "FASTETCD_FORCE_NEW_CLUSTER", default_value = "false")]
    force_new_cluster: bool,

    /// Take a safety backup of the data directory before starting a
    /// newer fastetcd version against it (and before any in-place
    /// format conversion). On by default; disable if you manage your
    /// own backups.
    #[arg(long, env = "FASTETCD_UPGRADE_BACKUP", default_value_t = true)]
    upgrade_backup: bool,

    /// Where startup safety backups are written. Defaults to
    /// `<data-dir>/backups`.
    #[arg(long, env = "FASTETCD_UPGRADE_BACKUP_DIR")]
    upgrade_backup_dir: Option<PathBuf>,

    /// Cluster ID token (etcd compatibility — used by etcd to
    /// detect cross-cluster member confusion). Accepted; fastetcd's
    /// cluster_id flag takes precedence if both are set.
    #[arg(long = "initial-cluster-token", env = "FASTETCD_INITIAL_CLUSTER_TOKEN")]
    initial_cluster_token: Option<String>,

    /// PEM-encoded server certificate for client gRPC.
    #[arg(long, env = "FASTETCD_CERT_FILE")]
    cert_file: Option<PathBuf>,

    /// PEM-encoded private key matching `--cert-file`.
    #[arg(long, env = "FASTETCD_KEY_FILE")]
    key_file: Option<PathBuf>,

    /// PEM-encoded CA bundle used to verify client certs.
    /// Required when `--client-cert-auth` is set.
    #[arg(long, env = "FASTETCD_TRUSTED_CA_FILE")]
    trusted_ca_file: Option<PathBuf>,

    /// Require clients to present a TLS certificate signed by
    /// `--trusted-ca-file`.
    #[arg(long, env = "FASTETCD_CLIENT_CERT_AUTH", default_value_t = false)]
    client_cert_auth: bool,

    /// PEM-encoded server certificate for peer gRPC. Defaults to
    /// `--cert-file` when unset, matching etcd's behavior.
    #[arg(long, env = "FASTETCD_PEER_CERT_FILE")]
    peer_cert_file: Option<PathBuf>,

    /// PEM-encoded private key for peer gRPC. Defaults to
    /// `--key-file`.
    #[arg(long, env = "FASTETCD_PEER_KEY_FILE")]
    peer_key_file: Option<PathBuf>,

    /// PEM CA bundle for peer cert verification.
    #[arg(long, env = "FASTETCD_PEER_TRUSTED_CA_FILE")]
    peer_trusted_ca_file: Option<PathBuf>,

    /// Require peer certs.
    #[arg(long, env = "FASTETCD_PEER_CLIENT_CERT_AUTH", default_value_t = false)]
    peer_client_cert_auth: bool,

    /// Address to serve Prometheus `/metrics` on. Empty disables.
    #[arg(
        long,
        env = "FASTETCD_LISTEN_METRICS_URL",
        default_value = "127.0.0.1:2381"
    )]
    listen_metrics_url: String,

    // ---- etcd-compat no-op flags --------------------------------
    //
    // These are flags etcd's e2e / robustness suite passes when
    // it spawns the binary. fastetcd accepts them so the harness
    // can launch without error; the values are logged but not
    // otherwise consumed (yet).
    //
    /// (etcd compat) Snapshot frequency in committed entries.
    /// Not yet honored by fastetcd; openraft handles snapshotting
    /// based on its own configuration.
    #[arg(long, env = "FASTETCD_SNAPSHOT_COUNT")]
    snapshot_count: Option<u64>,

    /// (etcd compat) Per-request quota in bytes.
    #[arg(long, env = "FASTETCD_QUOTA_BACKEND_BYTES")]
    quota_backend_bytes: Option<i64>,

    /// (etcd compat) Maximum gRPC request size.
    #[arg(long, env = "FASTETCD_MAX_REQUEST_BYTES")]
    max_request_bytes: Option<u64>,

    /// (etcd compat) Log level: debug / info / warn / error.
    #[arg(long, env = "FASTETCD_LOG_LEVEL")]
    log_level: Option<String>,

    /// (etcd compat) Log outputs: stderr, stdout, or a list of files.
    /// fastetcd always logs to stderr; the value is accepted but
    /// ignored.
    #[arg(long)]
    log_outputs: Option<String>,

    /// (etcd compat) Logger backend: capnslog or zap. Ignored.
    #[arg(long)]
    logger: Option<String>,

    /// (etcd compat) Where to expose Prometheus metrics: extensive
    /// or basic. Ignored; fastetcd's /metrics surface is fixed.
    #[arg(long)]
    metrics: Option<String>,

    /// (etcd compat) Enable Go pprof. Ignored.
    #[arg(long, default_value_t = false)]
    enable_pprof: bool,
}

/// Offline data-directory operations. The server must be stopped (each
/// opens the redb file exclusively and refuses if it is locked).
#[derive(Debug, clap::Subcommand)]
enum Command {
    /// Copy the data directory to a single-file backup.
    Backup {
        /// Destination file for the backup.
        #[arg(long)]
        out: PathBuf,
    },
    /// Restore a backup over the data directory. Refuses to overwrite a
    /// directory whose revision is newer than the backup unless --force;
    /// the pre-restore data file is kept as `fastetcd.redb.replaced-*`.
    Restore {
        /// Backup file to restore from.
        backup: PathBuf,
        /// Overwrite even if the current data directory is newer.
        #[arg(long, default_value_t = false)]
        force: bool,
    },
    /// Check the data directory for consistency, and with --repair fix
    /// the raft/format metadata that can strand a cluster.
    Fsck {
        /// Apply repairs instead of only reporting.
        #[arg(long, default_value_t = false)]
        repair: bool,
    },
}

fn build_tls_config(
    cert_file: &Option<PathBuf>,
    key_file: &Option<PathBuf>,
    trusted_ca_file: &Option<PathBuf>,
    client_cert_auth: bool,
) -> anyhow::Result<Option<ServerTlsConfig>> {
    match (cert_file, key_file) {
        (Some(c), Some(k)) => {
            let cert = std::fs::read(c)?;
            let key = std::fs::read(k)?;
            let identity = Identity::from_pem(cert, key);
            let mut cfg = ServerTlsConfig::new().identity(identity);
            if client_cert_auth {
                let ca = trusted_ca_file
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!(
                        "--client-cert-auth requires --trusted-ca-file"
                    ))?;
                let ca_bytes = std::fs::read(ca)?;
                cfg = cfg
                    .client_ca_root(tonic::transport::Certificate::from_pem(ca_bytes))
                    // Mandatory, not optional: a client that presents no
                    // certificate must fail the handshake. tonic 0.12
                    // already defaults `client_auth_optional` to false
                    // (mandatory), but set it explicitly so the security
                    // guarantee doesn't silently depend on that default.
                    .client_auth_optional(false);
            }
            Ok(Some(cfg))
        }
        (None, None) => Ok(None),
        _ => anyhow::bail!("--cert-file and --key-file must both be set or both unset"),
    }
}

/// Pick the first comma-separated URL from a list. Returns the
/// socket part — strips `http://` / `https://` prefix for parsing
/// as a SocketAddr in the listener calls.
fn first_url(list: &str) -> anyhow::Result<String> {
    let first = list
        .split(',')
        .next()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow::anyhow!("empty URL list"))?;
    let bare = first
        .strip_prefix("http://")
        .or_else(|| first.strip_prefix("https://"))
        .unwrap_or(&first)
        .to_string();
    Ok(bare)
}

/// (etcd-compat shim) For each fastetcd `FASTETCD_*` env var that
/// `Args` reads, fall back to etcd's corresponding `ETCD_*` var when
/// the `FASTETCD_*` one is unset. This lets an unmodified etcd
/// `EnvironmentFile` (systemd, container, Kubernetes) boot a
/// fastetcd cluster identically — `FASTETCD_*` still wins if both
/// are set. clap's `env` attribute only takes one key, so we resolve
/// the fallback into the process env before `Args::parse()` runs.
fn apply_etcd_env_compat() {
    const PAIRS: &[(&str, &str)] = &[
        ("FASTETCD_NAME", "ETCD_NAME"),
        ("FASTETCD_DATA_DIR", "ETCD_DATA_DIR"),
        ("FASTETCD_LISTEN_CLIENT_URLS", "ETCD_LISTEN_CLIENT_URLS"),
        ("FASTETCD_LISTEN_PEER_URLS", "ETCD_LISTEN_PEER_URLS"),
        (
            "FASTETCD_INITIAL_ADVERTISE_PEER_URLS",
            "ETCD_INITIAL_ADVERTISE_PEER_URLS",
        ),
        (
            "FASTETCD_ADVERTISE_CLIENT_URLS",
            "ETCD_ADVERTISE_CLIENT_URLS",
        ),
        ("FASTETCD_INITIAL_CLUSTER", "ETCD_INITIAL_CLUSTER"),
        (
            "FASTETCD_INITIAL_CLUSTER_STATE",
            "ETCD_INITIAL_CLUSTER_STATE",
        ),
        (
            "FASTETCD_INITIAL_CLUSTER_TOKEN",
            "ETCD_INITIAL_CLUSTER_TOKEN",
        ),
        ("FASTETCD_CERT_FILE", "ETCD_CERT_FILE"),
        ("FASTETCD_KEY_FILE", "ETCD_KEY_FILE"),
        ("FASTETCD_TRUSTED_CA_FILE", "ETCD_TRUSTED_CA_FILE"),
        ("FASTETCD_CLIENT_CERT_AUTH", "ETCD_CLIENT_CERT_AUTH"),
        ("FASTETCD_PEER_CERT_FILE", "ETCD_PEER_CERT_FILE"),
        ("FASTETCD_PEER_KEY_FILE", "ETCD_PEER_KEY_FILE"),
        ("FASTETCD_PEER_TRUSTED_CA_FILE", "ETCD_PEER_TRUSTED_CA_FILE"),
        (
            "FASTETCD_PEER_CLIENT_CERT_AUTH",
            "ETCD_PEER_CLIENT_CERT_AUTH",
        ),
        // etcd's flag is `--listen-metrics-urls` (plural); fastetcd's
        // is singular, but the env var fallback still maps across.
        ("FASTETCD_LISTEN_METRICS_URL", "ETCD_LISTEN_METRICS_URLS"),
        ("FASTETCD_SNAPSHOT_COUNT", "ETCD_SNAPSHOT_COUNT"),
        ("FASTETCD_QUOTA_BACKEND_BYTES", "ETCD_QUOTA_BACKEND_BYTES"),
        ("FASTETCD_MAX_REQUEST_BYTES", "ETCD_MAX_REQUEST_BYTES"),
        ("FASTETCD_LOG_LEVEL", "ETCD_LOG_LEVEL"),
    ];
    for (fastetcd_key, etcd_key) in PAIRS {
        if std::env::var_os(fastetcd_key).is_none() {
            if let Some(v) = std::env::var_os(etcd_key) {
                // SAFETY: called once, single-threaded, before any
                // other thread is spawned (start of `main`).
                unsafe { std::env::set_var(fastetcd_key, v) };
            }
        }
    }
}

/// etcd-compat plain-HTTP health probe on the client port. Matches
/// etcd's `GET /health` response shape so existing load-balancer /
/// k8s httpGet probes pointed at etcd work unchanged against
/// fastetcd. https://etcd.io/docs/latest/op-guide/monitoring/#health-check
async fn health_http_handler() -> impl axum::response::IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        r#"{"health":"true"}"#,
    )
}

/// etcd-compat `/livez` and `/readyz` — plain-text "ok" on success,
/// matching etcd's Kubernetes-style probe endpoints.
async fn livez_http_handler() -> &'static str {
    "ok"
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

/// Dispatch an offline data-directory subcommand and exit.
async fn run_subcommand(
    args: &Args,
    node_id: NodeId,
    command: &Command,
) -> anyhow::Result<()> {
    use fastetcd_server::admin;
    match command {
        Command::Backup { out } => admin::cmd_backup(&args.data_dir, out).await,
        Command::Restore { backup, force } => {
            admin::cmd_restore(&args.data_dir, backup, *force).await
        }
        Command::Fsck { repair } => {
            // Build the configured voter set for the recovery fallback,
            // the same way server startup does.
            let own_peer_url = first_url(
                args.initial_advertise_peer_urls
                    .as_deref()
                    .unwrap_or(&args.listen_peer_urls),
            )?;
            let mut all_members: BTreeMap<NodeId, openraft::BasicNode> = BTreeMap::new();
            all_members.insert(node_id, openraft::BasicNode::new(own_peer_url));
            for (name, url) in parse_initial_cluster(&args.initial_cluster)? {
                all_members.insert(derive_node_id(&name), openraft::BasicNode::new(url));
            }
            let code = admin::cmd_fsck(&args.data_dir, &all_members, node_id, *repair).await?;
            std::process::exit(code);
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    apply_etcd_env_compat();
    let args = Args::parse();
    let node_id = args.node_id.unwrap_or_else(|| derive_node_id(&args.name));

    // Offline data-directory subcommands. These run and exit; the server
    // must be stopped (each opens the redb file exclusively).
    if let Some(command) = &args.command {
        return run_subcommand(&args, node_id, command).await;
    }

    let initial_cluster = parse_initial_cluster(&args.initial_cluster)?;
    let is_bootstrap = args.initial_cluster_state.eq_ignore_ascii_case("new");

    // Pick the first entry from each URL list — etcd allows
    // multi-listen-URL fan-out but fastetcd binds to one socket
    // per role.
    let client_listen_url = first_url(&args.listen_client_urls)?;
    let peer_listen_url = first_url(&args.listen_peer_urls)?;

    tracing::info!(
        name = %args.name,
        node_id,
        cluster_id = args.cluster_id,
        data_dir = %args.data_dir.display(),
        listen_client = %client_listen_url,
        listen_peer = %peer_listen_url,
        cluster_state = %args.initial_cluster_state,
        peers = ?initial_cluster.keys().collect::<Vec<_>>(),
        "fastetcd starting"
    );

    // Log the no-op compat flags we received so operators can see
    // them in startup output even though we don't act on them.
    if let Some(v) = &args.snapshot_count {
        tracing::debug!(snapshot_count = v, "etcd-compat flag accepted (no-op)");
    }
    if let Some(v) = &args.quota_backend_bytes {
        tracing::debug!(quota_backend_bytes = v, "etcd-compat flag accepted (no-op)");
    }
    if let Some(v) = &args.max_request_bytes {
        tracing::debug!(max_request_bytes = v, "etcd-compat flag accepted (no-op)");
    }
    if args.enable_pprof {
        tracing::debug!("etcd-compat flag accepted (no-op): --enable-pprof");
    }
    let _ = (
        &args.log_level,
        &args.log_outputs,
        &args.logger,
        &args.metrics,
        &args.initial_cluster_token,
        &args.initial_advertise_peer_urls,
        &args.advertise_client_urls,
        &args.peer_cert_file,
        &args.peer_key_file,
        &args.peer_trusted_ca_file,
        &args.peer_client_cert_auth,
    );

    std::fs::create_dir_all(&args.data_dir)?;
    let engine: Arc<dyn fastetcd_storage::KvStore> =
        Arc::new(RedbEngine::open(args.data_dir.join("fastetcd.redb"))?);
    let mvcc = MvccStore::open(engine.clone()).await?;
    let sm = FastetcdStateMachine::open(mvcc).await?;
    let mut log = KvLogStore::new(engine);

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

    // Build a NodeId -> BasicNode map for the initial cluster,
    // addressed by peer URL (matching the convention `ClusterService`
    // already uses for dynamically added members — see
    // `cluster.rs`'s `add_learner` call). openraft's `initialize()`
    // otherwise defaults every member's `Node` to `BasicNode::default()`
    // (empty `addr`), which is the root cause of #4: a follower that
    // needs to forward a client write has no address for the leader
    // in its raft membership. fastetcd's own peer transport
    // (`GrpcNetworkFactory`) doesn't actually dial through this addr
    // — it resolves peers via the separate `peers` map below — but
    // openraft's own `ForwardToLeader` error surfaces this addr to
    // operators/logs, so it should be real regardless.
    let own_peer_url = split_list(
        args.initial_advertise_peer_urls
            .as_deref()
            .unwrap_or(&args.listen_peer_urls),
    )
    .into_iter()
    .next()
    .unwrap_or_else(|| peer_listen_url.clone());

    let mut peers_map: BTreeMap<NodeId, String> = BTreeMap::new();
    let mut all_members: BTreeMap<NodeId, openraft::BasicNode> = BTreeMap::new();
    all_members.insert(node_id, openraft::BasicNode::new(own_peer_url));
    for (name, url) in &initial_cluster {
        let nid = derive_node_id(name);
        all_members.insert(nid, openraft::BasicNode::new(url.clone()));
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

    // ---- On-disk format recovery (must run before Raft::new) ----
    //
    // Directories written before v1.0.1 never persisted raft membership
    // durably, and (pre-0.8.3) never persisted last_applied either. Once
    // such a cluster's log has been purged, a restart comes up with an
    // empty voter set and no leader, or crash-loops replaying purged
    // entries (fastetcd#9, #11). Detect the legacy format and repair it
    // in place, keeping the MVCC data.
    {
        // Safety backup before a newer version touches the data (#backup):
        // take it before recovery writes anything, while we hold the lock.
        if args.upgrade_backup {
            let backup_dir = args
                .upgrade_backup_dir
                .clone()
                .unwrap_or_else(|| args.data_dir.join("backups"));
            fastetcd_server::admin::backup_before_version(
                sm.mvcc(),
                &args.data_dir,
                &backup_dir,
                env!("CARGO_PKG_VERSION"),
            )
            .await?;
        }

        // In-place upgrade / recovery (#9, #11), shared with `fsck --repair`.
        fastetcd_server::admin::recover_data_dir(
            &sm,
            &mut log,
            &all_members,
            node_id,
            args.force_new_cluster,
        )
        .await?;

        // Record the version now running so the next start's backup check
        // fires only on an actual version change.
        sm.mvcc()
            .write_open_version(env!("CARGO_PKG_VERSION"))
            .await?;
    }

    // Clone the MVCC handle before `sm` is moved into ServerState; the
    // peer service uses it to serve forwarded linearizable reads (#10).
    let peer_mvcc = sm.mvcc().clone();
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

    let auth_state = AuthState::default();
    AuthService::load_persisted(sm.mvcc().engine(), &auth_state).await?;
    let forwarder = fastetcd_raft::WriteForwarder::new(peers.clone());
    let server_state = Arc::new(ServerState::new(
        raft.clone(),
        sm,
        args.cluster_id,
        node_id,
        auth_state.clone(),
        forwarder,
    ));

    // Spawn the lease auto-expiry ticker — leader-only, no-op on followers.
    fastetcd_server::lease_expiry::spawn(server_state.clone());

    // Metrics endpoint (Prometheus /metrics).
    if !args.listen_metrics_url.trim().is_empty() {
        let m = fastetcd_server::metrics::Metrics::new();
        let metrics_addr: std::net::SocketAddr = args.listen_metrics_url.parse()?;
        fastetcd_server::metrics::spawn_server(metrics_addr, m, server_state.clone());
    }

    // Build peer URLs / client URLs for Member representation.
    // Cluster directory's peer/client URLs go into Member.peerURLs /
    // clientURLs in MemberList responses. Honour the etcd-shaped
    // `--initial-advertise-peer-urls` / `--advertise-client-urls`
    // flags if set; otherwise reuse the raw listen URLs so the
    // scheme (http vs https) is preserved.
    fn split_list(s: &str) -> Vec<String> {
        s.split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect()
    }
    let peer_urls = match &args.initial_advertise_peer_urls {
        Some(s) => split_list(s),
        None => split_list(&args.listen_peer_urls),
    };
    let client_urls = match &args.advertise_client_urls {
        Some(s) => split_list(s),
        None => split_list(&args.listen_client_urls),
    };

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
    let lease = LeaseService::new(server_state.clone());
    let auth = AuthService::new(server_state, auth_state.clone());

    let peer_service = RaftPeerService::new(raft, peer_mvcc);

    let client_listen: std::net::SocketAddr = client_listen_url.parse()?;
    let peer_listen: std::net::SocketAddr = peer_listen_url.parse()?;

    // TLS config (client + peer share the same identity).
    let tls = build_tls_config(
        &args.cert_file,
        &args.key_file,
        &args.trusted_ca_file,
        args.client_cert_auth,
    )?;
    if tls.is_some() {
        tracing::info!("TLS enabled (client + peer)");
    }

    // Spawn the peer server on its own port.
    let tls_for_peer = tls.clone();
    let peer_handle = {
        tokio::spawn(async move {
            tracing::info!(%peer_listen, "serving RaftPeer gRPC");
            let mut builder = Server::builder();
            if let Some(t) = tls_for_peer {
                builder = builder.tls_config(t).expect("apply peer TLS config");
            }
            builder
                .add_service(RaftPeerServer::new(peer_service))
                .serve(peer_listen)
                .await
        })
    };

    // Client services on the client port. Every non-Auth service is
    // wrapped by AuthInterceptor; Auth stays open so clients can
    // call Authenticate without a pre-existing token.
    let interceptor = AuthInterceptor::new(auth_state.clone());
    let tls_for_client = tls;

    // Standard gRPC health service. Mark every service we serve as
    // SERVING so service-mesh / k8s probes pass.
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    use fastetcd_proto::etcdserverpb::auth_server::AuthServer as PbAuthServer;
    use fastetcd_proto::etcdserverpb::cluster_server::ClusterServer as PbClusterServer;
    use fastetcd_proto::etcdserverpb::kv_server::KvServer as PbKvServer;
    use fastetcd_proto::etcdserverpb::lease_server::LeaseServer as PbLeaseServer;
    use fastetcd_proto::etcdserverpb::maintenance_server::MaintenanceServer as PbMaintServer;
    use fastetcd_proto::etcdserverpb::watch_server::WatchServer as PbWatchServer;
    let r = health_reporter.clone();
    tokio::spawn(async move {
        let mut r = r;
        r.set_serving::<PbKvServer<KvService>>().await;
        r.set_serving::<PbClusterServer<ClusterService>>().await;
        r.set_serving::<PbMaintServer<MaintenanceService>>().await;
        r.set_serving::<PbWatchServer<WatchService>>().await;
        r.set_serving::<PbLeaseServer<LeaseService>>().await;
        r.set_serving::<PbAuthServer<AuthService>>().await;
    });

    let client_handle = {
        tokio::spawn(async move {
            tracing::info!(
                %client_listen,
                "serving KV / Cluster / Maintenance / Watch / Lease / Auth / Health gRPC + HTTP /health"
            );
            let mut grpc_routes = tonic::service::Routes::builder();
            grpc_routes.add_service(health_service);
            grpc_routes.add_service(KvServer::with_interceptor(kv, interceptor.clone()));
            grpc_routes.add_service(ClusterServer::with_interceptor(
                cluster,
                interceptor.clone(),
            ));
            grpc_routes.add_service(MaintenanceServer::with_interceptor(
                maintenance,
                interceptor.clone(),
            ));
            grpc_routes.add_service(WatchServer::with_interceptor(watch, interceptor.clone()));
            grpc_routes.add_service(LeaseServer::with_interceptor(lease, interceptor));
            grpc_routes.add_service(AuthServer::new(auth));

            // Same port also answers etcd's plain-HTTP health probes
            // (load balancers / k8s httpGet probes already pointed
            // at :2379 for etcd don't need a second port for this).
            // tonic 0.12 routes are convertible to/from axum::Router,
            // so the gRPC routes and the HTTP routes below share one
            // `Service` served on the same listener.
            let app: axum::Router = grpc_routes
                .routes()
                .into_axum_router()
                .route("/health", axum::routing::get(health_http_handler))
                .route("/livez", axum::routing::get(livez_http_handler))
                .route("/readyz", axum::routing::get(livez_http_handler));

            let mut builder = Server::builder().accept_http1(true);
            if let Some(t) = tls_for_client {
                builder = builder.tls_config(t).expect("apply client TLS config");
            }
            builder
                .add_routes(tonic::service::Routes::from(app))
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
