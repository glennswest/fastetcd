use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use openraft::{Config, Raft};
use tokio::sync::RwLock;
use tonic::transport::Server;

use fastetcd_proto::etcdserverpb::auth_server::AuthServer;
use fastetcd_proto::etcdserverpb::cluster_server::ClusterServer;
use fastetcd_proto::etcdserverpb::kv_server::KvServer;
use fastetcd_proto::etcdserverpb::lease_server::LeaseServer;
use fastetcd_proto::etcdserverpb::maintenance_server::MaintenanceServer;
use fastetcd_proto::etcdserverpb::watch_server::WatchServer;
use fastetcd_proto::fastetcd_raft::raft_peer_server::RaftPeerServer;
use fastetcd_raft::wal_log_store::WalLogStore;
use fastetcd_raft::network::{GrpcNetworkFactory, RaftPeerService};
use fastetcd_server::tls::{check_peer_url_schemes, Port, TlsFiles};
use fastetcd_raft::types::{NodeId, TypeConfig};
use fastetcd_raft::FastetcdStateMachine;
use fastetcd_proto::fastetcd_admin::fastetcd_admin_server::FastetcdAdminServer;
use fastetcd_server::auth::{AuthInterceptor, AuthService};
use fastetcd_server::auth_sync::AdminService;
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
/// With no subcommand, runs the server. `backup`, `restore`, `fsck` and
/// `defrag` operate on the data directory offline (the server must be
/// stopped); `sizing` needs no data directory.
///
/// Boolean flags take values as etcd's (Go's flag package) do (#53):
/// `--flag` alone is true and `--flag=true` / `--flag=false` set it.
/// `--flag false` is accepted too, as fastetcd 1.8-1.14 accepted it
/// for `--enable-grpc-gateway`. Their env vars take true/false, 1/0,
/// yes/no, on/off.
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

    /// Cluster id reported in every response header. Clients pin it to
    /// notice an endpoint repointed at a different store, so give each
    /// deployment its own. Unset: derived from `--initial-cluster-token`
    /// on a new store, else 1. Chosen once and persisted in the data dir;
    /// setting this flag later replaces it (fastetcd#17). Never 0.
    #[arg(
        long,
        env = "FASTETCD_CLUSTER_ID",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    cluster_id: Option<u64>,

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

    /// This member's peer URLs: its raft address and the peer URLs
    /// `MemberList` reports. Defaults to `--listen-peer-urls`. Other
    /// members dial the URL their own `--initial-cluster` gives for it.
    #[arg(long = "initial-advertise-peer-urls", env = "FASTETCD_INITIAL_ADVERTISE_PEER_URLS")]
    initial_advertise_peer_urls: Option<String>,

    /// This member's client URLs, as `MemberList` reports them.
    /// Defaults to `--listen-client-urls`.
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
    #[arg(
        long,
        env = "FASTETCD_FORCE_NEW_CLUSTER",
        default_value_t = false,
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = clap::builder::BoolishValueParser::new(),
        action = clap::ArgAction::Set
    )]
    force_new_cluster: bool,

    /// Take a safety backup of the data directory before starting a
    /// newer fastetcd version against it (and before any in-place
    /// format conversion). On by default; disable if you manage your
    /// own backups.
    #[arg(
        long,
        env = "FASTETCD_UPGRADE_BACKUP",
        default_value_t = true,
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = clap::builder::BoolishValueParser::new(),
        action = clap::ArgAction::Set
    )]
    upgrade_backup: bool,

    /// Where startup safety backups are written. Defaults to
    /// `<data-dir>/backups`.
    #[arg(long, env = "FASTETCD_UPGRADE_BACKUP_DIR")]
    upgrade_backup_dir: Option<PathBuf>,

    /// How many upgrade safety backups to keep. Each is a full copy of
    /// the database on the same volume the database has to keep running
    /// on, so they are rolled off oldest-first before a new one is
    /// written.
    #[arg(long, env = "FASTETCD_UPGRADE_BACKUP_RETAIN", default_value_t = 2)]
    upgrade_backup_retain: usize,

    /// Cluster token, as etcd's. On a new store, and without
    /// `--cluster-id`, the cluster id is derived from it (FNV-1a-64,
    /// masked to 63 bits), so give each deployment its own token and
    /// every member of one deployment the same one.
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
    #[arg(
        long,
        env = "FASTETCD_CLIENT_CERT_AUTH",
        default_value_t = false,
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = clap::builder::BoolishValueParser::new(),
        action = clap::ArgAction::Set
    )]
    client_cert_auth: bool,

    /// PEM-encoded certificate for the raft peer port. Independent of
    /// `--cert-file`, as in etcd: unset, the peer port is plaintext.
    /// Also presented as the client certificate when dialling other
    /// members. Requires `--peer-key-file` and `--peer-trusted-ca-file`,
    /// and every peer URL to be `https://`.
    #[arg(long, env = "FASTETCD_PEER_CERT_FILE")]
    peer_cert_file: Option<PathBuf>,

    /// PEM-encoded private key matching `--peer-cert-file`.
    #[arg(long, env = "FASTETCD_PEER_KEY_FILE")]
    peer_key_file: Option<PathBuf>,

    /// PEM CA bundle the other members' peer certificates are verified
    /// against, both those they serve and (with
    /// `--peer-client-cert-auth`) those they dial in with. Usually a
    /// narrower CA than `--trusted-ca-file`.
    #[arg(long, env = "FASTETCD_PEER_TRUSTED_CA_FILE")]
    peer_trusted_ca_file: Option<PathBuf>,

    /// Refuse any caller on the peer port that does not present a
    /// certificate signed by `--peer-trusted-ca-file`.
    #[arg(
        long,
        env = "FASTETCD_PEER_CLIENT_CERT_AUTH",
        default_value_t = false,
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = clap::builder::BoolishValueParser::new(),
        action = clap::ArgAction::Set
    )]
    peer_client_cert_auth: bool,

    /// Serve etcd's v3 JSON gateway (`POST /v3/...`) on the client
    /// port, as etcd does (#28). `--enable-grpc-gateway=false` turns it
    /// off; gRPC and `/health` are unaffected.
    #[arg(
        long,
        env = "FASTETCD_ENABLE_GRPC_GATEWAY",
        default_value_t = true,
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = clap::builder::BoolishValueParser::new(),
        action = clap::ArgAction::Set
    )]
    enable_grpc_gateway: bool,

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
    /// Time between a leader's heartbeats, in milliseconds (etcd's
    /// `--heartbeat-interval`). In fastetcd it is also how long one
    /// AppendEntries may take, sent and fsynced by the follower (openraft
    /// 0.9's replication timeout, #94), so do not set it below what the
    /// followers' disk needs for one fsync.
    #[arg(long, env = "FASTETCD_HEARTBEAT_INTERVAL", default_value_t = 250)]
    heartbeat_interval: u64,

    /// How long a follower waits without hearing from the leader before
    /// it calls an election, in milliseconds (etcd's
    /// `--election-timeout`); each member picks its own from [t, 2t). A
    /// leader sends no heartbeat while a WAL fsync is in progress
    /// (#103), so on a disk whose fsync can stall, set it above the
    /// longest stall (`fastetcd_wal_fsync_max_seconds`). At least twice
    /// `--heartbeat-interval`, at most 50 s.
    #[arg(long, env = "FASTETCD_ELECTION_TIMEOUT", default_value_t = 1000)]
    election_timeout: u64,

    /// Take a raft snapshot (and then purge the log) every N applied
    /// writes (proposals; a batched log entry counts each one, #80).
    /// Lower keeps the log smaller; higher lets a lagging follower catch
    /// up from the log instead of a snapshot. Matches etcd's
    /// `--snapshot-count`.
    #[arg(long, env = "FASTETCD_SNAPSHOT_COUNT", default_value_t = 5000)]
    snapshot_count: u64,

    /// Already-snapshotted writes (proposals, #80) to retain in the log
    /// after a purge: a catch-up buffer for followers just behind the
    /// snapshot.
    #[arg(
        long,
        env = "FASTETCD_MAX_IN_SNAPSHOT_LOG_TO_KEEP",
        default_value_t = 1000
    )]
    max_in_snapshot_log_to_keep: u64,

    /// Auto-compaction retention, in revisions. Keeps the most recent N
    /// key revisions and compacts older history, bounding the MVCC store
    /// (and therefore snapshot cost). `0` disables it (the default) —
    /// under Kubernetes the apiserver drives compaction itself. Matches
    /// etcd's `--auto-compaction-retention` in `revision` mode.
    #[arg(long, env = "FASTETCD_AUTO_COMPACTION_RETENTION", default_value_t = 0)]
    auto_compaction_retention: i64,

    /// How often the auto-compaction ticker runs, in seconds.
    #[arg(
        long,
        env = "FASTETCD_AUTO_COMPACTION_INTERVAL_SECS",
        default_value_t = 300
    )]
    auto_compaction_interval_secs: u64,

    /// Ceiling on the store's on-disk footprint, in bytes. `0` (the
    /// default) means the data volume is the only limit — which is the
    /// right setting for a fixed-size volume, since fastetcd measures
    /// the filesystem's real free space either way and a quota larger
    /// than the volume is a fiction. Crossing the high-water mark
    /// triggers reclaim; crossing the alarm mark raises NOSPACE and
    /// refuses writes. Matches etcd's `--quota-backend-bytes`.
    #[arg(long, env = "FASTETCD_QUOTA_BACKEND_BYTES", default_value_t = 0)]
    quota_backend_bytes: i64,

    /// Percentage of capacity at which the store starts reclaiming
    /// space (compact -> snapshot -> purge -> defragment).
    #[arg(
        long,
        env = "FASTETCD_SPACE_HIGH_WATER_PERCENT",
        default_value_t = 80
    )]
    space_high_water_percent: u8,

    /// Percentage of capacity at which the NOSPACE alarm is raised and
    /// writes are refused. Reads, deletes, compaction and defragment
    /// keep working so the store can be dug out.
    #[arg(long, env = "FASTETCD_SPACE_ALARM_PERCENT", default_value_t = 95)]
    space_alarm_percent: u8,

    /// Percentage of capacity the store must fall back below before the
    /// NOSPACE alarm clears itself.
    #[arg(long, env = "FASTETCD_SPACE_CLEAR_PERCENT", default_value_t = 70)]
    space_clear_percent: u8,

    /// How often occupancy is sampled, in seconds. `0` disables the
    /// space monitor entirely (not recommended on a bounded volume).
    #[arg(
        long,
        env = "FASTETCD_SPACE_CHECK_INTERVAL_SECS",
        default_value_t = 30
    )]
    space_check_interval_secs: u64,

    /// Revisions of MVCC history kept when compacting under disk
    /// pressure. Applies even with `--auto-compaction-retention 0`: an
    /// unbounded history is the usual reason a volume fills.
    #[arg(
        long,
        env = "FASTETCD_SPACE_RECLAIM_RETENTION",
        default_value_t = 1000
    )]
    space_reclaim_retention: i64,

    /// Let the reclaim path defragment the engine. Defragment pauses
    /// reads and writes while it runs, but it is the only step that
    /// returns freed pages to the filesystem.
    #[arg(
        long,
        env = "FASTETCD_AUTO_DEFRAG",
        default_value_t = true,
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = clap::builder::BoolishValueParser::new(),
        action = clap::ArgAction::Set
    )]
    auto_defrag: bool,

    /// Nodes this cluster is expected to run. Used to size the store:
    /// at startup fastetcd computes the volume that shape needs and
    /// warns if the data volume is smaller, which is the check that
    /// would have caught fastetcd#14 before it filled. It also enables
    /// MVCC auto-compaction when `--auto-compaction-retention` is left
    /// at its default, since an unbounded history is what fills a
    /// bounded volume. `0` (the default) disables the check.
    /// `fastetcd sizing --nodes N` prints the same arithmetic offline.
    #[arg(long, env = "FASTETCD_EXPECTED_NODES", default_value_t = 0)]
    expected_nodes: u64,

    /// Average pods per node, for `--expected-nodes`. This term
    /// dominates the estimate.
    #[arg(
        long,
        env = "FASTETCD_EXPECTED_PODS_PER_NODE",
        default_value_t = fastetcd_server::sizing::DEFAULT_PODS_PER_NODE
    )]
    expected_pods_per_node: u64,

    /// Raft snapshots retained on disk. Each is a full copy of the
    /// database, so on a fixed-size volume each extra copy is another
    /// whole database; the default of 1 keeps only the current one.
    /// Older snapshots are rolled off oldest-first *before* a new one is
    /// written. Matches etcd's `--max-snapshots`.
    #[arg(long, env = "FASTETCD_MAX_SNAPSHOTS", default_value_t = 1)]
    max_snapshots: usize,

    /// Byte budget of the latest-value cache: the newest record of
    /// recently used keys, kept in RAM so a GET of a hot key never reads
    /// the disk (fastetcd#82). Least recently used entries are evicted to
    /// stay inside it. Unset (the default): the smaller of 128 MiB and 5%
    /// of the memory this process may use (cgroup limit, else total
    /// RAM). `0` turns the value cache off. Every key's index is kept in
    /// RAM regardless, as etcd keeps its treeIndex.
    #[arg(long, env = "FASTETCD_VALUE_CACHE_BYTES")]
    value_cache_bytes: Option<u64>,

    /// Values larger than this are not put in the value cache (an entry
    /// is also never more than a quarter of one of its 16 shards).
    #[arg(
        long,
        env = "FASTETCD_VALUE_CACHE_MAX_ENTRY_BYTES",
        default_value_t = fastetcd_storage::mvcc::cache::DEFAULT_MAX_ENTRY_BYTES
    )]
    value_cache_max_entry_bytes: u64,

    /// The storage engine's (redb's) own page cache, in bytes. redb's
    /// default is 1 GiB; fastetcd sets 256 MiB, so memory stays near
    /// this plus `--value-cache-bytes` plus the key index (fastetcd#82).
    #[arg(
        long,
        env = "FASTETCD_ENGINE_CACHE_BYTES",
        default_value_t = 256 * 1024 * 1024
    )]
    engine_cache_bytes: u64,

    /// Size each raft WAL segment is preallocated to (fastetcd#85). The
    /// raft log is appended to these files in `<data-dir>/wal/`; a
    /// client's write waits for one sequential fsync of them.
    #[arg(
        long,
        env = "FASTETCD_WAL_SEGMENT_BYTES",
        default_value_t = fastetcd_storage::raft_wal::DEFAULT_SEGMENT_BYTES
    )]
    wal_segment_bytes: u64,

    /// Recent raft log entries kept in RAM, in bytes, so replicating to
    /// a follower rarely reads the WAL back from disk.
    #[arg(long, env = "FASTETCD_WAL_CACHE_BYTES", default_value_t = 64 * 1024 * 1024)]
    wal_cache_bytes: u64,

    /// The data file is made durable (one redb commit) at most this many
    /// milliseconds after an entry is applied. A crash loses at most
    /// this much applied state from the data file, and the WAL replays
    /// it on restart (fastetcd#85). Where a checkpoint is slow (a
    /// spinning disk), the next waits at least four times as long as
    /// the last one took, leaving the disk to the WAL (fastetcd#95).
    #[arg(long, env = "FASTETCD_WAL_CHECKPOINT_INTERVAL_MS", default_value_t = 100)]
    wal_checkpoint_interval_ms: u64,

    /// ...or as soon as this many raft entries were applied since the
    /// last checkpoint, whichever comes first.
    #[arg(long, env = "FASTETCD_WAL_CHECKPOINT_ENTRIES", default_value_t = 10_000)]
    wal_checkpoint_entries: u64,

    /// Applied writes held in RAM until a checkpoint writes them into the
    /// data file, in bytes. Past it, an apply writes them out first.
    #[arg(
        long,
        env = "FASTETCD_WRITE_BEHIND_BYTES",
        default_value_t = fastetcd_storage::write_behind::DEFAULT_MAX_BYTES as u64
    )]
    write_behind_bytes: u64,

    /// Directory for periodic backups of the whole store, on a
    /// **different volume** from `--data-dir`: a device that loses writes
    /// can corrupt the data file and, on the same volume, the backups
    /// with it. Unset (the default): no periodic backups. A lone member
    /// whose data file is found corrupt restores itself from the newest
    /// good backup here (fastetcd#37).
    #[arg(long, env = "FASTETCD_BACKUP_DIR")]
    backup_dir: Option<PathBuf>,

    /// Take a backup at least this often, when anything has changed.
    #[arg(long, env = "FASTETCD_BACKUP_INTERVAL_SECS", default_value_t = 900)]
    backup_interval_secs: u64,

    /// Also take a backup once this many revisions have been written
    /// since the last one. `0` disables this trigger.
    #[arg(long, env = "FASTETCD_BACKUP_EVERY_REVISIONS", default_value_t = 10_000)]
    backup_every_revisions: i64,

    /// Backups to keep in `--backup-dir`, newest first. Each is a full
    /// copy of the store.
    #[arg(long, env = "FASTETCD_BACKUP_RETAIN", default_value_t = 4)]
    backup_retain: usize,

    /// What to do when the data file is found corrupt at startup.
    /// `restore`: a lone member restores its newest good backup from
    /// `--backup-dir`, keeps the corrupt file, and raises the CORRUPT
    /// alarm. A member of a multi-node cluster always refuses: it must
    /// not forget its raft vote. `refuse`: never restore; leave the file
    /// untouched and exit.
    #[arg(
        long,
        env = "FASTETCD_ON_CORRUPTION",
        value_enum,
        default_value = "restore"
    )]
    on_corruption: fastetcd_server::recovery::OnCorruption,

    /// (etcd compat) Maximum gRPC request size. Accepted and ignored
    /// (fastetcd#54): requests are limited by tonic's 4 MiB default.
    #[arg(long, env = "FASTETCD_MAX_REQUEST_BYTES")]
    max_request_bytes: Option<u64>,

    /// (etcd compat) Log level. Accepted and ignored (fastetcd#54): set
    /// `RUST_LOG` (e.g. `RUST_LOG=debug`); the default is `info`.
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
    #[arg(
        long,
        default_value_t = false,
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = clap::builder::BoolishValueParser::new(),
        action = clap::ArgAction::Set
    )]
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
    /// Print how large a data volume this cluster shape needs, and the
    /// arithmetic behind it. Sizing for the data alone is how a volume
    /// fills: a raft snapshot is a full copy of the database, so the
    /// floor is roughly twice the data before any headroom.
    Sizing {
        /// Nodes the cluster will run.
        #[arg(long, default_value_t = 1)]
        nodes: u64,
        /// Average pods per node. `max-pods` defaults to 110; real
        /// clusters average far lower, and this term dominates.
        #[arg(long, default_value_t = fastetcd_server::sizing::DEFAULT_PODS_PER_NODE)]
        pods_per_node: u64,
    },
    /// Rewrite the data file so space freed by deletes, compaction and
    /// log purge is returned to the filesystem. The offline escape
    /// hatch for a volume that is already full: it needs no raft
    /// quorum, no read barrier and no snapshot write, so it works when
    /// the running server can no longer do anything (fastetcd#14).
    Defrag,
    /// Check the data directory for consistency, and with --repair fix
    /// the raft/format metadata that can strand a cluster.
    Fsck {
        /// Apply repairs instead of only reporting.
        #[arg(long, default_value_t = false)]
        repair: bool,
    },
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
        ("FASTETCD_ENABLE_GRPC_GATEWAY", "ETCD_ENABLE_GRPC_GATEWAY"),
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
        ("FASTETCD_HEARTBEAT_INTERVAL", "ETCD_HEARTBEAT_INTERVAL"),
        ("FASTETCD_ELECTION_TIMEOUT", "ETCD_ELECTION_TIMEOUT"),
        ("FASTETCD_QUOTA_BACKEND_BYTES", "ETCD_QUOTA_BACKEND_BYTES"),
        ("FASTETCD_MAX_SNAPSHOTS", "ETCD_MAX_SNAPSHOTS"),
        (
            "FASTETCD_AUTO_COMPACTION_RETENTION",
            "ETCD_AUTO_COMPACTION_RETENTION",
        ),
        (
            "FASTETCD_MAX_IN_SNAPSHOT_LOG_TO_KEEP",
            "ETCD_MAX_IN_SNAPSHOT_LOG_TO_KEEP",
        ),
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

/// Backups on the data volume share its fate: a device that loses
/// writes can take both. Allowed, but said out loud.
fn warn_if_same_volume(data_dir: &std::path::Path, backup_dir: &std::path::Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let _ = std::fs::create_dir_all(backup_dir);
        if let (Ok(d), Ok(b)) = (std::fs::metadata(data_dir), std::fs::metadata(backup_dir)) {
            if d.dev() == b.dev() {
                tracing::warn!(
                    data_dir = %data_dir.display(),
                    backup_dir = %backup_dir.display(),
                    "--backup-dir is on the same volume as --data-dir: a device that loses \
                     writes can corrupt the backups along with the data. Use a separate \
                     volume, and size it for --backup-retain full copies of the store."
                );
            }
        }
    }
    #[cfg(not(unix))]
    let _ = (data_dir, backup_dir);
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
        Command::Defrag => admin::cmd_defrag(&args.data_dir).await,
        Command::Sizing {
            nodes,
            pods_per_node,
        } => {
            print!(
                "{}",
                fastetcd_server::sizing::report(fastetcd_server::sizing::ClusterShape {
                    nodes: *nodes,
                    pods_per_node: *pods_per_node,
                    snapshot_count: args.snapshot_count,
                    max_snapshots: args.max_snapshots as u64,
                    upgrade_backup_retain: args.upgrade_backup_retain as u64,
                })
            );
            Ok(())
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

    // TLS, client and peer ports configured independently (#23). All of
    // it is checked here, before anything touches the data directory,
    // so a bad flag is a clean startup error.
    let client_tls_files = TlsFiles {
        cert_file: args.cert_file.clone(),
        key_file: args.key_file.clone(),
        trusted_ca_file: args.trusted_ca_file.clone(),
        client_cert_auth: args.client_cert_auth,
    };
    let peer_tls_files = TlsFiles {
        cert_file: args.peer_cert_file.clone(),
        key_file: args.peer_key_file.clone(),
        trusted_ca_file: args.peer_trusted_ca_file.clone(),
        client_cert_auth: args.peer_client_cert_auth,
    };
    let client_tls = client_tls_files.server_config(Port::Client)?;
    let peer_tls = peer_tls_files.server_config(Port::Peer)?;
    let peer_dial_tls = peer_tls_files.peer_client_config()?;
    {
        let mut urls: Vec<(&str, &str)> = Vec::new();
        urls.extend(args.listen_peer_urls.split(',').map(|u| ("--listen-peer-urls", u)));
        if let Some(a) = &args.initial_advertise_peer_urls {
            urls.extend(a.split(',').map(|u| ("--initial-advertise-peer-urls", u)));
        }
        urls.extend(initial_cluster.values().map(|u| ("--initial-cluster", u.as_str())));
        check_peer_url_schemes(peer_tls.is_some(), urls)?;
    }
    match (client_tls.is_some(), peer_tls.is_some()) {
        (_, true) => tracing::info!(
            client_tls = client_tls.is_some(),
            peer_client_cert_auth = args.peer_client_cert_auth,
            "peer TLS enabled (its own identity and CA)"
        ),
        (true, false) => tracing::warn!(
            "client TLS is on but peer TLS is off: raft traffic on the peer port is \
             plaintext and unauthenticated. Set --peer-cert-file, --peer-key-file, \
             --peer-trusted-ca-file and --peer-client-cert-auth to protect it."
        ),
        (false, false) => {}
    }

    tracing::info!(
        name = %args.name,
        node_id,
        data_dir = %args.data_dir.display(),
        listen_client = %client_listen_url,
        listen_peer = %peer_listen_url,
        cluster_state = %args.initial_cluster_state,
        peers = ?initial_cluster.keys().collect::<Vec<_>>(),
        "fastetcd starting"
    );

    // Log the no-op compat flags we received so operators can see
    // them in startup output even though we don't act on them.
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
        &args.initial_advertise_peer_urls,
        &args.advertise_client_urls,
    );

    std::fs::create_dir_all(&args.data_dir)?;
    let data_file = args.data_dir.join("fastetcd.redb");
    // A restore from backup that a crash cut short is finished first, so
    // the restored store is not taken for a new one (fastetcd#37).
    fastetcd_server::recovery::finish_interrupted_restore(&data_file)?;
    let store_is_new = !data_file.exists();

    // A corrupt data file is restored from a backup on a lone member
    // rather than crash-looping (fastetcd#37).
    let configured_members = initial_cluster
        .keys()
        .map(|name| derive_node_id(name))
        .chain(std::iter::once(node_id))
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    if let Some(dir) = &args.backup_dir {
        warn_if_same_volume(&args.data_dir, dir);
    } else {
        tracing::warn!(
            "no --backup-dir: no periodic backups are taken, so a corrupt data file \
             cannot be recovered. Give fastetcd a backup directory on a separate volume."
        );
    }
    let (engine, _recovered) = fastetcd_server::recovery::open_or_recover(
        &data_file,
        &fastetcd_server::recovery::OpenOptions {
            backup_dir: args.backup_dir.clone(),
            on_corruption: args.on_corruption,
            node_id,
            configured_members,
            engine_cache_bytes: Some(args.engine_cache_bytes as usize),
        },
    )
    .await?;
    // Applies (non-durable commits) are held in RAM and written into the
    // data file in batches by the checkpoint, so an apply never waits on
    // the data file's fsync (fastetcd#85).
    let write_behind = fastetcd_storage::write_behind::WriteBehind::new(
        Arc::new(engine),
        args.write_behind_bytes as usize,
    );
    let write_behind_stats = write_behind.stats();
    let engine: Arc<dyn fastetcd_storage::KvStore> = Arc::new(write_behind);
    let recovery_alarm =
        Arc::new(fastetcd_server::recovery::RecoveryAlarm::load(&engine).await?);
    if let Some(r) = recovery_alarm.active() {
        tracing::error!(
            backup_revision = r.backup_revision,
            backup = %r.backup_file,
            corrupt_file = %r.corrupt_file,
            "CORRUPT alarm raised: this store was restored from a backup and lost \
             everything written after it. Disarm with `etcdctl alarm disarm` once handled."
        );
    }
    let backup_engine = engine.clone();

    // Chosen once per store and persisted, so a restart or an upgrade
    // never changes the id clients have pinned (fastetcd#17).
    let (cluster_id, cluster_id_source) = fastetcd_server::cluster_id::resolve(
        &engine,
        args.cluster_id,
        store_is_new,
        args.initial_cluster_token.as_deref(),
    )
    .await?;
    tracing::info!(cluster_id, source = ?cluster_id_source, "cluster id");
    if cluster_id_source == fastetcd_server::cluster_id::Source::Default {
        tracing::warn!(
            cluster_id,
            "no --cluster-id or --initial-cluster-token: this store reports cluster id 1, \
             like every other unconfigured deployment, so clients cannot tell it apart \
             from another. Set a distinct --cluster-id or --initial-cluster-token."
        );
    }
    let cache_config = fastetcd_server::ram_cache::config(
        args.value_cache_bytes,
        args.value_cache_max_entry_bytes,
    );
    let mvcc = MvccStore::open_with(engine.clone(), cache_config).await?;
    let cache_stats = mvcc.cache_stats();
    tracing::info!(
        value_cache_bytes = cache_stats.value_budget_bytes,
        value_cache_max_entry_bytes = args.value_cache_max_entry_bytes,
        engine_cache_bytes = args.engine_cache_bytes,
        index_keys = cache_stats.index_keys,
        index_bytes = cache_stats.index_bytes,
        "RAM cache: every key's index is resident; latest values cached up to the budget"
    );
    let sm = FastetcdStateMachine::open_with_retention(
        mvcc,
        args.data_dir.join("snapshots"),
        args.max_snapshots,
    )
    .await?;
    // The raft log: a sequential WAL in `<data-dir>/wal/`, built from
    // the data file's own log tables on the first start of this version
    // (fastetcd#85).
    let mut log = WalLogStore::open(
        &fastetcd_raft::wal_log_store::wal_dir(&args.data_dir),
        engine,
        fastetcd_raft::wal_log_store::WalLogOptions {
            segment_bytes: args.wal_segment_bytes,
            cache_bytes: args.wal_cache_bytes as usize,
            election_timeout: std::time::Duration::from_millis(args.election_timeout),
            ..Default::default()
        },
    )
    .await?;
    let committed_index = log.committed_index();
    let wal_stats = log.stats();
    let checkpoint_log = log.clone();

    // Snapshot + purge is what bounds the raft log: openraft snapshots
    // every `snapshot_count` applied entries and then purges the log,
    // keeping only `max_in_snapshot_log_to_keep`. Exposed so operators
    // can tune the log-size vs follower-catch-up tradeoff (#13).
    // etcd's two flags, as openraft's three (#103).
    let (heartbeat_interval, election_timeout_min, election_timeout_max) =
        raft_timeouts(args.heartbeat_interval, args.election_timeout)?;
    let config = Arc::new(
        Config {
            heartbeat_interval,
            election_timeout_min,
            election_timeout_max,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(args.snapshot_count),
            max_in_snapshot_log_to_keep: args.max_in_snapshot_log_to_keep,
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
        //
        // Best-effort by design: a backup copies the whole db, so it can
        // fail (no disk space for a second copy, an unwritable backups
        // dir) in situations where normal operation would be fine. That
        // must never stop the node from starting — otherwise a failed
        // safety net crash-loops the control plane, the exact opposite of
        // its purpose. Log loudly and continue.
        if args.upgrade_backup {
            let backup_dir = args
                .upgrade_backup_dir
                .clone()
                .unwrap_or_else(|| args.data_dir.join("backups"));
            if let Err(e) = fastetcd_server::admin::backup_before_version(
                sm.mvcc(),
                &args.data_dir,
                &backup_dir,
                env!("CARGO_PKG_VERSION"),
                args.upgrade_backup_retain.max(1),
            )
            .await
            {
                tracing::error!(
                    error = %e,
                    backup_dir = %backup_dir.display(),
                    "pre-version safety backup FAILED — starting anyway. Free space or \
                     fix permissions on the backup directory, or set \
                     FASTETCD_UPGRADE_BACKUP=false to skip it. Take a manual backup with \
                     `fastetcd backup --out <path>` when the node is stopped."
                );
            }
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

    // From here on the state machine's applies commit without their own
    // fsync (#71): the checkpointer makes the data file durable in the
    // background, and the WAL replays whatever a crash loses (#85).
    // Startup recovery above wrote durably.
    sm.mvcc().defer_apply_sync();
    let _checkpointer = fastetcd_raft::wal_log_store::spawn_checkpointer(
        checkpoint_log,
        sm.applied_index(),
        fastetcd_raft::wal_log_store::CheckpointConfig {
            interval: std::time::Duration::from_millis(args.wal_checkpoint_interval_ms.max(1)),
            entries: args.wal_checkpoint_entries.max(1),
        },
    );
    let log_progress = log.progress();
    let factory = GrpcNetworkFactory::with_tls(peers.clone(), peer_dial_tls.clone());
    let raft = Raft::<TypeConfig>::new(node_id, config, factory, log, sm.clone()).await?;
    // `--snapshot-count` and `--max-in-snapshot-log-to-keep` in proposals,
    // as etcd counts them, not in log entries, which can be batches
    // (#80). openraft's entry-counted policy above stays as the backstop.
    fastetcd_raft::snapshot_policy::spawn(
        raft.clone(),
        sm.proposal_log(),
        args.snapshot_count,
        args.max_in_snapshot_log_to_keep,
    );

    // Bootstrap: only the `new` state initializes; `existing` waits
    // for an external add-learner call.
    if is_bootstrap {
        if let Err(e) = raft.initialize(all_members).await {
            tracing::warn!("raft initialize: {e} — assuming already initialized");
        }
    } else {
        tracing::info!("cluster_state=existing — skipping raft.initialize; waiting to be joined");
    }

    let forwarder = fastetcd_raft::WriteForwarder::with_tls(peers.clone(), peer_dial_tls);

    // Disk-space accounting (#14). A bounded data volume must never
    // reach ENOSPC: at that point the snapshot write fails, openraft
    // surfaces the storage error on every read and write, and even
    // deleting keys to make room is refused. The monitor reclaims at the
    // high-water mark and raises NOSPACE — refusing writes but not reads
    // or deletes — well before the wall.
    let space_cfg = fastetcd_server::space::SpaceConfig {
        quota_backend_bytes: args.quota_backend_bytes.max(0) as u64,
        high_water_percent: args.space_high_water_percent,
        alarm_percent: args.space_alarm_percent,
        clear_percent: args.space_clear_percent,
        interval: std::time::Duration::from_secs(args.space_check_interval_secs.max(1)),
        reclaim_retention: args.space_reclaim_retention.max(1),
        auto_defrag: args.auto_defrag,
        ..Default::default()
    };
    let space = Arc::new(if args.space_check_interval_secs == 0 {
        tracing::warn!(
            "space monitoring is disabled (--space-check-interval-secs 0) — \
             nothing will reclaim space or raise a NOSPACE alarm"
        );
        fastetcd_server::space::SpaceGuard::disabled()
    } else {
        fastetcd_server::space::SpaceGuard::new(args.data_dir.clone(), space_cfg)
    });

    // Declared cluster shape (#14 follow-up): check the volume is big
    // enough for what this cluster is expected to hold, while it is
    // still empty and the answer is actionable. Warn rather than refuse
    // — an operator who knows better than the model should not be
    // stopped from starting, and a control plane that will not boot is
    // worse than one that is going to need attention later.
    let mut derived_compaction = args.auto_compaction_retention;
    if args.expected_nodes > 0 {
        let shape = fastetcd_server::sizing::ClusterShape {
            nodes: args.expected_nodes,
            pods_per_node: args.expected_pods_per_node,
            snapshot_count: args.snapshot_count,
            max_snapshots: args.max_snapshots as u64,
            upgrade_backup_retain: args.upgrade_backup_retain as u64,
        };
        let est = fastetcd_server::sizing::estimate(shape);
        let fs = fastetcd_storage::fs_space::probe(&args.data_dir);
        let volume = fs.map(|f| f.total).unwrap_or(0);
        if volume > 0 && volume < est.recommended_volume_bytes {
            tracing::error!(
                expected_nodes = args.expected_nodes,
                volume = %fastetcd_server::sizing::human(volume),
                needs = %fastetcd_server::sizing::human(est.recommended_volume_bytes),
                provision = %fastetcd_server::sizing::human(est.provision_bytes),
                "DATA VOLUME IS TOO SMALL for the declared cluster size. A raft \
                 snapshot is a full copy of the database, so the floor is roughly \
                 twice the data before headroom. Run `fastetcd sizing --nodes N` \
                 for the breakdown. Starting anyway; the store will compact and \
                 defragment under pressure, and will refuse writes rather than \
                 fill."
            );
        } else {
            tracing::info!(
                expected_nodes = args.expected_nodes,
                volume = %fastetcd_server::sizing::human(volume),
                needs = %fastetcd_server::sizing::human(est.recommended_volume_bytes),
                "data volume is sized for the declared cluster"
            );
        }
        // An unbounded MVCC history is the usual reason a bounded volume
        // fills. If the operator declared a cluster size but left
        // compaction off, bound it.
        //
        // The retention window is in revisions, but what we actually
        // want to hold constant is *time*: node leases alone write about
        // 6 revisions per node per minute (a 10s renewal each), so
        // scaling retention with node count keeps roughly a fixed window
        // regardless of cluster size. 100 revisions per node is ~16
        // minutes at that rate — comfortably more than the apiserver's
        // own 5-minute compaction interval, so this never fights it, and
        // the floor keeps a tiny cluster from compacting too eagerly.
        if derived_compaction == 0 {
            derived_compaction = (args.expected_nodes as i64).saturating_mul(100).max(10_000);
            tracing::info!(
                retention = derived_compaction,
                "--expected-nodes set and --auto-compaction-retention left at 0: \
                 enabling revision-mode auto-compaction derived from the cluster size"
            );
        }
    }

    let server_state = Arc::new(
        ServerState::new(
            raft.clone(),
            sm,
            cluster_id,
            node_id,
            forwarder,
        )
        .with_space(space)
        .with_recovery(recovery_alarm)
        .with_client_cert_auth(args.client_cert_auth)
        .with_committed_index(committed_index)
        .with_wal_stats(wal_stats)
        .with_write_behind_stats(write_behind_stats)
        // Linearizable reads and batched writes without queueing in
        // openraft's RaftCore, on one member (#71) or several (#75).
        .with_peer_read_index_and_batching(log_progress.clone()),
    );

    // Periodic backups to a separate volume (fastetcd#37).
    if let Some(dir) = &args.backup_dir {
        let cfg = fastetcd_server::backup::BackupConfig {
            dir: dir.clone(),
            interval: std::time::Duration::from_secs(args.backup_interval_secs.max(1)),
            every_revisions: args.backup_every_revisions,
            retain: args.backup_retain.max(1),
            node_id,
        };
        tracing::info!(
            dir = %cfg.dir.display(),
            interval_secs = cfg.interval.as_secs(),
            every_revisions = cfg.every_revisions,
            retain = cfg.retain,
            "periodic backups enabled"
        );
        fastetcd_server::backup::spawn(
            cfg,
            backup_engine,
            server_state.sm.mvcc().clone(),
            raft.clone(),
        );
    }

    // Spawn the lease auto-expiry ticker — leader-only, no-op on followers.
    fastetcd_server::lease_expiry::spawn(server_state.clone());
    if fastetcd_server::space::spawn(server_state.clone()).is_some() {
        tracing::info!(
            quota_backend_bytes = args.quota_backend_bytes,
            high_water_percent = args.space_high_water_percent,
            alarm_percent = args.space_alarm_percent,
            max_snapshots = args.max_snapshots,
            auto_defrag = args.auto_defrag,
            "space monitor started"
        );
    }
    if let Some(_h) = fastetcd_server::compaction::spawn(
        server_state.clone(),
        fastetcd_server::compaction::Mode::Revision,
        derived_compaction,
        std::time::Duration::from_secs(args.auto_compaction_interval_secs.max(1)),
    ) {
        tracing::info!(
            retention = derived_compaction,
            "MVCC auto-compaction enabled (revision mode)"
        );
    }

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
    let admin = AdminService::new(server_state.clone());
    // The store's own auth state, updated by raft apply (#32).
    let auth_state = server_state.auth.clone();
    // Every gRPC call on the client port is counted for /metrics (#29).
    let traffic = server_state.traffic.clone();
    let peer_read_index = server_state.read_index.clone();
    let peer_proposer = server_state.proposer.clone();
    let auth = AuthService::new(server_state);

    // Answers other members' ConfirmLeader, serves forwarded reads behind
    // the same read barrier and batches forwarded writes (#75).
    let mut peer_service =
        RaftPeerService::new(raft, peer_mvcc.clone()).with_log_progress(log_progress);
    if let Some(r) = peer_read_index {
        peer_service = peer_service.with_read_index(r);
    }
    if let Some(p) = peer_proposer {
        peer_service = peer_service.with_proposer(p);
    }

    let client_listen: std::net::SocketAddr = client_listen_url.parse()?;
    let peer_listen: std::net::SocketAddr = peer_listen_url.parse()?;

    // Spawn the peer server on its own port, with the peer identity.
    let tls_for_peer = peer_tls;
    let peer_handle = {
        tokio::spawn(async move {
            tracing::info!(%peer_listen, "serving RaftPeer gRPC");
            let mut builder = Server::builder();
            if let Some(t) = tls_for_peer {
                builder = builder.tls_config(t).expect("apply peer TLS config");
            }
            builder
                .add_service(RaftPeerServer::new(peer_service)
                .max_decoding_message_size(fastetcd_raft::network::PEER_MAX_DECODE_BYTES))
                .serve(peer_listen)
                .await
        })
    };

    // Client services on the client port. Every non-Auth service is
    // wrapped by AuthInterceptor; Auth stays open so clients can
    // call Authenticate without a pre-existing token.
    let interceptor =
        AuthInterceptor::new(auth_state.clone()).with_client_cert_auth(args.client_cert_auth);
    let tls_for_client = client_tls;
    // etcd's v3 JSON gateway on the client port, through the same
    // services and interceptor as gRPC (#28).
    let gateway = args.enable_grpc_gateway.then(|| fastetcd_server::gateway::Gateway {
        kv: kv.clone(),
        lease: lease.clone(),
        cluster: cluster.clone(),
        maintenance: maintenance.clone(),
        auth: auth.clone(),
        watch: watch.clone(),
        interceptor: interceptor.clone(),
        traffic: traffic.clone(),
    });

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
            grpc_routes.add_service(LeaseServer::with_interceptor(lease, interceptor.clone()));
            grpc_routes.add_service(FastetcdAdminServer::with_interceptor(admin, interceptor));
            grpc_routes.add_service(AuthServer::new(auth));

            // Same port also answers etcd's plain-HTTP health probes
            // (load balancers / k8s httpGet probes already pointed
            // at :2379 for etcd don't need a second port for this).
            // tonic 0.12 routes are convertible to/from axum::Router,
            // so the gRPC routes and the HTTP routes below share one
            // `Service` served on the same listener.
            let mut app: axum::Router = grpc_routes
                .routes()
                .into_axum_router()
                .route("/health", axum::routing::get(health_http_handler))
                .route("/livez", axum::routing::get(livez_http_handler))
                .route("/readyz", axum::routing::get(livez_http_handler));
            if let Some(gw) = gateway {
                app = app.merge(fastetcd_server::gateway::router(gw));
            }
            let app = app
                .layer(axum::middleware::from_fn_with_state(
                    traffic,
                    fastetcd_server::traffic::grpc_middleware,
                ));

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

    // Persist the deferred applies so the next start need not replay
    // them (#71). Not needed for correctness: the log replays them.
    if let Err(e) = peer_mvcc.sync().await {
        tracing::warn!(error = %e, "flushing applied state on shutdown");
    }

    Ok(())
}

/// openraft's heartbeat interval and election range from etcd's
/// `--heartbeat-interval` and `--election-timeout` (fastetcd#103): the
/// election timeout t becomes [t, 2t). etcd wants t >= 5x the heartbeat;
/// fastetcd's defaults (250 ms, 1 s) predate the flags and openraft only
/// needs t > the heartbeat, so the floor here is 2x. etcd's 50 s ceiling.
fn raft_timeouts(heartbeat_ms: u64, election_ms: u64) -> anyhow::Result<(u64, u64, u64)> {
    if heartbeat_ms == 0 {
        anyhow::bail!("--heartbeat-interval must be at least 1 ms");
    }
    if election_ms < 2 * heartbeat_ms {
        anyhow::bail!(
            "--election-timeout ({election_ms} ms) must be at least twice --heartbeat-interval ({heartbeat_ms} ms)"
        );
    }
    if election_ms > 50_000 {
        anyhow::bail!("--election-timeout ({election_ms} ms) is too long: at most 50000 ms, as in etcd");
    }
    Ok((heartbeat_ms, election_ms, election_ms * 2))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Args, clap::Error> {
        Args::try_parse_from(std::iter::once("fastetcd").chain(args.iter().copied()))
    }

    // fastetcd#53: default-on flags could not be turned off, and
    // `--auto-defrag=true` (the Helm chart's) was a parse error.
    #[test]
    fn bool_flags_take_etcd_style_values() {
        type Get = fn(&Args) -> bool;
        let flags: [(&str, Get, bool); 7] = [
            ("auto-defrag", |a| a.auto_defrag, true),
            ("upgrade-backup", |a| a.upgrade_backup, true),
            ("enable-grpc-gateway", |a| a.enable_grpc_gateway, true),
            ("client-cert-auth", |a| a.client_cert_auth, false),
            ("peer-client-cert-auth", |a| a.peer_client_cert_auth, false),
            ("force-new-cluster", |a| a.force_new_cluster, false),
            ("enable-pprof", |a| a.enable_pprof, false),
        ];
        for (name, get, default) in flags {
            assert_eq!(get(&parse(&[]).unwrap()), default, "--{name} default");
            assert!(get(&parse(&[&format!("--{name}")]).unwrap()), "bare --{name}");
            for (v, want) in [("true", true), ("false", false), ("1", true), ("0", false)] {
                let a = parse(&[&format!("--{name}={v}")]).unwrap_or_else(|e| panic!("--{name}={v}: {e}"));
                assert_eq!(get(&a), want, "--{name}={v}");
            }
            assert!(parse(&[&format!("--{name}=maybe")]).is_err(), "--{name}=maybe");
        }
        // The value as the next argument, as `--enable-grpc-gateway false`
        // has been accepted since 1.8; a bare flag before another flag.
        let a = parse(&["--auto-defrag", "false", "--upgrade-backup", "--client-cert-auth", "0"]).unwrap();
        assert!(!a.auto_defrag && a.upgrade_backup && !a.client_cert_auth);
    }

    #[test]
    fn bool_env_vars_take_boolish_values() {
        use clap::{CommandFactory, FromArgMatches};
        // The real `--auto-defrag` and `--client-cert-auth`, each case
        // reading a variable of its own, so parallel tests cannot see it.
        for (id, v, want) in [
            ("auto_defrag", "false", false),
            ("auto_defrag", "0", false),
            ("auto_defrag", "off", false),
            ("client_cert_auth", "true", true),
            ("client_cert_auth", "1", true),
        ] {
            let var = format!("FASTETCD_TEST_{}_{}", id.to_uppercase(), v.to_uppercase());
            std::env::set_var(&var, v);
            // clap takes an env name it can keep: leak this test's one.
            let name: &'static str = Box::leak(var.clone().into_boxed_str());
            let m = Args::command()
                .mut_arg(id, |a| a.env(name))
                .try_get_matches_from(["fastetcd"])
                .unwrap_or_else(|e| panic!("{var}={v}: {e}"));
            let a = Args::from_arg_matches(&m).unwrap();
            let got = if id == "auto_defrag" { a.auto_defrag } else { a.client_cert_auth };
            assert_eq!(got, want, "{var}={v}");
        }
    }

    #[test]
    fn election_timeouts_from_etcd_flags() {
        assert_eq!(raft_timeouts(250, 1000).unwrap(), (250, 1000, 2000), "the defaults, as before");
        assert_eq!(raft_timeouts(100, 1000).unwrap(), (100, 1000, 2000), "etcd's defaults");
        assert_eq!(raft_timeouts(250, 10_000).unwrap(), (250, 10_000, 20_000));
        assert!(raft_timeouts(0, 1000).is_err());
        assert!(raft_timeouts(500, 999).is_err());
        assert!(raft_timeouts(250, 50_001).is_err());
        let a = parse(&["--heartbeat-interval=100", "--election-timeout=5000"]).unwrap();
        assert_eq!((a.heartbeat_interval, a.election_timeout), (100, 5000));
        let a = parse(&[]).unwrap();
        assert_eq!((a.heartbeat_interval, a.election_timeout), (250, 1000));
    }
}
