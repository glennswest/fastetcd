//! Prometheus `/metrics` endpoint.
//!
//! Exposes a small set of metrics with the same names etcd uses
//! where they map directly, so existing dashboards / alerts that
//! were authored against etcd work against fastetcd unchanged.
//!
//! Exported:
//!   - `etcd_server_has_leader` (gauge 0/1)
//!   - `etcd_server_leader_changes_seen_total` (counter)
//!   - `etcd_mvcc_db_total_size_in_bytes` (gauge)
//!   - `etcd_mvcc_db_total_size_in_use_in_bytes` (gauge)
//!   - `etcd_server_quota_backend_bytes` (gauge)
//!   - `fastetcd_store_space_used_ratio` (gauge, 0-1)
//!   - `fastetcd_store_snapshot_size_in_bytes` (gauge)
//!   - `fastetcd_disk_total_bytes` / `fastetcd_disk_available_bytes`
//!   - `fastetcd_nospace_alarm_active` (gauge 0/1)
//!   - `fastetcd_recovered_from_backup_total` (counter): restores from a
//!     backup over the life of the store (fastetcd#37)
//!   - `fastetcd_recovered_revision` (gauge): revision the last restore
//!     went back to, while the CORRUPT alarm is raised; 0 otherwise
//!   - `etcd_debugging_mvcc_current_revision` (gauge)
//!   - `etcd_debugging_mvcc_compact_revision` (gauge)
//!   - `etcd_debugging_mvcc_{put,delete,range,txn}_total` (counters):
//!     operations this member executed (`MvccStore::op_counts`)
//!   - `etcd_server_is_leader` (gauge 0/1): whether *this* member leads
//!   - `etcd_server_id{server_id=<hex>}` (gauge, always 1)
//!   - `etcd_server_proposals_committed_total` /
//!     `etcd_server_proposals_applied_total` (gauges, as in etcd): the
//!     raft committed and applied indexes
//!   - `fastetcd_engine_info{engine=…}` (info: redb / wal / iouring)
//!   - `fastetcd_read_index_total{path=sole_voter|quorum|raft}`
//!     (counters): linearizable read barriers this member served, by how
//!     (#71, #75): from local state as the only voter, confirmed by a
//!     quorum over `ConfirmLeader`, or through openraft's RaftCore
//!   - `fastetcd_proposal_batches_total` / `fastetcd_proposals_batched_total`
//!     / `fastetcd_proposals_single_total` (counters): batched log entries
//!     this member proposed, the proposals in them, and proposals that
//!     went alone (#75)
//!   - `process_resident_memory_bytes`, `process_virtual_memory_bytes`,
//!     `process_cpu_seconds_total`, `process_start_time_seconds`,
//!     `process_open_fds`, `process_max_fds`: etcd's (Go's) process
//!     metrics, from `/proc/self` (#84; `process_metrics.rs`)
//!   - `fastetcd_lease_renewals_total{path=ram|raft}` (counter): lease
//!     keep-alives this member renewed as the leader, in RAM or proposed
//!     through raft because a member is older than 1.23 (#92);
//!     `fastetcd_lease_promotions_total`: terms it gave every lease a
//!     full TTL in, on becoming leader
//!   - `fastetcd_watch_resyncs_total` / `fastetcd_watch_lag_cancels_total`
//!     (counters, process-wide): watchers caught up from history, and
//!     watchers cancelled because that history was gone (#16)
//!   - RAM cache (#82): `fastetcd_value_cache_{hits,misses,evictions}_total`
//!     (counters), `fastetcd_value_cache_bytes` / `_entries` /
//!     `_budget_bytes`, `fastetcd_key_index_keys` / `fastetcd_key_index_bytes`
//!     (gauges), and `fastetcd_mvcc_get_duration_seconds{cache=hit|miss}`
//!     (histogram): the store's time for each single-key Range, `hit` when
//!     it read nothing from the engine
//!
//!   - raft WAL (#85): `fastetcd_wal_fsyncs_total`,
//!     `fastetcd_wal_fsync_seconds_total`, `fastetcd_wal_bytes_appended_total`,
//!     `fastetcd_wal_entries_synced_total`, `fastetcd_wal_proposals_synced_total`
//!     (counters; per fsync = their rate / the fsync rate, #95),
//!     `fastetcd_wal_segments`, `fastetcd_wal_cached_bytes`,
//!     `fastetcd_wal_fsync_max_seconds`, `fastetcd_checkpoint_max_seconds`,
//!     `fastetcd_checkpoint_pace_seconds`
//!     (gauges); background checkpoints of the data file:
//!     `fastetcd_checkpoints_total`, `fastetcd_checkpoint_seconds_total`,
//!     `fastetcd_checkpoint_failures_total`,
//!     `fastetcd_checkpoint_commit_seconds_total` (counters),
//!     `fastetcd_checkpoint_commit_max_seconds` (gauge),
//!     `fastetcd_checkpoint_durable_applied_index` (gauge); the
//!     write-behind layer: `fastetcd_write_behind_bytes` / `_batches`
//!     (gauges), `fastetcd_write_behind_flushes_total`,
//!     `fastetcd_write_behind_flush_seconds_total`,
//!     `fastetcd_write_behind_backpressure_total`,
//!     `fastetcd_write_behind_backpressure_seconds_total` (time those
//!     applies waited), `fastetcd_write_behind_presync_seconds_total` (the
//!     checkpoints' fsyncs of the data file outside the flush lock, #93)
//!     (counters)
//!
//! Registered from the server's live [`Traffic`](crate::traffic::Traffic)
//! when the endpoint starts (fastetcd#29; see that module):
//!   - `grpc_server_started_total` / `grpc_server_handled_total`
//!   - `etcd_debugging_mvcc_watch_stream_total`,
//!     `etcd_debugging_mvcc_watcher_total`,
//!     `etcd_debugging_mvcc_slow_watcher_total` (gauges)
//!   - `etcd_server_proposals_pending` (gauge)
//!
//! Metrics are refreshed lazily on every scrape — no background
//! task — so we always report the current truth. Scrapes are
//! serialized, so two at once cannot count a counter's increase twice.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::metrics::info::Info;
use prometheus_client::registry::Registry;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use crate::state::ServerState;

/// Metric handles + the registry. Each metric is updated from the
/// scrape path; counters are append-only.
pub struct Metrics {
    pub registry: Mutex<Registry>,
    pub has_leader: Gauge,
    pub leader_changes_total: Counter,
    pub db_size_bytes: Gauge,
    pub db_size_in_use_bytes: Gauge,
    pub quota_backend_bytes: Gauge,
    pub snapshot_size_bytes: Gauge,
    pub disk_total_bytes: Gauge,
    pub disk_available_bytes: Gauge,
    pub space_used_ratio: Gauge<f64, std::sync::atomic::AtomicU64>,
    pub nospace_alarm: Gauge,
    pub recovered_total: Counter,
    pub recovered_revision: Gauge,
    pub current_revision: Gauge,
    pub compact_revision: Gauge,
    pub auth_diverged: Gauge,
    pub log_has_batched: Gauge,
    pub members_unable_to_read_batches: Gauge,
    pub is_leader: Gauge,
    pub proposals_committed: Gauge,
    pub proposals_applied: Gauge,
    pub put_total: Counter,
    pub delete_total: Counter,
    pub range_total: Counter,
    pub txn_total: Counter,
    pub watch_resyncs_total: Counter,
    pub watch_lag_cancels_total: Counter,
    pub read_index_total: Family<Vec<(String, String)>, Counter>,
    pub proposal_batches_total: Counter,
    pub proposals_batched_total: Counter,
    pub proposals_single_total: Counter,
    pub lease_renewals_total: Family<Vec<(String, String)>, Counter>,
    pub lease_promotions_total: Counter,
    pub cache: CacheMetrics,
    pub wal: WalMetrics,
    /// etcd's `process_*` (fastetcd#84).
    pub process: crate::process_metrics::ProcessMetrics,
    /// Last leader id we saw, so leader_changes_total tracks
    /// monotonic edges.
    last_leader: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        let registry = Registry::default();
        let has_leader = Gauge::default();
        let leader_changes_total = Counter::default();
        let db_size_bytes = Gauge::default();
        let db_size_in_use_bytes = Gauge::default();
        let quota_backend_bytes = Gauge::default();
        let snapshot_size_bytes = Gauge::default();
        let disk_total_bytes = Gauge::default();
        let disk_available_bytes = Gauge::default();
        let space_used_ratio = Gauge::<f64, std::sync::atomic::AtomicU64>::default();
        let nospace_alarm = Gauge::default();
        let recovered_total = Counter::default();
        let recovered_revision = Gauge::default();
        let current_revision = Gauge::default();
        let compact_revision = Gauge::default();
        let auth_diverged = Gauge::default();
        let log_has_batched = Gauge::default();
        let members_unable_to_read_batches = Gauge::default();
        let is_leader = Gauge::default();
        let proposals_committed = Gauge::default();
        let proposals_applied = Gauge::default();
        let put_total = Counter::default();
        let delete_total = Counter::default();
        let range_total = Counter::default();
        let txn_total = Counter::default();
        let watch_resyncs_total = Counter::default();
        let watch_lag_cancels_total = Counter::default();
        let read_index_total: Family<Vec<(String, String)>, Counter> = Family::default();
        let proposal_batches_total = Counter::default();
        let proposals_batched_total = Counter::default();
        let proposals_single_total = Counter::default();
        let lease_renewals_total: Family<Vec<(String, String)>, Counter> = Family::default();
        let lease_promotions_total = Counter::default();
        let m = Arc::new(Self {
            registry: Mutex::new(registry),
            has_leader: has_leader.clone(),
            leader_changes_total: leader_changes_total.clone(),
            db_size_bytes: db_size_bytes.clone(),
            db_size_in_use_bytes: db_size_in_use_bytes.clone(),
            quota_backend_bytes: quota_backend_bytes.clone(),
            snapshot_size_bytes: snapshot_size_bytes.clone(),
            disk_total_bytes: disk_total_bytes.clone(),
            disk_available_bytes: disk_available_bytes.clone(),
            space_used_ratio: space_used_ratio.clone(),
            nospace_alarm: nospace_alarm.clone(),
            recovered_total: recovered_total.clone(),
            recovered_revision: recovered_revision.clone(),
            current_revision: current_revision.clone(),
            compact_revision: compact_revision.clone(),
            auth_diverged: auth_diverged.clone(),
            log_has_batched: log_has_batched.clone(),
            members_unable_to_read_batches: members_unable_to_read_batches.clone(),
            is_leader: is_leader.clone(),
            proposals_committed: proposals_committed.clone(),
            proposals_applied: proposals_applied.clone(),
            put_total: put_total.clone(),
            delete_total: delete_total.clone(),
            range_total: range_total.clone(),
            txn_total: txn_total.clone(),
            watch_resyncs_total: watch_resyncs_total.clone(),
            watch_lag_cancels_total: watch_lag_cancels_total.clone(),
            read_index_total: read_index_total.clone(),
            proposal_batches_total: proposal_batches_total.clone(),
            proposals_batched_total: proposals_batched_total.clone(),
            proposals_single_total: proposals_single_total.clone(),
            lease_renewals_total: lease_renewals_total.clone(),
            lease_promotions_total: lease_promotions_total.clone(),
            cache: CacheMetrics::new(),
            wal: WalMetrics::new(),
            process: Default::default(),
            last_leader: AtomicU64::new(0),
        });
        {
            let r = m.registry.try_lock().expect("uncontended in new()");
            // Lifetime trick: we have a Mutex but try_lock returns a
            // MutexGuard. We need a mut reference to the inner
            // Registry — extract it via lock_owned or rebuild.
            drop(r);
        }
        // Re-acquire and register. Use blocking_lock semantics safely
        // because no other handle exists yet.
        {
            let mut reg = m.registry.try_lock().expect("uncontended in new()");
            reg.register(
                "etcd_server_has_leader",
                "Whether this node has a known leader (1) or not (0)",
                has_leader,
            );
            reg.register(
                "etcd_server_leader_changes_seen_total",
                "Total number of times the locally-observed leader has changed",
                leader_changes_total,
            );
            reg.register(
                "etcd_mvcc_db_total_size_in_bytes",
                "Total on-disk size of the backend engine in bytes",
                db_size_bytes,
            );
            reg.register(
                "etcd_mvcc_db_total_size_in_use_in_bytes",
                "Bytes of the backend engine actually holding live data; \
                 the gap to the total size is what a defragment would free",
                db_size_in_use_bytes,
            );
            reg.register(
                "etcd_server_quota_backend_bytes",
                "Effective ceiling on the store's footprint: the configured \
                 quota, or what the data volume can actually hold",
                quota_backend_bytes,
            );
            reg.register(
                "fastetcd_store_snapshot_size_in_bytes",
                "Bytes occupied by the retained raft snapshots on the data volume",
                snapshot_size_bytes,
            );
            reg.register(
                "fastetcd_disk_total_bytes",
                "Total size of the filesystem holding the data directory",
                disk_total_bytes,
            );
            reg.register(
                "fastetcd_disk_available_bytes",
                "Bytes still available to fastetcd on the data volume",
                disk_available_bytes,
            );
            reg.register(
                "fastetcd_store_space_used_ratio",
                "Store footprint as a fraction of its effective capacity; \
                 reclaim starts at the high-water mark and writes are \
                 refused at the alarm mark",
                space_used_ratio,
            );
            reg.register(
                "fastetcd_nospace_alarm_active",
                "1 while the NOSPACE alarm is raised and writes are refused",
                nospace_alarm,
            );
            // prometheus-client appends `_total` to a counter's name.
            reg.register(
                "fastetcd_recovered_from_backup",
                "Times this store has been restored from a backup after its data \
                 file was found corrupt",
                recovered_total,
            );
            reg.register(
                "fastetcd_recovered_revision",
                "Revision the store was restored to while the CORRUPT alarm is \
                 raised (writes after it were lost); 0 when no alarm is raised",
                recovered_revision,
            );
            reg.register(
                "etcd_debugging_mvcc_current_revision",
                "Latest MVCC revision applied to the state machine",
                current_revision,
            );
            reg.register(
                "etcd_debugging_mvcc_compact_revision",
                "MVCC revision below which historical reads return ErrCompacted",
                compact_revision,
            );
            reg.register(
                "fastetcd_log_has_batched",
                "1 once this member has applied a batched log entry (it never goes back to 0): \
                 a member older than 1.10 cannot read this cluster's log (fastetcd#77)",
                log_has_batched,
            );
            reg.register(
                "fastetcd_members_unable_to_read_batches",
                "On the leader: members that answer as older than 1.10 while the log holds \
                 batched entries. They stop at the first one; upgrade or replace them (fastetcd#77)",
                members_unable_to_read_batches,
            );
            reg.register(
                "fastetcd_auth_diverged",
                "1 when the last check found members holding different auth state: \
                 auth changes are refused until one is adopted with \
                 `fastetcd-ctl auth adopt` (fastetcd#32)",
                auth_diverged,
            );
            reg.register(
                "etcd_server_is_leader",
                "Whether this member is the leader (1) or not (0)",
                is_leader,
            );
            reg.register(
                "etcd_server_proposals_committed_total",
                "The total number of consensus proposals committed (the raft committed index)",
                proposals_committed,
            );
            reg.register(
                "etcd_server_proposals_applied_total",
                "The total number of consensus proposals applied (the raft applied index)",
                proposals_applied,
            );
            // prometheus-client appends `_total` to a counter's name.
            reg.register(
                "etcd_debugging_mvcc_put",
                "Total number of puts seen by this member",
                put_total,
            );
            reg.register(
                "etcd_debugging_mvcc_delete",
                "Total number of deletes seen by this member",
                delete_total,
            );
            reg.register(
                "etcd_debugging_mvcc_range",
                "Total number of ranges seen by this member",
                range_total,
            );
            reg.register(
                "etcd_debugging_mvcc_txn",
                "Total number of txns seen by this member",
                txn_total,
            );
            reg.register(
                "fastetcd_watch_resyncs",
                "Watchers caught up from MVCC history after missing live events",
                watch_resyncs_total,
            );
            reg.register(
                "fastetcd_watch_lag_cancels",
                "Watchers cancelled because the history they had missed was compacted \
                 or unreadable",
                watch_lag_cancels_total,
            );
            reg.register(
                "fastetcd_read_index",
                "Linearizable read barriers served, by path: sole_voter, quorum or raft",
                read_index_total,
            );
            reg.register(
                "fastetcd_proposal_batches",
                "Batched log entries proposed (group commit)",
                proposal_batches_total,
            );
            reg.register(
                "fastetcd_proposals_batched",
                "Proposals that went in batched log entries",
                proposals_batched_total,
            );
            reg.register(
                "fastetcd_proposals_single",
                "Proposals that went as a log entry of their own",
                proposals_single_total,
            );
            reg.register(
                "fastetcd_lease_renewals",
                "Lease keep-alives renewed as the leader, by path (ram, or raft while a member is older than 1.23)",
                lease_renewals_total,
            );
            reg.register(
                "fastetcd_lease_promotions",
                "Terms this member gave every lease a full TTL in, on becoming leader",
                lease_promotions_total,
            );
            m.cache.register(&mut reg);
            m.wal.register(&mut reg);
            m.process.register(&mut reg);
        }
        m
    }

    /// Register what belongs to a running server: its traffic handles,
    /// its member id and its engine. Called once, by [`spawn_server`].
    pub async fn attach(&self, state: &ServerState) {
        let mut reg = self.registry.lock().await;
        let t = &state.traffic;
        reg.register(
            "grpc_server_started",
            "Total number of RPCs started on the server",
            t.grpc_started.clone(),
        );
        reg.register(
            "grpc_server_handled",
            "Total number of RPCs completed on the server, regardless of success or failure",
            t.grpc_handled.clone(),
        );
        reg.register(
            "etcd_debugging_mvcc_watch_stream_total",
            "Total number of watch streams",
            t.watch_streams.clone(),
        );
        reg.register(
            "etcd_debugging_mvcc_watcher_total",
            "Total number of watchers",
            t.watchers.clone(),
        );
        reg.register(
            "etcd_debugging_mvcc_slow_watcher_total",
            "Total number of unsynced slow watchers",
            t.slow_watchers.clone(),
        );
        reg.register(
            "etcd_server_proposals_pending",
            "The current number of pending proposals to commit",
            t.proposals_pending.clone(),
        );
        reg.register(
            "etcd_server_health_success",
            "The total number of successful health checks",
            t.health_success.clone(),
        );
        reg.register(
            "etcd_server_health_failures",
            "The total number of failed health checks",
            t.health_failures.clone(),
        );
        let server_id: Family<Vec<(String, String)>, Gauge> = Family::default();
        server_id
            .get_or_create(&vec![(
                "server_id".to_string(),
                format!("{:x}", state.member_id),
            )])
            .set(1);
        reg.register(
            "etcd_server_id",
            "Server or member ID in hexadecimal format. 1 for 'server_id' label with current ID",
            server_id,
        );
        // prometheus-client appends `_info` to an info metric's name.
        reg.register(
            "fastetcd_engine",
            "The storage engine this member runs on",
            Info::new(vec![(
                "engine".to_string(),
                state.sm.mvcc().engine().engine_name().to_string(),
            )]),
        );
        state.sm.mvcc().set_get_observer(self.cache.observer());
    }

    /// Refresh gauges from live server state. Counters are not
    /// refreshed here — they're updated only on observed edges.
    pub async fn refresh(&self, state: &ServerState) {
        self.process.refresh();
        let raft_m = state.raft.metrics().borrow().clone();
        let leader = raft_m.current_leader.unwrap_or(0);
        let prev = self.last_leader.swap(leader, Ordering::Relaxed);
        if leader != 0 && leader != prev {
            self.leader_changes_total.inc();
        }
        self.has_leader.set(if leader != 0 { 1 } else { 0 });
        self.is_leader
            .set((leader != 0 && leader == state.member_id) as i64);
        self.proposals_applied
            .set(raft_m.last_applied.map_or(0, |l| l.index) as i64);
        self.proposals_committed.set(state.committed_index() as i64);
        let ops = state.sm.mvcc().op_counts();
        catch_up(&self.put_total, ops.put);
        catch_up(&self.delete_total, ops.delete);
        catch_up(&self.range_total, ops.range);
        catch_up(&self.txn_total, ops.txn);
        catch_up(&self.watch_resyncs_total, crate::watch::resync_count());
        catch_up(&self.watch_lag_cancels_total, crate::watch::lag_cancel_count());
        catch_up(&self.recovered_total, state.recovery.recoveries());
        self.cache.refresh(state.sm.mvcc().cache_stats());
        if let Some(w) = &state.wal {
            self.wal.refresh(w);
        }
        if let Some(w) = &state.write_behind {
            self.wal.refresh_write_behind(w);
        }
        if let Some(r) = &state.read_index {
            let s = r.stats();
            for (path, n) in [("sole_voter", &s.sole_voter), ("quorum", &s.quorum), ("raft", &s.raft)] {
                catch_up(
                    &self.read_index_total.get_or_create(&vec![("path".to_string(), path.to_string())]),
                    n.load(Ordering::Relaxed),
                );
            }
        }
        if let Some(p) = &state.proposer {
            let s = p.stats();
            catch_up(&self.proposal_batches_total, s.batches.load(Ordering::Relaxed));
            catch_up(&self.proposals_batched_total, s.batched.load(Ordering::Relaxed));
            catch_up(&self.proposals_single_total, s.single.load(Ordering::Relaxed));
        }
        let l = state.lessor.stats();
        for (path, n) in [("ram", &l.renewed_in_ram), ("raft", &l.proposed)] {
            catch_up(
                &self.lease_renewals_total.get_or_create(&vec![("path".to_string(), path.to_string())]),
                n.load(Ordering::Relaxed),
            );
        }
        catch_up(&self.lease_promotions_total, l.promotions.load(Ordering::Relaxed));
        self.recovered_revision.set(
            state
                .recovery
                .active()
                .map(|r| r.backup_revision)
                .unwrap_or(0),
        );
        let cur = state.sm.mvcc().current_revision().await;
        self.current_revision.set(cur);
        let comp = state.sm.mvcc().compact_revision().await;
        self.compact_revision.set(comp);
        self.auth_diverged.set(state.auth_gate.diverged(&state.auth) as i64);
        self.log_has_batched.set(state.sm.mvcc().has_batched() as i64);
        self.members_unable_to_read_batches
            .set(state.older_members.load(std::sync::atomic::Ordering::Relaxed) as i64);
        if let Ok(size) = state.sm.mvcc().engine().size_on_disk().await {
            self.db_size_bytes.set(size as i64);
        }
        // Occupancy of the data volume (fastetcd#14). Sampling is cheap
        // — a file size, a directory scan and a statvfs — so it runs on
        // the scrape like everything else here. `db_size_in_use` is the
        // one expensive number and comes from its own cache.
        if state.space.is_enabled() {
            let stats = state.space.clone().refresh(state).await;
            self.snapshot_size_bytes.set(stats.snapshot_bytes as i64);
            self.disk_total_bytes.set(stats.fs_total_bytes as i64);
            self.disk_available_bytes
                .set(stats.fs_available_bytes as i64);
            self.quota_backend_bytes.set(stats.capacity_bytes as i64);
            self.space_used_ratio.set(stats.used_ratio());
            self.nospace_alarm.set(if stats.nospace { 1 } else { 0 });
            // Live bytes come from the space monitor's cache — the
            // measurement is O(database) and must not run on a scrape.
            self.db_size_in_use_bytes.set(stats.db_in_use_bytes as i64);
        }
    }
}

/// Buckets of `fastetcd_mvcc_get_duration_seconds`: 25 µs to 2.5 s, so
/// a RAM hit and a disk seek land in different buckets.
const GET_BUCKETS: [f64; 16] = [
    0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1,
    0.25, 0.5, 1.0, 2.5,
];

fn get_histogram() -> Histogram {
    Histogram::new(GET_BUCKETS.iter().copied())
}

type HistogramFamily = Family<Vec<(String, String)>, Histogram, fn() -> Histogram>;

/// The RAM cache's metrics (fastetcd#82).
pub struct CacheMetrics {
    hits: Counter,
    misses: Counter,
    evictions: Counter,
    bytes: Gauge,
    entries: Gauge,
    budget: Gauge,
    index_keys: Gauge,
    index_bytes: Gauge,
    get_duration: HistogramFamily,
}

impl CacheMetrics {
    fn new() -> Self {
        Self {
            hits: Counter::default(),
            misses: Counter::default(),
            evictions: Counter::default(),
            bytes: Gauge::default(),
            entries: Gauge::default(),
            budget: Gauge::default(),
            index_keys: Gauge::default(),
            index_bytes: Gauge::default(),
            get_duration: Family::new_with_constructor(get_histogram as fn() -> Histogram),
        }
    }

    fn register(&self, reg: &mut Registry) {
        reg.register(
            "fastetcd_value_cache_hits",
            "Record lookups the value cache answered",
            self.hits.clone(),
        );
        reg.register(
            "fastetcd_value_cache_misses",
            "Record lookups the value cache could not answer (read from the engine)",
            self.misses.clone(),
        );
        reg.register(
            "fastetcd_value_cache_evictions",
            "Entries evicted from the value cache to stay inside its budget",
            self.evictions.clone(),
        );
        reg.register(
            "fastetcd_value_cache_bytes",
            "Bytes the value cache holds, entry overhead included",
            self.bytes.clone(),
        );
        reg.register(
            "fastetcd_value_cache_entries",
            "Records the value cache holds",
            self.entries.clone(),
        );
        reg.register(
            "fastetcd_value_cache_budget_bytes",
            "The value cache's byte budget (--value-cache-bytes); 0 = off",
            self.budget.clone(),
        );
        reg.register(
            "fastetcd_key_index_keys",
            "Keys in the resident key index (every key with history)",
            self.index_keys.clone(),
        );
        reg.register(
            "fastetcd_key_index_bytes",
            "Approximate bytes of the resident key index",
            self.index_bytes.clone(),
        );
        reg.register(
            "fastetcd_mvcc_get_duration_seconds",
            "Store time of a single-key Range; cache=hit when it read nothing from the engine",
            self.get_duration.clone(),
        );
    }

    fn refresh(&self, s: fastetcd_storage::mvcc::CacheStats) {
        catch_up(&self.hits, s.value_hits);
        catch_up(&self.misses, s.value_misses);
        catch_up(&self.evictions, s.value_evictions);
        self.bytes.set(s.value_bytes as i64);
        self.entries.set(s.value_entries as i64);
        self.budget.set(s.value_budget_bytes as i64);
        self.index_keys.set(s.index_keys as i64);
        self.index_bytes.set(s.index_bytes as i64);
    }

    fn observer(&self) -> fastetcd_storage::mvcc::store::GetObserver {
        let label = |v: &str| vec![("cache".to_string(), v.to_string())];
        let hit = self.get_duration.get_or_create(&label("hit")).clone();
        let miss = self.get_duration.get_or_create(&label("miss")).clone();
        Arc::new(move |from_ram, took| {
            let h = if from_ram { &hit } else { &miss };
            h.observe(took.as_secs_f64());
        })
    }
}

/// The raft WAL and the data file's background checkpoints (#85).
pub struct WalMetrics {
    fsyncs: Counter,
    fsync_seconds: Counter<f64, AtomicU64>,
    entries_synced: Counter,
    proposals_synced: Counter,
    checkpoint_pace_seconds: Gauge<f64, AtomicU64>,
    fsync_max_seconds: Gauge<f64, AtomicU64>,
    checkpoint_max_seconds: Gauge<f64, AtomicU64>,
    checkpoint_commit_seconds: Counter<f64, AtomicU64>,
    checkpoint_commit_max_seconds: Gauge<f64, AtomicU64>,
    bytes_appended: Counter,
    segments: Gauge,
    cached_bytes: Gauge,
    checkpoints: Counter,
    checkpoint_seconds: Counter<f64, AtomicU64>,
    checkpoint_failures: Counter,
    durable_applied: Gauge,
    wb_bytes: Gauge,
    wb_layers: Gauge,
    wb_flushes: Counter,
    wb_flush_seconds: Counter<f64, AtomicU64>,
    wb_backpressure: Counter,
    wb_backpressure_seconds: Counter<f64, AtomicU64>,
    fsync_inflight_seconds: Gauge<f64, AtomicU64>,
    checkpoint_inflight_seconds: Gauge<f64, AtomicU64>,
    wb_presync_seconds: Counter<f64, AtomicU64>,
}

impl WalMetrics {
    fn new() -> Self {
        Self {
            fsyncs: Counter::default(),
            fsync_seconds: Counter::default(),
            entries_synced: Counter::default(),
            proposals_synced: Counter::default(),
            checkpoint_pace_seconds: Gauge::default(),
            fsync_max_seconds: Gauge::default(),
            checkpoint_max_seconds: Gauge::default(),
            checkpoint_commit_seconds: Counter::default(),
            checkpoint_commit_max_seconds: Gauge::default(),
            bytes_appended: Counter::default(),
            segments: Gauge::default(),
            cached_bytes: Gauge::default(),
            checkpoints: Counter::default(),
            checkpoint_seconds: Counter::default(),
            checkpoint_failures: Counter::default(),
            durable_applied: Gauge::default(),
            wb_bytes: Gauge::default(),
            wb_layers: Gauge::default(),
            wb_flushes: Counter::default(),
            wb_flush_seconds: Counter::default(),
            wb_backpressure: Counter::default(),
            wb_backpressure_seconds: Counter::default(),
            fsync_inflight_seconds: Gauge::default(),
            checkpoint_inflight_seconds: Gauge::default(),
            wb_presync_seconds: Counter::default(),
        }
    }

    fn register(&self, reg: &mut Registry) {
        reg.register(
            "fastetcd_wal_fsyncs",
            "fdatasyncs of the raft WAL (each covers every write queued meanwhile)",
            self.fsyncs.clone(),
        );
        reg.register(
            "fastetcd_wal_fsync_seconds",
            "Time spent in raft WAL fdatasyncs",
            self.fsync_seconds.clone(),
        );
        reg.register(
            "fastetcd_wal_entries_synced",
            "Raft log entries made durable by WAL fdatasyncs (per fsync: divide by fastetcd_wal_fsyncs_total)",
            self.entries_synced.clone(),
        );
        reg.register(
            "fastetcd_wal_proposals_synced",
            "Client proposals in the raft log entries made durable (a batch entry holds several)",
            self.proposals_synced.clone(),
        );
        reg.register(
            "fastetcd_checkpoint_pace_seconds",
            "Least time after the last checkpoint before the next may start (4x its duration)",
            self.checkpoint_pace_seconds.clone(),
        );
        reg.register(
            "fastetcd_wal_fsync_max_seconds",
            "Longest raft WAL fdatasync since the process started",
            self.fsync_max_seconds.clone(),
        );
        reg.register(
            "fastetcd_checkpoint_max_seconds",
            "Longest background checkpoint of the data file since the process started",
            self.checkpoint_max_seconds.clone(),
        );
        reg.register(
            "fastetcd_checkpoint_commit_seconds",
            "Time checkpoints held the data file's writer (applies wait behind it)",
            self.checkpoint_commit_seconds.clone(),
        );
        reg.register(
            "fastetcd_checkpoint_commit_max_seconds",
            "Longest time one checkpoint held the data file's writer",
            self.checkpoint_commit_max_seconds.clone(),
        );
        reg.register(
            "fastetcd_wal_bytes_appended",
            "Bytes appended to the raft WAL",
            self.bytes_appended.clone(),
        );
        reg.register("fastetcd_wal_segments", "Raft WAL segment files", self.segments.clone());
        reg.register(
            "fastetcd_wal_cached_bytes",
            "Recent raft log entries held in RAM (--wal-cache-bytes)",
            self.cached_bytes.clone(),
        );
        reg.register(
            "fastetcd_checkpoints",
            "Durable commits of the data file made in the background",
            self.checkpoints.clone(),
        );
        reg.register(
            "fastetcd_checkpoint_seconds",
            "Time spent in background checkpoints of the data file",
            self.checkpoint_seconds.clone(),
        );
        reg.register(
            "fastetcd_checkpoint_failures",
            "Background checkpoints of the data file that failed",
            self.checkpoint_failures.clone(),
        );
        reg.register(
            "fastetcd_write_behind_bytes",
            "Applied writes held in RAM, not yet written into the data file",
            self.wb_bytes.clone(),
        );
        reg.register(
            "fastetcd_write_behind_batches",
            "Applied batches held in RAM, not yet written into the data file",
            self.wb_layers.clone(),
        );
        reg.register(
            "fastetcd_write_behind_flushes",
            "Writes of the held batches into the data file",
            self.wb_flushes.clone(),
        );
        reg.register(
            "fastetcd_write_behind_flush_seconds",
            "Time spent writing held batches into the data file (readers wait on this)",
            self.wb_flush_seconds.clone(),
        );
        reg.register(
            "fastetcd_write_behind_backpressure",
            "Applies that had to write the held batches out first (over --write-behind-bytes)",
            self.wb_backpressure.clone(),
        );
        reg.register(
            "fastetcd_wal_fsync_inflight_seconds",
            "How long the raft WAL fdatasync in flight has been running (0: none); every write waits on it (fastetcd#138)",
            self.fsync_inflight_seconds.clone(),
        );
        reg.register(
            "fastetcd_checkpoint_inflight_seconds",
            "How long the checkpoint of the data file in flight has been running (0: none)",
            self.checkpoint_inflight_seconds.clone(),
        );
        reg.register(
            "fastetcd_write_behind_backpressure_seconds",
            "Time those applies waited for their write-out",
            self.wb_backpressure_seconds.clone(),
        );
        reg.register(
            "fastetcd_write_behind_presync_seconds",
            "Checkpoints' fsyncs of the data file, run outside the flush lock (fastetcd#93)",
            self.wb_presync_seconds.clone(),
        );
        reg.register(
            "fastetcd_checkpoint_durable_applied_index",
            "Applied raft index (+1) the last checkpoint made durable in the data file",
            self.durable_applied.clone(),
        );
    }

    fn refresh(&self, s: &fastetcd_raft::wal_log_store::WalStats) {
        let load = |a: &AtomicU64| a.load(Ordering::Relaxed);
        catch_up(&self.fsyncs, load(&s.fsyncs));
        catch_up_seconds(&self.fsync_seconds, load(&s.fsync_nanos));
        catch_up(&self.entries_synced, load(&s.entries_synced));
        catch_up(&self.proposals_synced, load(&s.proposals_synced));
        self.checkpoint_pace_seconds.set(load(&s.checkpoint_pace_nanos) as f64 / 1e9);
        self.fsync_max_seconds.set(load(&s.fsync_max_nanos) as f64 / 1e9);
        self.checkpoint_max_seconds.set(load(&s.checkpoint_max_nanos) as f64 / 1e9);
        catch_up_seconds(&self.checkpoint_commit_seconds, load(&s.checkpoint_commit_nanos));
        self.checkpoint_commit_max_seconds
            .set(load(&s.checkpoint_commit_max_nanos) as f64 / 1e9);
        catch_up(&self.bytes_appended, load(&s.bytes_appended));
        self.segments.set(load(&s.segments) as i64);
        self.cached_bytes.set(load(&s.cached_bytes) as i64);
        catch_up(&self.checkpoints, load(&s.checkpoints));
        catch_up_seconds(&self.checkpoint_seconds, load(&s.checkpoint_nanos));
        catch_up(&self.checkpoint_failures, load(&s.checkpoint_failures));
        self.durable_applied.set(load(&s.durable_applied) as i64);
        self.fsync_inflight_seconds.set(s.fsync_inflight().map_or(0.0, |d| d.as_secs_f64()));
        self.checkpoint_inflight_seconds.set(s.checkpoint_inflight().map_or(0.0, |d| d.as_secs_f64()));
    }
}

impl WalMetrics {
    fn refresh_write_behind(&self, s: &fastetcd_storage::write_behind::WriteBehindStats) {
        let load = |a: &AtomicU64| a.load(Ordering::Relaxed);
        self.wb_bytes.set(load(&s.layer_bytes) as i64);
        self.wb_layers.set(load(&s.layers) as i64);
        catch_up(&self.wb_flushes, load(&s.flushes));
        catch_up_seconds(&self.wb_flush_seconds, load(&s.flush_nanos));
        catch_up(&self.wb_backpressure, load(&s.backpressure));
        catch_up_seconds(&self.wb_backpressure_seconds, load(&s.backpressure_nanos));
        catch_up_seconds(&self.wb_presync_seconds, load(&s.presync_nanos));
    }
}

/// [`catch_up`] for a seconds counter kept elsewhere in nanoseconds.
fn catch_up_seconds(counter: &Counter<f64, AtomicU64>, total_nanos: u64) {
    let total = total_nanos as f64 / 1e9;
    let counted = counter.get();
    if total > counted {
        counter.inc_by(total - counted);
    }
}

/// Bring a counter up to `total`, a count kept elsewhere. Callers hold
/// the registry lock, so two scrapes cannot both add the same increase.
fn catch_up(counter: &Counter, total: u64) {
    let counted = counter.get();
    if total > counted {
        counter.inc_by(total - counted);
    }
}

/// Spawn a minimal hyper HTTP/1 server on `addr` that serves the
/// Prometheus exposition text on `GET /metrics`.
pub fn spawn_server(addr: SocketAddr, metrics: Arc<Metrics>, state: Arc<ServerState>) {
    tokio::spawn(async move {
        metrics.attach(&state).await;
        let listener = match TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(target: "fastetcd::metrics", "bind {addr}: {e}");
                return;
            }
        };
        tracing::info!(target: "fastetcd::metrics", %addr, "serving /metrics");
        loop {
            let (stream, _peer) = match listener.accept().await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(target: "fastetcd::metrics", "accept: {e}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            let metrics = metrics.clone();
            let state = state.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                if let Err(e) = http1::Builder::new()
                    .serve_connection(
                        io,
                        service_fn(move |req| handle(req, metrics.clone(), state.clone())),
                    )
                    .await
                {
                    tracing::debug!(target: "fastetcd::metrics", "conn: {e}");
                }
            });
        }
    });
}

async fn handle(
    req: Request<hyper::body::Incoming>,
    metrics: Arc<Metrics>,
    state: Arc<ServerState>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = req.uri().path();
    if path != "/metrics" && path != "/" {
        let body = Bytes::from_static(b"not found\n");
        let mut r = Response::new(Full::new(body));
        *r.status_mut() = StatusCode::NOT_FOUND;
        return Ok(r);
    }
    // Refresh under the registry lock: scrapes take turns.
    let reg = metrics.registry.lock().await;
    metrics.refresh(&state).await;
    let mut buf = String::new();
    if let Err(e) = encode(&mut buf, &reg) {
        let body = Bytes::from(format!("encode error: {e}\n"));
        let mut r = Response::new(Full::new(body));
        *r.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
        return Ok(r);
    }
    drop(reg);
    let _ = req
        .into_body()
        .collect()
        .await
        .map(|_| ())
        .map_err(|_| ());
    let resp = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/plain; version=0.0.4")
        .body(Full::new(Bytes::from(buf)))
        .expect("response build");
    Ok(resp)
}
