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
//!   - `fastetcd_watch_resyncs_total` / `fastetcd_watch_lag_cancels_total`
//!     (counters, process-wide): watchers caught up from history, and
//!     watchers cancelled because that history was gone (#16)
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
    pub is_leader: Gauge,
    pub proposals_committed: Gauge,
    pub proposals_applied: Gauge,
    pub put_total: Counter,
    pub delete_total: Counter,
    pub range_total: Counter,
    pub txn_total: Counter,
    pub watch_resyncs_total: Counter,
    pub watch_lag_cancels_total: Counter,
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
        let is_leader = Gauge::default();
        let proposals_committed = Gauge::default();
        let proposals_applied = Gauge::default();
        let put_total = Counter::default();
        let delete_total = Counter::default();
        let range_total = Counter::default();
        let txn_total = Counter::default();
        let watch_resyncs_total = Counter::default();
        let watch_lag_cancels_total = Counter::default();
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
            is_leader: is_leader.clone(),
            proposals_committed: proposals_committed.clone(),
            proposals_applied: proposals_applied.clone(),
            put_total: put_total.clone(),
            delete_total: delete_total.clone(),
            range_total: range_total.clone(),
            txn_total: txn_total.clone(),
            watch_resyncs_total: watch_resyncs_total.clone(),
            watch_lag_cancels_total: watch_lag_cancels_total.clone(),
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
    }

    /// Refresh gauges from live server state. Counters are not
    /// refreshed here — they're updated only on observed edges.
    pub async fn refresh(&self, state: &ServerState) {
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
