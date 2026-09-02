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
//!   - `etcd_debugging_mvcc_current_revision` (gauge)
//!   - `etcd_debugging_mvcc_compact_revision` (gauge)
//!   - `fastetcd_engine` (info: redb / wal / iouring)
//!
//! Metrics are refreshed lazily on every scrape — no background
//! task — so we always report the current truth.

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
use prometheus_client::metrics::gauge::Gauge;
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
    pub current_revision: Gauge,
    pub compact_revision: Gauge,
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
        let current_revision = Gauge::default();
        let compact_revision = Gauge::default();
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
            current_revision: current_revision.clone(),
            compact_revision: compact_revision.clone(),
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
        }
        m
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
        let cur = state.sm.mvcc().current_revision().await;
        self.current_revision.set(cur);
        let comp = state.sm.mvcc().compact_revision().await;
        self.compact_revision.set(comp);
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
            self.db_size_in_use_bytes.set(stats.db_in_use_bytes as i64);
        }
    }
}

/// Spawn a minimal hyper HTTP/1 server on `addr` that serves the
/// Prometheus exposition text on `GET /metrics`.
pub fn spawn_server(addr: SocketAddr, metrics: Arc<Metrics>, state: Arc<ServerState>) {
    tokio::spawn(async move {
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
    metrics.refresh(&state).await;
    let mut buf = String::new();
    let reg = metrics.registry.lock().await;
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
