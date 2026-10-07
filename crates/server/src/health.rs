//! etcd's HTTP health routes on the client port, with etcd's checks
//! (fastetcd#69; etcd release-3.5 `server/etcdserver/api/etcdhttp/health.go`).
//! They used to answer healthy whatever the member's state.
//!
//! - `GET /health[?serializable=true][&exclude=NOSPACE&exclude=CORRUPT]`:
//!   an alarm on this member → 503 `{"health":"false","reason":"ALARM
//!   NOSPACE"}` (or `CORRUPT`); unless `serializable=true`, no leader →
//!   `RAFT NO LEADER`; then a keys-only Range, linearizable unless
//!   `serializable=true`, within etcd's request timeout (5 s + 2x the
//!   election timeout) → `RANGE ERROR:…`. Healthy: 200
//!   `{"health":"true","reason":""}`.
//! - `GET /livez`: `serializable_read`. `GET /readyz`: `data_corruption`
//!   (a CORRUPT alarm), `serializable_read`, `linearizable_read`,
//!   `non_learner`. Each check also at `/livez/<name>`, `/readyz/<name>`;
//!   `?exclude=<name>` drops one; `?verbose` lists them. 503 with
//!   `[-]<name> failed: <why>` lines when one fails, else `ok`.
//!
//! Alarms are this member's: fastetcd's NOSPACE and CORRUPT are raised
//! per member, not replicated as etcd's are. CORRUPT here means the store
//! was restored from a backup (#37): the member serves, but /health and
//! /readyz say false until it is disarmed, as etcd's do; a probe that
//! should keep routing to it excludes it.
//!
//! `grpc.health.v1` follows `/readyz` ([`spawn_grpc_health`]).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{RawQuery, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::state::ServerState;

/// etcd's request timeout: 5 s plus twice the election timeout.
fn request_timeout(state: &ServerState) -> Duration {
    Duration::from_secs(5) + Duration::from_millis(2 * state.raft.config().election_timeout_max)
}

/// Query parameters as etcd reads them: `exclude` may repeat.
#[derive(Default)]
struct Query {
    exclude: HashSet<String>,
    serializable: bool,
    verbose: bool,
}

fn parse_query(raw: Option<String>) -> Query {
    let mut q = Query::default();
    for pair in raw.as_deref().unwrap_or("").split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        let v = percent_decode(v);
        match k {
            "exclude" if !v.is_empty() => {
                q.exclude.insert(v);
            }
            "serializable" => q.serializable = v == "true",
            "verbose" => q.verbose = true,
            _ => {}
        }
    }
    q
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' if i + 2 < b.len() => match u8::from_str_radix(&s[i + 1..i + 3], 16) {
                Ok(v) => {
                    out.push(v);
                    i += 3;
                    continue;
                }
                Err(_) => out.push(b'%'),
            },
            b'+' => out.push(b' '),
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// This member's raised alarms, by etcd's names.
fn alarms(state: &ServerState) -> Vec<&'static str> {
    let mut out = Vec::new();
    if state.space.nospace() {
        out.push("NOSPACE");
    }
    if state.recovery.active().is_some() {
        out.push("CORRUPT");
    }
    out
}

fn has_leader(state: &ServerState) -> bool {
    state.raft.metrics().borrow().current_leader.is_some()
}

/// A keys-only, one-key Range, as etcd's health read: linearizable (the
/// read barrier, forwarded to the leader by a follower) unless
/// `serializable`, within etcd's request timeout.
async fn read(state: &ServerState, serializable: bool) -> Result<(), String> {
    let run = async {
        if !serializable {
            let probe = fastetcd_raft::ForwardedRead {
                key: b"\0".to_vec(),
                range_end: Vec::new(),
                limit: 1,
                revision: 0,
                keys_only: true,
                count_only: false,
            };
            if state.linearize_read(&probe).await.map_err(|s| s.message().to_string())?.is_some() {
                return Ok(());
            }
        }
        state
            .sm
            .mvcc()
            .range(b"\0", b"", 1, 0, true, false)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    };
    match tokio::time::timeout(request_timeout(state), run).await {
        Ok(r) => r,
        Err(_) => Err("context deadline exceeded".to_string()),
    }
}

/// `/health`'s verdict: `Ok(())`, or the reason.
pub async fn health(state: &ServerState, exclude: &HashSet<String>, serializable: bool) -> Result<(), String> {
    if let Some(alarm) = alarms(state).into_iter().find(|a| !exclude.contains(*a)) {
        return Err(format!("ALARM {alarm}"));
    }
    if !serializable && !has_leader(state) {
        return Err("RAFT NO LEADER".to_string());
    }
    read(state, serializable).await.map_err(|e| format!("RANGE ERROR:{e}"))
}

const LIVEZ: &[&str] = &["serializable_read"];
const READYZ: &[&str] = &["data_corruption", "serializable_read", "linearizable_read", "non_learner"];

async fn check(state: &ServerState, name: &str) -> Result<(), String> {
    match name {
        "data_corruption" => {
            if alarms(state).contains(&"CORRUPT") {
                Err("alarm activated: CORRUPT".to_string())
            } else {
                Ok(())
            }
        }
        "serializable_read" => read(state, true).await,
        "linearizable_read" => read(state, false).await,
        "non_learner" => {
            let m = state.raft.metrics().borrow().clone();
            if m.membership_config.membership().voter_ids().any(|id| id == m.id) {
                Ok(())
            } else {
                Err("not supported for learner".to_string())
            }
        }
        _ => Ok(()),
    }
}

/// Run `names` in order: whether all passed, and etcd's per-check lines.
pub async fn run_checks(state: &ServerState, names: &[&str]) -> (bool, String) {
    let mut ok = true;
    let mut out = String::new();
    for name in names {
        match check(state, name).await {
            Ok(()) => out.push_str(&format!("[+]{name} ok\n")),
            Err(e) => {
                ok = false;
                out.push_str(&format!("[-]{name} failed: {e}\n"));
            }
        }
    }
    (ok, out)
}

fn text(status: StatusCode, body: String) -> Response {
    (status, [(header::CONTENT_TYPE, "text/plain; charset=utf-8")], body).into_response()
}

/// `GET /health`.
pub async fn health_handler(State(state): State<Arc<ServerState>>, RawQuery(raw): RawQuery) -> Response {
    let q = parse_query(raw);
    let verdict = health(&state, &q.exclude, q.serializable).await;
    let (status, body) = match &verdict {
        Ok(()) => {
            state.traffic.health_success.inc();
            (StatusCode::OK, r#"{"health":"true","reason":""}"#.to_string())
        }
        Err(reason) => {
            state.traffic.health_failures.inc();
            tracing::warn!(target: "fastetcd::health", %reason, "serving /health false");
            let body = serde_json::json!({"health": "false", "reason": reason}).to_string();
            // http.Error in etcd: the body and a newline.
            (StatusCode::SERVICE_UNAVAILABLE, format!("{body}\n"))
        }
    };
    let ctype = if verdict.is_ok() { "application/json" } else { "text/plain; charset=utf-8" };
    (status, [(header::CONTENT_TYPE, ctype)], body).into_response()
}

async fn checks_response(state: &ServerState, path: &str, names: Vec<&str>, raw: Option<String>) -> Response {
    let q = parse_query(raw);
    let names: Vec<&str> = names.into_iter().filter(|n| !q.exclude.contains(*n)).collect();
    let (ok, detail) = run_checks(state, &names).await;
    if !ok {
        tracing::warn!(target: "fastetcd::health", path, reason = %detail.trim_end(), "health check failed");
        return text(StatusCode::SERVICE_UNAVAILABLE, detail);
    }
    text(StatusCode::OK, if q.verbose { format!("{detail}ok\n") } else { "ok\n".to_string() })
}

/// `GET /livez`.
pub async fn livez_handler(State(state): State<Arc<ServerState>>, RawQuery(raw): RawQuery) -> Response {
    checks_response(&state, "/livez", LIVEZ.to_vec(), raw).await
}

/// `GET /readyz`.
pub async fn readyz_handler(State(state): State<Arc<ServerState>>, RawQuery(raw): RawQuery) -> Response {
    checks_response(&state, "/readyz", READYZ.to_vec(), raw).await
}

/// The routes, on the client port's axum router: `/health`, `/livez`,
/// `/readyz`, and one route per check (`/readyz/linearizable_read`, ...).
pub fn router(state: Arc<ServerState>) -> axum::Router {
    let mut r = axum::Router::new()
        .route("/health", axum::routing::get(health_handler))
        .route("/livez", axum::routing::get(livez_handler))
        .route("/readyz", axum::routing::get(readyz_handler));
    for (kind, names) in [("livez", LIVEZ), ("readyz", READYZ)] {
        for name in names {
            let path = format!("/{kind}/{name}");
            let route = path.clone();
            r = r.route(
                &path,
                axum::routing::get(move |State(state): State<Arc<ServerState>>, RawQuery(raw): RawQuery| {
                    let route = route.clone();
                    async move { checks_response(&state, &route, vec![*name], raw).await }
                }),
            );
        }
    }
    r.with_state(state)
}

/// Keep `grpc.health.v1` in step with `/readyz`: every second, every
/// served service (and the server as a whole, "") is SERVING while
/// /readyz passes, NOT_SERVING otherwise.
pub fn spawn_grpc_health(
    state: Arc<ServerState>,
    mut reporter: tonic_health::server::HealthReporter,
    services: Vec<String>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last: Option<bool> = None;
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let (ok, detail) = run_checks(&state, READYZ).await;
            if last == Some(ok) {
                continue;
            }
            let status = if ok {
                tonic_health::ServingStatus::Serving
            } else {
                tracing::warn!(target: "fastetcd::health", reason = %detail.trim_end(), "grpc.health.v1: NOT_SERVING");
                tonic_health::ServingStatus::NotServing
            };
            reporter.set_service_status("", status).await;
            for s in &services {
                reporter.set_service_status(s, status).await;
            }
            last = Some(ok);
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queries_parse_as_etcd_reads_them() {
        let q = parse_query(Some("exclude=NOSPACE&exclude=CORRUPT&serializable=true&verbose".into()));
        assert!(q.serializable && q.verbose);
        assert_eq!(q.exclude, ["NOSPACE", "CORRUPT"].iter().map(|s| s.to_string()).collect());
        let q = parse_query(Some("serializable=false&exclude=".into()));
        assert!(!q.serializable && q.exclude.is_empty());
        assert_eq!(percent_decode("a%20b+c"), "a b c");
        assert!(parse_query(None).exclude.is_empty());
    }
}
