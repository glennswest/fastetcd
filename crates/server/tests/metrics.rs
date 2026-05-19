//! Smoke test: the metrics server returns Prometheus text with our
//! expected etcd-compatible metric names.

mod common;
use common::start_test_server_full;

use std::sync::Arc;

#[tokio::test]
async fn metrics_endpoint_exposes_etcd_compatible_names() {
    let h = start_test_server_full().await;

    let m = fastetcd_server::metrics::Metrics::new();
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    // Bind ourselves so we can capture the actual port.
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let bound = listener.local_addr().unwrap();
    drop(listener);
    fastetcd_server::metrics::spawn_server(bound, m, Arc::new(reconstruct(&h)).into());

    // Wait briefly for the server to come up.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let url = format!("http://{}/metrics", bound);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /metrics");
    assert!(resp.status().is_success(), "status: {}", resp.status());
    let body = resp.text().await.unwrap();
    for name in [
        "etcd_server_has_leader",
        "etcd_server_leader_changes_seen_total",
        "etcd_mvcc_db_total_size_in_bytes",
        "etcd_debugging_mvcc_current_revision",
        "etcd_debugging_mvcc_compact_revision",
    ] {
        assert!(body.contains(name), "missing metric {name} in body");
    }
}

// Pull the inner state out of the test handles. ServerState isn't
// `Default` so we re-use the one the harness already built.
fn reconstruct(h: &common::TestServerHandles) -> fastetcd_server::ServerState {
    (*h.state).clone()
}
