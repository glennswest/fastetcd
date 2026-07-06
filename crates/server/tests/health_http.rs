//! Regression test for #5: the client port must answer etcd's
//! plain-HTTP health probes (`GET /health`, `/livez`, `/readyz`)
//! alongside gRPC traffic on the same port. Spawns the real
//! `fastetcd` binary since the routing is wired directly in
//! `main()`, not behind a separately testable function.

use std::time::Duration;

async fn pick_free_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    port
}

#[tokio::test]
async fn client_port_serves_http_health_alongside_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let client_port = pick_free_port().await;
    let peer_port = pick_free_port().await;

    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_fastetcd"))
        .arg("--data-dir")
        .arg(dir.path())
        .arg("--listen-client-urls")
        .arg(format!("http://127.0.0.1:{client_port}"))
        .arg("--listen-peer-urls")
        .arg(format!("http://127.0.0.1:{peer_port}"))
        .arg("--listen-metrics-url")
        .arg("")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn fastetcd");

    let base = format!("http://127.0.0.1:{client_port}");
    let client = reqwest::Client::new();

    // Poll for the listener to come up — under a loaded test runner
    // (many test binaries running in parallel) a fixed short sleep
    // flakes.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let health = loop {
        match client.get(format!("{base}/health")).send().await {
            Ok(resp) => break resp,
            Err(e) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let _ = e;
            }
            Err(e) => panic!("GET /health did not come up in time: {e}"),
        }
    };
    assert!(health.status().is_success(), "status: {}", health.status());
    let body = health.text().await.unwrap();
    assert_eq!(body, r#"{"health":"true"}"#);

    for path in ["/livez", "/readyz"] {
        let resp = client
            .get(format!("{base}{path}"))
            .send()
            .await
            .unwrap_or_else(|e| panic!("GET {path}: {e}"));
        assert!(resp.status().is_success(), "{path} status: {}", resp.status());
        assert_eq!(resp.text().await.unwrap(), "ok");
    }

    // gRPC traffic must still work on the same port.
    let mut kv = fastetcd_proto::etcdserverpb::kv_client::KvClient::connect(base.clone())
        .await
        .expect("connect KV client");
    kv.put(fastetcd_proto::etcdserverpb::PutRequest {
        key: b"health-http-smoke".to_vec(),
        value: b"ok".to_vec(),
        ..Default::default()
    })
    .await
    .expect("PUT over gRPC on the same port");

    child.kill().await.ok();
}
