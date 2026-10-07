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
    fastetcd_server::metrics::spawn_server(bound, m, Arc::new(reconstruct(&h)));

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
        // etcd's (Go's) process metrics (#84).
        "process_resident_memory_bytes",
        "process_virtual_memory_bytes",
        "process_cpu_seconds_total",
        "process_start_time_seconds",
        "process_open_fds",
        "process_max_fds",
    ] {
        assert!(body.contains(name), "missing metric {name} in body");
    }
    let rss: f64 = body
        .lines()
        .find_map(|l| l.strip_prefix("process_resident_memory_bytes "))
        .expect("an RSS sample")
        .parse()
        .unwrap();
    assert!(rss > 1e6, "RSS {rss}");
}

// Pull the inner state out of the test handles. ServerState isn't
// `Default` so we re-use the one the harness already built.
fn reconstruct(h: &common::TestServerHandles) -> fastetcd_server::ServerState {
    (*h.state).clone()
}

// ---------- traffic (fastetcd#29) ----------

use std::time::Duration;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::kv_server::KvServer;
use fastetcd_proto::etcdserverpb::watch_client::WatchClient;
use fastetcd_proto::etcdserverpb::watch_server::WatchServer;
use fastetcd_server::auth::AuthInterceptor;
use fastetcd_server::kv::KvService;
use fastetcd_server::watch::WatchService;
use fastetcd_server::ServerState;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

/// A client port built as main.rs builds it: tonic routes → axum, with
/// the gRPC call counters layered over them.
async fn client_port(state: Arc<ServerState>) -> String {
    let interceptor = AuthInterceptor::new(state.auth.clone());
    let mut routes = tonic::service::Routes::builder();
    routes.add_service(KvServer::with_interceptor(KvService::new(state.clone()), interceptor.clone()));
    routes.add_service(WatchServer::with_interceptor(WatchService::new(state.clone()), interceptor));
    let app: axum::Router = routes.routes().into_axum_router().layer(
        axum::middleware::from_fn_with_state(
            state.traffic.clone(),
            fastetcd_server::traffic::grpc_middleware,
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .accept_http1(true)
            .add_routes(tonic::service::Routes::from(app))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    endpoint
}

/// Start `/metrics` for `state`; returns its URL.
async fn metrics_url(state: Arc<ServerState>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound = listener.local_addr().unwrap();
    drop(listener);
    fastetcd_server::metrics::spawn_server(bound, fastetcd_server::metrics::Metrics::new(), state);
    let url = format!("http://{bound}/metrics");
    for _ in 0..50 {
        if reqwest::get(&url).await.is_ok() {
            return url;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("/metrics did not come up");
}

async fn scrape(url: &str) -> String {
    reqwest::get(url).await.unwrap().text().await.unwrap()
}

/// The value of one series (`name` or `name{labels}`, exactly as
/// exposed), or `None` if it is not there.
fn value(body: &str, series: &str) -> Option<f64> {
    body.lines()
        .find_map(|l| l.strip_prefix(series)?.strip_prefix(' '))
        .map(|v| v.trim().parse().unwrap())
}

fn must(body: &str, series: &str) -> f64 {
    value(body, series).unwrap_or_else(|| panic!("no series {series} in:\n{body}"))
}

fn handled(service: &str, method: &str, grpc_type: &str, code: &str) -> String {
    format!(
        "grpc_server_handled_total{{grpc_type=\"{grpc_type}\",grpc_service=\"etcdserverpb.{service}\",\
         grpc_method=\"{method}\",grpc_code=\"{code}\"}}"
    )
}

fn started(service: &str, method: &str, grpc_type: &str) -> String {
    format!(
        "grpc_server_started_total{{grpc_type=\"{grpc_type}\",grpc_service=\"etcdserverpb.{service}\",\
         grpc_method=\"{method}\"}}"
    )
}

fn create(key: &[u8], range_end: &[u8]) -> pb::WatchRequest {
    pb::WatchRequest {
        request_union: Some(pb::watch_request::RequestUnion::CreateRequest(pb::WatchCreateRequest {
            key: key.to_vec(),
            range_end: range_end.to_vec(),
            ..Default::default()
        })),
    }
}

#[tokio::test]
async fn traffic_is_counted_by_method_and_watches_are_gauged() {
    let h = start_test_server_full().await;
    let endpoint = client_port(h.state.clone()).await;
    let url = metrics_url(h.state.clone()).await;
    let mut kv = KvClient::connect(endpoint.clone()).await.unwrap();

    for i in 0..3 {
        kv.put(pb::PutRequest { key: format!("k{i}").into_bytes(), value: b"v".to_vec(), ..Default::default() })
            .await
            .unwrap();
    }
    kv.delete_range(pb::DeleteRangeRequest { key: b"k0".to_vec(), ..Default::default() })
        .await
        .unwrap();
    for _ in 0..2 {
        kv.range(pb::RangeRequest { key: b"k1".to_vec(), ..Default::default() }).await.unwrap();
    }
    kv.txn(pb::TxnRequest {
        success: vec![pb::RequestOp {
            request: Some(pb::request_op::Request::RequestPut(pb::PutRequest {
                key: b"t".to_vec(),
                value: b"v".to_vec(),
                ..Default::default()
            })),
        }],
        ..Default::default()
    })
    .await
    .unwrap();
    let err = kv
        .range(pb::RangeRequest { key: b"k1".to_vec(), revision: 9999, ..Default::default() })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::OutOfRange);

    let body = scrape(&url).await;
    // gRPC calls by method and code.
    assert_eq!(must(&body, &started("KV", "Put", "unary")), 3.0);
    assert_eq!(must(&body, &handled("KV", "Put", "unary", "OK")), 3.0);
    assert_eq!(must(&body, &handled("KV", "DeleteRange", "unary", "OK")), 1.0);
    assert_eq!(must(&body, &handled("KV", "Range", "unary", "OK")), 2.0);
    assert_eq!(must(&body, &handled("KV", "Range", "unary", "OutOfRange")), 1.0);
    assert_eq!(must(&body, &handled("KV", "Txn", "unary", "OK")), 1.0);
    // Operations the store executed (the txn's put included).
    assert_eq!(must(&body, "etcd_debugging_mvcc_put_total"), 4.0);
    assert_eq!(must(&body, "etcd_debugging_mvcc_delete_total"), 1.0);
    assert_eq!(must(&body, "etcd_debugging_mvcc_txn_total"), 1.0);
    assert!(must(&body, "etcd_debugging_mvcc_range_total") >= 2.0);
    // Who this member is.
    assert_eq!(must(&body, "etcd_server_is_leader"), 1.0);
    assert_eq!(must(&body, "etcd_server_id{server_id=\"1\"}"), 1.0);
    assert_eq!(must(&body, "fastetcd_engine_info{engine=\"redb\"}"), 1.0);
    // Raft keeping up: 5 writes went through the log.
    let applied = must(&body, "etcd_server_proposals_applied_total");
    let committed = must(&body, "etcd_server_proposals_committed_total");
    assert!(applied >= 5.0, "applied {applied}");
    assert!(committed >= applied, "committed {committed} < applied {applied}");
    assert_eq!(must(&body, "etcd_server_proposals_pending"), 0.0);
    assert_eq!(must(&body, "etcd_debugging_mvcc_watch_stream_total"), 0.0);

    // One stream, two watchers.
    let mut watch = WatchClient::connect(endpoint.clone()).await.unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(create(b"a", b"")).await.unwrap();
    tx.send(create(b"b", b"c")).await.unwrap();
    let mut stream = watch.watch(ReceiverStream::new(rx)).await.unwrap().into_inner();
    for _ in 0..2 {
        assert!(stream.next().await.unwrap().unwrap().created);
    }
    let body = scrape(&url).await;
    assert_eq!(must(&body, "etcd_debugging_mvcc_watch_stream_total"), 1.0);
    assert_eq!(must(&body, "etcd_debugging_mvcc_watcher_total"), 2.0);
    assert_eq!(must(&body, "etcd_debugging_mvcc_slow_watcher_total"), 0.0);
    assert_eq!(must(&body, &started("Watch", "Watch", "bidi_stream")), 1.0);

    // The client goes away: the stream, its watchers and the call end.
    drop(stream);
    drop(tx);
    drop(watch);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let body = scrape(&url).await;
        let streams = must(&body, "etcd_debugging_mvcc_watch_stream_total");
        let watchers = must(&body, "etcd_debugging_mvcc_watcher_total");
        let ended = value(&body, &handled("Watch", "Watch", "bidi_stream", "Canceled"));
        if streams == 0.0 && watchers == 0.0 && ended == Some(1.0) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "after the client left: streams {streams}, watchers {watchers}, Watch Canceled {ended:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A watcher whose client stops reading is behind the head, and
/// `etcd_debugging_mvcc_slow_watcher_total` says so until it catches up.
#[tokio::test]
async fn a_watch_whose_client_stops_reading_is_slow() {
    let h = start_test_server_full().await;
    let endpoint = client_port(h.state.clone()).await;
    let url = metrics_url(h.state.clone()).await;

    // Small HTTP/2 windows, so an unread stream backs up quickly.
    let channel = tonic::transport::Endpoint::from_shared(endpoint.clone())
        .unwrap()
        .initial_stream_window_size(65_535)
        .initial_connection_window_size(65_535)
        .connect()
        .await
        .unwrap();
    let mut watch = WatchClient::new(channel);
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(create(b"big/", b"big0")).await.unwrap();
    let mut stream = watch.watch(ReceiverStream::new(rx)).await.unwrap().into_inner();
    assert!(stream.next().await.unwrap().unwrap().created);

    // Write until the unread stream is reported slow.
    let mut kv = KvClient::connect(endpoint.clone()).await.unwrap();
    let value4k = vec![b'x'; 4096];
    let mut slow = false;
    for i in 0..600 {
        kv.put(pb::PutRequest { key: format!("big/{i}").into_bytes(), value: value4k.clone(), ..Default::default() })
            .await
            .unwrap();
        if i % 20 == 19 && must(&scrape(&url).await, "etcd_debugging_mvcc_slow_watcher_total") >= 1.0 {
            slow = true;
            break;
        }
    }
    assert!(slow, "an unread watch stream was never reported slow");

    // Read again: it catches up and is no longer slow.
    let reader = tokio::spawn(async move {
        while let Some(Ok(_)) = stream.next().await {}
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let body = scrape(&url).await;
        if must(&body, "etcd_debugging_mvcc_slow_watcher_total") == 0.0 {
            assert_eq!(must(&body, "etcd_debugging_mvcc_watcher_total"), 1.0);
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "still slow after the client read again");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    drop(tx);
    reader.abort();
}
