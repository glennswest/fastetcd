//! fastetcd#28: etcd's v3 JSON gateway on the client port.
//!
//! The port is built as main.rs builds it: tonic routes → axum, the
//! gateway merged beside them, the gRPC call counters layered over it,
//! served by tonic with HTTP/1.1 on. Requests are plain HTTP/JSON
//! (reqwest), as a console or `curl` sends them.

mod common;
use common::start_test_server_full;

use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use fastetcd_proto::etcdserverpb::kv_server::KvServer;
use fastetcd_server::auth::{AuthInterceptor, AuthService};
use fastetcd_server::cluster::ClusterService;
use fastetcd_server::gateway::Gateway;
use fastetcd_server::kv::KvService;
use fastetcd_server::lease::LeaseService;
use fastetcd_server::maintenance::MaintenanceService;
use fastetcd_server::traffic::{GrpcHandledLabels, Traffic};
use serde_json::{json, Value};

struct Env {
    h: common::TestServerHandles,
    base: String,
    http: reqwest::Client,
}

async fn start() -> Env {
    let h = start_test_server_full().await;
    let state = h.state.clone();
    let directory: fastetcd_server::cluster::MemberDirectory =
        Arc::new(tokio::sync::RwLock::new(std::collections::BTreeMap::new()));
    ClusterService::seed_self(
        &directory,
        1,
        "test-node".to_string(),
        vec!["http://test-peer:0".to_string()],
        vec!["http://test-client:0".to_string()],
    )
    .await;
    let interceptor = AuthInterceptor::new(state.auth.clone());
    let kv = KvService::new(state.clone());
    let gw = Gateway {
        kv: kv.clone(),
        lease: LeaseService::new(state.clone()),
        cluster: ClusterService::new(state.clone(), 1, fastetcd_raft::network::empty_peers(), directory),
        maintenance: MaintenanceService::new(state.clone()),
        auth: AuthService::new(state.clone()),
        watch: fastetcd_server::watch::WatchService::new(state.clone()),
        interceptor: interceptor.clone(),
        traffic: state.traffic.clone(),
    };
    let mut routes = tonic::service::Routes::builder();
    routes.add_service(KvServer::with_interceptor(kv, interceptor));
    let app: axum::Router = routes
        .routes()
        .into_axum_router()
        .merge(fastetcd_server::gateway::router(gw))
        .layer(axum::middleware::from_fn_with_state(
            state.traffic.clone(),
            fastetcd_server::traffic::grpc_middleware,
        ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .accept_http1(true)
            .add_routes(tonic::service::Routes::from(app))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    Env { h, base, http: reqwest::Client::new() }
}

impl Env {
    /// POST `body` to `path`, with `token` as the `Authorization` header.
    async fn post(&self, path: &str, body: Value, token: Option<&str>) -> (u16, Value) {
        let mut req = self
            .http
            .post(format!("{}{path}", self.base))
            .header("content-type", "application/json")
            .body(body.to_string());
        if let Some(t) = token {
            req = req.header("Authorization", t);
        }
        let resp = req.send().await.unwrap();
        let status = resp.status().as_u16();
        let text = resp.text().await.unwrap();
        (status, serde_json::from_str(&text).unwrap_or_else(|e| panic!("{path}: not JSON ({e}): {text}")))
    }

    async fn ok(&self, path: &str, body: Value, token: Option<&str>) -> Value {
        let (status, v) = self.post(path, body, token).await;
        assert_eq!(status, 200, "{path}: {v}");
        v
    }

    fn handled(&self, service: &str, method: &str, code: &str) -> u64 {
        handled(&self.h.state.traffic, service, method, code)
    }
}

fn handled(t: &Traffic, service: &str, method: &str, code: &str) -> u64 {
    t.grpc_handled
        .get_or_create(&GrpcHandledLabels {
            grpc_type: "unary".into(),
            grpc_service: format!("etcdserverpb.{service}"),
            grpc_method: method.into(),
            grpc_code: code.into(),
        })
        .get()
}

fn b64(s: &str) -> String {
    B64.encode(s)
}

#[tokio::test]
async fn status_members_alarms_and_the_keyspace_over_json() {
    let env = start().await;

    // Status: what the console reads first. An empty body is the
    // default request, as with etcd's gateway.
    let resp = env.http.post(format!("{}/v3/maintenance/status", env.base)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers()["content-type"], "application/json");
    let status: Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(status["header"]["member_id"], "1");
    assert_eq!(status["header"]["cluster_id"], "7");
    assert_eq!(status["leader"], "1");
    assert!(status["raftIndex"].as_str().unwrap().parse::<u64>().unwrap() > 0, "{status}");
    assert!(status["raftTerm"].is_string(), "{status}");
    assert!(status["version"].is_string(), "{status}");

    // Members.
    let members = env.ok("/v3/cluster/member/list", json!({}), None).await;
    let m = &members["members"][0];
    assert_eq!(m["ID"], "1", "{members}");
    assert_eq!(m["name"], "test-node");
    assert_eq!(m["peerURLs"], json!(["http://test-peer:0"]));
    assert_eq!(m["clientURLs"], json!(["http://test-client:0"]));

    // Keyspace: puts, then a prefix range, count and limit.
    for k in ["/registry/pods/a", "/registry/pods/b", "/registry/svc/c"] {
        env.ok("/v3/kv/put", json!({"key": b64(k), "value": b64("v")}), None).await;
    }
    let prefix = json!({"key": b64("/registry/"), "range_end": b64("/registry0")});
    let all = env.ok("/v3/kv/range", prefix.clone(), None).await;
    assert_eq!(all["count"], "3");
    assert_eq!(all["kvs"][0]["key"], b64("/registry/pods/a"));
    assert_eq!(all["kvs"][0]["value"], b64("v"));
    assert!(all["kvs"][0]["mod_revision"].is_string());
    let keys = env
        .ok("/v3/kv/range", json!({"key": b64("/registry/"), "range_end": b64("/registry0"), "keys_only": true}), None)
        .await;
    assert!(keys["kvs"][0].get("value").is_none(), "{keys}");
    let count = env
        .ok("/v3/kv/range", json!({"key": b64("/registry/"), "range_end": b64("/registry0"), "countOnly": true}), None)
        .await;
    assert_eq!(count["count"], "3");
    assert!(count.get("kvs").is_none(), "{count}");
    let limited = env
        .ok("/v3/kv/range", json!({"key": b64("/registry/"), "range_end": b64("/registry0"), "limit": "2"}), None)
        .await;
    assert_eq!((limited["kvs"].as_array().unwrap().len(), &limited["more"]), (2, &json!(true)));

    // A txn: enums by name, a oneof compare target, nested responses.
    let txn = env
        .ok(
            "/v3/kv/txn",
            json!({
                "compare": [{"key": b64("/registry/new"), "target": "VERSION", "result": "EQUAL", "version": "0"}],
                "success": [{"request_put": {"key": b64("/registry/new"), "value": b64("n")}}],
                "failure": [{"request_range": {"key": b64("/registry/new")}}]
            }),
            None,
        )
        .await;
    assert_eq!(txn["succeeded"], true, "{txn}");
    assert!(txn["responses"][0]["response_put"].is_object(), "{txn}");
    let rev: i64 = txn["header"]["revision"].as_str().unwrap().parse().unwrap();

    // Compaction, then an older revision is an error with etcd's
    // gateway shape: HTTP 400 for OutOfRange, {"code":11,"message":…}.
    env.ok("/v3/kv/compaction", json!({"revision": rev.to_string()}), None).await;
    let (code, err) = env.post("/v3/kv/range", json!({"key": b64("/registry/new"), "revision": "1"}), None).await;
    assert_eq!(code, 400, "{err}");
    assert_eq!(err["code"], 11);
    assert!(err["message"].as_str().unwrap().contains("compacted"), "{err}");

    // Alarms: GET, and DEACTIVATE by name.
    env.ok("/v3/maintenance/alarm", json!({"action": "GET"}), None).await;
    env.ok("/v3/maintenance/alarm", json!({"action": "DEACTIVATE", "memberID": "1", "alarm": "NOSPACE"}), None)
        .await;

    // Defragment, hash, lease grant / time-to-live / list.
    env.ok("/v3/maintenance/defragment", json!({}), None).await;
    env.ok("/v3/maintenance/hashkv", json!({}), None).await;
    let lease = env.ok("/v3/lease/grant", json!({"TTL": "60"}), None).await;
    let id = lease["ID"].as_str().unwrap().to_string();
    let ttl = env.ok("/v3/kv/lease/timetolive", json!({"ID": id}), None).await;
    assert_eq!(ttl["ID"], id);
    let leases = env.ok("/v3/lease/leases", json!({}), None).await;
    assert_eq!(leases["leases"][0]["ID"], id);

    // Bad input and bad routes.
    let resp = env.http.post(format!("{}/v3/kv/range", env.base)).body("{nope").send().await.unwrap();
    assert_eq!(resp.status(), 400);
    let err: Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(err["code"], 3);
    let resp = env.http.get(format!("{}/v3/kv/range", env.base)).send().await.unwrap();
    assert_eq!(resp.status(), 405);
    let resp = env.http.post(format!("{}/v3/kv/nothing", env.base)).send().await.unwrap();
    assert_eq!(resp.status(), 404);
    let err: Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(err["code"], 5);

    // Snapshot: one {"result":…} per line; the blobs are the database.
    let resp = env.http.post(format!("{}/v3/maintenance/snapshot", env.base)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    let mut bytes = 0;
    let lines: Vec<&str> = text.lines().collect();
    assert!(!lines.is_empty());
    for line in &lines {
        let v: Value = serde_json::from_str(line).unwrap();
        bytes += B64.decode(v["result"]["blob"].as_str().unwrap()).unwrap().len();
    }
    assert!(bytes > 0);
    let last: Value = serde_json::from_str(lines.last().unwrap()).unwrap();
    assert!(last["result"].get("remaining_bytes").is_none(), "last chunk leaves nothing: {last}");

    // Gateway calls count as the gRPC methods they call.
    assert!(env.handled("KV", "Range", "OK") >= 4);
    assert_eq!(env.handled("KV", "Range", "OutOfRange"), 1);
    assert_eq!(env.handled("Maintenance", "Status", "OK"), 1);
    assert_eq!(env.handled("Cluster", "MemberList", "OK"), 1);
}

/// The gateway is not a way round auth: the same interceptor and the
/// same permission checks as gRPC, with the token in `Authorization`.
#[tokio::test]
async fn auth_applies_to_the_gateway() {
    let env = start().await;
    // Users and roles, set up through the gateway's Auth endpoints.
    for role in ["root", "config"] {
        env.ok("/v3/auth/role/add", json!({"name": role}), None).await;
    }
    for (name, pw) in [("root", "rootpw"), ("alice", "pw")] {
        env.ok("/v3/auth/user/add", json!({"name": name, "password": pw}), None).await;
    }
    env.ok("/v3/auth/user/grant", json!({"user": "root", "role": "root"}), None).await;
    env.ok(
        "/v3/auth/role/grant",
        json!({"name": "config", "perm": {"permType": "READWRITE", "key": b64("config/"), "range_end": b64("config0")}}),
        None,
    )
    .await;
    env.ok("/v3/auth/user/grant", json!({"user": "alice", "role": "config"}), None).await;
    env.ok("/v3/auth/enable", json!({}), None).await;

    let login = |name: &'static str, pw: &'static str| {
        let env = &env;
        async move {
            env.ok("/v3/auth/authenticate", json!({"name": name, "password": pw}), None).await["token"]
                .as_str()
                .unwrap()
                .to_string()
        }
    };
    let root = login("root", "rootpw").await;
    let alice = login("alice", "pw").await;

    // No token: etcd's ErrUserEmpty, 400 code 3; a bad one:
    // ErrInvalidAuthToken, 401 code 16 (#105).
    let (code, err) = env.post("/v3/kv/range", json!({"key": b64("config/a")}), None).await;
    assert_eq!((code, &err["code"]), (400, &json!(3)), "{err}");
    assert_eq!(err["message"], json!("etcdserver: user name is empty"));
    let (code, _) = env.post("/v3/maintenance/status", json!({}), Some("not-a-token")).await;
    assert_eq!(code, 401);

    // alice: her prefix only.
    env.ok("/v3/kv/put", json!({"key": b64("config/a"), "value": b64("1")}), Some(&alice)).await;
    let got = env.ok("/v3/kv/range", json!({"key": b64("config/a")}), Some(&alice)).await;
    assert_eq!(got["kvs"][0]["value"], b64("1"));
    let (code, err) = env.post("/v3/kv/range", json!({"key": b64("secret")}), Some(&alice)).await;
    assert_eq!((code, &err["code"]), (403, &json!(7)), "{err}");
    // A txn reaching outside it is denied too (#22).
    let (code, _) = env
        .post(
            "/v3/kv/txn",
            json!({"success": [{"request_range": {"key": b64("secret")}}]}),
            Some(&alice),
        )
        .await;
    assert_eq!(code, 403);
    // Status needs only a login; admin verbs need root (#31).
    env.ok("/v3/maintenance/status", json!({}), Some(&alice)).await;
    let (code, _) = env.post("/v3/maintenance/defragment", json!({}), Some(&alice)).await;
    assert_eq!(code, 403);
    let (code, _) = env.post("/v3/maintenance/snapshot", json!({}), Some(&alice)).await;
    assert_eq!(code, 403);
    let (code, _) = env.post("/v3/auth/user/list", json!({}), Some(&alice)).await;
    assert_eq!(code, 403);
    let (code, _) = env.post("/v3/auth/disable", json!({}), Some(&alice)).await;
    assert_eq!(code, 403);

    // root: everything.
    env.ok("/v3/kv/range", json!({"key": b64("secret")}), Some(&root)).await;
    env.ok("/v3/maintenance/defragment", json!({}), Some(&root)).await;
    let users = env.ok("/v3/auth/user/list", json!({}), Some(&root)).await;
    assert_eq!(users["users"], json!(["alice", "root"]));
    let resp = env
        .http
        .post(format!("{}/v3/maintenance/snapshot", env.base))
        .header("Authorization", &root)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(resp.text().await.unwrap().starts_with("{\"result\""));
    env.ok("/v3/auth/disable", json!({}), Some(&root)).await;
    env.ok("/v3/kv/range", json!({"key": b64("secret")}), None).await;
}

/// Reads a streamed gateway response a line at a time.
struct Lines {
    resp: reqwest::Response,
    buf: Vec<u8>,
}

impl Lines {
    async fn open(env: &Env, path: &str, body: String) -> Self {
        let resp = env.http.post(format!("{}{path}", env.base)).body(body).send().await.unwrap();
        assert_eq!(resp.status(), 200, "{path}");
        Lines { resp, buf: Vec::new() }
    }

    /// The next line as JSON, or `None` when the stream ends.
    async fn next(&mut self) -> Option<Value> {
        loop {
            if let Some(i) = self.buf.iter().position(|b| *b == b'\n') {
                let line: Vec<u8> = self.buf.drain(..=i).collect();
                return Some(serde_json::from_slice(&line).unwrap());
            }
            let chunk = tokio::time::timeout(std::time::Duration::from_secs(10), self.resp.chunk())
                .await
                .expect("no line in 10s")
                .unwrap()?;
            self.buf.extend_from_slice(&chunk);
        }
    }
}

/// `/v3/watch` as etcd's gateway serves it: create requests in the body,
/// then the created response and events streamed, one line each, for as
/// long as the client stays (the body ending does not end the watch).
#[tokio::test]
async fn watch_streams_events_as_json_lines() {
    let env = start().await;
    env.ok("/v3/kv/put", json!({"key": b64("w/old"), "value": b64("1")}), None).await;

    // Two creates in one body: a live watch on w/, and one replaying
    // history from revision 1.
    let body = format!(
        "{}\n{}",
        json!({"create_request": {"key": b64("w/"), "range_end": b64("w0")}}),
        json!({"create_request": {"key": b64("w/"), "range_end": b64("w0"), "start_revision": "1"}})
    );
    let mut lines = Lines::open(&env, "/v3/watch", body).await;
    let mut created = 0;
    let mut replayed = false;
    while created < 2 || !replayed {
        let v = lines.next().await.expect("stream ended early");
        let r = &v["result"];
        if r["created"] == true {
            created += 1;
        }
        if let Some(evs) = r["events"].as_array() {
            assert_eq!(evs[0]["kv"]["key"], b64("w/old"), "{v}");
            replayed = true;
        }
    }
    // A write after the body ended still arrives, on both watchers.
    env.ok("/v3/kv/put", json!({"key": b64("w/new"), "value": b64("2")}), None).await;
    let mut seen = 0;
    while seen < 2 {
        let v = lines.next().await.expect("stream ended");
        let evs = v["result"]["events"].as_array().unwrap_or_else(|| panic!("{v}"));
        assert_eq!(evs[0]["kv"]["key"], b64("w/new"));
        assert_eq!(evs[0]["kv"]["value"], b64("2"));
        seen += 1;
    }
    assert_eq!(handled(&env.h.state.traffic, "Watch", "Watch", "OK"), 0);
    let started = env
        .h
        .state
        .traffic
        .grpc_started
        .get_or_create(&fastetcd_server::traffic::GrpcLabels {
            grpc_type: "bidi_stream".into(),
            grpc_service: "etcdserverpb.Watch".into(),
            grpc_method: "Watch".into(),
        })
        .get();
    assert_eq!(started, 1);
}

/// `/v3/lease/keepalive`: one response per request in the body, then the
/// stream ends with the body, as in etcd. Bad JSON is an error line.
#[tokio::test]
async fn lease_keepalive_answers_each_request_and_ends_with_the_body() {
    let env = start().await;
    let lease = env.ok("/v3/lease/grant", json!({"TTL": "30"}), None).await;
    let id = lease["ID"].as_str().unwrap().to_string();

    // Back to back, no separator, as a JSON decoder allows.
    let body = format!("{}{}", json!({"ID": id}), json!({"ID": id}));
    let mut lines = Lines::open(&env, "/v3/lease/keepalive", body).await;
    for _ in 0..2 {
        let v = lines.next().await.expect("a keepalive response");
        assert_eq!(v["result"]["ID"], id, "{v}");
        assert_eq!(v["result"]["TTL"], "30", "{v}");
    }
    assert!(lines.next().await.is_none(), "the stream ends with the body");

    let mut lines = Lines::open(&env, "/v3/lease/keepalive", format!("{} {{nope", json!({"ID": id}))).await;
    let mut got = Vec::new();
    while let Some(v) = lines.next().await {
        got.push(v);
    }
    assert!(got.iter().any(|v| v["result"]["ID"] == id), "{got:?}");
    assert!(got.iter().any(|v| v["error"]["code"] == 3), "{got:?}");
}
