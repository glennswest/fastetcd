//! etcd's `--max-request-bytes` and `--log-level` work (fastetcd#54), on
//! the real binary, since both are wired in `main`.

use std::time::Duration;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;

async fn port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap().port()
}

/// Start the binary with `extra` flags and `env`; stderr goes to a file.
async fn start(
    dir: &std::path::Path,
    extra: &[&str],
    env: &[(&str, &str)],
) -> (tokio::process::Child, String, std::path::PathBuf) {
    let (cp, pp) = (port().await, port().await);
    let log = dir.join("stderr.log");
    let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_fastetcd"));
    cmd.arg("--data-dir")
        .arg(dir.join("data"))
        .args(["--listen-client-urls", &format!("http://127.0.0.1:{cp}")])
        .args(["--listen-peer-urls", &format!("http://127.0.0.1:{pp}")])
        .args(["--listen-metrics-url", ""])
        .args(extra)
        .env_remove("RUST_LOG")
        .stdout(std::process::Stdio::null())
        .stderr(std::fs::File::create(&log).unwrap());
    for (k, v) in env {
        cmd.env(k, v);
    }
    let child = cmd.spawn().unwrap();
    let ep = format!("http://127.0.0.1:{cp}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(mut kv) = KvClient::connect(ep.clone()).await {
            if kv.range(pb::RangeRequest { key: b"x".to_vec(), ..Default::default() }).await.is_ok() {
                break;
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "fastetcd did not come up");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    (child, ep, log)
}

fn put(n: usize) -> pb::PutRequest {
    pb::PutRequest { key: b"big".to_vec(), value: vec![b'x'; n], ..Default::default() }
}

#[tokio::test]
async fn max_request_bytes_refuses_a_larger_write_as_etcd_does() {
    let dir = tempfile::tempdir().unwrap();
    let (mut child, ep, _) = start(dir.path(), &["--max-request-bytes", "2048"], &[]).await;
    let mut kv = KvClient::connect(ep).await.unwrap().max_encoding_message_size(8 << 20);
    kv.put(put(1024)).await.expect("a write under the limit");
    let err = kv.put(put(4096)).await.expect_err("a write over the limit");
    assert_eq!(
        (err.code(), err.message()),
        (tonic::Code::InvalidArgument, "etcdserver: request is too large"),
        "{err:?}"
    );
    // A message past the limit + 512 KiB is refused by gRPC itself.
    let err = kv.put(put(1 << 20)).await.expect_err("a message over the receive limit");
    assert_eq!(err.code(), tonic::Code::OutOfRange, "{err:?}");
    child.kill().await.ok();
}

#[tokio::test]
async fn without_max_request_bytes_a_large_write_still_works() {
    let dir = tempfile::tempdir().unwrap();
    let (mut child, ep, _) = start(dir.path(), &[], &[]).await;
    let mut kv = KvClient::connect(ep).await.unwrap();
    kv.put(put(2 << 20)).await.expect("2 MiB: under the 4 MiB receive limit, as before");
    child.kill().await.ok();
}

#[tokio::test]
async fn log_level_sets_the_level_and_rust_log_wins() {
    let dir = tempfile::tempdir().unwrap();
    let (mut child, _, log) = start(dir.path(), &["--log-level", "debug"], &[]).await;
    child.kill().await.ok();
    let text = std::fs::read_to_string(&log).unwrap();
    assert!(text.contains("DEBUG"), "--log-level debug logs debug lines:\n{}", &text[..text.len().min(2000)]);

    let dir = tempfile::tempdir().unwrap();
    let (mut child, _, log) = start(dir.path(), &["--log-level", "debug"], &[("RUST_LOG", "warn")]).await;
    child.kill().await.ok();
    let text = std::fs::read_to_string(&log).unwrap();
    assert!(!text.contains(" INFO ") && !text.contains("DEBUG"), "RUST_LOG=warn wins:\n{text}");

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_fastetcd"))
        .args(["--log-level", "loud"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("unknown log level"));
}
