//! Peer connections time out and notice a dead peer (fastetcd#168).
//!
//! tonic sets no connect timeout and no HTTP/2 keepalive by default, so
//! a forwarded write or read to a peer that stopped answering waited for
//! the OS's TCP timeout (minutes). Three peers that never answer: one
//! that accepts the TCP connection and stays silent, one whose TLS
//! handshake never completes, and one that answers, then goes silent
//! with the connection still open (a host that lost power). Each call
//! must fail within a few seconds.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fastetcd_raft::{dial_peer_with, empty_peers, DialOptions, FastetcdLogEntry, WriteForwarder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Instant;

/// Short, so the test is quick; the server's are 1 s + the election
/// timeout.
const DIAL: DialOptions = DialOptions {
    connect_timeout: Duration::from_millis(500),
    keepalive_interval: Duration::from_millis(200),
    keepalive_timeout: Duration::from_millis(500),
};
/// Well past what DIAL allows, well short of a TCP timeout.
const BOUND: Duration = Duration::from_secs(5);

/// Accepts connections and never reads or writes; keeps them open.
async fn silent_peer() -> std::net::SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((s, _)) = l.accept().await {
            held.push(s);
        }
    });
    addr
}

async fn forward_put(fwd: &WriteForwarder) -> Result<(), String> {
    let entry = FastetcdLogEntry::Compact { rev: 1 };
    fwd.forward(2, &entry).await.map(|_| ())
}

#[tokio::test]
async fn a_forwarded_call_to_a_silent_peer_fails_in_seconds() {
    let addr = silent_peer().await;
    let peers = empty_peers();
    peers.write().await.insert(2, format!("http://{addr}"));
    let fwd = WriteForwarder::new(peers).with_dial(DIAL);
    let t = Instant::now();
    let res = tokio::time::timeout(Duration::from_secs(30), forward_put(&fwd))
        .await
        .expect("a forwarded write to a silent peer was still waiting after 30 s");
    assert!(res.is_err(), "a silent peer answered: {res:?}");
    assert!(t.elapsed() < BOUND, "it took {:?} to give up on a silent peer", t.elapsed());
}

#[tokio::test]
async fn a_tls_handshake_that_never_completes_times_out() {
    let addr = silent_peer().await;
    // Any CA: the handshake never gets as far as checking a certificate.
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let key = rcgen::KeyPair::generate().unwrap();
    let ca = params.self_signed(&key).unwrap();
    let tls = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(ca.pem()))
        .domain_name("localhost");
    let peers = empty_peers();
    peers.write().await.insert(2, format!("https://{addr}"));
    let fwd = WriteForwarder::with_tls(peers, Some(tls)).with_dial(DIAL);
    let t = Instant::now();
    let res = tokio::time::timeout(Duration::from_secs(30), forward_put(&fwd))
        .await
        .expect("a TLS dial to a silent peer was still waiting after 30 s");
    assert!(res.is_err(), "a silent TLS peer answered: {res:?}");
    assert!(t.elapsed() < BOUND, "it took {:?} to give up on a TLS handshake", t.elapsed());
}

/// Copies `r` to `w` until `frozen`, then passes nothing and holds both
/// open: a peer gone without a reset.
async fn pump(mut r: tokio::net::tcp::OwnedReadHalf, mut w: tokio::net::tcp::OwnedWriteHalf, frozen: Arc<AtomicBool>) {
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        let n = match r.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        while frozen.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if w.write_all(&buf[..n]).await.is_err() {
            break;
        }
    }
    while frozen.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A TCP proxy to `backend` that stops passing bytes once `frozen`.
async fn freezing_proxy(backend: std::net::SocketAddr, frozen: Arc<AtomicBool>) -> std::net::SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((client, _)) = l.accept().await {
            let server = TcpStream::connect(backend).await.unwrap();
            let (cr, cw) = client.into_split();
            let (sr, sw) = server.into_split();
            tokio::spawn(pump(cr, sw, frozen.clone()));
            tokio::spawn(pump(sr, cw, frozen.clone()));
        }
    });
    addr
}

#[tokio::test]
async fn a_peer_that_goes_silent_on_an_open_connection_is_noticed() {
    // A real gRPC server (grpc.health.v1) behind the proxy.
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend.local_addr().unwrap();
    let (_reporter, health) = tonic_health::server::health_reporter();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(health)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(backend)),
    );
    let frozen = Arc::new(AtomicBool::new(false));
    let proxy = freezing_proxy(backend_addr, frozen.clone()).await;

    let chan = dial_peer_with(&format!("http://{proxy}"), &None, DIAL).await.unwrap();
    let mut health = tonic_health::pb::health_client::HealthClient::new(chan);
    let check = || tonic_health::pb::HealthCheckRequest { service: String::new() };
    health.check(check()).await.expect("the peer answers before it goes silent");

    frozen.store(true, Ordering::Relaxed);
    let t = Instant::now();
    let res = tokio::time::timeout(Duration::from_secs(30), health.check(check()))
        .await
        .expect("a call on a connection whose peer went silent was still waiting after 30 s");
    assert!(res.is_err(), "a frozen peer answered: {res:?}");
    assert!(t.elapsed() < BOUND, "it took {:?} to notice the peer went silent", t.elapsed());
}

#[test]
fn the_servers_dial_options_follow_the_election_timeout() {
    let d = DialOptions::for_election_timeout(Duration::from_secs(10));
    assert_eq!(d.connect_timeout, Duration::from_secs(11));
    assert_eq!(d.keepalive_timeout, Duration::from_secs(11));
    assert_eq!(d.keepalive_interval, Duration::from_secs(1));
    assert_eq!(DialOptions::default().connect_timeout, Duration::from_secs(2));
}
