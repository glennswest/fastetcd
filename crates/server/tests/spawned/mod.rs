//! Start the real `fastetcd` binary on ports it cannot lose (fastetcd#63).
//!
//! Picking a free port, closing it and letting the child bind it later
//! is a race on a shared build box: another job can take the port in
//! between (#40). So each start binds a random address in 127.0.0.0/8
//! (all loopback on Linux; other tests bind 127.0.0.1, so nothing else
//! picks the same address and port), and a child that still finds its
//! port taken is started again on new ones. Its output goes to a log
//! file, whose tail every failure here shows.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::process::{Child, Command};

pub struct Spawned {
    pub child: Child,
    pub host: String,
    pub client_port: u16,
    pub peer_port: u16,
    pub log: PathBuf,
    _logs: tempfile::TempDir,
}

impl Spawned {
    pub fn client_url(&self) -> String {
        format!("http://{}:{}", self.host, self.client_port)
    }

    /// The last lines of the child's output, for a failure message.
    pub fn log_tail(&self) -> String {
        tail(&self.log)
    }
}

fn tail(path: &Path) -> String {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(30)..].join("\n")
}

/// A random loopback address other than 127.0.0.1.
pub fn random_loopback() -> String {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u32(std::process::id());
    let r = h.finish();
    format!("127.{}.{}.{}", 1 + (r % 254), (r >> 8) % 256, 1 + ((r >> 16) % 254))
}

async fn free_port(host: &str) -> u16 {
    let l = tokio::net::TcpListener::bind((host, 0)).await.unwrap();
    l.local_addr().unwrap().port()
}

/// Start `fastetcd` with `args` plus its listen flags, and return once
/// its client port accepts connections (callers wait for whatever else
/// they need: a leader, a health answer).
pub async fn spawn_fastetcd(args: &[&std::ffi::OsStr]) -> Spawned {
    let logs = tempfile::tempdir().unwrap();
    let log_dir = logs.path().to_path_buf();
    for attempt in 1..=5 {
        let host = random_loopback();
        let client_port = free_port(&host).await;
        let peer_port = free_port(&host).await;
        let log = log_dir.join(format!("fastetcd-{attempt}.log"));
        let out = std::fs::File::create(&log).unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_fastetcd"))
            .args(args)
            .arg("--listen-client-urls")
            .arg(format!("http://{host}:{client_port}"))
            .arg("--listen-peer-urls")
            .arg(format!("http://{host}:{peer_port}"))
            .arg("--listen-metrics-url")
            .arg("")
            .stdout(out.try_clone().unwrap())
            .stderr(out)
            .kill_on_drop(true)
            .spawn()
            .expect("spawn fastetcd");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                let text = std::fs::read_to_string(&log).unwrap_or_default();
                if text.contains("in use") && attempt < 5 {
                    eprintln!("fastetcd's port was taken (attempt {attempt}); starting it on new ones");
                    break;
                }
                panic!("fastetcd exited ({status}) before it listened:\n{}", tail(&log));
            }
            if tokio::net::TcpStream::connect((host.as_str(), client_port)).await.is_ok() {
                return Spawned { child, host, client_port, peer_port, log, _logs: logs };
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "fastetcd did not listen within 30 s:\n{}",
                tail(&log)
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    unreachable!("the last attempt panics or returns")
}
