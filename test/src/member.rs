//! fastetcd members this run starts itself: the commit's binary as a
//! child process on loopback, its data in the run's work directory. Used
//! by every suite for what must never be done to the node's live store.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::Context;
use tokio::process::{Child, Command};

use crate::client::Clients;

pub struct Member {
    pub name: String,
    pub dir: PathBuf,
    pub endpoint: String,
    args: Vec<String>,
    bin: PathBuf,
    child: Option<Child>,
}

/// Several members started as one cluster.
pub struct Cluster {
    pub members: Vec<Member>,
}

fn free_port() -> anyhow::Result<u16> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?.port())
}

impl Member {
    /// Start a lone member named `name` under `work`, with `extra` flags.
    pub async fn single(bin: &Path, work: &Path, name: &str, extra: &[&str]) -> anyhow::Result<Member> {
        let mut m = Member::prepare(bin, work, name, extra)?;
        m.start().await?;
        m.ready(Duration::from_secs(60)).await?;
        Ok(m)
    }

    /// A lone member, not started yet: its data directory can be filled
    /// first (a restore).
    pub fn prepare(bin: &Path, work: &Path, name: &str, extra: &[&str]) -> anyhow::Result<Member> {
        let client = free_port()?;
        let peer = free_port()?;
        let mut args = vec![
            format!("--name={name}"),
            format!("--listen-client-urls=http://127.0.0.1:{client}"),
            format!("--listen-peer-urls=http://127.0.0.1:{peer}"),
            "--listen-metrics-url=".into(),
        ];
        args.extend(extra.iter().map(|s| s.to_string()));
        let m = Member {
            name: name.into(),
            dir: work.join(name),
            endpoint: format!("http://127.0.0.1:{client}"),
            args,
            bin: bin.into(),
            child: None,
        };
        Ok(m)
    }

    pub fn data_dir(&self) -> PathBuf {
        self.dir.join("data")
    }

    /// Start (or restart) the process on its data directory.
    pub async fn start(&mut self) -> anyhow::Result<()> {
        std::fs::create_dir_all(&self.dir)?;
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.with_extension("log"))?;
        let child = Command::new(&self.bin)
            .arg(format!("--data-dir={}", self.dir.join("data").display()))
            .args(&self.args)
            .env("RUST_LOG", "warn")
            .stdout(Stdio::null())
            .stderr(log)
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("starting {}", self.bin.display()))?;
        self.child = Some(child);
        Ok(())
    }

    /// Wait until it answers a Range (any answer means it is serving).
    pub async fn ready(&mut self, within: Duration) -> anyhow::Result<()> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            if let Some(c) = self.child.as_mut() {
                if let Some(status) = c.try_wait()? {
                    anyhow::bail!("{} exited ({status}); its log ends: {}", self.name, self.log_tail());
                }
            }
            if let Ok(mut c) = Clients::connect(&self.endpoint).await {
                if c.serializable_count(b"/").await.is_ok() {
                    return Ok(());
                }
            }
            if tokio::time::Instant::now() > deadline {
                anyhow::bail!("{} did not serve within {within:?}; its log ends: {}", self.name, self.log_tail());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// SIGKILL: no shutdown path runs, as on a crash.
    pub async fn kill(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill().await;
        }
    }

    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().and_then(|c| c.id())
    }

    pub async fn clients(&self) -> anyhow::Result<Clients> {
        Clients::connect(&self.endpoint).await
    }

    pub fn data_file(&self) -> PathBuf {
        self.dir.join("data").join("fastetcd.redb")
    }

    pub fn log_tail(&self) -> String {
        let s = std::fs::read_to_string(self.dir.with_extension("log")).unwrap_or_default();
        let lines: Vec<&str> = s.lines().collect();
        lines[lines.len().saturating_sub(8)..].join(" | ")
    }
}

impl Cluster {
    /// `n` members named `<prefix>1..n`, as one new cluster.
    pub async fn start(bin: &Path, work: &Path, prefix: &str, n: usize) -> anyhow::Result<Cluster> {
        let mut ports = Vec::new();
        for _ in 0..n {
            ports.push((free_port()?, free_port()?));
        }
        let initial: Vec<String> = ports
            .iter()
            .enumerate()
            .map(|(i, (_, p))| format!("{prefix}{}=http://127.0.0.1:{p}", i + 1))
            .collect();
        let mut members = Vec::new();
        for (i, (c, p)) in ports.iter().enumerate() {
            let name = format!("{prefix}{}", i + 1);
            let mut m = Member {
                dir: work.join(&name),
                endpoint: format!("http://127.0.0.1:{c}"),
                args: vec![
                    format!("--name={name}"),
                    format!("--listen-client-urls=http://127.0.0.1:{c}"),
                    format!("--listen-peer-urls=http://127.0.0.1:{p}"),
                    format!("--initial-advertise-peer-urls=http://127.0.0.1:{p}"),
                    format!("--initial-cluster={}", initial.join(",")),
                    format!("--initial-cluster-token={prefix}-{}", std::process::id()),
                    "--listen-metrics-url=".into(),
                ],
                name,
                bin: bin.into(),
                child: None,
            };
            m.start().await?;
            members.push(m);
        }
        for m in members.iter_mut() {
            m.ready(Duration::from_secs(60)).await?;
        }
        Ok(Cluster { members })
    }

    /// The leader's index in `members`: the live member whose own id is
    /// the leader a live member reports.
    pub async fn leader(&self, within: Duration) -> anyhow::Result<usize> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let mut ids = Vec::new();
            let mut leader = 0;
            for m in &self.members {
                let mut id = None;
                if m.child.is_some() {
                    if let Ok(mut c) = m.clients().await {
                        if let Ok(st) = c.status().await {
                            id = st.header.map(|h| h.member_id);
                            if st.leader != 0 {
                                leader = st.leader;
                            }
                        }
                    }
                }
                ids.push(id);
            }
            if let Some(i) = ids.iter().position(|id| leader != 0 && *id == Some(leader)) {
                return Ok(i);
            }
            if tokio::time::Instant::now() > deadline {
                anyhow::bail!("no live member is the leader the others report within {within:?}");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}
