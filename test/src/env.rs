//! What the runner gives the test (docs/test-standard.md), and where it
//! may write.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub struct Env {
    pub suite: String,
    /// `storm.io/test-run`: names everything this run makes.
    pub run_id: String,
    pub commit: String,
    /// The node's address (`STORM_NODE`): its fastetcd serves on
    /// `node_port`, rustkube's live store.
    pub node: Option<String>,
    pub node_port: u16,
    /// The commit's own fastetcd, shipped in the image (`/fastetcd`).
    pub bin: PathBuf,
    /// A private directory for the members this run starts.
    pub work: PathBuf,
    started: Instant,
    budget: Duration,
}

impl Env {
    pub fn discover(suite: &str) -> anyhow::Result<Env> {
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        let run_id = var("STORM_RUN_ID").unwrap_or_else(|| format!("local-{}", std::process::id()));
        let default_budget = match suite {
            "short" => 120,
            "medium" => 1800,
            _ => 8 * 3600,
        };
        let budget = var("STORM_TIMEOUT").and_then(|s| s.parse().ok()).unwrap_or(default_budget);
        let bin = PathBuf::from(var("FASTETCD_TEST_BIN").unwrap_or_else(|| "/fastetcd".into()));
        if !bin.is_file() {
            anyhow::bail!("the fastetcd binary under test is missing: {}", bin.display());
        }
        Ok(Env {
            suite: suite.to_string(),
            commit: var("STORM_COMMIT").unwrap_or_default(),
            node: var("STORM_NODE"),
            node_port: var("FASTETCD_TEST_NODE_PORT").and_then(|s| s.parse().ok()).unwrap_or(2379),
            work: work_dir(&run_id)?,
            run_id,
            bin,
            started: Instant::now(),
            budget: Duration::from_secs(budget),
        })
    }

    /// Time left of the run's budget.
    pub fn left(&self) -> Duration {
        self.budget.saturating_sub(self.started.elapsed())
    }

    /// The key prefix everything this run writes on the node goes under.
    pub fn prefix(&self) -> String {
        format!("/storm-test/{}/", self.run_id)
    }
}

/// `<base>/fastetcd-test-<run>`, on the first base that is writable:
/// `$FASTETCD_TEST_DIR`, `/results` (the standard's artifact dir),
/// `$TMPDIR`, `/tmp`. A scratch image has no `/tmp` of its own.
fn work_dir(run_id: &str) -> anyhow::Result<PathBuf> {
    let mut bases: Vec<PathBuf> = Vec::new();
    for k in ["FASTETCD_TEST_DIR", "TMPDIR"] {
        if let Ok(v) = std::env::var(k) {
            if !v.is_empty() {
                bases.push(v.into());
            }
        }
    }
    bases.insert(bases.len().min(1), "/results".into());
    bases.push("/tmp".into());
    let mut tried = Vec::new();
    for b in bases {
        let d = b.join(format!("fastetcd-test-{run_id}"));
        match std::fs::create_dir_all(&d).and_then(|_| probe(&d)) {
            Ok(()) => return Ok(d),
            Err(e) => tried.push(format!("{}: {e}", d.display())),
        }
    }
    anyhow::bail!("no writable directory for the test's members ({})", tried.join("; "))
}

fn probe(d: &Path) -> std::io::Result<()> {
    let f = d.join(".probe");
    std::fs::write(&f, b"x")?;
    std::fs::remove_file(f)
}
