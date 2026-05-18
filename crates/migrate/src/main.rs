use clap::Parser;

/// Import an etcd v3 BoltDB snapshot into a fastetcd data directory.
///
/// Reads buckets `key`, `lease`, `auth`, `meta` from the BoltDB file,
/// replays MVCC entries preserving revisions, then writes a Raft snapshot
/// at the restored revision so a fresh cluster starts with that state.
#[derive(Debug, Parser)]
#[command(name = "fastetcd-migrate", version, about)]
struct Args {
    /// Path to the etcd v3 snapshot (BoltDB `.db` file).
    #[arg(long)]
    from: String,

    /// Path to the fastetcd data directory to populate (created if missing).
    #[arg(long)]
    to: String,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    tracing::info!(from = %args.from, to = %args.to, "migration not yet implemented");
    anyhow::bail!("migration tool is a skeleton — implementation in task #9");
}
