use std::path::PathBuf;

use clap::Parser;

use fastetcd_migrate::migrate_snapshot;

#[derive(Debug, Parser)]
#[command(name = "fastetcd-migrate", version, about)]
struct Args {
    /// Path to the etcd v3 snapshot (BoltDB `.db` file).
    #[arg(long)]
    from: PathBuf,

    /// Path to the fastetcd data directory to populate.
    #[arg(long)]
    to: PathBuf,

    /// Overwrite an existing target.
    #[arg(long, default_value_t = false)]
    force: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let summary = migrate_snapshot(&args.from, &args.to, args.force).await?;
    tracing::info!(
        scanned = summary.scanned,
        tombstones = summary.tombstones,
        imported = summary.imported,
        revision_after = summary.revision_after,
        from = %args.from.display(),
        to = %args.to.display(),
        "migration complete"
    );
    Ok(())
}
