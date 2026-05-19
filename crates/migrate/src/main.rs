use std::path::PathBuf;

use clap::Parser;

use fastetcd_migrate::{migrate_snapshot_with_mode, MigrationMode};

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

    /// Preserve every record's MVCC revisions instead of importing
    /// only the latest value per key. Larger output but `Range(rev)`
    /// and `Watch(start_rev)` behave the same as on the source.
    #[arg(long, default_value_t = false)]
    preserve_revisions: bool,
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
    let mode = if args.preserve_revisions {
        MigrationMode::PreserveRevisions
    } else {
        MigrationMode::LatestOnly
    };
    let summary =
        migrate_snapshot_with_mode(&args.from, &args.to, args.force, mode).await?;
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
