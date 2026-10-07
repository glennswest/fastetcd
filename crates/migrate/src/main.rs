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
        leases = summary.leases,
        users = summary.users,
        roles = summary.roles,
        auth_enabled = summary.auth_enabled,
        from = %args.from.display(),
        to = %args.to.display(),
        "migration complete"
    );
    if summary.keys_lease_dropped > 0 {
        tracing::warn!(
            keys = summary.keys_lease_dropped,
            "keys named a lease the snapshot does not hold (expired or revoked in etcd): \
             imported without a lease, so they will not expire"
        );
    }
    Ok(())
}
