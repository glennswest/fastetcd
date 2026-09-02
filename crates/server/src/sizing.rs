//! How big does the data volume need to be?
//!
//! "Make it big enough" is not an answer an operator can provision
//! against, and getting it wrong is not a soft failure: a volume that
//! fills deadlocks the store (fastetcd#14). This module turns a cluster
//! shape — node count, pod density — into a recommended volume size,
//! and shows the arithmetic so the estimate can be argued with rather
//! than trusted.
//!
//! # What actually occupies the volume
//!
//! The mistake is to size for the data. A fastetcd volume holds four
//! things, and the data is the smallest interesting one:
//!
//! 1. **The database** — live objects plus the MVCC history that has
//!    accumulated since the last compaction, times the engine's
//!    copy-on-write overhead.
//! 2. **A raft snapshot** — a *full serialized copy* of the database.
//!    This is the term people forget, and it roughly doubles the floor.
//! 3. **The raft log** between snapshots.
//! 4. **Upgrade safety backups** — another full copy of the database
//!    each, written on a version change and rolled off by retention.
//!
//! # Where the per-object numbers come from
//!
//! These are Kubernetes objects as an apiserver actually stores them,
//! not guesses at a serialized struct:
//!
//! - A `Node` is the big per-node object, and `status.images` is why:
//!   the kubelet reports up to `--node-status-max-images` (default 50)
//!   images, each with its names and size. With the `Lease` in
//!   `kube-node-lease` (small, but renewed every 10s) and `CSINode`,
//!   10 KiB per node is a fair working figure.
//! - A `Pod` with a full status, conditions and container statuses runs
//!   ~8 KiB. Density is the multiplier that dominates the estimate:
//!   `max-pods` defaults to 110, but 30 is a realistic average.
//! - Events have a TTL (1h by default) rather than a size, so they
//!   occupy a rolling window that scales with cluster activity.
//! - Everything not proportional to nodes — CRDs, RBAC, ServiceAccounts,
//!   ConfigMaps, Secrets, API priority-and-fairness config — is a fixed
//!   floor a small cluster pays as much as a large one.
//!
//! Every constant below is stated as a named value so an operator who
//! knows their own cluster can substitute it.

/// Bytes stored per node: `Node` object (including up to 50 reported
/// images in `status.images`), its `Lease`, and its `CSINode`.
pub const PER_NODE_BYTES: u64 = 10 * 1024;

/// Bytes stored per pod: spec, status, conditions, container statuses,
/// plus that pod's share of `EndpointSlice` membership.
pub const PER_POD_BYTES: u64 = 8 * 1024;

/// Pods per node when the caller doesn't say. `max-pods` defaults to
/// 110; real clusters average far lower, and sizing for the theoretical
/// maximum inflates every estimate.
pub const DEFAULT_PODS_PER_NODE: u64 = 30;

/// The part of the store that does not scale with the cluster: CRDs,
/// RBAC, ServiceAccounts, ConfigMaps, Secrets, apiserver leases, API
/// priority-and-fairness configuration.
pub const FIXED_CLUSTER_BYTES: u64 = 32 * 1024 * 1024;

/// Rolling window of `Event` objects, per node. Events expire on a TTL
/// (1h by default) rather than accumulating forever, so this is a
/// steady-state window, not growth.
pub const EVENT_WINDOW_BYTES_PER_NODE: u64 = 64 * 1024;

/// MVCC history held between compactions, as a fraction of live data.
/// The apiserver compacts every 5 minutes by default; node leases alone
/// write ~6 revisions per node per minute in that window.
pub const HISTORY_FRACTION_PCT: u64 = 30;

/// Engine overhead: a copy-on-write B-tree that grows its file by
/// doubling holds more file than live pages. 1.6x is what the redb
/// engine settles around in practice.
pub const COW_OVERHEAD_PCT: u64 = 160;

/// Average raft log entry, for turning `--snapshot-count` into bytes.
/// Lease renewals are small and pod updates are not; 2 KiB is the
/// middle of what a Kubernetes workload produces.
pub const AVG_LOG_ENTRY_BYTES: u64 = 2 * 1024;

/// The volume must have room for the store to sit below its high-water
/// mark, or reclaim runs continuously and never gets ahead.
pub const HIGH_WATER_PCT: u64 = 80;

/// The shape of the cluster this store has to serve.
#[derive(Debug, Clone, Copy)]
pub struct ClusterShape {
    pub nodes: u64,
    pub pods_per_node: u64,
    /// `--snapshot-count`: raft log entries between snapshots.
    pub snapshot_count: u64,
    /// `--max-snapshots`: retained snapshots, each a full copy.
    pub max_snapshots: u64,
    /// `--upgrade-backup-retain`: retained safety backups, each a full
    /// copy of the database. They exist only after a version change, so
    /// the estimate reports both the steady state and the peak.
    pub upgrade_backup_retain: u64,
}

impl Default for ClusterShape {
    fn default() -> Self {
        Self {
            nodes: 1,
            pods_per_node: DEFAULT_PODS_PER_NODE,
            snapshot_count: 5000,
            max_snapshots: 1,
            upgrade_backup_retain: 2,
        }
    }
}

/// A sizing estimate, broken into the terms that produced it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Estimate {
    /// Live Kubernetes objects.
    pub live_bytes: u64,
    /// MVCC history retained between compactions.
    pub history_bytes: u64,
    /// The database file: (live + history) with engine overhead.
    pub db_bytes: u64,
    /// Retained raft snapshots — full serialized copies.
    pub snapshot_bytes: u64,
    /// The raft log between snapshots.
    pub log_bytes: u64,
    /// One retained upgrade safety backup.
    pub backup_bytes: u64,
    /// Everything on the volume in normal operation (one backup).
    pub working_set_bytes: u64,
    /// Working set with every retained backup present — the peak, right
    /// after an upgrade.
    pub peak_bytes: u64,
    /// Volume needed to keep the peak below the high-water mark.
    pub recommended_volume_bytes: u64,
    /// `recommended_volume_bytes` rounded up to the next power of two,
    /// which is how volumes actually get provisioned.
    pub provision_bytes: u64,
}

/// Estimate the volume a cluster of this shape needs.
pub fn estimate(shape: ClusterShape) -> Estimate {
    let nodes = shape.nodes.max(1);
    let pods = nodes.saturating_mul(shape.pods_per_node);

    let live_bytes = FIXED_CLUSTER_BYTES
        + nodes.saturating_mul(PER_NODE_BYTES)
        + pods.saturating_mul(PER_POD_BYTES)
        + nodes.saturating_mul(EVENT_WINDOW_BYTES_PER_NODE);

    let history_bytes = live_bytes.saturating_mul(HISTORY_FRACTION_PCT) / 100;
    let logical = live_bytes.saturating_add(history_bytes);

    let db_bytes = logical.saturating_mul(COW_OVERHEAD_PCT) / 100;
    // A snapshot is the serialized state, so it pays no engine overhead
    // — but there is one per retained snapshot.
    let snapshot_bytes = logical.saturating_mul(shape.max_snapshots.max(1));
    let log_bytes = shape.snapshot_count.saturating_mul(AVG_LOG_ENTRY_BYTES);
    let backup_bytes = db_bytes;

    let base = db_bytes
        .saturating_add(snapshot_bytes)
        .saturating_add(log_bytes);
    // Steady state after an upgrade holds one backup; the peak holds
    // every one retention allows.
    let working_set_bytes = base.saturating_add(backup_bytes.min(db_bytes));
    let peak_bytes =
        base.saturating_add(backup_bytes.saturating_mul(shape.upgrade_backup_retain));

    let recommended_volume_bytes = peak_bytes.saturating_mul(100) / HIGH_WATER_PCT;

    Estimate {
        live_bytes,
        history_bytes,
        db_bytes,
        snapshot_bytes,
        log_bytes,
        backup_bytes,
        working_set_bytes,
        peak_bytes,
        recommended_volume_bytes,
        provision_bytes: round_up_pow2(recommended_volume_bytes),
    }
}

/// Round up to the next power of two, floored at 256 MiB — below that
/// the fixed cluster floor plus a snapshot leaves no useful headroom.
fn round_up_pow2(bytes: u64) -> u64 {
    const FLOOR: u64 = 256 * 1024 * 1024;
    let mut v = FLOOR;
    while v < bytes {
        match v.checked_mul(2) {
            Some(next) => v = next,
            None => return v,
        }
    }
    v
}

/// Render a byte count the way an operator reads one.
pub fn human(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = bytes as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit < UNITS.len() - 1 {
        v /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{v:.1} {}", UNITS[unit])
    }
}

/// Print the estimate with its arithmetic, for `fastetcd sizing`.
pub fn report(shape: ClusterShape) -> String {
    let e = estimate(shape);
    let mut out = String::new();
    out.push_str(&format!(
        "Cluster shape: {} node(s), {} pods/node ({} pods)\n",
        shape.nodes,
        shape.pods_per_node,
        shape.nodes * shape.pods_per_node
    ));
    out.push_str(&format!(
        "Store settings: --snapshot-count {}, --max-snapshots {}, \
         --upgrade-backup-retain {}\n\n",
        shape.snapshot_count, shape.max_snapshots, shape.upgrade_backup_retain
    ));
    out.push_str("What occupies the volume:\n");
    out.push_str(&format!(
        "  live objects              {:>10}   ({} fixed + {}/node + {}/pod)\n",
        human(e.live_bytes),
        human(FIXED_CLUSTER_BYTES),
        human(PER_NODE_BYTES),
        human(PER_POD_BYTES)
    ));
    out.push_str(&format!(
        "  MVCC history             +{:>10}   ({HISTORY_FRACTION_PCT}% of live, between compactions)\n",
        human(e.history_bytes)
    ));
    out.push_str(&format!(
        "  = database file           {:>10}   (x{}% copy-on-write overhead)\n",
        human(e.db_bytes),
        COW_OVERHEAD_PCT
    ));
    out.push_str(&format!(
        "  raft snapshot(s)         +{:>10}   (a FULL copy of the database)\n",
        human(e.snapshot_bytes)
    ));
    out.push_str(&format!(
        "  raft log                 +{:>10}   ({} entries x {})\n",
        human(e.log_bytes),
        shape.snapshot_count,
        human(AVG_LOG_ENTRY_BYTES)
    ));
    out.push_str(&format!(
        "  upgrade backup           +{:>10}   (another full copy, after a version change)\n",
        human(e.backup_bytes)
    ));
    out.push_str(&format!(
        "\n  working set               {:>10}\n",
        human(e.working_set_bytes)
    ));
    out.push_str(&format!(
        "  peak (all {} backups)      {:>10}\n",
        shape.upgrade_backup_retain,
        human(e.peak_bytes)
    ));
    out.push_str(&format!(
        "\nMinimum volume              {:>10}   (peak below the {HIGH_WATER_PCT}% high-water mark)\n",
        human(e.recommended_volume_bytes)
    ));
    out.push_str(&format!(
        "PROVISION                   {:>10}\n",
        human(e.provision_bytes)
    ));
    out.push_str(
        "\nThe snapshot term is the one that surprises people: it is a full copy\n\
         of the database, so the floor is roughly twice the data before any\n\
         headroom. Sizing for the data alone is how a volume fills.\n",
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(nodes: u64) -> ClusterShape {
        ClusterShape {
            nodes,
            ..ClusterShape::default()
        }
    }

    #[test]
    fn a_hundred_node_cluster_wants_a_gigabyte() {
        let e = estimate(shape(100));
        // Live data for 100 nodes at 30 pods each lands around 65 MiB:
        // 32 fixed + 1 node + 23 pods + 6 events.
        assert!(
            (60 * 1024 * 1024..80 * 1024 * 1024).contains(&e.live_bytes),
            "live {} outside the expected band",
            human(e.live_bytes)
        );
        // And the provisioned volume is 1 GiB — which is what the
        // snapshot-plus-backup terms do to a 65 MiB dataset.
        assert_eq!(e.provision_bytes, 1024 * 1024 * 1024, "{}", report(shape(100)));
    }

    #[test]
    fn the_snapshot_term_roughly_doubles_the_floor() {
        // Sizing for the database alone is the mistake this module
        // exists to prevent.
        let e = estimate(shape(100));
        assert!(
            e.snapshot_bytes * 2 > e.db_bytes,
            "a snapshot ({}) must be the same order as the database ({})",
            human(e.snapshot_bytes),
            human(e.db_bytes)
        );
        assert!(
            e.working_set_bytes > e.db_bytes * 2,
            "the working set {} must dwarf the database {}",
            human(e.working_set_bytes),
            human(e.db_bytes)
        );
    }

    #[test]
    fn a_single_node_cluster_still_pays_the_fixed_floor() {
        let e = estimate(shape(1));
        assert!(e.live_bytes > FIXED_CLUSTER_BYTES);
        // Small clusters are dominated by the fixed floor, so the
        // recommendation must not collapse to something unusable — the
        // 64 MiB volume in fastetcd#14 is exactly what this prevents.
        assert!(
            e.provision_bytes >= 256 * 1024 * 1024,
            "even one node needs {} provisioned, got {}",
            human(256 * 1024 * 1024),
            human(e.provision_bytes)
        );
    }

    /// The ladder, pinned. Without this the model's *shape* is implied
    /// rather than stated, and "does a 1000-node cluster really need
    /// more than a 100-node one?" is a fair question to have to answer
    /// from the code.
    #[test]
    fn the_sizing_ladder_is_what_the_docs_claim() {
        let ladder: Vec<(u64, u64, u64)> = [1u64, 10, 100, 500, 1000, 5000]
            .iter()
            .map(|n| {
                let e = estimate(shape(*n));
                (*n, e.recommended_volume_bytes, e.provision_bytes)
            })
            .collect();

        let mib = 1024 * 1024;
        let gib = 1024 * mib;
        let provisioned: Vec<u64> = ladder.iter().map(|(_, _, p)| *p).collect();
        assert_eq!(
            provisioned,
            vec![512 * mib, 512 * mib, gib, 2 * gib, 4 * gib, 16 * gib],
            "sizing ladder changed: {ladder:?}"
        );

        // 100 and 1000 nodes are emphatically not the same size.
        let hundred = estimate(shape(100)).recommended_volume_bytes;
        let thousand = estimate(shape(1000)).recommended_volume_bytes;
        assert!(
            thousand > hundred * 4,
            "1000 nodes ({}) must dwarf 100 nodes ({})",
            human(thousand),
            human(hundred)
        );
    }

    /// Small clusters *do* land in the same bucket, and that is the
    /// model telling the truth rather than failing: below ~50 nodes the
    /// estimate is fixed cluster overhead multiplied by the snapshot and
    /// backup terms, so there is nothing to shave by counting nodes.
    #[test]
    fn small_clusters_are_dominated_by_fixed_cost_not_node_count() {
        let one = estimate(shape(1));
        let ten = estimate(shape(10));
        assert_eq!(
            one.provision_bytes, ten.provision_bytes,
            "1 and 10 nodes provision the same; the difference is {} of live data",
            human(ten.live_bytes - one.live_bytes)
        );
        // The node-proportional part is a rounding error at this size.
        let variable = ten.live_bytes - FIXED_CLUSTER_BYTES;
        assert!(
            variable < FIXED_CLUSTER_BYTES / 8,
            "10 nodes' variable term {} should be dwarfed by the {} floor",
            human(variable),
            human(FIXED_CLUSTER_BYTES)
        );
        // The crossover — where nodes start to matter more than the
        // floor — is around 100 nodes at default density.
        let hundred = estimate(shape(100));
        assert!(
            hundred.live_bytes - FIXED_CLUSTER_BYTES > FIXED_CLUSTER_BYTES / 2,
            "by 100 nodes the variable term {} should rival the floor",
            human(hundred.live_bytes - FIXED_CLUSTER_BYTES)
        );
    }

    #[test]
    fn the_estimate_grows_with_the_cluster() {
        let sizes: Vec<u64> = [1u64, 10, 100, 500, 1000]
            .iter()
            .map(|n| estimate(shape(*n)).recommended_volume_bytes)
            .collect();
        for pair in sizes.windows(2) {
            assert!(pair[1] > pair[0], "estimate must be monotonic: {sizes:?}");
        }
    }

    #[test]
    fn pod_density_dominates_the_variable_term() {
        // Pods, not nodes, are what a Kubernetes datastore is mostly
        // made of — which is why `--pods-per-node` exists and why
        // sizing from node count alone under-reads a dense cluster.
        let sparse = estimate(shape(100));
        let dense = estimate(ClusterShape {
            nodes: 100,
            pods_per_node: 110,
            ..ClusterShape::default()
        });

        // The part that scales more than triples with density...
        let sparse_var = sparse.live_bytes - FIXED_CLUSTER_BYTES;
        let dense_var = dense.live_bytes - FIXED_CLUSTER_BYTES;
        assert!(
            dense_var > sparse_var * 3,
            "variable term should scale with density: {} -> {}",
            human(sparse_var),
            human(dense_var)
        );

        // ...while the total does not even double, because the fixed
        // cluster floor is half the estimate at this size. That floor is
        // why a small cluster cannot be given a small volume.
        assert!(
            dense.live_bytes < sparse.live_bytes * 2,
            "the fixed floor should damp the total: {} -> {}",
            human(sparse.live_bytes),
            human(dense.live_bytes)
        );
        assert!(
            FIXED_CLUSTER_BYTES > sparse.live_bytes / 3,
            "the fixed floor is a large share of a 100-node estimate"
        );
    }

    #[test]
    fn rounding_goes_up_to_a_provisionable_size() {
        assert_eq!(round_up_pow2(1), 256 * 1024 * 1024);
        assert_eq!(round_up_pow2(300 * 1024 * 1024), 512 * 1024 * 1024);
        assert_eq!(round_up_pow2(512 * 1024 * 1024), 512 * 1024 * 1024);
        assert_eq!(round_up_pow2(513 * 1024 * 1024), 1024 * 1024 * 1024);
    }
}
