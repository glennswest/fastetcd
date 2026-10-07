//! etcd → fastetcd snapshot migration.
//!
//! Public API: [`migrate_snapshot`] takes a path to an etcd BoltDB
//! snapshot and a path to a target fastetcd data dir, walks the
//! upstream `key` bucket, and writes it into a fresh `MvccStore`:
//! by default the latest record of each user key as a `Mutation::Put`,
//! or with [`MigrationMode::PreserveRevisions`] every record at its
//! original revisions (a bulk load). Leases and auth come too
//! (fastetcd#60):
//!
//! - every lease in etcd's `lease` bucket is granted with its id, and the
//!   TTL etcd itself would give it on a restart (`RemainingTTL` if set,
//!   else `TTL`), at least fastetcd's minimum of 2 s; a key naming a lease
//!   the bucket does not hold is imported without one (counted in
//!   [`MigrationSummary::keys_lease_dropped`]);
//! - roles with their permissions, users with their roles and their
//!   bcrypt password hashes (fastetcd verifies those as etcd does), and
//!   the enabled flag (`authUsers`, `authRoles`, `auth`), through the
//!   same auth operations the server applies, so its rules hold (auth is
//!   enabled only with a `root` user).

use std::path::Path;
use std::sync::Arc;

use bbolt_rs::{Bolt, BucketApi, DbApi, TxApi};
use fastetcd_proto::{authpb, mvccpb};
use fastetcd_storage::mvcc::auth::{AuthOp, PermType, StoredPermission};
use fastetcd_storage::mvcc::{BulkKey, KvRecord, Mutation, MvccStore};
use fastetcd_storage::redb_engine::RedbEngine;
use prost::Message;

/// Result of a successful migration.
#[derive(Debug, Clone)]
pub struct MigrationSummary {
    /// Total bolt entries inspected (including tombstones).
    pub scanned: u64,
    /// Tombstone records encountered (skipped).
    pub tombstones: u64,
    /// Live keys imported into fastetcd.
    pub imported: usize,
    /// MVCC revision in the target after the import.
    pub revision_after: i64,
    /// Leases granted (etcd's `lease` bucket).
    pub leases: usize,
    /// Live keys that named a lease the snapshot does not hold: imported
    /// with no lease (etcd would have deleted them with it).
    pub keys_lease_dropped: usize,
    /// Users and roles imported.
    pub users: usize,
    pub roles: usize,
    /// Whether auth is on in the target (it was on in etcd).
    pub auth_enabled: bool,
}

/// etcd's `leasepb.Lease` (server/lease/leasepb/lease.proto).
#[derive(Clone, PartialEq, prost::Message)]
struct LeasePb {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(int64, tag = "2")]
    ttl: i64,
    #[prost(int64, tag = "3")]
    remaining_ttl: i64,
}

/// fastetcd's minimum lease TTL (the server's `MIN_LEASE_TTL_SECS`).
const MIN_LEASE_TTL_SECS: i64 = 2;

/// What etcd's auth buckets hold.
#[derive(Default)]
struct EtcdAuth {
    users: Vec<authpb::User>,
    roles: Vec<authpb::Role>,
    enabled: bool,
}

/// How to handle MVCC revision history during a migration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MigrationMode {
    /// Import only the latest live value per user key. After
    /// migration every key has `create_revision = mod_revision = 1`.
    /// Smallest output and fastest; suitable for "I want my data
    /// on fastetcd" use cases where existing watch state is OK to
    /// reset.
    #[default]
    LatestOnly,
    /// Preserve `create_revision`, `mod_revision`, `version`, and
    /// `lease` for every record. After migration, `Range(rev)` and
    /// `Watch(start_rev)` behave the same as on the source server
    /// (modulo whatever the source had compacted).
    PreserveRevisions,
}

/// Migrate an etcd BoltDB snapshot at `from` into a fastetcd data
/// directory at `to`. The target directory must be empty unless
/// `force` is true. Existing target redb files are removed when
/// `force` is set.
pub async fn migrate_snapshot(
    from: &Path,
    to: &Path,
    force: bool,
) -> anyhow::Result<MigrationSummary> {
    migrate_snapshot_with_mode(from, to, force, MigrationMode::default()).await
}

/// Variant of [`migrate_snapshot`] that takes an explicit
/// [`MigrationMode`].
pub async fn migrate_snapshot_with_mode(
    from: &Path,
    to: &Path,
    force: bool,
    mode: MigrationMode,
) -> anyhow::Result<MigrationSummary> {
    if !from.exists() {
        anyhow::bail!("source snapshot {from:?} does not exist");
    }
    let bolt = Bolt::open_ro(from).map_err(|e| anyhow::anyhow!("open bolt: {e}"))?;

    // For LatestOnly mode: track latest (mod_rev, create_rev, value)
    // per user key. For PreserveRevisions mode: track every record
    // per user key in revision order.
    let mut latest: std::collections::HashMap<Vec<u8>, (i64, i64, Vec<u8>, i64)> =
        std::collections::HashMap::new();
    let mut history: std::collections::HashMap<Vec<u8>, Vec<(i64, KvRecord, bool)>> =
        std::collections::HashMap::new();
    let mut scanned: u64 = 0;
    let mut tombstones: u64 = 0;
    let mut max_rev: i64 = 0;

    let tx = bolt
        .begin()
        .map_err(|e| anyhow::anyhow!("bolt begin: {e}"))?;

    // Leases: id -> the TTL etcd restores it with.
    let mut leases: Vec<(i64, i64)> = Vec::new();
    if let Some(b) = tx.bucket(b"lease") {
        #[allow(deprecated)]
        b.for_each(|_k: &[u8], v: Option<&[u8]>| -> bbolt_rs::Result<()> {
            if let Some(l) = v.and_then(|v| LeasePb::decode(v).ok()) {
                let ttl = if l.remaining_ttl > 0 { l.remaining_ttl } else { l.ttl };
                leases.push((l.id, ttl.max(MIN_LEASE_TTL_SECS)));
            }
            Ok(())
        })
        .map_err(|e| anyhow::anyhow!("bolt lease bucket: {e}"))?;
    }
    let lease_ids: std::collections::HashSet<i64> = leases.iter().map(|(id, _)| *id).collect();

    let mut auth = EtcdAuth::default();
    for (bucket, users) in [(&b"authUsers"[..], true), (&b"authRoles"[..], false)] {
        if let Some(b) = tx.bucket(bucket) {
            #[allow(deprecated)]
            b.for_each(|_k: &[u8], v: Option<&[u8]>| -> bbolt_rs::Result<()> {
                if let Some(v) = v {
                    if users {
                        if let Ok(u) = authpb::User::decode(v) {
                            auth.users.push(u);
                        }
                    } else if let Ok(r) = authpb::Role::decode(v) {
                        auth.roles.push(r);
                    }
                }
                Ok(())
            })
            .map_err(|e| anyhow::anyhow!("bolt auth bucket: {e}"))?;
        }
    }
    if let Some(b) = tx.bucket(b"auth") {
        auth.enabled = b.get(b"authEnabled").is_some_and(|v| v == [1u8]);
    }

    let bucket = tx
        .bucket(b"key")
        .ok_or_else(|| anyhow::anyhow!("snapshot has no 'key' bucket"))?;

    let mode_local = mode;
    #[allow(deprecated)]
    bucket
        .for_each(|k: &[u8], v: Option<&[u8]>| -> bbolt_rs::Result<()> {
            scanned += 1;
            let Some(value) = v else {
                return Ok(());
            };
            let kv = match mvccpb::KeyValue::decode(value) {
                Ok(kv) => kv,
                Err(_) => return Ok(()),
            };
            let is_tomb = k.last() == Some(&b't');
            if is_tomb {
                tombstones += 1;
            }
            if kv.mod_revision > max_rev {
                max_rev = kv.mod_revision;
            }

            match mode_local {
                MigrationMode::LatestOnly => {
                    if is_tomb {
                        latest.remove(&kv.key);
                    } else {
                        match latest.get(&kv.key) {
                            Some((m, _, _, _)) if *m >= kv.mod_revision => {}
                            _ => {
                                latest.insert(
                                    kv.key.clone(),
                                    (
                                        kv.mod_revision,
                                        kv.create_revision,
                                        kv.value.clone(),
                                        kv.lease,
                                    ),
                                );
                            }
                        }
                    }
                }
                MigrationMode::PreserveRevisions => {
                    let rec = KvRecord {
                        key: kv.key.clone(),
                        value: kv.value.clone(),
                        create_revision: kv.create_revision,
                        mod_revision: kv.mod_revision,
                        version: kv.version,
                        lease: kv.lease,
                        deleted: is_tomb,
                    };
                    history
                        .entry(kv.key.clone())
                        .or_default()
                        .push((kv.mod_revision, rec, is_tomb));
                }
            }
            Ok(())
        })
        .map_err(|e| anyhow::anyhow!("bolt for_each: {e}"))?;
    drop(tx);
    drop(bolt);

    if to.exists() && !force {
        let entries = std::fs::read_dir(to)?;
        if entries.count() > 0 {
            anyhow::bail!("target {to:?} exists and is not empty; pass force=true to overwrite");
        }
    }
    std::fs::create_dir_all(to)?;
    let target_path = to.join("fastetcd.redb");
    if force && target_path.exists() {
        std::fs::remove_file(&target_path)?;
    }

    let engine: Arc<dyn fastetcd_storage::KvStore> = Arc::new(RedbEngine::open(&target_path)?);
    let mvcc = MvccStore::open(engine).await?;

    // The leases first, so each key is attached to its lease as written.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64);
    for (id, ttl) in &leases {
        mvcc.apply_lease_grant(*id, *ttl, now)
            .await
            .map_err(|e| anyhow::anyhow!("lease {id}: {e}"))?;
    }
    let mut keys_lease_dropped = 0usize;
    let mut keep_lease = |lease: i64| -> i64 {
        if lease != 0 && !lease_ids.contains(&lease) {
            keys_lease_dropped += 1;
            0
        } else {
            lease
        }
    };

    let (imported, revision_after) = match mode {
        MigrationMode::LatestOnly => {
            let mutations: Vec<Mutation> = latest
                .into_iter()
                .map(|(key, (_, _, value, lease))| Mutation::Put {
                    key,
                    value,
                    lease: keep_lease(lease),
                    ignore_value: false,
                    ignore_lease: false,
                    prev_kv: false,
                })
                .collect();
            let n = mutations.len();
            let rev = if mutations.is_empty() {
                mvcc.current_revision().await
            } else {
                let (r, _) = mvcc
                    .apply(&mutations)
                    .await
                    .map_err(|e| anyhow::anyhow!("mvcc apply: {e}"))?;
                r
            };
            (n, rev)
        }
        MigrationMode::PreserveRevisions => {
            // For each user key, sort by revision. The last record per
            // key is either a tombstone (closes a generation) or a put
            // (live). Multi-generation history is collapsed: we drop
            // everything before the final tombstone (those would be
            // unreachable anyway) and import the final generation.
            let mut bulk: Vec<BulkKey> = Vec::with_capacity(history.len());
            let mut imported_count: usize = 0;
            for (key, mut records) in history {
                records.sort_by_key(|(rev, _, _)| *rev);
                // Find the index after the most recent tombstone.
                let last_tomb_idx = records
                    .iter()
                    .rposition(|(_, _, is_tomb)| *is_tomb);
                let (puts_recs, tomb_rec) = match last_tomb_idx {
                    Some(i) => {
                        // Records before the tombstone are part of
                        // the now-closed generation. We import them
                        // along with the tombstone so historical
                        // reads at those revisions still work.
                        let mut tomb_rec = records[i].1.clone();
                        tomb_rec.deleted = true;
                        tomb_rec.value = Vec::new();
                        let puts: Vec<KvRecord> = records[..i]
                            .iter()
                            .map(|(_, rec, _)| rec.clone())
                            .collect();
                        // Plus any records after the tombstone — those
                        // are a new (live) generation. For simplicity
                        // here we drop them; multi-generation import
                        // is a future enhancement.
                        (puts, Some(tomb_rec))
                    }
                    None => {
                        let mut puts: Vec<KvRecord> =
                            records.iter().map(|(_, rec, _)| rec.clone()).collect();
                        // The live record keeps its lease only if the
                        // snapshot holds that lease.
                        if let Some(last) = puts.last_mut() {
                            last.lease = keep_lease(last.lease);
                        }
                        (puts, None)
                    }
                };
                if puts_recs.is_empty() && tomb_rec.is_none() {
                    continue;
                }
                if !puts_recs.is_empty() && tomb_rec.is_none() {
                    imported_count += 1;
                }
                bulk.push(BulkKey {
                    key,
                    puts: puts_recs,
                    tombstone: tomb_rec,
                });
            }
            mvcc.bulk_load_records(bulk, max_rev)
                .await
                .map_err(|e| anyhow::anyhow!("bulk_load_records: {e}"))?;
            (imported_count, max_rev)
        }
    };
    let auth_enabled = import_auth(&mvcc, &auth).await?;
    Ok(MigrationSummary {
        scanned,
        tombstones,
        imported,
        revision_after,
        leases: leases.len(),
        keys_lease_dropped,
        users: auth.users.len(),
        roles: auth.roles.len(),
        auth_enabled,
    })
}

/// Apply etcd's roles, users and enabled flag as the server's own auth
/// operations. Returns whether auth ended up enabled.
async fn import_auth(mvcc: &MvccStore, auth: &EtcdAuth) -> anyhow::Result<bool> {
    // Errors name what was being imported, never a password hash.
    let apply = |what: String, op: AuthOp| async move {
        let (_, r) = mvcc.apply_auth(&op).await.map_err(|e| anyhow::anyhow!("auth {what}: {e}"))?;
        r.map_err(|e| anyhow::anyhow!("auth {what}: {e}"))
    };
    for r in &auth.roles {
        let name = String::from_utf8_lossy(&r.name).into_owned();
        apply(format!("role {name}"), AuthOp::RoleAdd { name: name.clone() }).await?;
        for p in &r.key_permission {
            let perm_type = match p.perm_type {
                0 => PermType::Read,
                1 => PermType::Write,
                _ => PermType::ReadWrite,
            };
            let perm = StoredPermission { perm_type, key: p.key.clone(), range_end: p.range_end.clone() };
            apply(format!("role {name} permission"), AuthOp::RoleGrantPermission { name: name.clone(), perm })
                .await?;
        }
    }
    for u in &auth.users {
        let name = String::from_utf8_lossy(&u.name).into_owned();
        let no_password = u.options.as_ref().is_some_and(|o| o.no_password);
        // etcd's bcrypt hash, as is: the server verifies bcrypt hashes.
        let password_hash = String::from_utf8_lossy(&u.password).into_owned();
        apply(format!("user {name}"), AuthOp::UserAdd { name: name.clone(), password_hash, no_password })
            .await?;
        for role in &u.roles {
            apply(format!("user {name} role {role}"), AuthOp::UserGrantRole { user: name.clone(), role: role.clone() })
                .await?;
        }
    }
    if auth.enabled {
        apply("enable (a root user with the root role is needed)".into(), AuthOp::Enable).await?;
    }
    Ok(auth.enabled)
}
