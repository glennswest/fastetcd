//! The `cluster_id` reported in every `ResponseHeader` (fastetcd#17).
//!
//! etcd clients use it as a safety pin: a client records the id it
//! first saw on an endpoint and compares it on every response. If the
//! endpoint is later repointed at a different store, the id changes and
//! the client drains and reconnects instead of merging one cluster's
//! data into another's caches. That only works if the id is distinct
//! between deployments and stable for a given store. It used to be `1`
//! everywhere.
//!
//! **This member's own id** is chosen once, on the first start of a
//! store, and persisted in the data directory. In order:
//!
//! 1. An explicit `--cluster-id` (never 0).
//! 2. The persisted id, if this store has one.
//! 3. A store that existed before ids were persisted keeps `1`, the id
//!    it has always reported.
//! 4. A new store: FNV-1a-64 of `--initial-cluster-token`, masked to 63
//!    bits, never 0.
//! 5. Otherwise a random 63-bit id, never 0 (fastetcd#38; was `1`).
//!
//! **The cluster's id is replicated** (fastetcd#38). Once a cluster has
//! none and every member answers as 1.33 or later, its leader proposes
//! its own id in a `ClusterId` log entry; the first one applied is the
//! cluster's, in `mvcc_meta`, carried by snapshots. Every member then
//! reports it ([`follow`]), persisting it as its own (so a restart
//! starts with it), with a warning and `fastetcd_cluster_id_adoptions`
//! when it differs from what it reported. A member started with an
//! explicit `--cluster-id` that contradicts the cluster's refuses to
//! start ([`reconcile`]), and stops if it learns of it while running: an
//! operator error. A mismatch an upgrade or a restore can cause is never
//! refused (owner's decision on #38). An existing cluster keeps its
//! leader's id.

use std::sync::Arc;

use fastetcd_storage::{KvStore, WriteBatch, WriteOptions};

/// Node-local metadata that must not travel in a raft snapshot.
pub const TABLE_NODE_META: &str = "node_meta";
const KEY_CLUSTER_ID: &[u8] = b"cluster_id";

/// The id reported when nothing distinguishes the deployment, and the
/// id every store reported before it was persisted.
pub const LEGACY_CLUSTER_ID: u64 = 1;

/// Where the cluster id came from, for the startup log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// `--cluster-id`.
    Flag,
    /// Persisted by an earlier start of this store.
    Persisted,
    /// A store from before ids were persisted: kept at the legacy id.
    Legacy,
    /// Derived from `--initial-cluster-token`.
    Token,
    /// Nothing to derive it from: random (fastetcd#38).
    Random,
}

/// A random 63-bit id, never 0, from the process's random hash seed (the
/// OS's randomness; no extra dependency).
pub fn random_id() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    loop {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u64(std::process::id() as u64);
        let id = h.finish() & (u64::MAX >> 1);
        if id != 0 {
            return id;
        }
    }
}

/// FNV-1a-64 of `token`, masked to 63 bits (so it survives a signed
/// 64-bit representation), and never 0 (etcd treats 0 as "unset").
pub fn id_from_token(token: &str) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = OFFSET;
    for b in token.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(PRIME);
    }
    match h & (u64::MAX >> 1) {
        0 => LEGACY_CLUSTER_ID,
        id => id,
    }
}

/// Apply the rules in the module docs. `store_is_new` is true when the
/// data file did not exist before this start.
pub fn choose(
    flag: Option<u64>,
    persisted: Option<u64>,
    store_is_new: bool,
    token: Option<&str>,
) -> (u64, Source) {
    if let Some(id) = flag {
        return (id, Source::Flag);
    }
    if let Some(id) = persisted {
        return (id, Source::Persisted);
    }
    if !store_is_new {
        return (LEGACY_CLUSTER_ID, Source::Legacy);
    }
    match token.map(str::trim).filter(|t| !t.is_empty()) {
        Some(t) => (id_from_token(t), Source::Token),
        None => (random_id(), Source::Random),
    }
}

/// Resolve this store's cluster id and persist it. Call once at
/// startup, before the node serves a request.
pub async fn resolve(
    engine: &Arc<dyn KvStore>,
    flag: Option<u64>,
    store_is_new: bool,
    token: Option<&str>,
) -> anyhow::Result<(u64, Source)> {
    if flag == Some(0) {
        anyhow::bail!("--cluster-id must not be 0");
    }
    let persisted = read(engine).await?;
    let (id, source) = choose(flag, persisted, store_is_new, token);
    if persisted != Some(id) {
        if let Some(old) = persisted {
            tracing::warn!(
                old,
                new = id,
                "--cluster-id replaces this store's cluster id: clients pinned to the \
                 old id will drain and reconnect"
            );
        }
        persist(engine, id).await?;
    }
    Ok((id, source))
}

async fn persist(engine: &Arc<dyn KvStore>, id: u64) -> anyhow::Result<()> {
    let mut batch = WriteBatch::new();
    batch.put(TABLE_NODE_META, KEY_CLUSTER_ID, &id.to_be_bytes());
    engine.commit(batch, WriteOptions::default()).await?;
    Ok(())
}

/// Since this version the leader proposes the cluster's id: a member
/// older than it cannot decode the entry.
pub const REPLICATED_SINCE: (u64, u64, u64) = (1, 33, 0);

/// At startup, before serving: the id to report given the cluster's
/// replicated one, if the store has it. An explicit `--cluster-id` that
/// contradicts it is refused; any other difference is adopted (and
/// persisted).
pub async fn reconcile(
    engine: &Arc<dyn KvStore>,
    own: u64,
    source: Source,
    cluster: Option<u64>,
) -> anyhow::Result<u64> {
    match cluster {
        None => Ok(own),
        Some(c) if c == own => Ok(own),
        Some(c) if source == Source::Flag => anyhow::bail!(
            "--cluster-id {own} contradicts this cluster's id {c} (replicated, fastetcd#38): \
             start this member with --cluster-id {c}, or without --cluster-id"
        ),
        Some(c) => {
            tracing::warn!(
                was = own,
                cluster = c,
                "this member reports the cluster's id from now on: its clients pinned to the \
                 old id drain and reconnect once"
            );
            persist(engine, c).await?;
            Ok(c)
        }
    }
}

/// While running: propose the cluster's id when this member leads a
/// cluster that has none and every member can read the entry; and
/// report the cluster's id once it is replicated, as [`reconcile`] does
/// at startup (a contradicting explicit `--cluster-id` stops the
/// process).
pub fn follow(state: Arc<crate::state::ServerState>, source: Source) {
    use std::sync::atomic::Ordering;
    let proposer = {
        let state = state.clone();
        async move {
            let gate = crate::version_gate::MembersAtLeast::new(REPLICATED_SINCE);
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(2));
            loop {
                tick.tick().await;
                if state.sm.mvcc().cluster_id().is_some() {
                    return;
                }
                if !fastetcd_raft::precheck::is_leader(&state.raft) {
                    continue;
                }
                if let Err(e) = gate.check(&state.raft, &state.forwarder).await {
                    tracing::debug!(error = ?e, "cluster id not proposed yet: a member is older or unreachable");
                    continue;
                }
                let id = state.cluster_id.load(Ordering::Relaxed);
                match state.propose(fastetcd_raft::FastetcdLogEntry::ClusterId(id)).await {
                    Ok(_) => tracing::info!(cluster_id = id, "proposed this cluster's id"),
                    Err(e) => tracing::debug!(error = %e, "proposing the cluster id"),
                }
            }
        }
    };
    tokio::spawn(proposer);
    tokio::spawn(async move {
        let mut cluster = state.sm.mvcc().watch_cluster_id();
        loop {
            let c = *cluster.borrow_and_update();
            let own = state.cluster_id.load(Ordering::Relaxed);
            if c != 0 && c != own {
                if source == Source::Flag {
                    tracing::error!(
                        own,
                        cluster = c,
                        "--cluster-id contradicts the cluster's id, now replicated (fastetcd#38): \
                         stopping. Start this member with --cluster-id {c}, or without it"
                    );
                    std::process::exit(78);
                }
                tracing::warn!(
                    was = own,
                    cluster = c,
                    "this member reports the cluster's id from now on: its clients pinned to the \
                     old id drain and reconnect once"
                );
                state.cluster_id.store(c, Ordering::Relaxed);
                state.cluster_id_adopted.fetch_add(1, Ordering::Relaxed);
                if let Err(e) = persist(state.sm.mvcc().engine(), c).await {
                    tracing::error!(error = %e, "could not persist the cluster's id");
                }
            }
            if cluster.changed().await.is_err() {
                return;
            }
        }
    });
}

/// The persisted cluster id, if any.
pub async fn read(engine: &Arc<dyn KvStore>) -> anyhow::Result<Option<u64>> {
    let snap = engine.snapshot().await?;
    let Some(bytes) = snap.get(TABLE_NODE_META, KEY_CLUSTER_ID).await? else {
        return Ok(None);
    };
    let bytes: [u8; 8] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| anyhow::anyhow!("persisted cluster id is {} bytes, expected 8", bytes.len()))?;
    Ok(Some(u64::from_be_bytes(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv1a_64_matches_the_reference_vectors() {
        // Published FNV-1a-64 test vectors, then masked to 63 bits.
        assert_eq!(id_from_token(""), 0xcbf2_9ce4_8422_2325 & (u64::MAX >> 1));
        assert_eq!(id_from_token("a"), 0xaf63_dc4c_8601_ec8c & (u64::MAX >> 1));
        assert_eq!(id_from_token("foobar"), 0x8594_4171_f739_67e8 & (u64::MAX >> 1));
    }

    #[test]
    fn a_token_id_fits_63_bits_and_is_never_zero() {
        for t in ["etcd-cluster", "prod-east", "x", "cluster-2"] {
            let id = id_from_token(t);
            assert_ne!(id, 0);
            assert_eq!(id >> 63, 0, "{t}: top bit must be clear");
        }
        assert_ne!(id_from_token("prod-east"), id_from_token("prod-west"));
    }

    #[tokio::test]
    async fn only_an_explicit_contradicting_flag_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let engine: Arc<dyn KvStore> =
            Arc::new(fastetcd_storage::redb_engine::RedbEngine::open(dir.path().join("db")).unwrap());
        // No replicated id yet, or the same one: our own.
        assert_eq!(reconcile(&engine, 5, Source::Flag, None).await.unwrap(), 5);
        assert_eq!(reconcile(&engine, 5, Source::Flag, Some(5)).await.unwrap(), 5);
        // A different one: an explicit flag is refused, naming both...
        let e = reconcile(&engine, 5, Source::Flag, Some(9)).await.unwrap_err().to_string();
        assert!(e.contains("--cluster-id 5") && e.contains("9"), "{e}");
        // ...anything else is adopted, and persisted for the next start.
        for src in [Source::Persisted, Source::Legacy, Source::Token, Source::Random] {
            assert_eq!(reconcile(&engine, 5, src, Some(9)).await.unwrap(), 9);
        }
        assert_eq!(read(&engine).await.unwrap(), Some(9));
    }

    #[test]
    fn the_rules_apply_in_order() {
        let t = Some("tok");
        // An explicit flag beats everything, including a persisted id.
        assert_eq!(choose(Some(42), Some(7), false, t), (42, Source::Flag));
        // Then the persisted id, whatever the token says now.
        assert_eq!(choose(None, Some(7), false, t), (7, Source::Persisted));
        // A pre-existing store without one keeps the legacy id.
        assert_eq!(choose(None, None, false, t), (1, Source::Legacy));
        // A new store derives from the token...
        assert_eq!(choose(None, None, true, t), (id_from_token("tok"), Source::Token));
        // ...or is random without one (#38): 63 bits, never 0, distinct.
        let (a, src) = choose(None, None, true, None);
        assert_eq!(src, Source::Random);
        assert_eq!(choose(None, None, true, Some("  ")).1, Source::Random);
        let b = random_id();
        assert!(a != 0 && a >> 63 == 0 && b != 0 && a != b);
    }
}
