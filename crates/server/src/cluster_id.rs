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
//! Chosen once, on the first start of a store, and persisted in the data
//! directory. Every later start uses the persisted id, so a restart or
//! an upgrade never changes what clients see. In order:
//!
//! 1. An explicit `--cluster-id` (never 0). It also replaces an id
//!    already persisted: the one way to change it deliberately.
//! 2. The persisted id, if this store has one.
//! 3. A store that existed before ids were persisted keeps `1`, the id
//!    it has always reported. Deriving a new one would change it under
//!    clients mid-upgrade, and members upgraded at different times
//!    would disagree.
//! 4. A new store: FNV-1a-64 of `--initial-cluster-token`, masked to 63
//!    bits, never 0. Every member of a cluster is given the same token,
//!    so they agree without talking to each other.
//! 5. Otherwise `1`, with a warning at startup.
//!
//! The id is node-local metadata, like the format-version markers: it
//! lives in its own table, which a raft snapshot does not carry, so a
//! follower installing a snapshot keeps its own. Making the id
//! replicated cluster state (and distinct by default, with no flags) is
//! a separate change.

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
    /// Nothing to derive it from.
    Default,
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
        None => (LEGACY_CLUSTER_ID, Source::Default),
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
        let mut batch = WriteBatch::new();
        batch.put(TABLE_NODE_META, KEY_CLUSTER_ID, &id.to_be_bytes());
        engine.commit(batch, WriteOptions::default()).await?;
    }
    Ok((id, source))
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
        // ...or falls back to 1 without one.
        assert_eq!(choose(None, None, true, None), (1, Source::Default));
        assert_eq!(choose(None, None, true, Some("  ")), (1, Source::Default));
    }
}
