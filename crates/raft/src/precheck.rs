//! Checks the leader makes on a client write just before proposing it.
//!
//! **Leases (fastetcd#19).** A `Put` naming a lease that does not exist
//! is refused with etcd's `etcdserver: requested lease not found`, the
//! error a client uses to learn its lease expired between grant and use.
//! etcd refuses it at apply. fastetcd refuses it here instead, before
//! the entry is proposed, because a refusal at apply is only safe when
//! every member holds the same lease table and runs the same check: raft
//! snapshots do not carry the lease tables yet (#41), and a member older
//! than this check would apply what newer ones refuse, so members'
//! data would diverge. A check before proposing changes nothing that is
//! applied. What it leaves: a lease that expires between this check and
//! the apply still gets the key attached.
//!
//! **Requests that cannot apply (fastetcd#49).** A put with
//! `ignore_value`/`ignore_lease` on a missing key, `Compact` out of
//! range, `LeaseKeepAlive` of an unknown lease, `LeaseGrant` with TTL
//! <= 0. The state machine refuses these itself (`Refused`, nothing
//! applied), which is the authority, since the state can change between
//! here and the apply. Refusing them here too answers the client without
//! a raft round trip and keeps them out of the log, where a member older
//! than 1.14 would stop on them.
//!
//! Only the leader checks. A follower can lag a lease just granted on the
//! leader; it forwards the write (`ForwardWrite`) and the leader checks.
//! On a miss the leader runs a read barrier and looks again, so a newly
//! elected leader that has not applied every committed entry yet does
//! not refuse a live lease.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;

use fastetcd_storage::mvcc::{Compare, LeaseId, Mutation, MvccStore, Refusal, TxnOp};
use openraft::Raft;

use crate::types::{FastetcdLogEntry, TypeConfig};

/// etcd's `ErrGRPCLeaseNotFound` message (gRPC code NotFound).
pub const LEASE_NOT_FOUND: &str = "etcdserver: requested lease not found";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrecheckError {
    /// A put names a lease that does not exist.
    LeaseNotFound,
    /// The entry would be refused at apply (fastetcd#49).
    Refused(Refusal),
    /// The check itself could not run (storage, or the read barrier).
    Unavailable(String),
}

impl std::fmt::Display for PrecheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrecheckError::LeaseNotFound => f.write_str(LEASE_NOT_FOUND),
            PrecheckError::Refused(r) => f.write_str(r.message()),
            PrecheckError::Unavailable(m) => f.write_str(m),
        }
    }
}

/// Whether this node currently believes it is the leader.
pub fn is_leader(raft: &Raft<TypeConfig>) -> bool {
    let m = raft.metrics().borrow().clone();
    m.current_leader == Some(m.id)
}

fn put_lease(m: &Mutation) -> Option<LeaseId> {
    match m {
        Mutation::Put { lease, ignore_lease: false, .. } if *lease != 0 => Some(*lease),
        _ => None,
    }
}

/// The leases the entry's puts would attach keys to: every put of an
/// `Apply`, and the puts of the branch a `Txn` would take now.
async fn leases_named(mvcc: &MvccStore, entry: &FastetcdLogEntry) -> Result<Vec<LeaseId>, String> {
    Ok(match entry {
        FastetcdLogEntry::Apply { mutations } => mutations.iter().filter_map(put_lease).collect(),
        FastetcdLogEntry::Txn { compares, success, failure } => {
            // Most txns name no lease; they need no compare evaluation.
            let names_a_lease = |m: &Mutation| put_lease(m).is_some();
            if !any_mutation(success, &names_a_lease) && !any_mutation(failure, &names_a_lease) {
                return Ok(Vec::new());
            }
            chosen_mutations(mvcc, compares, success, failure)
                .await?
                .into_iter()
                .filter_map(put_lease)
                .collect()
        }
        _ => Vec::new(),
    })
}

async fn any_missing(mvcc: &MvccStore, entry: &FastetcdLogEntry) -> Result<bool, String> {
    for id in leases_named(mvcc, entry).await? {
        if !mvcc.lease_exists(id).await.map_err(|e| e.to_string())? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn ignores_prev(m: &Mutation) -> bool {
    matches!(m, Mutation::Put { ignore_value, ignore_lease, .. } if *ignore_value || *ignore_lease)
}

async fn key_exists(mvcc: &MvccStore, key: &[u8]) -> Result<bool, String> {
    let r = mvcc
        .range(key, b"", 1, 0, true, false)
        .await
        .map_err(|e| e.to_string())?;
    Ok(!r.kvs.is_empty())
}

/// Whether a put in `ops` (applied in order) keeps the value or lease of
/// a key that does not exist. Exact where it refuses: the key is missing
/// now and nothing earlier in `ops` puts it, so the apply refuses too. A
/// key that exists now but an earlier op deletes is left to the apply.
async fn keeps_missing_key<'a>(
    mvcc: &MvccStore,
    ops: impl Iterator<Item = &'a Mutation>,
) -> Result<bool, String> {
    let mut put: HashSet<&[u8]> = HashSet::new();
    for m in ops {
        if let Mutation::Put { key, .. } = m {
            if ignores_prev(m) && !put.contains(key.as_slice()) && !key_exists(mvcc, key).await? {
                return Ok(true);
            }
            put.insert(key);
        }
    }
    Ok(false)
}

/// Whether any mutation in `ops`, nested txns' branches included,
/// satisfies `pred`.
fn any_mutation(ops: &[TxnOp], pred: &dyn Fn(&Mutation) -> bool) -> bool {
    ops.iter().any(|op| match op {
        TxnOp::Mutation(m) => pred(m),
        TxnOp::Range(_) => false,
        TxnOp::Txn(t) => any_mutation(&t.success, pred) || any_mutation(&t.failure, pred),
    })
}

/// The mutations a txn would run now, in order: the branch its compares
/// choose, and inside it the branches its nested txns' compares choose
/// (#56). Every compare is judged on the state now, as the apply judges
/// them all on the state before the txn.
fn chosen_mutations<'a>(
    mvcc: &'a MvccStore,
    compares: &'a [Compare],
    success: &'a [TxnOp],
    failure: &'a [TxnOp],
) -> Pin<Box<dyn Future<Output = Result<Vec<&'a Mutation>, String>> + Send + 'a>> {
    Box::pin(async move {
        let ops = if mvcc.txn_would_succeed(compares).await.map_err(|e| e.to_string())? {
            success
        } else {
            failure
        };
        let mut out = Vec::new();
        for op in ops {
            match op {
                TxnOp::Mutation(m) => out.push(m),
                TxnOp::Range(_) => {}
                TxnOp::Txn(t) => {
                    out.extend(chosen_mutations(mvcc, &t.compares, &t.success, &t.failure).await?)
                }
            }
        }
        Ok(out)
    })
}

/// What the state machine would refuse `entry` with, judged on this
/// member's state now (fastetcd#49).
async fn refusal(mvcc: &MvccStore, entry: &FastetcdLogEntry) -> Result<Option<Refusal>, String> {
    Ok(match entry {
        FastetcdLogEntry::Apply { mutations } => {
            if mutations.iter().any(ignores_prev) && keeps_missing_key(mvcc, mutations.iter()).await? {
                Some(Refusal::KeyNotFound)
            } else {
                None
            }
        }
        FastetcdLogEntry::Txn { compares, success, failure } => {
            if !any_mutation(success, &ignores_prev) && !any_mutation(failure, &ignores_prev) {
                return Ok(None);
            }
            let ops = chosen_mutations(mvcc, compares, success, failure).await?;
            if keeps_missing_key(mvcc, ops.into_iter()).await? {
                Some(Refusal::KeyNotFound)
            } else {
                None
            }
        }
        FastetcdLogEntry::Compact { rev } => {
            if *rev <= 0 {
                Some(Refusal::InvalidArgument(format!("compact rev must be > 0, got {rev}")))
            } else if *rev > mvcc.current_revision().await {
                Some(Refusal::FutureRevision)
            } else if *rev < mvcc.compact_revision().await {
                Some(Refusal::Compacted)
            } else {
                None
            }
        }
        FastetcdLogEntry::LeaseKeepAlive { id, .. } => {
            if mvcc.lease_exists(*id).await.map_err(|e| e.to_string())? {
                None
            } else {
                Some(Refusal::LeaseNotFound)
            }
        }
        FastetcdLogEntry::LeaseGrant { ttl_secs, .. } if *ttl_secs <= 0 => Some(
            Refusal::InvalidArgument(format!("lease TTL must be positive, got {ttl_secs}")),
        ),
        _ => None,
    })
}

async fn find(mvcc: &MvccStore, entry: &FastetcdLogEntry) -> Result<Option<PrecheckError>, String> {
    if any_missing(mvcc, entry).await? {
        return Ok(Some(PrecheckError::LeaseNotFound));
    }
    Ok(refusal(mvcc, entry).await?.map(PrecheckError::Refused))
}

/// Refuse `entry` if a put in it names a lease that does not exist
/// (#19), or if the state machine would refuse it (#49). Call on the
/// leader, before proposing.
pub async fn check(
    raft: &Raft<TypeConfig>,
    local: Option<&crate::read_index::LocalReadIndex>,
    mvcc: &MvccStore,
    entry: &FastetcdLogEntry,
) -> Result<(), PrecheckError> {
    if find(mvcc, entry).await.map_err(PrecheckError::Unavailable)?.is_none() {
        return Ok(());
    }
    // Refused: make sure that is not just this leader lagging.
    crate::read_index::read_barrier(raft, local)
        .await
        .map_err(|e| PrecheckError::Unavailable(format!("precheck read barrier: {e}")))?;
    match find(mvcc, entry).await.map_err(PrecheckError::Unavailable)? {
        Some(e) => Err(e),
        None => Ok(()),
    }
}
