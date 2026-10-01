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
//! Only the leader checks. A follower can lag a lease just granted on the
//! leader; it forwards the write (`ForwardWrite`) and the leader checks.
//! On a miss the leader runs a read barrier and looks again, so a newly
//! elected leader that has not applied every committed entry yet does
//! not refuse a live lease.

use fastetcd_storage::mvcc::{LeaseId, Mutation, MvccStore, TxnOp};
use openraft::Raft;

use crate::types::{FastetcdLogEntry, TypeConfig};

/// etcd's `ErrGRPCLeaseNotFound` message (gRPC code NotFound).
pub const LEASE_NOT_FOUND: &str = "etcdserver: requested lease not found";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrecheckError {
    /// A put names a lease that does not exist.
    LeaseNotFound,
    /// The check itself could not run (storage, or the read barrier).
    Unavailable(String),
}

impl std::fmt::Display for PrecheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrecheckError::LeaseNotFound => f.write_str(LEASE_NOT_FOUND),
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
            let ops = if mvcc.txn_would_succeed(compares).await.map_err(|e| e.to_string())? {
                success
            } else {
                failure
            };
            ops.iter()
                .filter_map(|op| match op {
                    TxnOp::Mutation(m) => put_lease(m),
                    TxnOp::Range(_) => None,
                })
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

/// Refuse `entry` if a put in it names a lease that does not exist. Call
/// on the leader, before proposing.
pub async fn check_leases(
    raft: &Raft<TypeConfig>,
    local: Option<&crate::read_index::LocalReadIndex>,
    mvcc: &MvccStore,
    entry: &FastetcdLogEntry,
) -> Result<(), PrecheckError> {
    if !any_missing(mvcc, entry).await.map_err(PrecheckError::Unavailable)? {
        return Ok(());
    }
    // Not found: make sure that is not just this leader lagging.
    crate::read_index::read_barrier(raft, local)
        .await
        .map_err(|e| PrecheckError::Unavailable(format!("lease check read barrier: {e}")))?;
    if any_missing(mvcc, entry).await.map_err(PrecheckError::Unavailable)? {
        Err(PrecheckError::LeaseNotFound)
    } else {
        Ok(())
    }
}
