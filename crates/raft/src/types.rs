//! openraft type configuration and the application-level log entry /
//! response types for fastetcd.
//!
//! `FastetcdLogEntry` is what we serialize into every Raft log entry.
//! `FastetcdLogResponse` is what comes back from the state machine
//! after `apply()`. Both are versioned (via the enum tag) so we can
//! add new variants without breaking existing logs.

use std::io::Cursor;

use openraft::BasicNode;
use openraft::TokioRuntime;
use serde::{Deserialize, Serialize};

use fastetcd_storage::mvcc::{
    Compare, LeaseGrantResult, LeaseId, LeaseRevokeResult, LeaseTtlResult, Mutation,
    MutationResult, RangeOp, RangeResult, TxnOp, TxnResult,
};

/// Cluster-unique node identifier. We use u64 to match openraft's
/// default expectations; fastetcd assigns node IDs from `--name` at
/// bootstrap (hash, or just an explicit `--node-id` flag).
pub type NodeId = u64;

/// Top-level openraft type configuration. The `declare_raft_types!`
/// macro could be used here; spelling it out lets us put detail in
/// docstrings.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct TypeConfig;

impl openraft::RaftTypeConfig for TypeConfig {
    type D = FastetcdLogEntry;
    type R = FastetcdLogResponse;
    type NodeId = NodeId;
    type Node = BasicNode;
    type Entry = openraft::Entry<Self>;
    type SnapshotData = Cursor<Vec<u8>>;
    type AsyncRuntime = TokioRuntime;
    type Responder = openraft::impls::OneshotResponder<Self>;
}

/// A cluster-membership change forwarded to the leader.
///
/// Membership changes go through openraft's own APIs rather than the
/// replicated log, so they can't ride on [`FastetcdLogEntry`] — this is
/// the payload of the `RaftPeer.ForwardMembership` RPC instead. Only a
/// leader may apply one; a follower that receives `MemberAdd` /
/// `MemberRemove` forwards it here (#7).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MembershipChange {
    /// Add `node_id` as a learner reachable at `addr`.
    AddLearner { node_id: NodeId, addr: String },
    /// Replace the voter set wholesale. Used for promotion (MemberAdd
    /// of a voter, MemberPromote) and removal (MemberRemove).
    SetVoters { voters: Vec<NodeId> },
}

/// The application-level log entry. Every committed Raft entry decodes
/// to one of these variants and is dispatched to [`MvccStore`].
///
/// [`MvccStore`]: fastetcd_storage::mvcc::MvccStore
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FastetcdLogEntry {
    /// Apply a batch of mutations atomically at a new revision.
    /// Used by Put and DeleteRange RPCs (and any non-conditional
    /// multi-op call that doesn't need Compare evaluation).
    Apply { mutations: Vec<Mutation> },

    /// Run a Txn — evaluate compares, then apply success or failure
    /// branch ops atomically.
    Txn {
        compares: Vec<Compare>,
        success: Vec<TxnOp>,
        failure: Vec<TxnOp>,
    },

    /// Compact MVCC history up to (and including) `rev`. Doesn't
    /// advance the revision counter.
    Compact { rev: i64 },

    /// Grant a new lease. `id == 0` allocates one; `now_unix` is the
    /// leader's wall-clock when the entry was proposed.
    LeaseGrant {
        id: LeaseId,
        ttl_secs: i64,
        now_unix: i64,
    },

    /// Revoke a lease and cascade-delete every key attached to it.
    LeaseRevoke { id: LeaseId },

    /// Refresh a lease's deadline (KeepAlive).
    LeaseKeepAlive { id: LeaseId, now_unix: i64 },

    /// No-op heartbeat / membership change marker. State machine
    /// records the applied log id and returns the current revision.
    Noop,
}

/// Response shape from a state machine `apply()` call. Wire RPC
/// handlers map this back into the corresponding `*Response` proto.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FastetcdLogResponse {
    /// Result of an `Apply` entry. `revision` is the new MVCC
    /// `main` revision (or the unchanged current revision if no
    /// mutation produced an effect).
    Apply {
        revision: i64,
        results: Vec<MutationResult>,
    },
    /// Result of a `Txn` entry.
    Txn(TxnResult),
    /// Result of a `Compact` entry.
    Compact { compact_rev: i64 },
    /// Result of `LeaseGrant`.
    LeaseGrant(LeaseGrantResult),
    /// Result of `LeaseRevoke`.
    LeaseRevoke(LeaseRevokeResult),
    /// Result of `LeaseKeepAlive`.
    LeaseKeepAlive(LeaseTtlResult),
    /// `Noop` carries the current revision so callers can sequence
    /// reads against it (linearizable read-index path).
    Noop { revision: i64 },
}

impl FastetcdLogResponse {
    /// The revision a wire response header should carry for this
    /// log apply. For Txn, etcd reports the txn's effective revision.
    pub fn header_revision(&self) -> i64 {
        match self {
            FastetcdLogResponse::Apply { revision, .. } => *revision,
            FastetcdLogResponse::Txn(t) => t.revision,
            FastetcdLogResponse::Compact { compact_rev } => *compact_rev,
            FastetcdLogResponse::LeaseGrant(g) => g.revision,
            FastetcdLogResponse::LeaseRevoke(r) => r.revision,
            FastetcdLogResponse::LeaseKeepAlive(_) => 0,
            FastetcdLogResponse::Noop { revision } => *revision,
        }
    }
}

/// A read-only `Range` request that does NOT go through Raft. The
/// gRPC service routes serializable reads here; linearizable reads
/// pipeline a `Noop` through Raft for the read-index, then call this.
#[derive(Debug, Clone)]
pub struct RangeRead {
    pub op: RangeOp,
}

/// What `RangeRead` returns. Same shape as `MutationResult` doesn't
/// fit; this is the dedicated read response shape.
#[derive(Debug, Clone)]
pub struct RangeReadResponse {
    pub result: RangeResult,
    pub revision: i64,
}

/// `TxnOpResult` is re-exported so the gRPC layer can build responses
/// without depending on `fastetcd-storage` directly.
pub use fastetcd_storage::mvcc::TxnOpResult as ReExportTxnOpResult;
