//! A txn inside a txn is proposed only once every member can read it
//! (fastetcd#56).
//!
//! A nested txn is a new variant of the raft entry's `TxnOp`, which a
//! member older than 1.22 cannot decode: its AppendEntries (or its own
//! log replay) fails at that entry and it stops, and a voter that stops
//! costs the quorum. So before a txn that nests is proposed, every other
//! member (voters and learners) is asked `ConfirmLeader`, which answers
//! with its version (`fastetcd_raft::version_gate`); all at 1.22 or later
//! opens the gate for that membership, and a membership change closes it
//! again. Flat txns never wait on it.
//!
//! Left open: a member older than 1.22 added after a nested txn was
//! applied cannot read the log either; `MemberAdd` refuses only members
//! older than 1.10 (#77, #134). Do not add one (03-deploy).

use fastetcd_raft::version_gate::GateError;
pub use fastetcd_raft::version_gate::{parse_version, MembersAtLeast};
use tonic::Status;

use crate::state::ServerState;

/// The first version that decodes a nested txn.
pub const NESTED_TXN_SINCE: (u64, u64, u64) = (1, 22, 0);

/// Ok once every member runs 1.22 or later. An older member is refused
/// with FailedPrecondition naming it; one that does not answer, with
/// Unavailable (its version is unknown, and it may be older).
pub async fn require_nested_txn(state: &ServerState) -> Result<(), Status> {
    let (a, b, c) = NESTED_TXN_SINCE;
    let need = format!("a txn inside a txn needs every member at fastetcd {a}.{b}.{c} or later");
    match state.nested_txn_gate.check(&state.raft, &state.forwarder).await {
        Ok(()) => Ok(()),
        Err(GateError::Older { member, version: Some(v) }) => Err(Status::failed_precondition(format!(
            "{need}, and member {member:x} runs {v}: it could not read the entry and would stop \
             (fastetcd#56). Upgrade it first."
        ))),
        Err(GateError::Older { member, version: None }) => Err(Status::failed_precondition(format!(
            "{need}, and member {member:x} runs a fastetcd older than 1.10 (fastetcd#56). Upgrade \
             it first."
        ))),
        Err(GateError::Unreachable { member, error }) => Err(Status::unavailable(format!(
            "{need}, and member {member:x} cannot be asked its version ({error}) (fastetcd#56)"
        ))),
    }
}
