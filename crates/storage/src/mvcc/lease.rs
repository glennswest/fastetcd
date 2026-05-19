//! Lease management on top of [`MvccStore`].
//!
//! A lease has an ID, a granted TTL (seconds), and a deadline
//! (Unix seconds, leader-clock). Keys put with `lease != 0` are
//! associated with that lease; revoking the lease cascades a delete
//! to every attached key.
//!
//! Storage:
//!   - `lease`        — lease_id_be(8) -> bincode(LeaseRecord)
//!   - `lease_keys`   — lease_id_be(8) || user_key -> ()
//!
//! All lease mutations go through Raft (see [`super::MvccStore`]'s
//! `apply_lease_*` entry points); single-node servers still serialize
//! them through the same code path so the multi-node story is the
//! same.

use std::ops::Bound;

use serde::{Deserialize, Serialize};

use super::store::{MvccError, MvccResult};

pub const TABLE_LEASE: &str = "lease";
pub const TABLE_LEASE_KEYS: &str = "lease_keys";

/// Lease IDs are arbitrary i64; etcd allows clients to pick them. We
/// follow the same shape so existing client logic works unchanged.
pub type LeaseId = i64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseRecord {
    pub id: LeaseId,
    /// Granted TTL in seconds (the value the client asked for, or
    /// the server-clamped value).
    pub ttl_secs: i64,
    /// Unix seconds (leader clock) at which the lease expires unless
    /// refreshed by a KeepAlive.
    pub deadline_unix_secs: i64,
}

/// Encode a lease id as 8 big-endian bytes for table keys.
pub fn lease_id_key(id: LeaseId) -> [u8; 8] {
    id.to_be_bytes()
}

/// Compose a `lease_keys` table key: `lease_id_be(8) || user_key`.
pub fn lease_key_index(id: LeaseId, user_key: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(8 + user_key.len());
    v.extend_from_slice(&lease_id_key(id));
    v.extend_from_slice(user_key);
    v
}

/// Iterate the lease_keys range for `id` — used by revoke to find
/// what to cascade-delete.
pub fn lease_keys_bounds(id: LeaseId) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let start = lease_id_key(id).to_vec();
    let mut end = lease_id_key(id).to_vec();
    // Successor of the 8-byte id: increment as a u64. If overflow,
    // end is unbounded.
    let next = (i64::from_be_bytes(end.clone().try_into().expect("8 bytes")))
        .checked_add(1);
    match next {
        Some(n) => {
            end = n.to_be_bytes().to_vec();
            (Bound::Included(start), Bound::Excluded(end))
        }
        None => (Bound::Included(start), Bound::Unbounded),
    }
}

/// Parse `lease_keys`-table key back into `(lease_id, user_key)`.
pub fn parse_lease_keys_key(bytes: &[u8]) -> MvccResult<(LeaseId, Vec<u8>)> {
    if bytes.len() < 8 {
        return Err(MvccError::Internal(format!(
            "lease_keys key too short: {} bytes",
            bytes.len()
        )));
    }
    let id = i64::from_be_bytes(bytes[..8].try_into().expect("8 bytes"));
    let key = bytes[8..].to_vec();
    Ok((id, key))
}
