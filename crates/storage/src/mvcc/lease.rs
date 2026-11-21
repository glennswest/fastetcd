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
use crate::{Snapshot, StorageError, WriteBatch};

pub const TABLE_LEASE: &str = "lease";
pub const TABLE_LEASE_KEYS: &str = "lease_keys";

/// The raw contents of the two lease tables, in key order. Raft
/// snapshots carry them (fastetcd#41): without them a member caught up
/// by a snapshot has no record of leases granted before it, so their
/// keys never expire there and a revoke leaves them behind.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseTables {
    pub leases: Vec<(Vec<u8>, Vec<u8>)>,
    pub keys: Vec<(Vec<u8>, Vec<u8>)>,
}

impl LeaseTables {
    /// Read both tables from one engine snapshot.
    pub async fn read(snap: &dyn Snapshot) -> Result<Self, StorageError> {
        let all = |t| snap.range(t, Bound::Unbounded, Bound::Unbounded, 0);
        Ok(Self { leases: all(TABLE_LEASE).await?, keys: all(TABLE_LEASE_KEYS).await? })
    }

    /// Replace both tables with these contents, in `batch`: every row
    /// `current` holds is deleted first. Rows, not a key range, since a
    /// `lease_keys` key is the lease id followed by a user key of any
    /// length.
    pub fn replace_into(&self, current: &LeaseTables, batch: &mut WriteBatch) {
        for (k, _) in &current.leases {
            batch.delete(TABLE_LEASE, k);
        }
        for (k, _) in &current.keys {
            batch.delete(TABLE_LEASE_KEYS, k);
        }
        for (k, v) in &self.leases {
            batch.put(TABLE_LEASE, k, v);
        }
        for (k, v) in &self.keys {
            batch.put(TABLE_LEASE_KEYS, k, v);
        }
    }
}

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
///
/// The table orders its keys by their bytes, so the end is the next
/// 8-byte prefix as an unsigned number (fastetcd#215): the id's
/// big-endian bits plus one. Only the all-ones prefix (lease -1) has
/// none, and its range runs to the end of the table. Taking the
/// successor as a signed id gave i64::MAX an unbounded end, over every
/// negative lease's keys, and -1 an end of 0, below its start.
pub fn lease_keys_bounds(id: LeaseId) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let start = lease_id_key(id).to_vec();
    match u64::from_be_bytes(lease_id_key(id)).checked_add(1) {
        Some(n) => (Bound::Included(start), Bound::Excluded(n.to_be_bytes().to_vec())),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn contains(b: &(Bound<Vec<u8>>, Bound<Vec<u8>>), key: &[u8]) -> bool {
        let lo = match &b.0 {
            Bound::Included(s) => key >= s.as_slice(),
            Bound::Excluded(s) => key > s.as_slice(),
            Bound::Unbounded => true,
        };
        let hi = match &b.1 {
            Bound::Included(e) => key <= e.as_slice(),
            Bound::Excluded(e) => key < e.as_slice(),
            Bound::Unbounded => true,
        };
        lo && hi
    }

    /// fastetcd#215 (finding fb3adfe70c94): a lease's range holds every
    /// key of that lease and none of another's, whatever the id's sign:
    /// the table orders bytes, and a negative id's bytes sort above every
    /// positive one's.
    #[test]
    fn a_lease_range_holds_its_keys_and_no_others() {
        let ids = [0, 1, 2, 255, 256, i64::MAX - 1, i64::MAX, i64::MIN, i64::MIN + 1, -256, -5, -2, -1];
        for &id in &ids {
            let b = lease_keys_bounds(id);
            if let (Bound::Included(s), Bound::Excluded(e)) = (&b.0, &b.1) {
                assert!(s < e, "lease {id}: an empty or inverted range");
            }
            for &other in &ids {
                for key in [&b""[..], b"k", &[0xFF; 70][..]] {
                    let entry = lease_key_index(other, key);
                    assert_eq!(contains(&b, &entry), other == id, "lease {id}'s range and lease {other}'s key {key:?}");
                }
            }
        }
        assert_eq!(lease_keys_bounds(i64::MAX).1, Bound::Excluded(i64::MIN.to_be_bytes().to_vec()));
        assert_eq!(lease_keys_bounds(-1).1, Bound::Unbounded);
    }
}
