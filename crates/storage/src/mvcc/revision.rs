//! MVCC revisions.
//!
//! A revision is a pair `(main, sub)`. The `main` part is the global,
//! monotonic counter incremented per Raft-applied mutation. The `sub`
//! part disambiguates multiple writes that share the same `main` — used
//! by `Txn` to give every op in a single transaction a distinct
//! revision while still recording them as "logically simultaneous."
//!
//! The on-disk encoding is 16 bytes, big-endian: 8 bytes `main` then 8
//! bytes `sub`. This makes the byte ordering of revisions match their
//! logical ordering, so a range scan in revision order on a `KvStore`
//! is just a byte-range scan.

use serde::{Deserialize, Serialize};

/// MVCC revision. Defaults to `(0, 0)` which is "no revision."
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Revision {
    pub main: i64,
    pub sub: i64,
}

impl Revision {
    pub const ZERO: Revision = Revision { main: 0, sub: 0 };

    pub fn new(main: i64, sub: i64) -> Self {
        Self { main, sub }
    }

    /// Pack into a 16-byte big-endian representation. The on-disk key
    /// layout in `mvcc_kv` is `key || revision_bytes`, so this encoding
    /// is what makes "newest revision of a key" reachable via a single
    /// reverse range scan.
    pub fn to_bytes(self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[..8].copy_from_slice(&self.main.to_be_bytes());
        out[8..].copy_from_slice(&self.sub.to_be_bytes());
        out
    }

    /// Inverse of [`to_bytes`]. Returns `None` if `bytes.len() != 16`.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 16 {
            return None;
        }
        let main = i64::from_be_bytes(bytes[..8].try_into().ok()?);
        let sub = i64::from_be_bytes(bytes[8..].try_into().ok()?);
        Some(Self { main, sub })
    }

    /// Is this revision "less than or equal" the other in the
    /// MVCC-historical sense?
    pub fn less_or_equal(self, other: Revision) -> bool {
        self <= other
    }
}

/// Build the `mvcc_kv` key for a `(user_key, revision)` pair:
/// `user_key || 0x00 || revision_bytes`. The `0x00` separator
/// distinguishes the user key from the revision suffix during reverse
/// scans; user keys may legitimately contain any byte sequence
/// including `0x00`, so we additionally length-prefix the user key.
///
/// Layout:
/// ```text
/// [ 4 bytes BE: user_key length ][ user_key bytes ][ 16 bytes BE: revision ]
/// ```
pub fn make_kv_key(user_key: &[u8], rev: Revision) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + user_key.len() + 16);
    out.extend_from_slice(&(user_key.len() as u32).to_be_bytes());
    out.extend_from_slice(user_key);
    out.extend_from_slice(&rev.to_bytes());
    out
}

/// Inverse of [`make_kv_key`]. Returns `(user_key, revision)`.
pub fn parse_kv_key(bytes: &[u8]) -> Option<(Vec<u8>, Revision)> {
    if bytes.len() < 4 + 16 {
        return None;
    }
    let len = u32::from_be_bytes(bytes[..4].try_into().ok()?) as usize;
    if bytes.len() != 4 + len + 16 {
        return None;
    }
    let user_key = bytes[4..4 + len].to_vec();
    let rev = Revision::from_bytes(&bytes[4 + len..])?;
    Some((user_key, rev))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revision_bytes_roundtrip() {
        let r = Revision::new(42, 7);
        assert_eq!(Revision::from_bytes(&r.to_bytes()), Some(r));
    }

    #[test]
    fn revision_bytes_preserve_order() {
        let a = Revision::new(1, 0).to_bytes();
        let b = Revision::new(1, 5).to_bytes();
        let c = Revision::new(2, 0).to_bytes();
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn kv_key_roundtrip() {
        for user_key in [b"".as_ref(), b"a", b"\x00\x01\x02", b"/registry/pods/default/foo"] {
            for rev in [Revision::new(1, 0), Revision::new(1, 5), Revision::new(100, 0)] {
                let bytes = make_kv_key(user_key, rev);
                let (uk, r) = parse_kv_key(&bytes).expect("roundtrip");
                assert_eq!(uk, user_key);
                assert_eq!(r, rev);
            }
        }
    }

    #[test]
    fn kv_key_orders_by_user_key_then_revision() {
        let a = make_kv_key(b"alpha", Revision::new(5, 0));
        let b = make_kv_key(b"alpha", Revision::new(10, 0));
        let c = make_kv_key(b"bravo", Revision::new(1, 0));
        assert!(a < b);
        assert!(b < c);
    }
}
