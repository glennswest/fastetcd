//! Persisted Auth state on [`MvccStore`].
//!
//! Tables:
//!   - `auth_state` — small key/value: `b"enabled"` -> single byte 0/1
//!   - `auth_users` — `username -> bincode(StoredUser)`
//!   - `auth_roles` — `rolename -> bincode(StoredRole)`
//!
//! Phase 1 stores the data; permission enforcement on KV is wired
//! through a tonic interceptor (see `crates/server/src/auth.rs`).

use serde::{Deserialize, Serialize};

pub const TABLE_AUTH_STATE: &str = "auth_state";
pub const TABLE_AUTH_USERS: &str = "auth_users";
pub const TABLE_AUTH_ROLES: &str = "auth_roles";

pub const META_AUTH_ENABLED: &[u8] = b"enabled";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StoredUser {
    pub name: String,
    /// Encoded password hash (argon2 PHC string). Empty if the user
    /// was created with `no_password`.
    pub password_hash: String,
    pub roles: Vec<String>,
    /// True if the user was created with options.no_password — they
    /// can only Authenticate via TLS client cert / external auth.
    pub no_password: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StoredRole {
    pub name: String,
    pub permissions: Vec<StoredPermission>,
}

/// Single permission grant — matches etcd's `authpb::Permission`
/// shape (READ / WRITE / READWRITE × key range).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredPermission {
    pub perm_type: PermType,
    pub key: Vec<u8>,
    pub range_end: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PermType {
    Read,
    Write,
    ReadWrite,
}

impl PermType {
    pub fn allows_read(self) -> bool {
        matches!(self, PermType::Read | PermType::ReadWrite)
    }
    pub fn allows_write(self) -> bool {
        matches!(self, PermType::Write | PermType::ReadWrite)
    }
}

impl StoredPermission {
    /// Does this permission cover `key` (single-key request, no range)?
    pub fn covers(&self, key: &[u8]) -> bool {
        if self.range_end.is_empty() {
            key == self.key.as_slice()
        } else if self.range_end == [0u8] {
            key >= self.key.as_slice()
        } else {
            key >= self.key.as_slice() && key < self.range_end.as_slice()
        }
    }
}
