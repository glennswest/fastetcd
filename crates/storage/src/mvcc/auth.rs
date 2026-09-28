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

// ---------------------------------------------------------------------
// Replicated auth (fastetcd#32).
//
// Every auth change is a raft log entry, an [`AuthOp`], validated and
// applied by each member's state machine against its own auth tables,
// as etcd applies its auth requests. Given the same tables (which the
// entries themselves and the raft snapshot keep identical) every member
// reaches the same result, including the same error.
//
// What lives only in memory, the enabled flag the interceptor reads and
// the token set, is [`AuthMemory`], owned by the `MvccStore` so the
// state machine updates it on apply and the server reads the same one.
// ---------------------------------------------------------------------

use std::collections::HashMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use sha2::{Digest, Sha256};

use crate::{Snapshot, StorageError, WriteBatch};

/// One auth change, as proposed through raft. Passwords arrive hashed:
/// the member serving the call hashes, so every member stores the same
/// hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuthOp {
    Enable,
    Disable,
    /// A successful `Authenticate`: the serving member checked the
    /// password and chose the token. Every member adds it on apply, so
    /// any member accepts it (etcd's "simple" token provider).
    Authenticate { user: String, token: String },
    UserAdd { name: String, password_hash: String, no_password: bool },
    UserDelete { name: String },
    UserChangePassword { name: String, password_hash: String },
    UserGrantRole { user: String, role: String },
    UserRevokeRole { name: String, role: String },
    RoleAdd { name: String },
    RoleDelete { name: String },
    RoleGrantPermission { name: String, perm: StoredPermission },
    RoleRevokePermission { name: String, key: Vec<u8>, range_end: Vec<u8> },
    /// Replace every auth table with `tables`: `fastetcd-ctl auth adopt`,
    /// converging members whose tables diverged before replication.
    Adopt { tables: AuthTables },
}

/// Why an [`AuthOp`] was refused at apply. Deterministic: every member
/// refuses the same entry the same way.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuthApplyError {
    NotFound(String),
    AlreadyExists(String),
    FailedPrecondition(String),
}

impl std::fmt::Display for AuthApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthApplyError::NotFound(m)
            | AuthApplyError::AlreadyExists(m)
            | AuthApplyError::FailedPrecondition(m) => f.write_str(m),
        }
    }
}

type KvPair = (Vec<u8>, Vec<u8>);

/// The raw contents of the three auth tables, in key order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthTables {
    pub users: Vec<KvPair>,
    pub roles: Vec<KvPair>,
    pub state: Vec<KvPair>,
}

impl AuthTables {
    /// Read all three tables from one engine snapshot.
    pub async fn read(snap: &dyn Snapshot) -> Result<Self, StorageError> {
        let all = |t| snap.range(t, Bound::Unbounded, Bound::Unbounded, 0);
        Ok(Self {
            users: all(TABLE_AUTH_USERS).await?,
            roles: all(TABLE_AUTH_ROLES).await?,
            state: all(TABLE_AUTH_STATE).await?,
        })
    }

    pub fn enabled(&self) -> bool {
        self.state
            .iter()
            .any(|(k, v)| k.as_slice() == META_AUTH_ENABLED && v.first().copied().unwrap_or(0) != 0)
    }

    /// Replace the three tables with these contents, in `batch`.
    pub fn replace_into(&self, batch: &mut WriteBatch) {
        for t in [TABLE_AUTH_USERS, TABLE_AUTH_ROLES, TABLE_AUTH_STATE] {
            batch.delete_range(t, b"", &[0xFFu8; 64]);
        }
        for (k, v) in &self.users {
            batch.put(TABLE_AUTH_USERS, k, v);
        }
        for (k, v) in &self.roles {
            batch.put(TABLE_AUTH_ROLES, k, v);
        }
        for (k, v) in &self.state {
            batch.put(TABLE_AUTH_STATE, k, v);
        }
    }

    /// A digest of what the tables *mean*: users, roles, and whether
    /// auth is on. A store that never touched the enabled flag and one
    /// that disabled auth hash the same. Members compare this before
    /// replicating (fastetcd#32).
    pub fn digest(&self) -> String {
        let mut h = Sha256::new();
        for (tag, rows) in [(b'u', &self.users), (b'r', &self.roles)] {
            for (k, v) in rows {
                h.update([tag]);
                h.update((k.len() as u64).to_be_bytes());
                h.update(k);
                h.update((v.len() as u64).to_be_bytes());
                h.update(v);
            }
        }
        h.update([b'e', self.enabled() as u8]);
        h.finalize().iter().map(|b| format!("{b:02x}")).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.users.is_empty() && self.roles.is_empty() && !self.enabled()
    }
}

/// The in-memory half of auth: whether it is on (read by the request
/// interceptor on every call, so an atomic) and the token set. Cheaply
/// clonable; every clone is the same state.
#[derive(Clone, Default)]
pub struct AuthMemory {
    enabled: Arc<AtomicBool>,
    tokens: Arc<StdMutex<HashMap<String, String>>>,
}

impl AuthMemory {
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }
    pub fn user_for_token(&self, token: &str) -> Option<String> {
        self.tokens.lock().ok()?.get(token).cloned()
    }
    pub fn insert_token(&self, token: &str, user: &str) {
        if let Ok(mut g) = self.tokens.lock() {
            g.insert(token.to_string(), user.to_string());
        }
    }
    pub fn revoke_user_tokens(&self, user: &str) {
        if let Ok(mut g) = self.tokens.lock() {
            g.retain(|_, u| u != user);
        }
    }
    fn retain_users(&self, keep: impl Fn(&str) -> bool) {
        if let Ok(mut g) = self.tokens.lock() {
            g.retain(|_, u| keep(u));
        }
    }
}

/// What applying an [`AuthOp`] does after its batch commits: the
/// in-memory effects, which must never run for a write that failed.
#[derive(Debug, Default)]
pub struct AuthEffects {
    set_enabled: Option<bool>,
    add_token: Option<(String, String)>,
    revoke_user: Option<String>,
    keep_only_users: Option<Vec<String>>,
}

impl AuthEffects {
    pub fn apply(self, mem: &AuthMemory) {
        if let Some(e) = self.set_enabled {
            mem.set_enabled(e);
        }
        if let Some(u) = &self.revoke_user {
            mem.revoke_user_tokens(u);
        }
        if let Some(keep) = &self.keep_only_users {
            mem.retain_users(|u| keep.iter().any(|k| k == u));
        }
        if let Some((token, user)) = self.add_token {
            mem.insert_token(&token, &user);
        }
    }
}

fn other(msg: String) -> StorageError {
    StorageError::Io(Box::new(std::io::Error::other(msg)))
}

fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>, StorageError> {
    bincode::serialize(v).map_err(|e| other(format!("auth encode: {e}")))
}

async fn get_user(snap: &dyn Snapshot, name: &str) -> Result<Option<StoredUser>, StorageError> {
    match snap.get(TABLE_AUTH_USERS, name.as_bytes()).await? {
        Some(b) => bincode::deserialize(&b)
            .map(Some)
            .map_err(|e| other(format!("auth decode user {name}: {e}"))),
        None => Ok(None),
    }
}

async fn get_role(snap: &dyn Snapshot, name: &str) -> Result<Option<StoredRole>, StorageError> {
    match snap.get(TABLE_AUTH_ROLES, name.as_bytes()).await? {
        Some(b) => bincode::deserialize(&b)
            .map(Some)
            .map_err(|e| other(format!("auth decode role {name}: {e}"))),
        None => Ok(None),
    }
}

fn user_not_found(name: &str) -> AuthApplyError {
    AuthApplyError::NotFound(format!("auth: user {name} not found"))
}

fn role_not_found(name: &str) -> AuthApplyError {
    AuthApplyError::NotFound(format!("auth: role {name} not found"))
}

/// Validate `op` against the tables in `snap` and stage its writes in
/// `batch`. Returns the in-memory effects to run once the batch has
/// committed, or the refusal (nothing staged). Semantics are those the
/// Auth service had when it wrote locally.
pub async fn plan(
    snap: &dyn Snapshot,
    op: &AuthOp,
    batch: &mut WriteBatch,
) -> Result<Result<AuthEffects, AuthApplyError>, StorageError> {
    let mut fx = AuthEffects::default();
    let put_user = |batch: &mut WriteBatch, u: &StoredUser| -> Result<(), StorageError> {
        batch.put(TABLE_AUTH_USERS, u.name.as_bytes(), &encode(u)?);
        Ok(())
    };
    let put_role = |batch: &mut WriteBatch, r: &StoredRole| -> Result<(), StorageError> {
        batch.put(TABLE_AUTH_ROLES, r.name.as_bytes(), &encode(r)?);
        Ok(())
    };
    match op {
        AuthOp::Enable => {
            // As etcd: a root user must exist before auth can be on.
            if get_user(snap, "root").await?.is_none() {
                return Ok(Err(AuthApplyError::FailedPrecondition(
                    "root user must exist before AuthEnable".into(),
                )));
            }
            batch.put(TABLE_AUTH_STATE, META_AUTH_ENABLED, &[1u8]);
            fx.set_enabled = Some(true);
        }
        AuthOp::Disable => {
            batch.put(TABLE_AUTH_STATE, META_AUTH_ENABLED, &[0u8]);
            fx.set_enabled = Some(false);
        }
        AuthOp::Authenticate { user, token } => {
            // The user may have been deleted between the password check
            // and this entry.
            if get_user(snap, user).await?.is_none() {
                return Ok(Err(user_not_found(user)));
            }
            fx.add_token = Some((token.clone(), user.clone()));
        }
        AuthOp::UserAdd { name, password_hash, no_password } => {
            if get_user(snap, name).await?.is_some() {
                return Ok(Err(AuthApplyError::AlreadyExists(format!(
                    "auth: user {name} already exists"
                ))));
            }
            put_user(
                batch,
                &StoredUser {
                    name: name.clone(),
                    password_hash: password_hash.clone(),
                    roles: Vec::new(),
                    no_password: *no_password,
                },
            )?;
        }
        AuthOp::UserDelete { name } => {
            if get_user(snap, name).await?.is_none() {
                return Ok(Err(user_not_found(name)));
            }
            batch.delete(TABLE_AUTH_USERS, name.as_bytes());
            fx.revoke_user = Some(name.clone());
        }
        AuthOp::UserChangePassword { name, password_hash } => {
            let Some(mut u) = get_user(snap, name).await? else {
                return Ok(Err(user_not_found(name)));
            };
            u.password_hash = password_hash.clone();
            u.no_password = false;
            put_user(batch, &u)?;
            fx.revoke_user = Some(name.clone());
        }
        AuthOp::UserGrantRole { user, role } => {
            let Some(mut u) = get_user(snap, user).await? else {
                return Ok(Err(user_not_found(user)));
            };
            if get_role(snap, role).await?.is_none() {
                return Ok(Err(role_not_found(role)));
            }
            if !u.roles.contains(role) {
                u.roles.push(role.clone());
            }
            put_user(batch, &u)?;
        }
        AuthOp::UserRevokeRole { name, role } => {
            let Some(mut u) = get_user(snap, name).await? else {
                return Ok(Err(user_not_found(name)));
            };
            u.roles.retain(|r| r != role);
            put_user(batch, &u)?;
        }
        AuthOp::RoleAdd { name } => {
            if get_role(snap, name).await?.is_some() {
                return Ok(Err(AuthApplyError::AlreadyExists(format!(
                    "auth: role {name} already exists"
                ))));
            }
            put_role(batch, &StoredRole { name: name.clone(), permissions: Vec::new() })?;
        }
        AuthOp::RoleDelete { name } => {
            if get_role(snap, name).await?.is_none() {
                return Ok(Err(role_not_found(name)));
            }
            batch.delete(TABLE_AUTH_ROLES, name.as_bytes());
            // Drop the role from every user that holds it.
            for (_, v) in snap
                .range(TABLE_AUTH_USERS, Bound::Unbounded, Bound::Unbounded, 0)
                .await?
            {
                let Ok(mut u) = bincode::deserialize::<StoredUser>(&v) else { continue };
                let before = u.roles.len();
                u.roles.retain(|r| r != name);
                if u.roles.len() != before {
                    put_user(batch, &u)?;
                }
            }
        }
        AuthOp::RoleGrantPermission { name, perm } => {
            let Some(mut r) = get_role(snap, name).await? else {
                return Ok(Err(role_not_found(name)));
            };
            r.permissions.push(perm.clone());
            put_role(batch, &r)?;
        }
        AuthOp::RoleRevokePermission { name, key, range_end } => {
            let Some(mut r) = get_role(snap, name).await? else {
                return Ok(Err(role_not_found(name)));
            };
            r.permissions.retain(|p| &p.key != key || &p.range_end != range_end);
            put_role(batch, &r)?;
        }
        AuthOp::Adopt { tables } => {
            tables.replace_into(batch);
            fx.set_enabled = Some(tables.enabled());
            fx.keep_only_users = Some(
                tables
                    .users
                    .iter()
                    .filter_map(|(k, _)| String::from_utf8(k.clone()).ok())
                    .collect(),
            );
        }
    }
    Ok(Ok(fx))
}

#[cfg(all(test, feature = "redb-engine"))]
mod tests {
    use super::*;
    use crate::mvcc::MvccStore;
    use crate::redb_engine::RedbEngine;

    async fn open() -> (tempfile::TempDir, MvccStore) {
        let dir = tempfile::tempdir().unwrap();
        let eng = RedbEngine::open(dir.path().join("a.redb")).unwrap();
        let store = MvccStore::open(Arc::new(eng)).await.unwrap();
        (dir, store)
    }

    async fn ok(s: &MvccStore, op: AuthOp) {
        let (_, r) = s.apply_auth(&op).await.unwrap();
        r.unwrap_or_else(|e| panic!("{op:?} refused: {e}"));
    }

    async fn refused(s: &MvccStore, op: AuthOp) -> AuthApplyError {
        s.apply_auth(&op).await.unwrap().1.unwrap_err()
    }

    fn user_add(name: &str) -> AuthOp {
        AuthOp::UserAdd { name: name.into(), password_hash: "h".into(), no_password: false }
    }

    #[tokio::test]
    async fn enable_needs_root_and_sets_the_flag_in_memory_and_on_disk() {
        let (dir, s) = open().await;
        assert!(matches!(
            refused(&s, AuthOp::Enable).await,
            AuthApplyError::FailedPrecondition(_)
        ));
        assert!(!s.auth_memory().is_enabled());
        ok(&s, user_add("root")).await;
        ok(&s, AuthOp::Enable).await;
        assert!(s.auth_memory().is_enabled());
        assert!(s.auth_tables().await.unwrap().enabled());

        // Survives reopening the store.
        drop(s);
        let eng = RedbEngine::open(dir.path().join("a.redb")).unwrap();
        let s = MvccStore::open(Arc::new(eng)).await.unwrap();
        assert!(s.auth_memory().is_enabled());
        ok(&s, AuthOp::Disable).await;
        assert!(!s.auth_memory().is_enabled());
    }

    #[tokio::test]
    async fn refusals_write_nothing_and_are_the_same_errors_as_before() {
        let (_d, s) = open().await;
        ok(&s, user_add("alice")).await;
        let before = s.auth_tables().await.unwrap();
        assert!(matches!(refused(&s, user_add("alice")).await, AuthApplyError::AlreadyExists(_)));
        assert!(matches!(
            refused(&s, AuthOp::UserGrantRole { user: "alice".into(), role: "nope".into() }).await,
            AuthApplyError::NotFound(_)
        ));
        assert!(matches!(
            refused(&s, AuthOp::UserDelete { name: "bob".into() }).await,
            AuthApplyError::NotFound(_)
        ));
        assert_eq!(s.auth_tables().await.unwrap(), before);
    }

    #[tokio::test]
    async fn tokens_follow_authenticate_delete_and_password_change() {
        let (_d, s) = open().await;
        ok(&s, user_add("alice")).await;
        ok(&s, AuthOp::Authenticate { user: "alice".into(), token: "t1".into() }).await;
        assert_eq!(s.auth_memory().user_for_token("t1").as_deref(), Some("alice"));
        ok(&s, AuthOp::UserChangePassword { name: "alice".into(), password_hash: "h2".into() })
            .await;
        assert_eq!(s.auth_memory().user_for_token("t1"), None);
        ok(&s, AuthOp::Authenticate { user: "alice".into(), token: "t2".into() }).await;
        ok(&s, AuthOp::UserDelete { name: "alice".into() }).await;
        assert_eq!(s.auth_memory().user_for_token("t2"), None);
        // A token for a user deleted before its entry applied is refused.
        assert!(matches!(
            refused(&s, AuthOp::Authenticate { user: "alice".into(), token: "t3".into() }).await,
            AuthApplyError::NotFound(_)
        ));
        assert_eq!(s.auth_memory().user_for_token("t3"), None);
    }

    #[tokio::test]
    async fn role_delete_strips_the_role_from_its_users() {
        let (_d, s) = open().await;
        ok(&s, user_add("alice")).await;
        ok(&s, AuthOp::RoleAdd { name: "r".into() }).await;
        ok(&s, AuthOp::UserGrantRole { user: "alice".into(), role: "r".into() }).await;
        ok(&s, AuthOp::RoleDelete { name: "r".into() }).await;
        let t = s.auth_tables().await.unwrap();
        assert!(t.roles.is_empty());
        let u: StoredUser = bincode::deserialize(&t.users[0].1).unwrap();
        assert!(u.roles.is_empty());
    }

    #[tokio::test]
    async fn adopt_replaces_everything_and_digests_match() {
        let (_a, a) = open().await;
        let (_b, b) = open().await;
        ok(&a, user_add("root")).await;
        ok(&a, AuthOp::RoleAdd { name: "r".into() }).await;
        ok(&a, AuthOp::Enable).await;
        ok(&b, user_add("mallory")).await;
        ok(&b, AuthOp::Authenticate { user: "mallory".into(), token: "tm".into() }).await;
        let ta = a.auth_tables().await.unwrap();
        assert_ne!(ta.digest(), b.auth_tables().await.unwrap().digest());

        ok(&b, AuthOp::Adopt { tables: ta.clone() }).await;
        let tb = b.auth_tables().await.unwrap();
        assert_eq!(tb, ta);
        assert_eq!(tb.digest(), ta.digest());
        assert!(b.auth_memory().is_enabled());
        // mallory is gone, and so is her token.
        assert_eq!(b.auth_memory().user_for_token("tm"), None);
    }

    #[tokio::test]
    async fn digest_ignores_a_disabled_flag_that_was_never_set() {
        let (_a, a) = open().await;
        let (_b, b) = open().await;
        ok(&b, AuthOp::Disable).await;
        assert_eq!(
            a.auth_tables().await.unwrap().digest(),
            b.auth_tables().await.unwrap().digest()
        );
        assert!(a.auth_tables().await.unwrap().is_empty());
    }
}
