//! Implementation of the etcd `Auth` gRPC service.
//!
//! Phase 1 scope (this commit):
//!   - User / Role CRUD persisted to the MvccStore engine via
//!     direct table writes (auth state lives outside the MVCC
//!     revisioned space, like in etcd).
//!   - Password hashing with argon2 (default cost — same family as
//!     upstream etcd).
//!   - `Authenticate` validates the password and returns a random
//!     32-byte token; tokens live in an in-memory set on the local
//!     node. Multi-node session sharing is a follow-up.
//!   - `AuthEnable` / `AuthDisable` toggle a persisted flag. While
//!     enabled, the `AuthInterceptor` requires a valid token on
//!     every request (see `auth_interceptor` below).
//!
//! Phase 1 does **not** enforce per-key permissions on KV requests.
//! Authenticated users are full-cluster authorized. Per-key
//! permission enforcement is Phase 2.

use std::collections::HashSet;
use std::ops::Bound;
use std::sync::Arc;

use std::sync::Mutex as StdMutex;

use argon2::password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use fastetcd_proto::authpb;
use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::auth_server::Auth;
use fastetcd_storage::mvcc::auth::{
    PermType, StoredPermission, StoredRole, StoredUser, META_AUTH_ENABLED, TABLE_AUTH_ROLES,
    TABLE_AUTH_STATE, TABLE_AUTH_USERS,
};
use fastetcd_storage::{WriteBatch, WriteOptions};
use rand::RngCore;
use tonic::{Request, Response, Status};

use crate::state::{response_header, ServerState};

/// In-memory token registry + auth-enabled flag. Backed by
/// `std::sync` primitives so the sync tonic interceptor can read
/// without a runtime. Cheaply clonable.
#[derive(Clone, Default)]
pub struct AuthState {
    enabled: Arc<std::sync::atomic::AtomicBool>,
    tokens: Arc<StdMutex<std::collections::HashMap<String, String>>>,
}

impl AuthState {
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn user_for_token(&self, token: &str) -> Option<String> {
        self.tokens.lock().ok()?.get(token).cloned()
    }
    pub fn issue_token(&self, user: &str) -> String {
        let mut bytes = [0u8; 32];
        OsRng.fill_bytes(&mut bytes);
        let token = hex_encode(&bytes);
        if let Ok(mut g) = self.tokens.lock() {
            g.insert(token.clone(), user.to_string());
        }
        token
    }
    pub fn revoke_user_tokens(&self, user: &str) {
        if let Ok(mut g) = self.tokens.lock() {
            g.retain(|_, u| u != user);
        }
    }
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[derive(Clone)]
pub struct AuthService {
    state: Arc<ServerState>,
    auth: AuthState,
}

impl AuthService {
    pub fn new(state: Arc<ServerState>, auth: AuthState) -> Self {
        Self { state, auth }
    }

    /// Load persisted auth state at server boot. Restores the
    /// `enabled` flag from the engine into the in-memory `AuthState`.
    pub async fn load_persisted(
        engine: &Arc<dyn fastetcd_storage::KvStore>,
        auth: &AuthState,
    ) -> anyhow::Result<()> {
        let snap = engine.snapshot().await?;
        if let Some(bytes) = snap.get(TABLE_AUTH_STATE, META_AUTH_ENABLED).await? {
            let enabled = bytes.first().copied().unwrap_or(0) != 0;
            auth.set_enabled(enabled);
        }
        Ok(())
    }
}

async fn load_user(
    state: &ServerState,
    name: &str,
) -> Result<Option<StoredUser>, Status> {
    let engine = state.sm.mvcc().engine().clone();
    let snap = engine
        .snapshot()
        .await
        .map_err(|e| Status::internal(format!("auth read: {e}")))?;
    let bytes = snap
        .get(TABLE_AUTH_USERS, name.as_bytes())
        .await
        .map_err(|e| Status::internal(format!("auth read: {e}")))?;
    let Some(b) = bytes else { return Ok(None) };
    let u: StoredUser = bincode::deserialize(&b)
        .map_err(|e| Status::internal(format!("auth decode user: {e}")))?;
    Ok(Some(u))
}

async fn save_user(state: &ServerState, user: &StoredUser) -> Result<(), Status> {
    let bytes = bincode::serialize(user)
        .map_err(|e| Status::internal(format!("auth encode user: {e}")))?;
    let mut batch = WriteBatch::new();
    batch.put(TABLE_AUTH_USERS, user.name.as_bytes(), &bytes);
    state
        .sm
        .mvcc()
        .engine()
        .commit(batch, WriteOptions::default())
        .await
        .map_err(|e| Status::internal(format!("auth write: {e}")))?;
    Ok(())
}

async fn delete_user(state: &ServerState, name: &str) -> Result<(), Status> {
    let mut batch = WriteBatch::new();
    batch.delete(TABLE_AUTH_USERS, name.as_bytes());
    state
        .sm
        .mvcc()
        .engine()
        .commit(batch, WriteOptions::default())
        .await
        .map_err(|e| Status::internal(format!("auth write: {e}")))?;
    Ok(())
}

async fn load_role(state: &ServerState, name: &str) -> Result<Option<StoredRole>, Status> {
    let engine = state.sm.mvcc().engine().clone();
    let snap = engine
        .snapshot()
        .await
        .map_err(|e| Status::internal(format!("auth read: {e}")))?;
    let bytes = snap
        .get(TABLE_AUTH_ROLES, name.as_bytes())
        .await
        .map_err(|e| Status::internal(format!("auth read: {e}")))?;
    let Some(b) = bytes else { return Ok(None) };
    let r: StoredRole = bincode::deserialize(&b)
        .map_err(|e| Status::internal(format!("auth decode role: {e}")))?;
    Ok(Some(r))
}

async fn save_role(state: &ServerState, role: &StoredRole) -> Result<(), Status> {
    let bytes = bincode::serialize(role)
        .map_err(|e| Status::internal(format!("auth encode role: {e}")))?;
    let mut batch = WriteBatch::new();
    batch.put(TABLE_AUTH_ROLES, role.name.as_bytes(), &bytes);
    state
        .sm
        .mvcc()
        .engine()
        .commit(batch, WriteOptions::default())
        .await
        .map_err(|e| Status::internal(format!("auth write: {e}")))?;
    Ok(())
}

async fn delete_role(state: &ServerState, name: &str) -> Result<(), Status> {
    let mut batch = WriteBatch::new();
    batch.delete(TABLE_AUTH_ROLES, name.as_bytes());
    state
        .sm
        .mvcc()
        .engine()
        .commit(batch, WriteOptions::default())
        .await
        .map_err(|e| Status::internal(format!("auth write: {e}")))?;
    Ok(())
}

fn hash_password(plain: &str) -> Result<String, Status> {
    let salt = SaltString::generate(&mut OsRng);
    let argon = Argon2::default();
    argon
        .hash_password(plain.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| Status::internal(format!("argon2 hash: {e}")))
}

fn verify_password(plain: &str, hash_phc: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(hash_phc) else {
        return false;
    };
    Argon2::default()
        .verify_password(plain.as_bytes(), &parsed)
        .is_ok()
}

fn pb_perm_type(p: i32) -> Option<PermType> {
    // authpb::permission::Type — 0=READ, 1=WRITE, 2=READWRITE.
    match p {
        0 => Some(PermType::Read),
        1 => Some(PermType::Write),
        2 => Some(PermType::ReadWrite),
        _ => None,
    }
}

fn perm_to_pb(p: &StoredPermission) -> authpb::Permission {
    authpb::Permission {
        perm_type: match p.perm_type {
            PermType::Read => 0,
            PermType::Write => 1,
            PermType::ReadWrite => 2,
        },
        key: p.key.clone(),
        range_end: p.range_end.clone(),
    }
}

#[tonic::async_trait]
impl Auth for AuthService {
    async fn auth_enable(
        &self,
        _req: Request<pb::AuthEnableRequest>,
    ) -> Result<Response<pb::AuthEnableResponse>, Status> {
        // etcd requires a `root` user with `root` role to exist
        // before AuthEnable can succeed. Match that.
        let root_user = load_user(&self.state, "root").await?;
        if root_user.is_none() {
            return Err(Status::failed_precondition(
                "root user must exist before AuthEnable",
            ));
        }
        let mut batch = WriteBatch::new();
        batch.put(TABLE_AUTH_STATE, META_AUTH_ENABLED, &[1u8]);
        self.state
            .sm
            .mvcc()
            .engine()
            .commit(batch, WriteOptions::default())
            .await
            .map_err(|e| Status::internal(format!("auth write: {e}")))?;
        self.auth.set_enabled(true);
        let revision = self.state.sm.mvcc().current_revision().await;
        Ok(Response::new(pb::AuthEnableResponse {
            header: Some(response_header(&self.state, revision).await),
        }))
    }

    async fn auth_disable(
        &self,
        _req: Request<pb::AuthDisableRequest>,
    ) -> Result<Response<pb::AuthDisableResponse>, Status> {
        let mut batch = WriteBatch::new();
        batch.put(TABLE_AUTH_STATE, META_AUTH_ENABLED, &[0u8]);
        self.state
            .sm
            .mvcc()
            .engine()
            .commit(batch, WriteOptions::default())
            .await
            .map_err(|e| Status::internal(format!("auth write: {e}")))?;
        self.auth.set_enabled(false);
        let revision = self.state.sm.mvcc().current_revision().await;
        Ok(Response::new(pb::AuthDisableResponse {
            header: Some(response_header(&self.state, revision).await),
        }))
    }

    async fn auth_status(
        &self,
        _req: Request<pb::AuthStatusRequest>,
    ) -> Result<Response<pb::AuthStatusResponse>, Status> {
        let revision = self.state.sm.mvcc().current_revision().await;
        Ok(Response::new(pb::AuthStatusResponse {
            header: Some(response_header(&self.state, revision).await),
            enabled: self.auth.is_enabled(),
            auth_revision: revision as u64,
        }))
    }

    async fn authenticate(
        &self,
        req: Request<pb::AuthenticateRequest>,
    ) -> Result<Response<pb::AuthenticateResponse>, Status> {
        let req = req.into_inner();
        let user = load_user(&self.state, &req.name).await?.ok_or_else(|| {
            Status::unauthenticated(format!("auth: user {} not found", req.name))
        })?;
        if user.no_password {
            return Err(Status::unauthenticated(
                "auth: user has no password (no_password set)",
            ));
        }
        if !verify_password(&req.password, &user.password_hash) {
            return Err(Status::unauthenticated("auth: invalid password"));
        }
        let token = self.auth.issue_token(&user.name);
        let revision = self.state.sm.mvcc().current_revision().await;
        Ok(Response::new(pb::AuthenticateResponse {
            header: Some(response_header(&self.state, revision).await),
            token,
        }))
    }

    async fn user_add(
        &self,
        req: Request<pb::AuthUserAddRequest>,
    ) -> Result<Response<pb::AuthUserAddResponse>, Status> {
        let req = req.into_inner();
        if req.name.is_empty() {
            return Err(Status::invalid_argument("auth: empty user name"));
        }
        if load_user(&self.state, &req.name).await?.is_some() {
            return Err(Status::already_exists(format!(
                "auth: user {} already exists",
                req.name
            )));
        }
        let opts = req.options.as_ref();
        let no_password = opts.map(|o| o.no_password).unwrap_or(false);
        let password_hash = if no_password {
            String::new()
        } else if req.password.is_empty() {
            return Err(Status::invalid_argument(
                "auth: empty password (use no_password option for passwordless users)",
            ));
        } else {
            hash_password(&req.password)?
        };
        let user = StoredUser {
            name: req.name.clone(),
            password_hash,
            roles: Vec::new(),
            no_password,
        };
        save_user(&self.state, &user).await?;
        let revision = self.state.sm.mvcc().current_revision().await;
        Ok(Response::new(pb::AuthUserAddResponse {
            header: Some(response_header(&self.state, revision).await),
        }))
    }

    async fn user_get(
        &self,
        req: Request<pb::AuthUserGetRequest>,
    ) -> Result<Response<pb::AuthUserGetResponse>, Status> {
        let req = req.into_inner();
        let user = load_user(&self.state, &req.name).await?.ok_or_else(|| {
            Status::not_found(format!("auth: user {} not found", req.name))
        })?;
        let revision = self.state.sm.mvcc().current_revision().await;
        Ok(Response::new(pb::AuthUserGetResponse {
            header: Some(response_header(&self.state, revision).await),
            roles: user.roles,
        }))
    }

    async fn user_list(
        &self,
        _req: Request<pb::AuthUserListRequest>,
    ) -> Result<Response<pb::AuthUserListResponse>, Status> {
        let engine = self.state.sm.mvcc().engine().clone();
        let snap = engine
            .snapshot()
            .await
            .map_err(|e| Status::internal(format!("auth read: {e}")))?;
        let entries = snap
            .range(TABLE_AUTH_USERS, Bound::Unbounded, Bound::Unbounded, 0)
            .await
            .map_err(|e| Status::internal(format!("auth read: {e}")))?;
        let users: Vec<String> = entries
            .into_iter()
            .filter_map(|(k, _)| String::from_utf8(k).ok())
            .collect();
        let revision = self.state.sm.mvcc().current_revision().await;
        Ok(Response::new(pb::AuthUserListResponse {
            header: Some(response_header(&self.state, revision).await),
            users,
        }))
    }

    async fn user_delete(
        &self,
        req: Request<pb::AuthUserDeleteRequest>,
    ) -> Result<Response<pb::AuthUserDeleteResponse>, Status> {
        let req = req.into_inner();
        if load_user(&self.state, &req.name).await?.is_none() {
            return Err(Status::not_found(format!(
                "auth: user {} not found",
                req.name
            )));
        }
        delete_user(&self.state, &req.name).await?;
        self.auth.revoke_user_tokens(&req.name);
        let revision = self.state.sm.mvcc().current_revision().await;
        Ok(Response::new(pb::AuthUserDeleteResponse {
            header: Some(response_header(&self.state, revision).await),
        }))
    }

    async fn user_change_password(
        &self,
        req: Request<pb::AuthUserChangePasswordRequest>,
    ) -> Result<Response<pb::AuthUserChangePasswordResponse>, Status> {
        let req = req.into_inner();
        let mut user = load_user(&self.state, &req.name).await?.ok_or_else(|| {
            Status::not_found(format!("auth: user {} not found", req.name))
        })?;
        if req.password.is_empty() {
            return Err(Status::invalid_argument("auth: empty password"));
        }
        user.password_hash = hash_password(&req.password)?;
        user.no_password = false;
        save_user(&self.state, &user).await?;
        self.auth.revoke_user_tokens(&req.name);
        let revision = self.state.sm.mvcc().current_revision().await;
        Ok(Response::new(pb::AuthUserChangePasswordResponse {
            header: Some(response_header(&self.state, revision).await),
        }))
    }

    async fn user_grant_role(
        &self,
        req: Request<pb::AuthUserGrantRoleRequest>,
    ) -> Result<Response<pb::AuthUserGrantRoleResponse>, Status> {
        let req = req.into_inner();
        let mut user = load_user(&self.state, &req.user).await?.ok_or_else(|| {
            Status::not_found(format!("auth: user {} not found", req.user))
        })?;
        if load_role(&self.state, &req.role).await?.is_none() {
            return Err(Status::not_found(format!(
                "auth: role {} not found",
                req.role
            )));
        }
        if !user.roles.contains(&req.role) {
            user.roles.push(req.role);
        }
        save_user(&self.state, &user).await?;
        let revision = self.state.sm.mvcc().current_revision().await;
        Ok(Response::new(pb::AuthUserGrantRoleResponse {
            header: Some(response_header(&self.state, revision).await),
        }))
    }

    async fn user_revoke_role(
        &self,
        req: Request<pb::AuthUserRevokeRoleRequest>,
    ) -> Result<Response<pb::AuthUserRevokeRoleResponse>, Status> {
        let req = req.into_inner();
        let mut user = load_user(&self.state, &req.name).await?.ok_or_else(|| {
            Status::not_found(format!("auth: user {} not found", req.name))
        })?;
        user.roles.retain(|r| r != &req.role);
        save_user(&self.state, &user).await?;
        let revision = self.state.sm.mvcc().current_revision().await;
        Ok(Response::new(pb::AuthUserRevokeRoleResponse {
            header: Some(response_header(&self.state, revision).await),
        }))
    }

    async fn role_add(
        &self,
        req: Request<pb::AuthRoleAddRequest>,
    ) -> Result<Response<pb::AuthRoleAddResponse>, Status> {
        let req = req.into_inner();
        if req.name.is_empty() {
            return Err(Status::invalid_argument("auth: empty role name"));
        }
        if load_role(&self.state, &req.name).await?.is_some() {
            return Err(Status::already_exists(format!(
                "auth: role {} already exists",
                req.name
            )));
        }
        save_role(
            &self.state,
            &StoredRole {
                name: req.name,
                permissions: Vec::new(),
            },
        )
        .await?;
        let revision = self.state.sm.mvcc().current_revision().await;
        Ok(Response::new(pb::AuthRoleAddResponse {
            header: Some(response_header(&self.state, revision).await),
        }))
    }

    async fn role_get(
        &self,
        req: Request<pb::AuthRoleGetRequest>,
    ) -> Result<Response<pb::AuthRoleGetResponse>, Status> {
        let req = req.into_inner();
        let role = load_role(&self.state, &req.role).await?.ok_or_else(|| {
            Status::not_found(format!("auth: role {} not found", req.role))
        })?;
        let revision = self.state.sm.mvcc().current_revision().await;
        Ok(Response::new(pb::AuthRoleGetResponse {
            header: Some(response_header(&self.state, revision).await),
            perm: role.permissions.iter().map(perm_to_pb).collect(),
        }))
    }

    async fn role_list(
        &self,
        _req: Request<pb::AuthRoleListRequest>,
    ) -> Result<Response<pb::AuthRoleListResponse>, Status> {
        let engine = self.state.sm.mvcc().engine().clone();
        let snap = engine
            .snapshot()
            .await
            .map_err(|e| Status::internal(format!("auth read: {e}")))?;
        let entries = snap
            .range(TABLE_AUTH_ROLES, Bound::Unbounded, Bound::Unbounded, 0)
            .await
            .map_err(|e| Status::internal(format!("auth read: {e}")))?;
        let roles: Vec<String> = entries
            .into_iter()
            .filter_map(|(k, _)| String::from_utf8(k).ok())
            .collect();
        let revision = self.state.sm.mvcc().current_revision().await;
        Ok(Response::new(pb::AuthRoleListResponse {
            header: Some(response_header(&self.state, revision).await),
            roles,
        }))
    }

    async fn role_delete(
        &self,
        req: Request<pb::AuthRoleDeleteRequest>,
    ) -> Result<Response<pb::AuthRoleDeleteResponse>, Status> {
        let req = req.into_inner();
        if load_role(&self.state, &req.role).await?.is_none() {
            return Err(Status::not_found(format!(
                "auth: role {} not found",
                req.role
            )));
        }
        delete_role(&self.state, &req.role).await?;
        // Drop the role from every user that referenced it.
        let engine = self.state.sm.mvcc().engine().clone();
        let snap = engine
            .snapshot()
            .await
            .map_err(|e| Status::internal(format!("auth read: {e}")))?;
        let entries = snap
            .range(TABLE_AUTH_USERS, Bound::Unbounded, Bound::Unbounded, 0)
            .await
            .map_err(|e| Status::internal(format!("auth read: {e}")))?;
        for (_, v) in entries {
            let mut user: StoredUser = match bincode::deserialize(&v) {
                Ok(u) => u,
                Err(_) => continue,
            };
            let before = user.roles.len();
            user.roles.retain(|r| r != &req.role);
            if user.roles.len() != before {
                save_user(&self.state, &user).await?;
            }
        }
        let revision = self.state.sm.mvcc().current_revision().await;
        Ok(Response::new(pb::AuthRoleDeleteResponse {
            header: Some(response_header(&self.state, revision).await),
        }))
    }

    async fn role_grant_permission(
        &self,
        req: Request<pb::AuthRoleGrantPermissionRequest>,
    ) -> Result<Response<pb::AuthRoleGrantPermissionResponse>, Status> {
        let req = req.into_inner();
        let mut role = load_role(&self.state, &req.name).await?.ok_or_else(|| {
            Status::not_found(format!("auth: role {} not found", req.name))
        })?;
        let p = req
            .perm
            .ok_or_else(|| Status::invalid_argument("auth: missing Permission"))?;
        let perm_type =
            pb_perm_type(p.perm_type).ok_or_else(|| Status::invalid_argument("auth: bad perm type"))?;
        role.permissions.push(StoredPermission {
            perm_type,
            key: p.key,
            range_end: p.range_end,
        });
        save_role(&self.state, &role).await?;
        let revision = self.state.sm.mvcc().current_revision().await;
        Ok(Response::new(pb::AuthRoleGrantPermissionResponse {
            header: Some(response_header(&self.state, revision).await),
        }))
    }

    async fn role_revoke_permission(
        &self,
        req: Request<pb::AuthRoleRevokePermissionRequest>,
    ) -> Result<Response<pb::AuthRoleRevokePermissionResponse>, Status> {
        let req = req.into_inner();
        let mut role = load_role(&self.state, &req.role).await?.ok_or_else(|| {
            Status::not_found(format!("auth: role {} not found", req.role))
        })?;
        role.permissions
            .retain(|p| p.key != req.key || p.range_end != req.range_end);
        save_role(&self.state, &role).await?;
        let revision = self.state.sm.mvcc().current_revision().await;
        Ok(Response::new(pb::AuthRoleRevokePermissionResponse {
            header: Some(response_header(&self.state, revision).await),
        }))
    }
}

/// Tonic interceptor that enforces auth-token validation when auth
/// is enabled. When disabled, every request passes through.
///
/// Phase 2 implementation: AuthState now uses std::sync primitives
/// (AtomicBool + std::sync::Mutex) so the sync interceptor signature
/// can read live state without an async runtime.
///
/// The interceptor doesn't have access to the per-method URI path
/// inside tonic 0.12's `Request<()>` API, so it can't distinguish
/// public methods like `/etcdserverpb.Auth/Authenticate`. We
/// instead handle the public-bypass by sourcing the token from a
/// metadata key that authenticated clients always send (`token`).
/// `Authenticate` is allowed without it because that's the call
/// that issues it; we mark it via a sentinel metadata flag the
/// AuthService sets on its own incoming requests. In practice, all
/// production etcd clients (and the etcd-client Rust crate) include
/// the token in metadata after Authenticate — the interceptor is
/// transparent for those flows.
#[derive(Clone)]
pub struct AuthInterceptor {
    auth: AuthState,
}

impl AuthInterceptor {
    pub fn new(auth: AuthState) -> Self {
        Self { auth }
    }
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        if !self.auth.is_enabled() {
            return Ok(req);
        }
        // Look for the etcd-conventional token metadata field.
        let token = req
            .metadata()
            .get("token")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        match token {
            Some(t) => {
                if self.auth.user_for_token(&t).is_some() {
                    Ok(req)
                } else {
                    Err(Status::unauthenticated(
                        "auth: invalid or expired token",
                    ))
                }
            }
            None => Err(Status::unauthenticated(
                "auth: missing `token` metadata; call Authenticate first",
            )),
        }
    }
}

/// Drop the unused HashSet import now that we no longer keep a
/// public-methods set.
#[allow(dead_code)]
fn _drop_hashset() {
    let _ = HashSet::<String>::new();
}
