//! Implementation of the etcd `Auth` gRPC service.
//!
//! - User / role CRUD lives in the auth tables (`auth_users`,
//!   `auth_roles`, `auth_state`), outside the MVCC revisioned space, as
//!   in etcd.
//! - **Every change is replicated through Raft (fastetcd#32).** A
//!   mutation is proposed as a `FastetcdLogEntry::Auth` and applied by
//!   every member's state machine (`MvccStore::apply_auth`), which also
//!   validates it, so every member reaches the same result. Passwords
//!   are hashed (argon2) by the member serving the call, before
//!   proposing. Changes are gated on every member running replicated
//!   auth and holding the same tables: see [`crate::auth_sync`].
//! - `Authenticate` checks the password locally, then proposes the
//!   token ("simple" tokens, as etcd's default provider): every member
//!   adds it on apply, so a token issued by one member is accepted by
//!   all. Tokens are in memory only: a restarted member has none, and
//!   clients re-authenticate, as with etcd.
//! - The enabled flag and tokens live in [`AuthState`], owned by the
//!   `MvccStore` so apply and the interceptor share one.
//! - Reads (`UserGet`, `RoleList`, `AuthStatus`, …) are served from this
//!   member's applied state.
//!
//! Per-key permissions are enforced in `crate::authz` on every KV path
//! and on `Watch`. Not yet enforced: the root-only check on the etcd
//! admin RPCs, including this service's own mutations (#31).

use std::ops::Bound;
use std::sync::Arc;

use argon2::password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use fastetcd_proto::authpb;
use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::auth_server::Auth;
use fastetcd_storage::mvcc::auth::{
    AuthOp, PermType, StoredPermission, StoredRole, StoredUser, TABLE_AUTH_ROLES,
    TABLE_AUTH_USERS,
};
use rand::RngCore;
use tonic::{Request, Response, Status};

use crate::auth_sync::propose_auth;
use crate::state::{response_header, ServerState};

/// Auth's in-memory state: the enabled flag and the token set. The one
/// the server uses is `sm.mvcc().auth_memory()`, updated by raft apply.
pub type AuthState = fastetcd_storage::mvcc::auth::AuthMemory;

/// A new random 32-byte token, hex encoded.
fn new_token() -> String {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    let mut s = String::with_capacity(64);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[derive(Clone)]
pub struct AuthService {
    state: Arc<ServerState>,
}

impl AuthService {
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }

    /// Gate, propose, and answer with the header for the result.
    async fn change(&self, op: AuthOp) -> Result<pb::ResponseHeader, Status> {
        if matches!(op, AuthOp::Authenticate { .. }) {
            self.state.auth_gate.check_upgraded(&self.state).await?;
        } else {
            self.state.auth_gate.check(&self.state).await?;
        }
        let revision = propose_auth(&self.state, op).await?;
        Ok(response_header(&self.state, revision).await)
    }

    async fn header(&self) -> pb::ResponseHeader {
        let revision = self.state.sm.mvcc().current_revision().await;
        response_header(&self.state, revision).await
    }
}

async fn load_user(state: &ServerState, name: &str) -> Result<Option<StoredUser>, Status> {
    let snap = state
        .sm
        .mvcc()
        .engine()
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

async fn load_role(state: &ServerState, name: &str) -> Result<Option<StoredRole>, Status> {
    let snap = state
        .sm
        .mvcc()
        .engine()
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

async fn list_names(state: &ServerState, table: &str) -> Result<Vec<String>, Status> {
    let snap = state
        .sm
        .mvcc()
        .engine()
        .snapshot()
        .await
        .map_err(|e| Status::internal(format!("auth read: {e}")))?;
    let entries = snap
        .range(table, Bound::Unbounded, Bound::Unbounded, 0)
        .await
        .map_err(|e| Status::internal(format!("auth read: {e}")))?;
    Ok(entries
        .into_iter()
        .filter_map(|(k, _)| String::from_utf8(k).ok())
        .collect())
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
        // Refused at apply unless a `root` user exists, as in etcd.
        let header = self.change(AuthOp::Enable).await?;
        Ok(Response::new(pb::AuthEnableResponse { header: Some(header) }))
    }

    async fn auth_disable(
        &self,
        _req: Request<pb::AuthDisableRequest>,
    ) -> Result<Response<pb::AuthDisableResponse>, Status> {
        let header = self.change(AuthOp::Disable).await?;
        Ok(Response::new(pb::AuthDisableResponse { header: Some(header) }))
    }

    async fn auth_status(
        &self,
        _req: Request<pb::AuthStatusRequest>,
    ) -> Result<Response<pb::AuthStatusResponse>, Status> {
        let revision = self.state.sm.mvcc().current_revision().await;
        Ok(Response::new(pb::AuthStatusResponse {
            header: Some(response_header(&self.state, revision).await),
            enabled: self.state.auth.is_enabled(),
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
        // Replicated, so any member accepts the token. The entry carries
        // the hash checked here; if the password changed meanwhile it is
        // refused at apply.
        let token = new_token();
        let header = self
            .change(AuthOp::Authenticate {
                user: user.name.clone(),
                token: token.clone(),
                checked_hash: user.password_hash,
            })
            .await
            .map_err(|s| match s.code() {
                tonic::Code::NotFound => Status::unauthenticated(s.message().to_string()),
                _ => s,
            })?;
        Ok(Response::new(pb::AuthenticateResponse { header: Some(header), token }))
    }

    async fn user_add(
        &self,
        req: Request<pb::AuthUserAddRequest>,
    ) -> Result<Response<pb::AuthUserAddResponse>, Status> {
        let req = req.into_inner();
        if req.name.is_empty() {
            return Err(Status::invalid_argument("auth: empty user name"));
        }
        let no_password = req.options.as_ref().map(|o| o.no_password).unwrap_or(false);
        let password_hash = if no_password {
            String::new()
        } else if req.password.is_empty() {
            return Err(Status::invalid_argument(
                "auth: empty password (use no_password option for passwordless users)",
            ));
        } else {
            hash_password(&req.password)?
        };
        let header = self
            .change(AuthOp::UserAdd { name: req.name, password_hash, no_password })
            .await?;
        Ok(Response::new(pb::AuthUserAddResponse { header: Some(header) }))
    }

    async fn user_get(
        &self,
        req: Request<pb::AuthUserGetRequest>,
    ) -> Result<Response<pb::AuthUserGetResponse>, Status> {
        let req = req.into_inner();
        let user = load_user(&self.state, &req.name).await?.ok_or_else(|| {
            Status::not_found(format!("auth: user {} not found", req.name))
        })?;
        Ok(Response::new(pb::AuthUserGetResponse {
            header: Some(self.header().await),
            roles: user.roles,
        }))
    }

    async fn user_list(
        &self,
        _req: Request<pb::AuthUserListRequest>,
    ) -> Result<Response<pb::AuthUserListResponse>, Status> {
        let users = list_names(&self.state, TABLE_AUTH_USERS).await?;
        Ok(Response::new(pb::AuthUserListResponse { header: Some(self.header().await), users }))
    }

    async fn user_delete(
        &self,
        req: Request<pb::AuthUserDeleteRequest>,
    ) -> Result<Response<pb::AuthUserDeleteResponse>, Status> {
        let req = req.into_inner();
        let header = self.change(AuthOp::UserDelete { name: req.name }).await?;
        Ok(Response::new(pb::AuthUserDeleteResponse { header: Some(header) }))
    }

    async fn user_change_password(
        &self,
        req: Request<pb::AuthUserChangePasswordRequest>,
    ) -> Result<Response<pb::AuthUserChangePasswordResponse>, Status> {
        let req = req.into_inner();
        if req.password.is_empty() {
            return Err(Status::invalid_argument("auth: empty password"));
        }
        let password_hash = hash_password(&req.password)?;
        let header = self
            .change(AuthOp::UserChangePassword { name: req.name, password_hash })
            .await?;
        Ok(Response::new(pb::AuthUserChangePasswordResponse { header: Some(header) }))
    }

    async fn user_grant_role(
        &self,
        req: Request<pb::AuthUserGrantRoleRequest>,
    ) -> Result<Response<pb::AuthUserGrantRoleResponse>, Status> {
        let req = req.into_inner();
        let header = self
            .change(AuthOp::UserGrantRole { user: req.user, role: req.role })
            .await?;
        Ok(Response::new(pb::AuthUserGrantRoleResponse { header: Some(header) }))
    }

    async fn user_revoke_role(
        &self,
        req: Request<pb::AuthUserRevokeRoleRequest>,
    ) -> Result<Response<pb::AuthUserRevokeRoleResponse>, Status> {
        let req = req.into_inner();
        let header = self
            .change(AuthOp::UserRevokeRole { name: req.name, role: req.role })
            .await?;
        Ok(Response::new(pb::AuthUserRevokeRoleResponse { header: Some(header) }))
    }

    async fn role_add(
        &self,
        req: Request<pb::AuthRoleAddRequest>,
    ) -> Result<Response<pb::AuthRoleAddResponse>, Status> {
        let req = req.into_inner();
        if req.name.is_empty() {
            return Err(Status::invalid_argument("auth: empty role name"));
        }
        let header = self.change(AuthOp::RoleAdd { name: req.name }).await?;
        Ok(Response::new(pb::AuthRoleAddResponse { header: Some(header) }))
    }

    async fn role_get(
        &self,
        req: Request<pb::AuthRoleGetRequest>,
    ) -> Result<Response<pb::AuthRoleGetResponse>, Status> {
        let req = req.into_inner();
        let role = load_role(&self.state, &req.role).await?.ok_or_else(|| {
            Status::not_found(format!("auth: role {} not found", req.role))
        })?;
        Ok(Response::new(pb::AuthRoleGetResponse {
            header: Some(self.header().await),
            perm: role.permissions.iter().map(perm_to_pb).collect(),
        }))
    }

    async fn role_list(
        &self,
        _req: Request<pb::AuthRoleListRequest>,
    ) -> Result<Response<pb::AuthRoleListResponse>, Status> {
        let roles = list_names(&self.state, TABLE_AUTH_ROLES).await?;
        Ok(Response::new(pb::AuthRoleListResponse { header: Some(self.header().await), roles }))
    }

    async fn role_delete(
        &self,
        req: Request<pb::AuthRoleDeleteRequest>,
    ) -> Result<Response<pb::AuthRoleDeleteResponse>, Status> {
        let req = req.into_inner();
        // Also drops the role from every user holding it, at apply.
        let header = self.change(AuthOp::RoleDelete { name: req.role }).await?;
        Ok(Response::new(pb::AuthRoleDeleteResponse { header: Some(header) }))
    }

    async fn role_grant_permission(
        &self,
        req: Request<pb::AuthRoleGrantPermissionRequest>,
    ) -> Result<Response<pb::AuthRoleGrantPermissionResponse>, Status> {
        let req = req.into_inner();
        let p = req
            .perm
            .ok_or_else(|| Status::invalid_argument("auth: missing Permission"))?;
        let perm_type =
            pb_perm_type(p.perm_type).ok_or_else(|| Status::invalid_argument("auth: bad perm type"))?;
        let perm = StoredPermission { perm_type, key: p.key, range_end: p.range_end };
        let header = self
            .change(AuthOp::RoleGrantPermission { name: req.name, perm })
            .await?;
        Ok(Response::new(pb::AuthRoleGrantPermissionResponse { header: Some(header) }))
    }

    async fn role_revoke_permission(
        &self,
        req: Request<pb::AuthRoleRevokePermissionRequest>,
    ) -> Result<Response<pb::AuthRoleRevokePermissionResponse>, Status> {
        let req = req.into_inner();
        let header = self
            .change(AuthOp::RoleRevokePermission {
                name: req.role,
                key: req.key,
                range_end: req.range_end,
            })
            .await?;
        Ok(Response::new(pb::AuthRoleRevokePermissionResponse { header: Some(header) }))
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
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        if !self.auth.is_enabled() {
            return Ok(req);
        }
        // Look for the etcd-conventional token metadata field.
        let token = req
            .metadata()
            .get("token")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let user_name = match token {
            Some(t) => self.auth.user_for_token(&t),
            None => None,
        };
        match user_name {
            Some(name) => {
                // Attach user identity to the request extensions so
                // per-handler authz (Phase 3) can read it.
                req.extensions_mut()
                    .insert(crate::authz::UserIdentity { name });
                Ok(req)
            }
            None => Err(Status::unauthenticated(
                "auth: missing or invalid `token` metadata; call Authenticate first",
            )),
        }
    }
}
