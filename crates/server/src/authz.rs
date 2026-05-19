//! Per-key permission enforcement (Auth Phase 3).
//!
//! Looks up the authenticated user's roles and the permissions
//! attached to each role, and verifies the request's key/range is
//! covered by a matching permission. The `root` user (and any user
//! holding the `root` role) is exempt.

use std::ops::Bound;
use std::sync::Arc;

use fastetcd_storage::mvcc::auth::{
    StoredPermission, StoredRole, StoredUser, TABLE_AUTH_ROLES, TABLE_AUTH_USERS,
};
use fastetcd_storage::KvStore;
use tonic::Status;

use crate::auth::AuthState;

/// User identity attached to the request by the interceptor. The
/// extensions field carries this so per-handler authz checks can
/// look up the user that's making the call.
#[derive(Debug, Clone)]
pub struct UserIdentity {
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequiredPerm {
    Read,
    Write,
}

/// Authorize a single-key request against the user identified by
/// `name`. `range_end` follows etcd's convention (empty for
/// single-key, `b"\0"` for `>=` key, otherwise `[key, range_end)`).
///
/// Returns `Ok(())` if the user is authorized (or if auth is
/// disabled), otherwise a tonic `Status::permission_denied`.
pub async fn authorize(
    engine: &Arc<dyn KvStore>,
    auth: &AuthState,
    user: Option<&UserIdentity>,
    perm: RequiredPerm,
    key: &[u8],
    range_end: &[u8],
) -> Result<(), Status> {
    // Auth disabled => everything allowed.
    if !auth.is_enabled() {
        return Ok(());
    }
    let user = match user {
        Some(u) => u,
        None => {
            // The interceptor should have rejected this before we
            // got here; defensive.
            return Err(Status::unauthenticated("auth: no user identity on request"));
        }
    };
    // Root user is unconditionally authorized.
    if user.name == "root" {
        return Ok(());
    }

    let snap = engine
        .snapshot()
        .await
        .map_err(|e| Status::internal(format!("authz: read user: {e}")))?;
    let user_bytes = snap
        .get(TABLE_AUTH_USERS, user.name.as_bytes())
        .await
        .map_err(|e| Status::internal(format!("authz: read user: {e}")))?
        .ok_or_else(|| {
            Status::permission_denied(format!("authz: user {} not found", user.name))
        })?;
    let user_rec: StoredUser = bincode::deserialize(&user_bytes)
        .map_err(|e| Status::internal(format!("authz: decode user: {e}")))?;

    // Users granted the `root` role bypass per-key checks.
    if user_rec.roles.iter().any(|r| r == "root") {
        return Ok(());
    }

    // For every role the user has, check whether any permission
    // covers the request.
    for role_name in &user_rec.roles {
        let Some(role_bytes) = snap
            .get(TABLE_AUTH_ROLES, role_name.as_bytes())
            .await
            .map_err(|e| Status::internal(format!("authz: read role: {e}")))?
        else {
            continue; // stale grant; skip
        };
        let role: StoredRole = bincode::deserialize(&role_bytes)
            .map_err(|e| Status::internal(format!("authz: decode role: {e}")))?;
        for p in &role.permissions {
            if perm_covers(p, perm, key, range_end) {
                return Ok(());
            }
        }
    }

    Err(Status::permission_denied(format!(
        "authz: user {} lacks {:?} on key {:?}",
        user.name,
        perm,
        String::from_utf8_lossy(key)
    )))
}

fn perm_covers(p: &StoredPermission, required: RequiredPerm, key: &[u8], range_end: &[u8]) -> bool {
    // Permission type gate.
    let matches_type = match required {
        RequiredPerm::Read => p.perm_type.allows_read(),
        RequiredPerm::Write => p.perm_type.allows_write(),
    };
    if !matches_type {
        return false;
    }
    // Range containment: the request's range
    // [key, request_end) must fit within the perm's range
    // [p.key, p.range_end).
    let (req_lo, req_hi) = bounds_for(key, range_end);
    let (perm_lo, perm_hi) = bounds_for(&p.key, &p.range_end);

    // perm_lo <= req_lo and perm_hi >= req_hi  (interpreting
    // Unbounded high as "+inf"). Lo bounds are inclusive starts;
    // hi bounds are exclusive ends.
    let lo_ok = match (&perm_lo, &req_lo) {
        (Bound::Unbounded, _) => true,
        (Bound::Included(pv), Bound::Included(rv)) => pv.as_slice() <= rv.as_slice(),
        _ => false,
    };
    if !lo_ok {
        return false;
    }
    let hi_ok = match (&perm_hi, &req_hi) {
        (Bound::Unbounded, _) => true,
        (_, Bound::Unbounded) => false,
        (Bound::Excluded(pv), Bound::Excluded(rv)) => pv.as_slice() >= rv.as_slice(),
        _ => false,
    };
    hi_ok
}

fn bounds_for(key: &[u8], range_end: &[u8]) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    if range_end.is_empty() {
        // Single-key: [key, successor_of_key).
        let mut succ = key.to_vec();
        succ.push(0);
        (
            Bound::Included(key.to_vec()),
            Bound::Excluded(succ),
        )
    } else if range_end == [0u8] {
        (Bound::Included(key.to_vec()), Bound::Unbounded)
    } else {
        (
            Bound::Included(key.to_vec()),
            Bound::Excluded(range_end.to_vec()),
        )
    }
}
