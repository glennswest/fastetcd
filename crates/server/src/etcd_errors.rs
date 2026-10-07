//! etcd's auth errors, with its codes and texts (fastetcd#105, #127).
//!
//! clientv3 turns a gRPC status into its typed errors by the message
//! (`rpctypes.Error`), not the code, and its retry interceptor
//! re-authenticates and retries only on `ErrInvalidAuthToken`,
//! `ErrUserEmpty` and `ErrAuthOldRevision` (`retry_interceptor.go`
//! `shouldRefreshToken`). So a client whose token a member does not know
//! (the member restarted, or has not applied the `Authenticate` entry
//! yet) refreshes it only if the answer is etcd's, byte for byte. Texts
//! and codes from etcd release-3.5 `api/v3rpc/rpctypes/error.go` and
//! `server/etcdserver/api/v3rpc/util.go`.

use tonic::Status;

pub const INVALID_AUTH_TOKEN: &str = "etcdserver: invalid auth token";
pub const USER_EMPTY: &str = "etcdserver: user name is empty";
pub const PERMISSION_DENIED: &str = "etcdserver: permission denied";
pub const AUTH_FAILED: &str = "etcdserver: authentication failed, invalid user ID or password";
pub const AUTH_NOT_ENABLED: &str = "etcdserver: authentication is not enabled";
pub const AUTH_OLD_REVISION: &str = "etcdserver: revision of auth store is old";
/// Not in etcd's gRPC table, so etcd answers it `Unknown` with the auth
/// package's own text.
pub const NO_PASSWORD_USER: &str = "auth: authentication failed, password was given for no password user";
pub const USER_NOT_FOUND: &str = "etcdserver: user name not found";
pub const USER_ALREADY_EXIST: &str = "etcdserver: user name already exists";
pub const ROLE_NOT_FOUND: &str = "etcdserver: role name not found";
pub const ROLE_ALREADY_EXIST: &str = "etcdserver: role name already exists";
pub const ROLE_EMPTY: &str = "etcdserver: role name is empty";
pub const ROOT_USER_NOT_EXIST: &str = "etcdserver: root user does not exist";
pub const PERMISSION_NOT_GIVEN: &str = "etcdserver: permission not given";

/// A token was sent and names no user here.
pub fn invalid_auth_token() -> Status {
    Status::unauthenticated(INVALID_AUTH_TOKEN)
}

/// No token (and no client-certificate user) while auth is on.
pub fn user_empty() -> Status {
    Status::invalid_argument(USER_EMPTY)
}

pub fn permission_denied() -> Status {
    Status::permission_denied(PERMISSION_DENIED)
}

pub fn auth_failed() -> Status {
    Status::invalid_argument(AUTH_FAILED)
}

pub fn auth_not_enabled() -> Status {
    Status::failed_precondition(AUTH_NOT_ENABLED)
}

pub fn auth_old_revision() -> Status {
    Status::invalid_argument(AUTH_OLD_REVISION)
}

pub fn no_password_user() -> Status {
    Status::unknown(NO_PASSWORD_USER)
}

pub fn user_not_found() -> Status {
    Status::failed_precondition(USER_NOT_FOUND)
}

pub fn role_not_found() -> Status {
    Status::failed_precondition(ROLE_NOT_FOUND)
}

pub fn role_empty() -> Status {
    Status::invalid_argument(ROLE_EMPTY)
}

pub fn permission_not_given() -> Status {
    Status::invalid_argument(PERMISSION_NOT_GIVEN)
}

/// The caller could not be identified while auth is on: etcd answers
/// `ErrInvalidAuthToken` when a token was sent (it names no one here),
/// else `ErrUserEmpty`.
pub fn unidentified<T>(request: &tonic::Request<T>) -> Status {
    let md = request.metadata();
    if md.get("token").is_some() || md.get("authorization").is_some() {
        invalid_auth_token()
    } else {
        user_empty()
    }
}
