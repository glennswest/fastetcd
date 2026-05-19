//! Leader-side lease expiry ticker.
//!
//! Runs as a background task spawned from the server bootstrap.
//! Every `interval` it:
//!
//!  1. Reads `raft.metrics().current_leader` — bails this tick if
//!     we're not the leader.
//!  2. Walks the persisted lease set via `MvccStore::lease_list`.
//!  3. For each lease whose `deadline_unix_secs < now`, proposes a
//!     `FastetcdLogEntry::LeaseRevoke` through `Raft::client_write`.
//!     Revoke replicates and cascades attached-key deletes through
//!     the same path explicit revokes use.
//!
//! Followers are no-ops: only the leader has the authority to
//! propose. On leadership change the new leader picks up the work
//! at the next tick.

use std::sync::Arc;
use std::time::Duration;

use fastetcd_raft::{FastetcdLogEntry, FastetcdLogResponse};

use crate::state::ServerState;

/// Default tick cadence. Etcd's expiry resolution is typically
/// seconds-grained; once per second matches that and bounds the
/// post-deadline lag at ~1s.
pub const DEFAULT_TICK: Duration = Duration::from_secs(1);

/// Spawn the lease-expiry ticker. Returns immediately; the task
/// runs until the server shuts down.
pub fn spawn(state: Arc<ServerState>) -> tokio::task::JoinHandle<()> {
    spawn_with_tick(state, DEFAULT_TICK)
}

pub fn spawn_with_tick(
    state: Arc<ServerState>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await; // skip the immediate first tick
        loop {
            tick.tick().await;
            if let Err(e) = sweep_once(&state).await {
                tracing::warn!(target: "fastetcd::lease_expiry", "sweep error: {e}");
            }
        }
    })
}

async fn sweep_once(state: &ServerState) -> anyhow::Result<()> {
    let metrics = state.raft.metrics().borrow().clone();
    if metrics.current_leader != Some(state.member_id) {
        return Ok(());
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    let ids = state.sm.mvcc().lease_list().await?;
    for id in ids {
        let Some(ttl) = state.sm.mvcc().lease_ttl(id, false, now).await? else {
            continue; // raced with a manual revoke
        };
        if ttl.remaining_ttl_secs > 0 {
            continue;
        }
        tracing::info!(
            target: "fastetcd::lease_expiry",
            lease_id = id,
            "auto-revoking expired lease"
        );
        match state
            .raft
            .client_write(FastetcdLogEntry::LeaseRevoke { id })
            .await
        {
            Ok(w) => {
                if let FastetcdLogResponse::LeaseRevoke(r) = w.data {
                    tracing::debug!(
                        target: "fastetcd::lease_expiry",
                        lease_id = id,
                        deleted_keys = r.deleted_keys,
                        "expired lease revoked"
                    );
                }
            }
            Err(e) => {
                // ForwardToLeader is the common case during a leader
                // transition — quiet about it; the new leader will
                // pick up the sweep on its next tick.
                tracing::debug!(
                    target: "fastetcd::lease_expiry",
                    lease_id = id,
                    "revoke proposal failed: {e}"
                );
            }
        }
    }
    Ok(())
}
