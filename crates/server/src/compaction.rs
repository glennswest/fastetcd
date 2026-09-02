//! Leader-side MVCC auto-compaction ticker (opt-in).
//!
//! Every write keeps the prior revision of a key, so without compaction
//! the MVCC store grows forever — and a bigger store makes every raft
//! snapshot more expensive, which is what lets the log run away
//! (fastetcd#13). etcd bounds this the same way, via
//! `--auto-compaction-*`.
//!
//! This ticker runs only on the leader. Each interval, if retention is
//! enabled and there is more history than the retention window, it
//! proposes a `FastetcdLogEntry::Compact` through Raft (all state
//! changes go through the log, so followers compact identically). It is
//! disabled by default: a Kubernetes apiserver drives its own
//! compaction, and enabling both would be redundant.

use std::sync::Arc;
use std::time::Duration;

use fastetcd_raft::{FastetcdLogEntry, FastetcdLogResponse};

use crate::state::ServerState;

/// How the retention window is measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Keep the most recent `retention` revisions.
    Revision,
}

/// Spawn the auto-compaction ticker. `retention` is the number of most
/// recent revisions to keep; `0` disables compaction (returns `None`).
pub fn spawn(
    state: Arc<ServerState>,
    _mode: Mode,
    retention: i64,
    interval: Duration,
) -> Option<tokio::task::JoinHandle<()>> {
    if retention <= 0 {
        return None;
    }
    Some(tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await; // skip the immediate first tick
        loop {
            tick.tick().await;
            if let Err(e) = compact_once(&state, retention).await {
                tracing::warn!(target: "fastetcd::compaction", "compaction error: {e}");
            }
        }
    }))
}

async fn compact_once(state: &ServerState, retention: i64) -> anyhow::Result<()> {
    compact_to_retention(state, retention, "auto-compacting MVCC history").await?;
    Ok(())
}

/// Propose a compaction that keeps the most recent `retention`
/// revisions. Returns the revision compacted to, or `None` when there
/// was nothing to do (not the leader, history shorter than the window,
/// or the compaction point is already there).
///
/// Shared by the auto-compaction ticker and the space monitor's reclaim
/// path (fastetcd#14), which compacts under disk pressure even when
/// auto-compaction is switched off.
pub async fn compact_to_retention(
    state: &ServerState,
    retention: i64,
    why: &'static str,
) -> anyhow::Result<Option<i64>> {
    if state.raft.metrics().borrow().current_leader != Some(state.member_id) {
        return Ok(None); // followers don't propose
    }
    let current = state.sm.mvcc().current_revision().await;
    let already = state.sm.mvcc().compact_revision().await;
    let target = current - retention;
    // Nothing to do until there is more history than the window, and
    // never move the compaction point backwards.
    if target <= already || target <= 0 {
        return Ok(None);
    }
    tracing::info!(
        target: "fastetcd::compaction",
        current_rev = current,
        compact_to = target,
        retention,
        "{why}"
    );
    match state
        .raft
        .client_write(FastetcdLogEntry::Compact { rev: target })
        .await
    {
        Ok(w) => {
            if let FastetcdLogResponse::Compact { compact_rev } = w.data {
                tracing::debug!(target: "fastetcd::compaction", compact_rev, "compacted");
                return Ok(Some(compact_rev));
            }
            Ok(Some(target))
        }
        Err(e) => {
            // ForwardToLeader during a transition is expected; the next
            // leader picks it up on its next tick.
            tracing::debug!(target: "fastetcd::compaction", "compact proposal failed: {e}");
            Ok(None)
        }
    }
}
