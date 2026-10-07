//! Snapshots and log purges counted in proposals, not log entries
//! (fastetcd#80).
//!
//! `--snapshot-count` and `--max-in-snapshot-log-to-keep` are etcd's,
//! where every proposal is its own log entry. Since #75 a fastetcd log
//! entry can be a batch of hundreds of proposals, and openraft's
//! `LogsSinceLast` and `max_in_snapshot_log_to_keep` count entries, so
//! the log between snapshots, and what is kept after a purge, held far
//! more writes (and bytes) than the flags say and the sizing model
//! assumes (`fastetcd sizing`, docs/04-disk-space.md).
//!
//! So the state machine counts the proposals in each entry it applies
//! ([`ProposalLog`]), and a task on every member ([`spawn`]) triggers a
//! snapshot once `snapshot_count` proposals have been applied since the
//! last one, and after each snapshot purges the log down to about
//! `keep` proposals. openraft's own entry-counted policy stays as the
//! backstop; without batching the two agree.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openraft::Raft;

use crate::types::TypeConfig;

/// Proposals applied, per log index, since this process started.
#[derive(Debug, Default)]
pub struct ProposalLog {
    /// `(index, proposals applied up to and including it)`, ascending,
    /// trimmed below the log's purge point.
    entries: Mutex<VecDeque<(u64, u64)>>,
    /// The newest entry forgotten, so counts at or after it stay right.
    floor: Mutex<(u64, u64)>,
    total: AtomicU64,
}

impl ProposalLog {
    /// The entry at `index` held `n` proposals (a batch's length; 1 for
    /// any other request; 0 for blank and membership entries).
    pub fn record(&self, index: u64, n: u64) {
        if n == 0 {
            return;
        }
        let total = self.total.fetch_add(n, Ordering::Relaxed) + n;
        self.entries.lock().unwrap().push_back((index, total));
    }

    /// Proposals applied since this process started.
    pub fn total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    /// Proposals applied up to and including `index` (0 if before the
    /// first one recorded).
    pub fn cumulative_at(&self, index: u64) -> u64 {
        let e = self.entries.lock().unwrap();
        let n = e.partition_point(|(i, _)| *i <= index);
        if n > 0 {
            return e[n - 1].1;
        }
        let (fi, fc) = *self.floor.lock().unwrap();
        if index >= fi {
            fc
        } else {
            0
        }
    }

    /// Where to purge after a snapshot at `snapshot_index` so that the
    /// log after the purge point still holds at least `keep` proposals:
    /// the newest index `p` with `cumulative(snapshot) - cumulative(p) >=
    /// keep`. `None` if fewer than `keep` were recorded.
    pub fn purge_point(&self, snapshot_index: u64, keep: u64) -> Option<u64> {
        let at = self.cumulative_at(snapshot_index);
        if keep == 0 {
            return Some(snapshot_index);
        }
        let e = self.entries.lock().unwrap();
        let mut point = None;
        for &(i, c) in e.iter() {
            if i > snapshot_index || at - c < keep {
                break;
            }
            point = Some(i);
        }
        point
    }

    /// Forget the entries the log no longer has.
    pub fn forget_upto(&self, index: u64) {
        let mut e = self.entries.lock().unwrap();
        while let Some(&(i, c)) = e.front().filter(|(i, _)| *i <= index) {
            *self.floor.lock().unwrap() = (i, c);
            e.pop_front();
        }
    }
}

/// Run the policy for this member until the process ends.
pub fn spawn(raft: Raft<TypeConfig>, log: Arc<ProposalLog>, snapshot_count: u64, keep: u64) {
    tokio::spawn(async move {
        let mut asked_after: Option<u64> = None;
        let mut purged_for: Option<u64> = None;
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let m = raft.metrics().borrow().clone();
            let snap = m.snapshot.map(|l| l.index);
            let since = log.total().saturating_sub(snap.map_or(0, |s| log.cumulative_at(s)));
            // Once per snapshot: openraft builds it in the background, and
            // the next request only once it shows in the metrics.
            if snapshot_count > 0 && since >= snapshot_count && asked_after != Some(snap.unwrap_or(0)) {
                if raft.trigger().snapshot().await.is_err() {
                    return;
                }
                tracing::debug!(target: "fastetcd::snapshot_policy", since, "snapshot: proposals since the last");
                asked_after = Some(snap.unwrap_or(0));
            }
            if let Some(s) = snap.filter(|s| purged_for != Some(*s)) {
                if let Some(p) = log.purge_point(s, keep) {
                    if Some(p) > m.purged.map(|l| l.index) && raft.trigger().purge_log(p).await.is_err() {
                        return;
                    }
                    log.forget_upto(p);
                }
                purged_for = Some(s);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_and_purge_points() {
        let l = ProposalLog::default();
        l.record(1, 0); // blank
        l.record(2, 1);
        l.record(3, 200); // a batch
        l.record(4, 200);
        l.record(5, 1);
        assert_eq!(l.total(), 402);
        assert_eq!((l.cumulative_at(1), l.cumulative_at(3), l.cumulative_at(9)), (0, 201, 402));
        // Keep 150 proposals after a snapshot at 5: entries 4..=5 hold 201.
        assert_eq!(l.purge_point(5, 150), Some(3));
        assert_eq!(l.purge_point(5, 202), Some(2));
        assert_eq!(l.purge_point(5, 0), Some(5));
        assert_eq!(l.purge_point(5, 1000), None, "fewer than keep recorded");
        l.forget_upto(3);
        assert_eq!(l.cumulative_at(3), 201, "the floor keeps counts after a forget");
        assert_eq!(l.cumulative_at(2), 0);
        assert_eq!(l.cumulative_at(4), 401);
        // Purged right up to a snapshot: nothing new since it.
        l.forget_upto(5);
        assert_eq!(l.total() - l.cumulative_at(5), 0);
    }
}
