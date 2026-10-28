//! Trigger an action to RaftCore by external caller.

use crate::core::raft_msg::external_command::ExternalCommand;
use crate::error::Fatal;
use crate::raft::RaftInner;
use crate::LogId;
use crate::RaftTypeConfig;
use crate::Vote;

/// Trigger is an interface to trigger an action to RaftCore by external caller.
///
/// It is create with [`Raft::trigger()`].
///
/// For example, to trigger an election at once, you can use the following code
/// ```ignore
/// raft.trigger().elect().await?;
/// ```
///
/// Or to fire an heartbeat, building a snapshot, or purging logs:
///
/// ```ignore
/// raft.trigger().heartbeat().await?;
/// raft.trigger().snapshot().await?;
/// raft.trigger().purge_log().await?;
/// ```
///
/// [`Raft::trigger()`]: crate::Raft::trigger
pub struct Trigger<'r, C>
where C: RaftTypeConfig
{
    raft_inner: &'r RaftInner<C>,
}

impl<'r, C> Trigger<'r, C>
where C: RaftTypeConfig
{
    pub(in crate::raft) fn new(raft_inner: &'r RaftInner<C>) -> Self {
        Self { raft_inner }
    }

    /// Trigger election at once and return at once.
    ///
    /// Returns error when RaftCore has [`Fatal`] error, e.g. shut down or having storage error.
    /// It is not affected by `Raft::enable_elect(false)`.
    pub async fn elect(&self) -> Result<(), Fatal<C::NodeId>> {
        self.raft_inner.send_external_command(ExternalCommand::Elect, "trigger_elect").await
    }

    /// Trigger a heartbeat at once and return at once.
    ///
    /// Returns error when RaftCore has [`Fatal`] error, e.g. shut down or having storage error.
    /// It is not affected by `Raft::enable_heartbeat(false)`.
    pub async fn heartbeat(&self) -> Result<(), Fatal<C::NodeId>> {
        self.raft_inner.send_external_command(ExternalCommand::Heartbeat, "trigger_heartbeat").await
    }

    /// Trigger to build a snapshot at once and return at once.
    ///
    /// Returns error when RaftCore has [`Fatal`] error, e.g. shut down or having storage error.
    pub async fn snapshot(&self) -> Result<(), Fatal<C::NodeId>> {
        self.raft_inner.send_external_command(ExternalCommand::Snapshot, "trigger_snapshot").await
    }

    /// Initiate the log purge up to and including the given `upto` log index.
    ///
    /// Logs that are not included in a snapshot will **NOT** be purged.
    /// In such scenario it will delete as many log as possible.
    /// The [`max_in_snapshot_log_to_keep`] config is not taken into account
    /// when purging logs.
    ///
    /// It returns error only when RaftCore has [`Fatal`] error, e.g. shut down or having storage
    /// error.
    ///
    /// Openraft won't purge logs at once, e.g. it may be delayed by several seconds, because if it
    /// is a leader and a replication task has been replicating the logs to a follower, the logs
    /// can't be purged until the replication task is finished.
    ///
    /// [`max_in_snapshot_log_to_keep`]: `crate::Config::max_in_snapshot_log_to_keep`
    pub async fn purge_log(&self, upto: u64) -> Result<(), Fatal<C::NodeId>> {
        self.raft_inner.send_external_command(ExternalCommand::PurgeLog { upto }, "purge_log").await
    }

    /// fastetcd: accept a leader transfer from the leader of `leader_vote` to `to`.
    ///
    /// The application sends this to every voter (the leader included) when the leader of
    /// `leader_vote` hands leadership to `to`, after `to`'s log has caught up with
    /// `last_log_id`. On a node whose vote is still `leader_vote`:
    ///
    /// - vote requests are no longer refused because of that vote's leader lease, for twice the
    ///   lease (only that vote; election timers are unchanged);
    /// - if the node is `to`, a voter, and its log holds `last_log_id`, it starts an election.
    ///
    /// A node whose vote differs ignores it. The election is an ordinary one: it wins only with
    /// a quorum whose logs are no newer than the candidate's. Like openraft 0.10's
    /// `transfer_leader`, minus the leader side (the application holds proposals and waits for
    /// the target to catch up). Returns at once; the caller watches the metrics for the result.
    pub async fn accept_leader_transfer(
        &self,
        leader_vote: Vote<C::NodeId>,
        to: C::NodeId,
        last_log_id: Option<LogId<C::NodeId>>,
    ) -> Result<(), Fatal<C::NodeId>> {
        self.raft_inner
            .send_external_command(
                ExternalCommand::AcceptLeaderTransfer {
                    leader_vote,
                    to,
                    last_log_id,
                },
                "accept_leader_transfer",
            )
            .await
    }
}
