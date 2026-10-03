//! Shared state passed into each gRPC service.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use openraft::Raft;
use tonic::Status;

use crate::auth::AuthState;
use crate::auth_sync::AuthGate;
use crate::recovery::RecoveryAlarm;
use crate::space::SpaceGuard;
use crate::traffic::{GaugeHold, Traffic};
use fastetcd_raft::precheck::{self, PrecheckError};
use fastetcd_raft::{ProposeError, Proposer};
use fastetcd_raft::types::MembershipChange;
use fastetcd_raft::{
    FastetcdLogEntry, FastetcdLogResponse, FastetcdStateMachine, TypeConfig, WriteForwarder,
};

/// Bundle of handles every gRPC service needs: the Raft node (for
/// proposing writes), the state machine (for direct reads and
/// snapshots), and stable identifiers (`cluster_id`, `member_id`)
/// that go into every `ResponseHeader`.
#[derive(Clone)]
pub struct ServerState {
    pub raft: Raft<TypeConfig>,
    pub sm: FastetcdStateMachine,
    pub cluster_id: u64,
    pub member_id: u64,
    /// Auth's in-memory state, the store's own (`sm.mvcc().auth_memory()`),
    /// so what raft apply changes is what the interceptor reads (#32).
    pub auth: AuthState,
    /// When a replicated auth change may be proposed (#32).
    pub auth_gate: Arc<AuthGate>,
    /// `--client-cert-auth`: a request with no token is made by the user
    /// its client certificate's Common Name names (#20).
    pub client_cert_auth: bool,
    pub forwarder: WriteForwarder,
    /// Disk-space accounting and the NOSPACE alarm (fastetcd#14).
    /// Defaults to a disabled guard so embeddings and tests that don't
    /// manage space see no behavior change; the server installs a live
    /// one via [`ServerState::with_space`].
    pub space: Arc<SpaceGuard>,
    /// The CORRUPT alarm, raised after this store was restored from a
    /// backup (fastetcd#37). Empty by default.
    pub recovery: Arc<RecoveryAlarm>,
    /// Watch, proposal and gRPC call accounting for `/metrics` (#29).
    pub traffic: Arc<Traffic>,
    /// The raft log's committed index, from the log store (#29). `None`
    /// when the embedding did not provide it; `/metrics` then reports
    /// the applied index, which the committed index is never below.
    pub committed_index: Option<Arc<AtomicU64>>,
    /// The raft WAL's counters, for `/metrics` (fastetcd#85).
    pub wal: Option<Arc<fastetcd_raft::wal_log_store::WalStats>>,
    /// The write-behind layer's counters (fastetcd#85).
    pub write_behind: Option<Arc<fastetcd_storage::write_behind::WriteBehindStats>>,
    /// Serves a sole-voter leader's read index without RaftCore
    /// (fastetcd#71). `None`: every read barrier is openraft's.
    pub read_index: Option<fastetcd_raft::LocalReadIndex>,
    /// Batches proposals (group commit, fastetcd#75). `None`: each
    /// proposal is its own `client_write`.
    pub proposer: Option<Proposer>,
}

impl ServerState {
    pub fn new(
        raft: Raft<TypeConfig>,
        sm: FastetcdStateMachine,
        cluster_id: u64,
        member_id: u64,
        forwarder: WriteForwarder,
    ) -> Self {
        let auth = sm.mvcc().auth_memory().clone();
        Self {
            raft,
            sm,
            cluster_id,
            member_id,
            auth,
            auth_gate: Arc::new(AuthGate::default()),
            client_cert_auth: false,
            forwarder,
            space: Arc::new(SpaceGuard::disabled()),
            recovery: Arc::new(RecoveryAlarm::default()),
            traffic: Arc::new(Traffic::default()),
            committed_index: None,
            wal: None,
            write_behind: None,
            read_index: None,
            proposer: None,
        }
    }

    /// Report the write-behind layer's counters on `/metrics` (fastetcd#85).
    pub fn with_write_behind_stats(
        mut self,
        stats: Arc<fastetcd_storage::write_behind::WriteBehindStats>,
    ) -> Self {
        self.write_behind = Some(stats);
        self
    }

    /// Report the raft WAL's counters on `/metrics` (fastetcd#85).
    pub fn with_wal_stats(mut self, stats: Arc<fastetcd_raft::wal_log_store::WalStats>) -> Self {
        self.wal = Some(stats);
        self
    }

    /// Report the log store's committed index on `/metrics`
    /// (`WalLogStore::committed_index`).
    pub fn with_committed_index(mut self, committed: Arc<AtomicU64>) -> Self {
        self.committed_index = Some(committed);
        self
    }

    /// Let a sole-voter leader serve linearizable reads from its own
    /// log and state machine progress (fastetcd#71): `progress` from
    /// the node's `WalLogStore`, and its state machine's applied index.
    pub fn with_local_read_index(mut self, progress: fastetcd_raft::kv_log_store::LogProgress) -> Self {
        self.read_index = Some(fastetcd_raft::LocalReadIndex::new(
            progress,
            self.sm.applied_index(),
        ));
        self
    }

    /// Serve linearizable reads and batch writes without RaftCore's
    /// queue on a multi-member cluster too (fastetcd#75): leadership is
    /// confirmed over the peer channel, and proposals go through a
    /// [`Proposer`]. The peer service shares both (`read_index`,
    /// `proposer`).
    pub fn with_peer_read_index_and_batching(
        mut self,
        progress: fastetcd_raft::kv_log_store::LogProgress,
    ) -> Self {
        self.read_index = Some(
            fastetcd_raft::LocalReadIndex::new(progress.clone(), self.sm.applied_index())
                .with_peers(self.forwarder.clone()),
        );
        self.proposer = Some(Proposer::spawn(
            self.raft.clone(),
            progress,
            self.forwarder.clone(),
        ));
        self
    }

    /// Index of the last committed raft entry this member knows of.
    pub fn committed_index(&self) -> u64 {
        let applied = self
            .raft
            .metrics()
            .borrow()
            .last_applied
            .map_or(0, |l| l.index);
        let committed = self
            .committed_index
            .as_ref()
            .map_or(0, |c| c.load(Ordering::Relaxed));
        committed.max(applied)
    }

    /// Under `--client-cert-auth`, identify callers without a token by
    /// their client certificate's Common Name (#20).
    pub fn with_client_cert_auth(mut self, on: bool) -> Self {
        self.client_cert_auth = on;
        self
    }

    /// Install the recovery alarm loaded from the store at startup.
    pub fn with_recovery(mut self, recovery: Arc<RecoveryAlarm>) -> Self {
        self.recovery = recovery;
        self
    }

    /// Install a live space guard. Called by the server binary once it
    /// knows the data directory and the quota settings.
    pub fn with_space(mut self, space: Arc<SpaceGuard>) -> Self {
        self.space = space;
        self
    }

    /// Fetch the current raft term — used for `ResponseHeader.raft_term`.
    pub async fn current_term(&self) -> u64 {
        self.raft.metrics().borrow().current_term
    }

    /// Propose a write. If this node isn't the raft leader, openraft's
    /// `client_write` fails with `ForwardToLeader` — rather than
    /// surfacing that to the caller (who has no way to act on it; see
    /// #4), hand the same entry off to the leader over the peer
    /// channel via `forwarder` and return its result as if it had
    /// been applied locally.
    pub async fn propose(
        &self,
        entry: FastetcdLogEntry,
    ) -> Result<FastetcdLogResponse, Status> {
        let _pending = GaugeHold::new(&self.traffic.proposals_pending, 1);
        // On the leader, refuse a put naming a lease that does not exist
        // before proposing it (#19; see `fastetcd_raft::precheck`). A
        // follower forwards and the leader checks.
        if precheck::is_leader(&self.raft) {
            match precheck::check_leases(&self.raft, self.read_index.as_ref(), self.sm.mvcc(), &entry).await {
                Ok(()) => {}
                Err(PrecheckError::LeaseNotFound) => {
                    return Err(Status::not_found(precheck::LEASE_NOT_FOUND))
                }
                Err(PrecheckError::Unavailable(m)) => return Err(Status::unavailable(m)),
            }
        }
        let result = match &self.proposer {
            Some(p) => p.propose(entry.clone()).await,
            None => self
                .raft
                .client_write(entry.clone())
                .await
                .map(|w| w.data)
                .map_err(ProposeError::from),
        };
        match result {
            Ok(r) => Ok(r),
            Err(ProposeError::ForwardToLeader { leader_id: Some(leader_id), .. }) => {
                self.forwarder.forward(leader_id, &entry).await.map_err(|msg| {
                    if msg == precheck::LEASE_NOT_FOUND {
                        return Status::not_found(msg);
                    }
                    Status::unavailable(format!("forwarded write to leader {leader_id}: {msg}"))
                })
            }
            Err(e) => Err(Status::unavailable(format!("raft client_write: {e}"))),
        }
    }

    /// Perform a linearizable read barrier before a local range read.
    ///
    /// etcd's default read is linearizable: it must never return state
    /// older than a write that completed before the read began. On the
    /// leader, `ensure_linearizable` confirms leadership via a heartbeat
    /// quorum and waits for the state machine to catch up to the read
    /// index; a sole voter does the same from local state, without
    /// queueing behind writes in RaftCore (`read_barrier`, #71). On a
    /// follower it returns `ForwardToLeader`, so we hand the whole range
    /// to the leader, which does the barrier and reads its own state
    /// machine (#10).
    ///
    /// Returns `Ok(None)` when the caller should read locally (this node
    /// is the leader and the barrier passed), or `Ok(Some((result,
    /// revision)))` when the leader already produced the result via
    /// forwarding. `revision` is the one the leader read at (#50), or
    /// `None` from a leader older than that.
    pub async fn linearize_read(
        &self,
        read: &fastetcd_raft::ForwardedRead,
    ) -> Result<Option<(fastetcd_storage::mvcc::RangeResult, Option<i64>)>, Status> {
        match fastetcd_raft::read_barrier(&self.raft, self.read_index.as_ref()).await {
            Ok(()) => Ok(None),
            Err(e) => {
                if let Some(fwd) = e.forward_to_leader::<openraft::BasicNode>() {
                    if let Some(leader_id) = fwd.leader_id {
                        return self
                            .forwarder
                            .forward_read(leader_id, read)
                            .await
                            .map(Some)
                            .map_err(|msg| {
                                Status::unavailable(format!(
                                    "forwarded linearizable read to leader {leader_id}: {msg}"
                                ))
                            });
                    }
                }
                Err(Status::unavailable(format!(
                    "linearizable read barrier: {e}"
                )))
            }
        }
    }

    /// Add a learner, forwarding to the leader if this node isn't it.
    pub async fn propose_add_learner(
        &self,
        node_id: fastetcd_raft::NodeId,
        addr: &str,
    ) -> Result<(), Status> {
        match self
            .raft
            .add_learner(node_id, openraft::BasicNode::new(addr), false)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                self.forward_membership(
                    &e,
                    MembershipChange::AddLearner {
                        node_id,
                        addr: addr.to_string(),
                    },
                )
                .await
                .unwrap_or_else(|| {
                    Err(Status::unavailable(format!("raft add_learner: {e}")))
                })
            }
        }
    }

    /// Replace the voter set, forwarding to the leader if this node
    /// isn't it.
    pub async fn propose_set_voters(
        &self,
        voters: std::collections::BTreeSet<fastetcd_raft::NodeId>,
    ) -> Result<(), Status> {
        match self.raft.change_membership(voters.clone(), false).await {
            Ok(_) => Ok(()),
            Err(e) => {
                self.forward_membership(
                    &e,
                    MembershipChange::SetVoters {
                        voters: voters.into_iter().collect(),
                    },
                )
                .await
                .unwrap_or_else(|| {
                    Err(Status::unavailable(format!("raft change_membership: {e}")))
                })
            }
        }
    }

    /// If `err` is a `ForwardToLeader` naming a leader, send `change`
    /// there and return the outcome. `None` means the error wasn't a
    /// forwardable one (or no leader is known yet), so the caller
    /// should surface its own error.
    ///
    /// etcd forwards membership changes transparently, so `etcdctl
    /// member remove` works against any endpoint; returning
    /// ForwardToLeader to the client instead is the #7 compat gap.
    async fn forward_membership<E>(
        &self,
        err: &openraft::error::RaftError<fastetcd_raft::NodeId, E>,
        change: MembershipChange,
    ) -> Option<Result<(), Status>>
    where
        E: std::error::Error
            + openraft::TryAsRef<
                openraft::error::ForwardToLeader<fastetcd_raft::NodeId, openraft::BasicNode>,
            >,
    {
        let fwd = err.forward_to_leader::<openraft::BasicNode>()?;
        let leader_id = fwd.leader_id?;
        Some(
            self.forwarder
                .forward_membership(leader_id, &change)
                .await
                .map_err(|msg| {
                    Status::unavailable(format!(
                        "forwarded membership change to leader {leader_id}: {msg}"
                    ))
                }),
        )
    }
}

/// Build a `ResponseHeader` with the current cluster/member/raft_term
/// and the given revision.
pub async fn response_header(
    state: &ServerState,
    revision: i64,
) -> fastetcd_proto::etcdserverpb::ResponseHeader {
    fastetcd_proto::etcdserverpb::ResponseHeader {
        cluster_id: state.cluster_id,
        member_id: state.member_id,
        revision,
        raft_term: state.current_term().await,
    }
}
