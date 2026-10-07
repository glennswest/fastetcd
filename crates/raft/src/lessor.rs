//! Lease keep-alives renewed in the leader's RAM, as etcd's lessor does
//! (fastetcd#92).
//!
//! A keep-alive used to be a `FastetcdLogEntry::LeaseKeepAlive`
//! proposal: a WAL append, an fsync and an apply per renewal. etcd
//! (release-3.5 `lessor.go`, `v3_server.go` `LeaseRenew`) renews on the
//! leader in memory, after confirming it is still the leader, and never
//! logs it; followers forward keep-alives to the leader, and TimeToLive
//! is answered by the leader. What makes that safe across a leader
//! change is `Promote`: a new leader gives every lease a full TTL from
//! the moment it takes over, so a renewal that lived only in the old
//! leader's RAM is never needed.
//!
//! Here: the leader keeps a deadline per lease in RAM. The first time
//! this member acts as leader in a term, every lease's RAM deadline is
//! set to now + its TTL (promote). A lease's effective deadline on the
//! leader is the later of that and the persisted one (which grants, and
//! keep-alives logged by older members, still set). Expiry (the
//! leader's sweeper) and TimeToLive use the effective deadline; the
//! revoke itself still goes through Raft.
//!
//! **Mixed versions.** A member older than 1.23 that becomes leader
//! reads only persisted deadlines, which RAM renewals never update: it
//! would expire every lease kept alive that way at once. So RAM renewal
//! is used only while every member answers `ConfirmLeader` with 1.23 or
//! later ([`MembersAtLeast`]); until then a keep-alive is proposed
//! through Raft, as before.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fastetcd_storage::mvcc::{LeaseId, LeaseTtlResult, MvccResult, MvccStore};
use openraft::Raft;

use crate::network::WriteForwarder;
use crate::read_index::LocalReadIndex;
use crate::types::TypeConfig;
use crate::version_gate::MembersAtLeast;

/// The first version whose leader honours RAM deadlines.
pub const RAM_RENEW_SINCE: (u64, u64, u64) = (1, 23, 0);
/// How often a closed gate asks the members again.
const PROBE_EVERY: Duration = Duration::from_secs(5);

/// What a renewal did.
#[derive(Debug, Clone)]
pub enum Renewal {
    /// Renewed in RAM: the lease's granted TTL starts again now.
    Renewed(LeaseTtlResult),
    /// No such lease, or it has expired (the sweeper revokes it): etcd
    /// answers TTL 0.
    NotFound,
    /// Not renewed here: propose (or forward) the keep-alive through
    /// Raft, as before. Not the leader, leadership not confirmed, or a
    /// member older than [`RAM_RENEW_SINCE`].
    Propose,
}

/// Counters for `/metrics`.
#[derive(Debug, Default)]
pub struct LessorStats {
    /// Keep-alives renewed in RAM.
    pub renewed_in_ram: AtomicU64,
    /// Keep-alives that went through Raft instead (a closed gate).
    pub proposed: AtomicU64,
    /// Terms this member promoted its leases in.
    pub promotions: AtomicU64,
}

#[derive(Default)]
struct Ram {
    /// The term the deadlines were promoted for; 0 = never.
    term: u64,
    /// Unix seconds.
    deadlines: HashMap<LeaseId, i64>,
}

struct Shared {
    raft: Raft<TypeConfig>,
    mvcc: MvccStore,
    forwarder: WriteForwarder,
    read_index: Option<LocalReadIndex>,
    gate: MembersAtLeast,
    probing: AtomicBool,
    last_probe: Mutex<Option<Instant>>,
    ram: Mutex<Ram>,
    stats: Arc<LessorStats>,
}

/// The leader's lease deadlines. Cheap to clone.
#[derive(Clone)]
pub struct Lessor {
    inner: Arc<Shared>,
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl Lessor {
    pub fn new(
        raft: Raft<TypeConfig>,
        mvcc: MvccStore,
        forwarder: WriteForwarder,
        read_index: Option<LocalReadIndex>,
    ) -> Self {
        Self {
            inner: Arc::new(Shared {
                raft,
                mvcc,
                forwarder,
                read_index,
                gate: MembersAtLeast::new(RAM_RENEW_SINCE),
                probing: AtomicBool::new(false),
                last_probe: Mutex::new(None),
                ram: Mutex::new(Ram::default()),
                stats: Arc::default(),
            }),
        }
    }

    pub fn stats(&self) -> Arc<LessorStats> {
        self.inner.stats.clone()
    }

    fn is_leader(&self) -> bool {
        let m = self.inner.raft.metrics().borrow().clone();
        m.current_leader == Some(m.id)
    }

    /// Whether every member honours RAM deadlines. Not yet known: asks in
    /// the background (at most every [`PROBE_EVERY`]) and says no.
    async fn ram_mode(&self) -> bool {
        let s = &self.inner;
        if s.gate.passed(&s.raft) {
            return true;
        }
        let alone = {
            let m = s.raft.metrics().borrow().clone();
            m.membership_config.membership().nodes().all(|(id, _)| *id == m.id)
        };
        if alone {
            return s.gate.check(&s.raft, &s.forwarder).await.is_ok();
        }
        {
            let mut last = s.last_probe.lock().unwrap();
            if last.is_some_and(|t| t.elapsed() < PROBE_EVERY) || s.probing.swap(true, Ordering::AcqRel) {
                return false;
            }
            *last = Some(Instant::now());
        }
        let me = self.clone();
        tokio::spawn(async move {
            let s = &me.inner;
            match s.gate.check(&s.raft, &s.forwarder).await {
                Ok(()) => tracing::info!(
                    target: "fastetcd::lessor",
                    "every member runs 1.23 or later: lease keep-alives are renewed in the leader's RAM"
                ),
                Err(e) => tracing::info!(
                    target: "fastetcd::lessor",
                    "lease keep-alives go through raft until every member runs 1.23 or later: {e:?}"
                ),
            }
            s.probing.store(false, Ordering::Release);
        });
        false
    }

    /// On the leader: make sure this term's deadlines exist, promoting
    /// every lease to a full TTL from now the first time. False when this
    /// member is not the leader.
    async fn promote(&self) -> MvccResult<bool> {
        let s = &self.inner;
        let (leader, term) = {
            let m = s.raft.metrics().borrow().clone();
            (m.current_leader == Some(m.id), m.current_term)
        };
        if !leader {
            return Ok(false);
        }
        if s.ram.lock().unwrap().term == term {
            return Ok(true);
        }
        let records = s.mvcc.lease_records().await?;
        let now = now_unix();
        let mut ram = s.ram.lock().unwrap();
        if ram.term != term {
            ram.deadlines =
                records.iter().map(|r| (r.id, now.saturating_add(r.ttl_secs))).collect();
            ram.term = term;
            s.stats.promotions.fetch_add(1, Ordering::Relaxed);
            tracing::info!(
                target: "fastetcd::lessor",
                term,
                leases = records.len(),
                "leader: every lease's deadline is a full TTL from now"
            );
        }
        Ok(true)
    }

    /// The later of the persisted deadline's remaining seconds and this
    /// leader's RAM deadline (only while promoted for the current term).
    fn effective_remaining(&self, id: LeaseId, persisted_remaining: i64, now: i64) -> i64 {
        let term = self.inner.raft.metrics().borrow().current_term;
        let ram = self.inner.ram.lock().unwrap();
        match ram.deadlines.get(&id) {
            Some(d) if ram.term == term => persisted_remaining.max(d.saturating_sub(now)),
            _ => persisted_remaining,
        }
    }

    /// Renew lease `id` (a keep-alive).
    pub async fn renew(&self, id: LeaseId) -> Renewal {
        let s = &self.inner;
        if !self.is_leader() {
            return Renewal::Propose;
        }
        if !self.ram_mode().await {
            s.stats.proposed.fetch_add(1, Ordering::Relaxed);
            return Renewal::Propose;
        }
        // etcd confirms leadership before renewing (`ensureLeadership`):
        // a deposed leader must not tell a client its lease is safe.
        if crate::read_index::read_barrier(&s.raft, s.read_index.as_ref()).await.is_err() {
            return Renewal::Propose;
        }
        match self.promote().await {
            Ok(true) => {}
            _ => return Renewal::Propose,
        }
        let now = now_unix();
        let Ok(found) = s.mvcc.lease_ttl(id, false, now).await else {
            return Renewal::Propose;
        };
        let Some(t) = found else {
            return Renewal::NotFound;
        };
        if self.effective_remaining(id, t.remaining_ttl_secs, now) <= 0 {
            return Renewal::NotFound;
        }
        s.ram.lock().unwrap().deadlines.insert(id, now.saturating_add(t.granted_ttl_secs));
        s.stats.renewed_in_ram.fetch_add(1, Ordering::Relaxed);
        Renewal::Renewed(LeaseTtlResult {
            id,
            granted_ttl_secs: t.granted_ttl_secs,
            remaining_ttl_secs: t.granted_ttl_secs,
            keys: Vec::new(),
        })
    }

    /// TimeToLive with the effective deadline on the leader; elsewhere
    /// the persisted one.
    pub async fn time_to_live(&self, id: LeaseId, keys: bool) -> MvccResult<Option<LeaseTtlResult>> {
        let leading = self.promote().await?;
        let now = now_unix();
        let mut found = self.inner.mvcc.lease_ttl(id, keys, now).await?;
        if leading {
            if let Some(t) = &mut found {
                t.remaining_ttl_secs = self.effective_remaining(id, t.remaining_ttl_secs, now);
            }
        }
        Ok(found)
    }

    /// On the leader: the leases whose effective deadline has passed
    /// (for the expiry sweeper), dropping RAM deadlines of leases that
    /// are gone. Empty elsewhere.
    pub async fn expired(&self) -> MvccResult<Vec<LeaseId>> {
        if !self.promote().await? {
            return Ok(Vec::new());
        }
        let records = self.inner.mvcc.lease_records().await?;
        let now = now_unix();
        {
            let mut ram = self.inner.ram.lock().unwrap();
            if ram.deadlines.len() > records.len() {
                let live: std::collections::HashSet<LeaseId> = records.iter().map(|r| r.id).collect();
                ram.deadlines.retain(|id, _| live.contains(id));
            }
        }
        Ok(records
            .iter()
            .filter(|r| self.effective_remaining(r.id, r.deadline_unix_secs.saturating_sub(now), now) <= 0)
            .map(|r| r.id)
            .collect())
    }
}
