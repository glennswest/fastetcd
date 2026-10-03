//! Batched proposals: group commit (fastetcd#75).
//!
//! openraft 0.9's RaftCore takes client writes one message at a time,
//! and appending each one's log entry waits for that entry's fsync
//! (`RaftCore::append_to_log` awaits the `LogFlushed` callback, even
//! with a log store whose `append` returns at once, as the WAL's does).
//! So one fsync carries exactly one log entry, and a member's write rate
//! was bounded by one fsync per write. The proposer queues proposals
//! instead and proposes what queued as one [`FastetcdLogEntry::Batch`]
//! — one RaftCore message, one append, one fsync — and each caller gets
//! its own response back. A proposal that finds nothing in flight goes
//! as a plain entry at once, exactly as without the proposer.
//!
//! **Group commit** (fastetcd#95): only [`IN_FLIGHT`] batch is in
//! RaftCore at a time. The next one is formed the moment the previous
//! is answered, from everything that arrived while it was written and
//! applied, and goes to RaftCore at once. One, not more: RaftCore cannot
//! answer a batch while it waits on the next one's fsync, so a second
//! batch in flight delays the first one's answers by a whole fsync and
//! splits the writers into more, smaller groups. #75 had three in
//! flight; they queued in RaftCore behind each other, a write waited
//! about four fsyncs, and 20 writers put ~4 writes in each fsync of a
//! slow disk. Freeing the slot once a batch is durable rather than
//! answered was tried too: the same answers wait an fsync behind the
//! next batch (light load on a slow disk: 70 ms answered vs 210 ms).
//!
//! A `Batch` entry cannot be decoded by a member older than this, so
//! nothing is batched until every member (voters and learners) has
//! answered the `ConfirmLeader` peer RPC, which came with it. The check
//! is redone whenever the membership changes. Until then each proposal
//! is its own `client_write`, as before. Do not add a member running an
//! older fastetcd to a cluster that has batched: it cannot read the log.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use openraft::Raft;
use tokio::sync::{mpsc, oneshot, Semaphore};

use crate::kv_log_store::LogProgress;
use crate::network::{ConfirmError, WriteForwarder};
use crate::types::{FastetcdLogEntry, FastetcdLogResponse, NodeId, TypeConfig};

/// `client_write`s in flight at once (see the module docs).
pub const IN_FLIGHT: usize = 1;
/// Most proposals in one batch.
pub const MAX_BATCH: usize = 256;
/// Most encoded bytes in one batch. Peer gRPC decodes up to 4 MiB per
/// message and a log entry cannot be split across AppendEntries, so a
/// batch stays far below that; a proposal larger than this goes alone.
pub const MAX_BATCH_BYTES: u64 = 512 * 1024;
/// How often a closed gate asks the members again.
const PROBE_EVERY: Duration = Duration::from_secs(5);
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Why a proposal was not applied.
#[derive(Debug, Clone)]
pub enum ProposeError {
    /// This member is not the leader; `leader_id` is, if known.
    ForwardToLeader { leader_id: Option<NodeId>, message: String },
    Failed(String),
}

impl std::fmt::Display for ProposeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProposeError::ForwardToLeader { message, .. } | ProposeError::Failed(message) => {
                f.write_str(message)
            }
        }
    }
}

type Reply = oneshot::Sender<Result<FastetcdLogResponse, ProposeError>>;

struct Pending {
    entry: FastetcdLogEntry,
    bytes: u64,
    reply: Reply,
}

/// Whether every member can decode a `Batch` entry.
struct BatchGate {
    raft: Raft<TypeConfig>,
    progress: LogProgress,
    forwarder: WriteForwarder,
    /// Open for the membership whose log index (+1) is `checked`.
    open: AtomicBool,
    checked: AtomicU64,
    probing: AtomicBool,
    last_probe: std::sync::Mutex<Option<Instant>>,
}

impl BatchGate {
    /// The membership as of now: its key (log index + 1 of the newest
    /// membership entry, from metrics and the log store) and every
    /// member but this one. `None` while a membership entry has been
    /// appended that metrics do not show yet.
    fn membership(&self) -> Option<(u64, BTreeSet<NodeId>)> {
        let m = self.raft.metrics();
        let m = m.borrow();
        let key = crate::state_machine::next_index(m.membership_config.log_id().as_ref());
        if key < self.progress.membership.load(Ordering::Acquire) {
            return None;
        }
        let others = m
            .membership_config
            .membership()
            .nodes()
            .map(|(id, _)| *id)
            .filter(|id| *id != m.id)
            .collect();
        Some((key, others))
    }

    /// Open for the current membership; starts a probe when it is not.
    fn is_open(self: &Arc<Self>) -> bool {
        let Some((key, others)) = self.membership() else {
            return false;
        };
        if self.open.load(Ordering::Acquire) && self.checked.load(Ordering::Acquire) == key {
            return true;
        }
        if others.is_empty() {
            self.checked.store(key, Ordering::Release);
            self.open.store(true, Ordering::Release);
            return true;
        }
        self.open.store(false, Ordering::Release);
        self.start_probe(key, others);
        false
    }

    fn start_probe(self: &Arc<Self>, key: u64, others: BTreeSet<NodeId>) {
        {
            let mut last = self.last_probe.lock().unwrap();
            if last.is_some_and(|t| t.elapsed() < PROBE_EVERY) {
                return;
            }
            if self.probing.swap(true, Ordering::AcqRel) {
                return;
            }
            *last = Some(Instant::now());
        }
        let gate = self.clone();
        tokio::spawn(async move {
            let mut all = true;
            for id in others {
                match gate.forwarder.confirm_leader(id, 0, PROBE_TIMEOUT).await {
                    Ok(_) => {}
                    Err(e) => {
                        if matches!(e, ConfirmError::Older) {
                            tracing::info!(
                                target: "fastetcd::proposer",
                                member = format_args!("{id:x}"),
                                "member runs an older fastetcd: writes are not batched"
                            );
                        }
                        all = false;
                        break;
                    }
                }
            }
            // Still the membership that was asked about?
            if all && gate.membership().is_some_and(|(k, _)| k == key) {
                gate.checked.store(key, Ordering::Release);
                gate.open.store(true, Ordering::Release);
                tracing::info!(target: "fastetcd::proposer", "every member decodes batched entries: writes are batched");
            }
            gate.probing.store(false, Ordering::Release);
        });
    }
}

/// What the proposer has proposed (for `/metrics` and tests).
#[derive(Default, Debug)]
pub struct ProposerStats {
    /// `Batch` entries proposed.
    pub batches: AtomicU64,
    /// Proposals that went in them.
    pub batched: AtomicU64,
    /// Proposals that went as plain entries.
    pub single: AtomicU64,
}

/// Queues proposals and proposes them in batches. Cheap to clone.
#[derive(Clone)]
pub struct Proposer {
    tx: mpsc::UnboundedSender<Pending>,
    stats: Arc<ProposerStats>,
}

impl Proposer {
    /// Start the proposer's task. `progress` is the node's log store
    /// progress; `forwarder` reaches the other members.
    pub fn spawn(raft: Raft<TypeConfig>, progress: LogProgress, forwarder: WriteForwarder) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let gate = Arc::new(BatchGate {
            raft: raft.clone(),
            progress,
            forwarder,
            open: AtomicBool::new(false),
            checked: AtomicU64::new(0),
            probing: AtomicBool::new(false),
            last_probe: std::sync::Mutex::new(None),
        });
        let stats = Arc::new(ProposerStats::default());
        tokio::spawn(run(raft, rx, gate, stats.clone()));
        Self { tx, stats }
    }

    /// What has been proposed so far.
    pub fn stats(&self) -> &ProposerStats {
        &self.stats
    }

    /// Propose `entry` and wait for its result, as `client_write` would.
    pub async fn propose(&self, entry: FastetcdLogEntry) -> Result<FastetcdLogResponse, ProposeError> {
        let bytes = bincode::serialized_size(&entry).unwrap_or(u64::MAX);
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Pending { entry, bytes, reply })
            .map_err(|_| ProposeError::Failed("proposer stopped".into()))?;
        rx.await
            .unwrap_or_else(|_| Err(ProposeError::Failed("proposer dropped the proposal".into())))
    }
}

async fn run(
    raft: Raft<TypeConfig>,
    mut rx: mpsc::UnboundedReceiver<Pending>,
    gate: Arc<BatchGate>,
    stats: Arc<ProposerStats>,
) {
    let permits = Arc::new(Semaphore::new(IN_FLIGHT));
    let mut carry: Option<Pending> = None;
    loop {
        let first = match carry.take() {
            Some(p) => p,
            None => match rx.recv().await {
                Some(p) => p,
                None => return,
            },
        };
        if !gate.is_open() {
            // As before batching: every proposal its own client_write.
            stats.single.fetch_add(1, Ordering::Relaxed);
            let raft = raft.clone();
            tokio::spawn(async move { submit(&raft, vec![first]).await });
            continue;
        }
        let Ok(permit) = permits.clone().acquire_owned().await else {
            return;
        };
        // Everything that queued while waiting for a permit.
        let mut bytes = first.bytes;
        let mut batch = vec![first];
        while batch.len() < MAX_BATCH {
            match rx.try_recv() {
                Ok(p) if bytes.saturating_add(p.bytes) <= MAX_BATCH_BYTES => {
                    bytes += p.bytes;
                    batch.push(p);
                }
                Ok(p) => {
                    carry = Some(p);
                    break;
                }
                Err(_) => break,
            }
        }
        if batch.len() == 1 {
            stats.single.fetch_add(1, Ordering::Relaxed);
        } else {
            stats.batches.fetch_add(1, Ordering::Relaxed);
            stats.batched.fetch_add(batch.len() as u64, Ordering::Relaxed);
        }
        let raft = raft.clone();
        tokio::spawn(async move {
            submit(&raft, batch).await;
            drop(permit);
        });
    }
}

type ClientWriteError =
    openraft::error::RaftError<NodeId, openraft::error::ClientWriteError<NodeId, openraft::BasicNode>>;

impl From<ClientWriteError> for ProposeError {
    fn from(e: ClientWriteError) -> Self {
        let message = e.to_string();
        match e.forward_to_leader::<openraft::BasicNode>() {
            Some(fwd) => ProposeError::ForwardToLeader { leader_id: fwd.leader_id, message },
            None => ProposeError::Failed(message),
        }
    }
}

async fn submit(raft: &Raft<TypeConfig>, mut batch: Vec<Pending>) {
    if batch.len() == 1 {
        let p = batch.pop().unwrap();
        let r = raft.client_write(p.entry).await.map(|w| w.data).map_err(ProposeError::from);
        let _ = p.reply.send(r);
        return;
    }
    let n = batch.len();
    let (entries, replies): (Vec<_>, Vec<_>) = batch.into_iter().map(|p| (p.entry, p.reply)).unzip();
    match raft.client_write(FastetcdLogEntry::Batch(entries)).await {
        Ok(w) => match w.data {
            FastetcdLogResponse::Batch(rs) if rs.len() == n => {
                for (reply, r) in replies.into_iter().zip(rs) {
                    let _ = reply.send(Ok(r));
                }
            }
            other => {
                let msg = format!("batched proposal of {n} got an unexpected response: {other:?}");
                for reply in replies {
                    let _ = reply.send(Err(ProposeError::Failed(msg.clone())));
                }
            }
        },
        Err(e) => {
            let err = ProposeError::from(e);
            for reply in replies {
                let _ = reply.send(Err(err.clone()));
            }
        }
    }
}
