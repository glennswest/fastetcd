//! The raft log in a WAL (fastetcd#85): openraft's own log-store
//! checks, restart, the upgrade from the log in redb, and a node that
//! snapshots, purges and drops segments, then restarts.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use openraft::storage::RaftLogStorage;
use openraft::testing::{StoreBuilder, Suite};
use openraft::{Config, Raft, RaftLogReader, StorageError};
use tempfile::TempDir;

use fastetcd_raft::kv_log_store::KvLogStore;
use fastetcd_raft::types::{FastetcdLogEntry, NodeId, TypeConfig};
use fastetcd_raft::wal_log_store::{
    spawn_checkpointer, wal_dir, CheckpointConfig, WalLogOptions, WalLogStore,
};
use fastetcd_raft::FastetcdStateMachine;
use fastetcd_storage::mvcc::{Mutation, MvccStore};
use fastetcd_storage::redb_engine::RedbEngine;
use fastetcd_storage::write_behind::WriteBehind;
use fastetcd_storage::KvStore;

fn small() -> WalLogOptions {
    // Tiny segments and cache: rolls, segment drops and disk reads all
    // happen within a few entries.
    WalLogOptions { segment_bytes: 4096, cache_bytes: 512, ..Default::default() }
}

async fn open_dir(dir: &std::path::Path) -> (Arc<dyn KvStore>, WalLogStore, FastetcdStateMachine) {
    // A restart in the same process: the previous node's tasks (a
    // snapshot build openraft started, on a loaded build box) may hold
    // the file for a while after shutdown. A real leak still fails.
    let mut tries = 0;
    let engine: Arc<dyn KvStore> = loop {
        match RedbEngine::open(dir.join("fastetcd.redb")) {
            Ok(e) => break Arc::new(e),
            Err(e) if tries < 300 => {
                tries += 1;
                let _ = e;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("open: {e}"),
        }
    };
    // As the server runs it: applies held in RAM in front of redb.
    let engine: Arc<dyn KvStore> = Arc::new(WriteBehind::new(engine, 64 * 1024));
    let mvcc = MvccStore::open(engine.clone()).await.unwrap();
    let sm = FastetcdStateMachine::open(mvcc, dir.join("snapshots")).await.unwrap();
    let log = WalLogStore::open(&wal_dir(dir), engine.clone(), small()).await.unwrap();
    (engine, log, sm)
}

struct Builder;

impl StoreBuilder<TypeConfig, WalLogStore, FastetcdStateMachine, TempDir> for Builder {
    async fn build(
        &self,
    ) -> Result<(TempDir, WalLogStore, FastetcdStateMachine), StorageError<NodeId>> {
        let dir = tempfile::tempdir().unwrap();
        let (_engine, log, sm) = open_dir(dir.path()).await;
        Ok((dir, log, sm))
    }
}

type S = Suite<TypeConfig, WalLogStore, FastetcdStateMachine, Builder, TempDir>;

macro_rules! suite_test {
    ($name:ident) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $name() {
            let (_dir, log, sm) = Builder.build().await.unwrap();
            S::$name(log, sm).await.unwrap();
        }
    };
}

// openraft's checks of a log store (openraft::testing::Suite).
suite_test!(last_membership_in_log_initial);
suite_test!(last_membership_in_log);
suite_test!(last_membership_in_log_multi_step);
suite_test!(get_membership_from_log_and_empty_sm);
suite_test!(get_initial_state_without_init);
suite_test!(get_initial_state_with_state);
suite_test!(get_initial_state_last_log_gt_sm);
suite_test!(get_initial_state_last_log_lt_sm);
suite_test!(get_initial_state_log_ids);
suite_test!(get_initial_state_re_apply_committed);
suite_test!(save_vote);
suite_test!(get_log_entries);
suite_test!(limited_get_log_entries);
suite_test!(try_get_log_entry);
suite_test!(initial_logs);
suite_test!(get_log_state);
suite_test!(get_log_id);
suite_test!(last_id_in_log);
suite_test!(purge_logs_upto_0);
suite_test!(purge_logs_upto_5);
suite_test!(purge_logs_upto_20);
suite_test!(delete_logs_since_11);
suite_test!(delete_logs_since_0);
suite_test!(append_to_log);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn log_and_vote_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (_e, mut log, _sm) = open_dir(dir.path()).await;
        S::feed_10_logs_vote_self(&mut log).await.unwrap();
        log.truncate(openraft::LogId::new(openraft::CommittedLeaderId::new(1, 0), 9))
            .await
            .unwrap();
    }
    let (_e, mut log, _sm) = open_dir(dir.path()).await;
    let st = log.get_log_state().await.unwrap();
    assert_eq!(st.last_log_id.unwrap().index, 8, "the truncate survived");
    assert_eq!(log.try_get_log_entries(0..100).await.unwrap().len(), 9);
    assert_eq!(log.read_vote().await.unwrap().unwrap().leader_id.term, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upgrade_moves_the_log_out_of_redb() {
    let dir = tempfile::tempdir().unwrap();
    let engine: Arc<dyn KvStore> =
        Arc::new(RedbEngine::open(dir.path().join("fastetcd.redb")).unwrap());
    // A data file written by an older version: the log in redb.
    let mut legacy = KvLogStore::new(engine.clone());
    Suite::<TypeConfig, KvLogStore, FastetcdStateMachine, LegacyBuilder, TempDir>::feed_10_logs_vote_self(
        &mut legacy,
    )
    .await
    .unwrap();
    legacy.purge(openraft::LogId::new(openraft::CommittedLeaderId::new(1, 0), 3)).await.unwrap();
    drop(legacy);

    let mut log = WalLogStore::open(&wal_dir(dir.path()), engine.clone(), small()).await.unwrap();
    let st = log.get_log_state().await.unwrap();
    assert_eq!(st.last_purged_log_id.unwrap().index, 3);
    assert_eq!(st.last_log_id.unwrap().index, 10);
    let entries = log.try_get_log_entries(0..100).await.unwrap();
    assert_eq!(entries.iter().map(|e| e.log_id.index).collect::<Vec<_>>(), (4..=10).collect::<Vec<_>>());
    assert!(log.read_vote().await.unwrap().is_some());
    // redb's copy of the log is gone; its vote stays as the mirror.
    let snap = engine.snapshot().await.unwrap();
    assert!(snap.last("raft_log").await.unwrap().is_none());
    assert!(snap.get("raft_meta", b"vote").await.unwrap().is_some());
}

struct LegacyBuilder;

impl StoreBuilder<TypeConfig, KvLogStore, FastetcdStateMachine, TempDir> for LegacyBuilder {
    async fn build(&self) -> Result<(TempDir, KvLogStore, FastetcdStateMachine), StorageError<NodeId>> {
        unreachable!()
    }
}

// ---- a real node ---------------------------------------------------------

#[derive(Clone)]
struct NopNetwork;

impl openraft::network::RaftNetworkFactory<TypeConfig> for NopNetwork {
    type Network = NopNet;
    async fn new_client(&mut self, _target: NodeId, _node: &openraft::BasicNode) -> Self::Network {
        NopNet
    }
}

struct NopNet;

fn no_network<E: std::error::Error>() -> openraft::error::RPCError<NodeId, openraft::BasicNode, E> {
    openraft::error::RPCError::Network(openraft::error::NetworkError::new(&std::io::Error::other(
        "no network in single-node test",
    )))
}

impl openraft::network::RaftNetwork<TypeConfig> for NopNet {
    async fn append_entries(
        &mut self,
        _rpc: openraft::raft::AppendEntriesRequest<TypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::AppendEntriesResponse<NodeId>,
        openraft::error::RPCError<NodeId, openraft::BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        Err(no_network())
    }

    async fn install_snapshot(
        &mut self,
        _rpc: openraft::raft::InstallSnapshotRequest<TypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<NodeId>,
        openraft::error::RPCError<
            NodeId,
            openraft::BasicNode,
            openraft::error::RaftError<NodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        Err(no_network())
    }

    async fn vote(
        &mut self,
        _rpc: openraft::raft::VoteRequest<NodeId>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::VoteResponse<NodeId>,
        openraft::error::RPCError<NodeId, openraft::BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        Err(no_network())
    }
}

fn config() -> Arc<Config> {
    Arc::new(
        Config {
            heartbeat_interval: 100,
            election_timeout_min: 300,
            election_timeout_max: 600,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(50),
            max_in_snapshot_log_to_keep: 0,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    )
}

async fn wait_leader(raft: &Raft<TypeConfig>) {
    raft.wait(Some(Duration::from_secs(10)))
        .state(openraft::ServerState::Leader, "leader")
        .await
        .unwrap();
}

fn put(i: usize) -> FastetcdLogEntry {
    FastetcdLogEntry::Apply {
        mutations: vec![Mutation::Put {
            key: format!("k{i:04}").into_bytes(),
            value: vec![b'v'; 200],
            lease: 0,
            prev_kv: false,
            ignore_value: false,
            ignore_lease: false,
        }],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_purges_wal_segments_and_restarts_with_everything() {
    const N: usize = 400;
    let dir = tempfile::tempdir().unwrap();
    {
        let (_engine, log, sm) = open_dir(dir.path()).await;
        sm.mvcc().defer_apply_sync();
        let stats = log.stats();
        let checkpointer = spawn_checkpointer(
            log.clone(),
            sm.applied_index(),
            CheckpointConfig { interval: Duration::from_millis(20), entries: 25 },
        );
        let raft = Raft::<TypeConfig>::new(1, config(), NopNetwork, log.clone(), sm.clone())
            .await
            .unwrap();
        raft.initialize(BTreeSet::from([1])).await.unwrap();
        wait_leader(&raft).await;
        for i in 0..N {
            raft.client_write(put(i)).await.unwrap();
        }
        // Snapshots every 50 entries purge the log; checkpoints let the
        // WAL drop the segments behind them.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let segs = stats.segments.load(std::sync::atomic::Ordering::Relaxed);
            if segs <= 8 && log.pending_purge().is_none() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "WAL segments were never dropped ({segs} left, pending purge {:?})",
                log.pending_purge()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(stats.checkpoints.load(std::sync::atomic::Ordering::Relaxed) > 0);
        assert!(stats.fsyncs.load(std::sync::atomic::Ordering::Relaxed) > 0);
        raft.shutdown().await.unwrap();
        checkpointer.abort();
        let _ = checkpointer.await;
    }

    let (_engine, log, sm) = open_dir(dir.path()).await;
    let raft = Raft::<TypeConfig>::new(1, config(), NopNetwork, log.clone(), sm.clone())
        .await
        .unwrap();
    wait_leader(&raft).await;
    raft.client_write(put(N)).await.unwrap();
    let out = sm.mvcc().range(b"k", b"l", 0, 0, false, false).await.unwrap();
    assert_eq!(out.kvs.len(), N + 1, "every write is there after the restart");
    raft.shutdown().await.unwrap();
}

// ---- a slow disk (fastetcd#95) -------------------------------------------

/// The data file's engine with a slow durable commit, as a checkpoint
/// is on a spinning disk.
struct SlowCheckpoint(Arc<dyn KvStore>, Duration);

#[async_trait::async_trait]
impl KvStore for SlowCheckpoint {
    async fn snapshot(&self) -> fastetcd_storage::StorageResult<Arc<dyn fastetcd_storage::Snapshot>> {
        self.0.snapshot().await
    }
    async fn commit(
        &self,
        batch: fastetcd_storage::WriteBatch,
        opts: fastetcd_storage::WriteOptions,
    ) -> fastetcd_storage::StorageResult<()> {
        if opts.sync {
            tokio::time::sleep(self.1).await;
        }
        self.0.commit(batch, opts).await
    }
    async fn sync(&self) -> fastetcd_storage::StorageResult<()> {
        tokio::time::sleep(self.1).await;
        self.0.sync().await
    }
    async fn size_on_disk(&self) -> fastetcd_storage::StorageResult<u64> {
        self.0.size_on_disk().await
    }
    fn engine_name(&self) -> &'static str {
        "slow-checkpoint"
    }
}

/// A single-member node on a WAL whose fsync takes `fsync` and a data
/// file whose durable commit takes `checkpoint`.
async fn slow_node(
    dir: &std::path::Path,
    fsync: Duration,
    checkpoint: Duration,
) -> (Raft<TypeConfig>, WalLogStore, FastetcdStateMachine) {
    let base: Arc<dyn KvStore> = Arc::new(RedbEngine::open(dir.join("fastetcd.redb")).unwrap());
    let wb: Arc<dyn KvStore> = Arc::new(WriteBehind::new(base, 64 * 1024 * 1024));
    let engine: Arc<dyn KvStore> = Arc::new(SlowCheckpoint(wb, checkpoint));
    let mvcc = MvccStore::open(engine.clone()).await.unwrap();
    let sm = FastetcdStateMachine::open(mvcc, dir.join("snapshots")).await.unwrap();
    sm.mvcc().defer_apply_sync();
    let log = WalLogStore::open(
        &wal_dir(dir),
        engine,
        WalLogOptions { sync_delay: fsync, ..Default::default() },
    )
    .await
    .unwrap();
    let raft = Raft::<TypeConfig>::new(1, config(), NopNetwork, log.clone(), sm.clone())
        .await
        .unwrap();
    raft.initialize(BTreeSet::from([1])).await.unwrap();
    wait_leader(&raft).await;
    (raft, log, sm)
}

/// Proposals per WAL fsync for `clients` writers that each wait for
/// their write before the next, as the kubelet's status updates do.
async fn proposals_per_fsync(in_flight: usize) -> f64 {
    use std::sync::atomic::Ordering::Relaxed;
    const CLIENTS: usize = 20;
    const EACH: usize = 10;
    let dir = tempfile::tempdir().unwrap();
    let (raft, log, _sm) = slow_node(dir.path(), Duration::from_millis(20), Duration::ZERO).await;
    let proposer = fastetcd_raft::Proposer::spawn(
        raft.clone(),
        log.progress(),
        fastetcd_raft::WriteForwarder::new(fastetcd_raft::empty_peers()),
        in_flight,
    );
    let stats = log.stats();
    let (f0, p0) = (stats.fsyncs.load(Relaxed), stats.proposals_synced.load(Relaxed));
    let tasks: Vec<_> = (0..CLIENTS)
        .map(|c| {
            let p = proposer.clone();
            tokio::spawn(async move {
                for i in 0..EACH {
                    p.propose(put(c * EACH + i)).await.expect("proposal applied");
                }
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    let (f1, p1) = (stats.fsyncs.load(Relaxed), stats.proposals_synced.load(Relaxed));
    assert_eq!(p1 - p0, (CLIENTS * EACH) as u64, "every proposal counted once");
    let per = (p1 - p0) as f64 / (f1 - f0) as f64;
    eprintln!("in flight {in_flight}: {} proposals in {} fsyncs, {per:.1} per fsync", p1 - p0, f1 - f0);
    raft.shutdown().await.unwrap();
    per
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_share_a_wal_fsync() {
    // The server's cap: every waiting writer's proposal is appended at
    // once, so one fsync carries about all of them.
    let wide = proposals_per_fsync(fastetcd_raft::proposer::IN_FLIGHT).await;
    // #75's cap of 3: a proposal waits for an earlier one's apply.
    let narrow = proposals_per_fsync(fastetcd_raft::proposer::IN_FLIGHT_BLOCKING_LOG).await;
    assert!(wide >= 8.0, "20 writers, {wide:.1} proposals per fsync");
    assert!(wide > narrow * 1.5, "in flight 64: {wide:.1} per fsync, 3: {narrow:.1}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_checkpoints_leave_the_disk_to_the_wal() {
    use std::sync::atomic::Ordering::Relaxed;
    const CHECKPOINT: Duration = Duration::from_millis(40);
    let dir = tempfile::tempdir().unwrap();
    let (raft, log, sm) = slow_node(dir.path(), Duration::ZERO, CHECKPOINT).await;
    let stats = log.stats();
    // Asked for every 5 ms: unpaced, checkpoints would run back to back.
    let checkpointer = spawn_checkpointer(
        log.clone(),
        sm.applied_index(),
        CheckpointConfig { interval: Duration::from_millis(5), entries: 10_000 },
    );
    let started = tokio::time::Instant::now();
    let mut i = 0;
    while started.elapsed() < Duration::from_secs(2) {
        raft.client_write(put(i)).await.unwrap();
        i += 1;
    }
    let ran = started.elapsed();
    let n = stats.checkpoints.load(Relaxed);
    let pace = Duration::from_nanos(stats.checkpoint_pace_nanos.load(Relaxed));
    eprintln!("{i} writes, {n} checkpoints in {ran:?}, pace {pace:?}");
    // Each cycle is at least 40 ms + 4 x 40 ms: about 10 in 2 s, not 50.
    let most = (ran.as_millis() / (5 * CHECKPOINT.as_millis())) as u64 + 2;
    assert!(n >= 2, "{n} checkpoints");
    assert!(n <= most, "{n} checkpoints in {ran:?}, at most {most} when paced");
    assert!(pace >= CHECKPOINT * fastetcd_raft::wal_log_store::CHECKPOINT_PACE);
    // Paced, not skipped: the last write still becomes durable.
    let applied = *sm.applied_index().borrow();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while stats.durable_applied.load(Relaxed) < applied {
        assert!(tokio::time::Instant::now() < deadline, "no checkpoint covered the last write");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    raft.shutdown().await.unwrap();
    checkpointer.abort();
}
