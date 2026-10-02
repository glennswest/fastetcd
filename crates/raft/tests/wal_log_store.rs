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
use fastetcd_storage::KvStore;

fn small() -> WalLogOptions {
    // Tiny segments and cache: rolls, segment drops and disk reads all
    // happen within a few entries.
    WalLogOptions { segment_bytes: 4096, cache_bytes: 512 }
}

async fn open_dir(dir: &std::path::Path) -> (Arc<dyn KvStore>, WalLogStore, FastetcdStateMachine) {
    // A restart in the same process: the previous node's tasks may hold
    // the file for a moment after shutdown.
    let mut tries = 0;
    let engine: Arc<dyn KvStore> = loop {
        match RedbEngine::open(dir.join("fastetcd.redb")) {
            Ok(e) => break Arc::new(e),
            Err(e) if tries < 50 => {
                tries += 1;
                let _ = e;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("open: {e}"),
        }
    };
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
            dir.path().join("fastetcd.redb"),
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
