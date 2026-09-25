//! fastetcd#30: a snapshot moves between nodes through files, not RAM.
//!
//! These drive the state machine through the same calls openraft's
//! chunked transport makes (`Chunked::send_snapshot` on the leader,
//! `Streaming::receive` on the follower): seek to an offset and read a
//! chunk from the leader's `get_current_snapshot`, then seek and
//! `write_all` it into the follower's `begin_receiving_snapshot`, then
//! `shutdown` and `install_snapshot`.

use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{Entry, EntryPayload, LeaderId, LogId};
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use fastetcd_raft::types::{FastetcdLogEntry, NodeId, TypeConfig};
use fastetcd_raft::{FastetcdStateMachine, SnapshotFile};
use fastetcd_storage::mvcc::{Mutation, MvccStore};
use fastetcd_storage::redb_engine::RedbEngine;
use fastetcd_storage::KvStore;

/// Small, so a snapshot takes many chunks and the seek/offset logic is
/// exercised; openraft's default is 3 MiB.
const CHUNK: usize = 4 * 1024;

fn log_id(index: u64) -> LogId<NodeId> {
    LogId {
        leader_id: LeaderId::new(1, 1),
        index,
    }
}

fn put_entry(index: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(index),
        payload: EntryPayload::Normal(FastetcdLogEntry::Apply {
            mutations: vec![Mutation::Put {
                key: format!("key/{index:05}").into_bytes(),
                // Big enough that the snapshot spans dozens of chunks.
                value: vec![b'v'; 512],
                lease: 0,
                prev_kv: false,
                ignore_value: false,
                ignore_lease: false,
            }],
        }),
    }
}

struct Node {
    sm: FastetcdStateMachine,
    mvcc: MvccStore,
    snap_dir: PathBuf,
}

async fn open_node(dir: &Path, name: &str) -> Node {
    let path = dir.join(format!("{name}.redb"));
    let snap_dir = dir.join(format!("{name}.snapshots"));
    let engine: Arc<dyn KvStore> = Arc::new(RedbEngine::open(&path).unwrap());
    let mvcc = MvccStore::open(engine).await.unwrap();
    let sm = FastetcdStateMachine::open(mvcc.clone(), &snap_dir).await.unwrap();
    Node { sm, mvcc, snap_dir }
}

/// A leader with `n` keys and a built snapshot at index `n`.
async fn leader_with_snapshot(dir: &Path, n: u64) -> Node {
    let mut leader = open_node(dir, "leader").await;
    for i in 1..=n {
        leader.sm.apply(vec![put_entry(i)]).await.unwrap();
    }
    leader
        .sm
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    leader
}

async fn visible_keys(mvcc: &MvccStore) -> usize {
    mvcc.range(b"", &[0xFF; 16], 0, 0, true, false)
        .await
        .unwrap()
        .kvs
        .len()
}

fn files_in(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

/// Read the next chunk at `offset`, as `Chunked::send_snapshot` does.
async fn read_chunk(src: &mut SnapshotFile, offset: u64) -> Vec<u8> {
    src.seek(SeekFrom::Start(offset)).await.unwrap();
    let mut buf = Vec::with_capacity(CHUNK);
    while buf.capacity() > buf.len() {
        if src.read_buf(&mut buf).await.unwrap() == 0 {
            break;
        }
    }
    buf
}

#[tokio::test]
async fn a_snapshot_is_sent_and_received_through_files() {
    let dir = tempdir().unwrap();
    let mut leader = leader_with_snapshot(dir.path(), 200).await;
    let mut follower = open_node(dir.path(), "follower").await;

    // Send side: the retained file, not a copy in memory.
    let sent = leader.sm.get_current_snapshot().await.unwrap().unwrap();
    assert!(!sent.snapshot.is_in_memory(), "the leader sends from the file");
    let meta = sent.meta.clone();
    let mut src = sent.snapshot;
    let end = src.seek(SeekFrom::End(0)).await.unwrap();
    assert!(end > (20 * CHUNK) as u64, "the snapshot must span many chunks");

    // Receive side: a temp file in the follower's snapshot directory.
    let mut dst = follower.sm.begin_receiving_snapshot().await.unwrap();
    assert!(!dst.is_in_memory(), "the follower receives into a file");
    assert!(
        files_in(&follower.snap_dir).iter().any(|f| f.ends_with(".snap.tmp")),
        "chunks land in a temp file: {:?}",
        files_in(&follower.snap_dir)
    );

    let mut offset = 0u64;
    while offset < end {
        let chunk = read_chunk(&mut src, offset).await;
        dst.seek(SeekFrom::Start(offset)).await.unwrap();
        dst.write_all(&chunk).await.unwrap();
        offset += chunk.len() as u64;
    }
    dst.shutdown().await.unwrap();

    follower.sm.install_snapshot(&meta, dst).await.unwrap();

    assert_eq!(follower.mvcc.current_revision().await, 200);
    assert_eq!(visible_keys(&follower.mvcc).await, 200);
    assert_eq!(follower.sm.applied_state().await.unwrap().0, Some(log_id(200)));

    // The received file was renamed into place: the follower retains a
    // byte-identical copy and no temp file is left behind.
    assert_eq!(
        files_in(&follower.snap_dir),
        vec![
            "00000000000000000200.meta".to_string(),
            "00000000000000000200.snap".to_string()
        ]
    );
    assert_eq!(
        std::fs::read(follower.snap_dir.join("00000000000000000200.snap")).unwrap(),
        std::fs::read(leader.snap_dir.join("00000000000000000200.snap")).unwrap(),
    );
    // And the follower serves it onwards from that file.
    let onward = follower.sm.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(onward.meta.last_log_id, Some(log_id(200)));
    assert!(!onward.snapshot.is_in_memory());
}

/// A transfer that is cut off leaves the follower's live database
/// untouched and its temp file gone.
#[tokio::test]
async fn a_cut_off_transfer_leaves_nothing_behind() {
    let dir = tempdir().unwrap();
    let mut leader = leader_with_snapshot(dir.path(), 100).await;
    let mut follower = open_node(dir.path(), "follower").await;
    for i in 1..=3 {
        follower.sm.apply(vec![put_entry(i)]).await.unwrap();
    }

    let mut src = leader.sm.get_current_snapshot().await.unwrap().unwrap().snapshot;
    let mut dst = follower.sm.begin_receiving_snapshot().await.unwrap();
    let chunk = read_chunk(&mut src, 0).await;
    dst.write_all(&chunk).await.unwrap();

    // The leader stepped down / the stream was replaced: openraft drops
    // the half-written handle.
    drop(dst);

    assert!(
        !files_in(&follower.snap_dir).iter().any(|f| f.ends_with(".tmp")),
        "an abandoned transfer must not leave its temp file holding space: {:?}",
        files_in(&follower.snap_dir)
    );
    assert_eq!(follower.mvcc.current_revision().await, 3, "live data untouched");
    assert_eq!(visible_keys(&follower.mvcc).await, 3);
    assert_eq!(follower.sm.applied_state().await.unwrap().0, Some(log_id(3)));
}

/// A corrupt transfer fails the install without touching the live
/// database, and its temp file is removed.
#[tokio::test]
async fn a_corrupt_snapshot_is_refused_before_it_touches_the_database() {
    let dir = tempdir().unwrap();
    let mut leader = leader_with_snapshot(dir.path(), 50).await;
    let mut follower = open_node(dir.path(), "follower").await;
    follower.sm.apply(vec![put_entry(1)]).await.unwrap();

    let meta = leader.sm.get_current_snapshot().await.unwrap().unwrap().meta;
    let mut dst = follower.sm.begin_receiving_snapshot().await.unwrap();
    dst.write_all(&[0xFF; 100]).await.unwrap();
    dst.shutdown().await.unwrap();

    assert!(follower.sm.install_snapshot(&meta, dst).await.is_err());
    assert_eq!(follower.mvcc.current_revision().await, 1);
    assert!(
        !files_in(&follower.snap_dir).iter().any(|f| f.ends_with(".tmp")),
        "{:?}",
        files_in(&follower.snap_dir)
    );
}

/// A retained snapshot that disappears — rolled off to make room for an
/// incoming one, or deleted — is rebuilt on demand instead of reported
/// as `None`, which openraft's replication would turn into a storage
/// error.
#[tokio::test]
async fn a_missing_snapshot_file_is_rebuilt_on_demand() {
    let dir = tempdir().unwrap();
    let mut leader = leader_with_snapshot(dir.path(), 30).await;
    for f in files_in(&leader.snap_dir) {
        std::fs::remove_file(leader.snap_dir.join(f)).unwrap();
    }

    let rebuilt = leader
        .sm
        .get_current_snapshot()
        .await
        .expect("a missing file must not be a storage error")
        .expect("a node with applied state must serve a snapshot");
    assert_eq!(rebuilt.meta.last_log_id, Some(log_id(30)));
    assert!(!rebuilt.snapshot.is_in_memory(), "rebuilt onto disk");

    // And it installs on another node.
    let mut follower = open_node(dir.path(), "follower").await;
    follower
        .sm
        .install_snapshot(&rebuilt.meta, rebuilt.snapshot)
        .await
        .unwrap();
    assert_eq!(visible_keys(&follower.mvcc).await, 30);
}

/// Receiving rolls the follower's own retained snapshot off first, as
/// every snapshot write does, so the volume never needs room for two.
/// Until the new one lands, the follower still serves a snapshot.
#[tokio::test]
async fn receiving_rolls_off_first_and_still_serves_a_snapshot() {
    let dir = tempdir().unwrap();
    let mut leader = leader_with_snapshot(dir.path(), 40).await;
    let mut follower = open_node(dir.path(), "follower").await;
    for i in 1..=5 {
        follower.sm.apply(vec![put_entry(i)]).await.unwrap();
    }
    follower
        .sm
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    assert!(files_in(&follower.snap_dir).contains(&"00000000000000000005.snap".to_string()));

    let mut src = leader.sm.get_current_snapshot().await.unwrap().unwrap().snapshot;
    let mut dst = follower.sm.begin_receiving_snapshot().await.unwrap();
    assert!(
        !files_in(&follower.snap_dir).iter().any(|f| f.ends_with(".snap")),
        "the retained snapshot is rolled off before the incoming one is written: {:?}",
        files_in(&follower.snap_dir)
    );

    // Mid-transfer, this node is asked for a snapshot (it became leader).
    let served = follower.sm.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(served.meta.last_log_id, Some(log_id(5)));
    drop(served);

    let chunk = read_chunk(&mut src, 0).await;
    dst.write_all(&chunk).await.unwrap();
    drop(dst);
}
