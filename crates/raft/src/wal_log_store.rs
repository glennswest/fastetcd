//! The raft log in a sequential WAL, in front of redb (fastetcd#85).
//!
//! Before this, the raft log was a pair of redb tables: every append was
//! a durable redb commit, and because the state machine's applies are
//! non-durable redb commits (#71), that commit also wrote every B-tree
//! page the applies since the last one had dirtied. The fsync on a
//! client's write was a scatter of page writes: seeks on a spinning
//! disk.
//!
//! Now:
//!
//! - **The log** is [`fastetcd_storage::raft_wal`]: records appended to
//!   preallocated segment files. One writer thread takes whatever
//!   openraft appended while the previous fsync ran and makes it durable
//!   with one `fdatasync` (group commit), then calls each `LogFlushed`.
//!   `append` returns as soon as the entries are readable, so RaftCore
//!   does not wait for the disk.
//! - **The vote** is a record synced before `save_vote` returns. The
//!   committed id is a record that rides the next sync (openraft accepts
//!   an older committed id after a crash).
//! - **redb** is made durable by the [checkpointer](spawn_checkpointer),
//!   every `interval` or `entries` applied entries: fdatasync the data
//!   file outside redb's writer lock, then one durable commit. A crash
//!   loses at most the applies since the last checkpoint, and the WAL
//!   replays them (openraft re-applies up to the committed id; a leader
//!   re-commits the rest).
//! - **Purge** (after a snapshot) removes entries from memory at once.
//!   The WAL keeps them until a checkpoint has made redb durable past
//!   them; only then is a purge record synced and old segments deleted.
//!   The WAL never drops an entry redb could still need. A purge past
//!   the last entry (a follower installing a snapshot) would leave a
//!   hole, so it checkpoints and syncs the purge before returning.
//!
//! **Upgrade:** a data directory without `wal/` has its log in redb's
//! `raft_log`/`raft_meta`; [`WalLogStore::open`] copies it into a new WAL
//! (built in `wal.tmp`, renamed into place) and then clears `raft_log`.
//! The vote and the last purged id stay mirrored in `raft_meta`
//! (non-durable writes, persisted by the next checkpoint) so that a
//! backup of the data file restores to a consistent log. Restoring a
//! data file moves `wal/` aside, and the next start builds a new WAL
//! from that mirror.

use std::collections::BTreeMap;
use std::io;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{
    AnyError, Entry, ErrorSubject, ErrorVerb, LogId, LogState, RaftLogReader, StorageError,
    StorageIOError, Vote,
};
use tokio::sync::{oneshot, watch};

use fastetcd_storage::raft_wal::{
    Loc, Wal, WalOptions, WalReader, HEADER_LEN, KIND_COMMITTED, KIND_ENTRY, KIND_PURGE,
    KIND_TRUNCATE, KIND_VOTE,
};
use fastetcd_storage::{KvStore, WriteBatch, WriteOptions};

use crate::kv_log_store::{
    LogProgress, META_COMMITTED, META_LAST_PURGED, META_VOTE, TABLE_LOG, TABLE_META,
};
use crate::types::{NodeId, TypeConfig};

/// The WAL directory inside a data directory.
pub fn wal_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("wal")
}

#[derive(Debug, Clone, Copy)]
pub struct WalLogOptions {
    /// Preallocated size of each segment.
    pub segment_bytes: u64,
    /// Bytes of recent entries kept in RAM (followers read them to
    /// replicate); older ones are read back from the segments.
    pub cache_bytes: usize,
}

impl Default for WalLogOptions {
    fn default() -> Self {
        Self {
            segment_bytes: fastetcd_storage::raft_wal::DEFAULT_SEGMENT_BYTES,
            cache_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Counters for `/metrics`.
#[derive(Debug, Default)]
pub struct WalStats {
    pub fsyncs: AtomicU64,
    pub fsync_nanos: AtomicU64,
    /// Longest single fdatasync since the process started.
    pub fsync_max_nanos: AtomicU64,
    pub bytes_appended: AtomicU64,
    pub segments: AtomicU64,
    pub cached_bytes: AtomicU64,
    pub checkpoints: AtomicU64,
    pub checkpoint_nanos: AtomicU64,
    /// Longest single checkpoint since the process started.
    pub checkpoint_max_nanos: AtomicU64,
    /// The part of each checkpoint that holds redb's writer (the
    /// durable commit), which applies wait behind.
    pub checkpoint_commit_nanos: AtomicU64,
    pub checkpoint_commit_max_nanos: AtomicU64,
    pub checkpoint_failures: AtomicU64,
    /// `index + 1` of the last applied entry a checkpoint made durable
    /// in the data file (0 = none yet in this process).
    pub durable_applied: AtomicU64,
}

type Done = Box<dyn FnOnce(io::Result<()>) + Send>;

struct Cmd {
    records: Vec<(u8, u64, Arc<Vec<u8>>)>,
    sync: bool,
    /// After the sync, delete segments whose entries are all `<=` this.
    drop_upto: Option<u64>,
    done: Option<Done>,
}

struct Slot {
    bytes: Option<Arc<Vec<u8>>>,
    loc: Option<Loc>,
}

struct State {
    entries: BTreeMap<u64, Slot>,
    cached_bytes: usize,
    cache_limit: usize,
    /// Every slot below this has been evicted or has no bytes.
    evict_cursor: u64,
    vote: Option<Vote<NodeId>>,
    committed: Option<LogId<NodeId>>,
    last_purged: Option<LogId<NodeId>>,
    /// Purged in memory, not yet in the WAL (see the module docs).
    pending_purge: Option<LogId<NodeId>>,
}

impl State {
    fn forget(&mut self, gone: BTreeMap<u64, Slot>) {
        for s in gone.values() {
            if let Some(b) = &s.bytes {
                self.cached_bytes -= b.len();
            }
        }
    }

    /// Drop every entry at `>= index`.
    fn remove_from(&mut self, index: u64) {
        let gone = self.entries.split_off(&index);
        self.forget(gone);
        self.evict_cursor = self.evict_cursor.min(index);
    }

    /// Drop every entry at `<= index`.
    fn remove_upto(&mut self, index: u64) {
        let keep = self.entries.split_off(&index.saturating_add(1));
        let gone = std::mem::replace(&mut self.entries, keep);
        self.forget(gone);
    }

    fn place(&mut self, placed: Vec<(u64, Arc<Vec<u8>>, Loc)>) {
        for (index, bytes, loc) in placed {
            if let Some(slot) = self.entries.get_mut(&index) {
                if slot.bytes.as_ref().is_some_and(|b| Arc::ptr_eq(b, &bytes)) {
                    slot.loc = Some(loc);
                }
            }
        }
        if self.cached_bytes <= self.cache_limit {
            return;
        }
        let mut cursor = self.evict_cursor;
        for (index, slot) in self.entries.range_mut(self.evict_cursor..) {
            if self.cached_bytes <= self.cache_limit || slot.loc.is_none() {
                break;
            }
            if let Some(b) = slot.bytes.take() {
                self.cached_bytes -= b.len();
            }
            cursor = index + 1;
        }
        self.evict_cursor = cursor;
    }
}

fn io_err<E: std::fmt::Display>(verb: ErrorVerb, e: E) -> StorageError<NodeId> {
    StorageIOError::new(
        ErrorSubject::Log(LogId { leader_id: Default::default(), index: 0 }),
        verb,
        AnyError::error(format!("{e}")),
    )
    .into()
}

/// The writer thread: owns the [`Wal`], groups commands into one sync.
fn writer(
    mut wal: Wal,
    rx: mpsc::Receiver<Cmd>,
    state: Arc<Mutex<State>>,
    stats: Arc<WalStats>,
) {
    // Once a write fails, every later one fails too: a gap in the log
    // must never be followed by an entry.
    let mut failed: Option<(io::ErrorKind, String)> = None;
    while let Ok(first) = rx.recv() {
        let mut cmds = vec![first];
        while cmds.len() < 4096 {
            match rx.try_recv() {
                Ok(c) => cmds.push(c),
                Err(_) => break,
            }
        }
        let mut placed = Vec::new();
        let mut sync = false;
        let mut drop_upto: Option<u64> = None;
        if failed.is_none() {
            for cmd in &cmds {
                let recs: Vec<(u8, u64, &[u8])> = cmd
                    .records
                    .iter()
                    .map(|(k, i, p)| (*k, *i, p.as_slice()))
                    .collect();
                match wal.append(&recs) {
                    Ok(locs) => {
                        for ((kind, index, payload), loc) in cmd.records.iter().zip(locs) {
                            stats
                                .bytes_appended
                                .fetch_add(HEADER_LEN + payload.len() as u64, Ordering::Relaxed);
                            if *kind == KIND_ENTRY {
                                placed.push((*index, payload.clone(), loc));
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "raft WAL append failed");
                        failed = Some((e.kind(), format!("raft WAL append: {e}")));
                        break;
                    }
                }
                sync |= cmd.sync;
                if let Some(d) = cmd.drop_upto {
                    drop_upto = Some(drop_upto.map_or(d, |x| x.max(d)));
                }
            }
        }
        if failed.is_none() && sync {
            let t = Instant::now();
            if let Err(e) = wal.sync() {
                tracing::error!(error = %e, "raft WAL fdatasync failed");
                failed = Some((e.kind(), format!("raft WAL fdatasync: {e}")));
            }
            let took = t.elapsed().as_nanos() as u64;
            stats.fsyncs.fetch_add(1, Ordering::Relaxed);
            stats.fsync_nanos.fetch_add(took, Ordering::Relaxed);
            stats.fsync_max_nanos.fetch_max(took, Ordering::Relaxed);
        }
        if failed.is_none() {
            if let Some(d) = drop_upto {
                if let Err(e) = wal.drop_segments_upto(d) {
                    // Space not returned yet; the next purge retries.
                    tracing::warn!(error = %e, "deleting purged raft WAL segments");
                }
            }
            if !placed.is_empty() {
                let mut st = state.lock().unwrap();
                st.place(placed);
                stats.cached_bytes.store(st.cached_bytes as u64, Ordering::Relaxed);
            }
        }
        stats.segments.store(wal.segment_count() as u64, Ordering::Relaxed);
        for cmd in cmds {
            if let Some(done) = cmd.done {
                done(match &failed {
                    None => Ok(()),
                    Some((kind, msg)) => Err(io::Error::new(*kind, msg.clone())),
                });
            }
        }
    }
}

/// openraft's log storage over the WAL. Cheap to clone; every clone
/// shares the same log.
#[derive(Clone)]
pub struct WalLogStore {
    state: Arc<Mutex<State>>,
    tx: mpsc::Sender<Cmd>,
    reader: Arc<WalReader>,
    /// The data file's engine: legacy log on upgrade, vote/purge mirror,
    /// and the durable commit a hole-making purge needs.
    engine: Arc<dyn KvStore>,
    stats: Arc<WalStats>,
    committed_index: Arc<AtomicU64>,
    progress: LogProgress,
}

fn has_segments(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .any(|e| e.file_name().to_string_lossy().ends_with(".wal"))
        })
        .unwrap_or(false)
}

fn sync_parent(dir: &Path) -> io::Result<()> {
    if let Some(parent) = dir.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

/// Build `dir` from the log in the data file's `raft_log`/`raft_meta`
/// (a data directory from before #85, or one just restored from a
/// backup). Returns how many entries were copied.
async fn migrate(dir: &Path, engine: &Arc<dyn KvStore>, opts: WalOptions) -> anyhow::Result<u64> {
    let snap = engine.snapshot().await?;
    let vote = snap.get(TABLE_META, META_VOTE).await?;
    let committed = snap.get(TABLE_META, META_COMMITTED).await?;
    let purged = snap.get(TABLE_META, META_LAST_PURGED).await?;

    let name = dir
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "wal".to_string());
    let tmp = dir.with_file_name(format!("{name}.tmp"));
    if tmp.exists() {
        std::fs::remove_dir_all(&tmp)?;
    }
    let (mut wal, _) = Wal::open(&tmp, opts)?;
    if let Some(v) = &vote {
        wal.append(&[(KIND_VOTE, 0, v.as_slice())])?;
    }
    if let Some(p) = &purged {
        let id: LogId<NodeId> = bincode::deserialize(p)?;
        wal.append(&[(KIND_PURGE, id.index, p.as_slice())])?;
    }
    if let Some(c) = &committed {
        wal.append(&[(KIND_COMMITTED, 0, c.as_slice())])?;
    }
    let mut copied = 0u64;
    let mut start: Bound<Vec<u8>> = Bound::Unbounded;
    loop {
        let rows = snap.range(TABLE_LOG, start.clone(), Bound::Unbounded, 1024).await?;
        let Some((last_key, _)) = rows.last() else { break };
        let last_key = last_key.clone();
        for (k, v) in &rows {
            let index = u64::from_be_bytes(
                k.as_slice()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("raft_log key of {} bytes", k.len()))?,
            );
            wal.append(&[(KIND_ENTRY, index, v.as_slice())])?;
            copied += 1;
        }
        start = Bound::Excluded(last_key);
    }
    wal.sync()?;
    drop(wal);
    drop(snap);
    if dir.exists() {
        // An empty or half-made directory with no segments.
        std::fs::remove_dir_all(dir)?;
    }
    std::fs::rename(&tmp, dir)?;
    sync_parent(dir)?;
    Ok(copied)
}

impl WalLogStore {
    /// Open the WAL in `dir`, first building it from the data file's
    /// legacy log if there is none (see the module docs).
    pub async fn open(
        dir: &Path,
        engine: Arc<dyn KvStore>,
        opts: WalLogOptions,
    ) -> anyhow::Result<Self> {
        let wal_opts = WalOptions { segment_bytes: opts.segment_bytes };
        if !has_segments(dir) {
            let copied = migrate(dir, &engine, wal_opts).await?;
            if copied > 0 {
                tracing::info!(
                    entries = copied,
                    dir = %dir.display(),
                    "raft log moved from the data file into the WAL (fastetcd#85)"
                );
            }
        }
        // The legacy log is not read again; clear it (also after a crash
        // between the rename above and this commit).
        {
            let snap = engine.snapshot().await?;
            if snap.last(TABLE_LOG).await?.is_some() {
                drop(snap);
                let mut batch = WriteBatch::new();
                batch.delete_range(TABLE_LOG, &[], &[0xFFu8; 9]);
                batch.delete(TABLE_META, META_COMMITTED);
                engine.commit(batch, WriteOptions::default()).await?;
            }
        }

        let dir_owned = dir.to_path_buf();
        let (wal, replay) =
            tokio::task::spawn_blocking(move || Wal::open(&dir_owned, wal_opts)).await??;
        if replay.torn_tail {
            tracing::warn!(
                dir = %dir.display(),
                "raft WAL had a torn tail (a crash during a write); it was cut. Nothing \
                 acknowledged is lost: an entry counts only once its fsync returned."
            );
        }
        let decode_meta = |kind: u8| -> anyhow::Result<Option<Vec<u8>>> {
            Ok(replay.meta.get(&kind).map(|(_, p)| p.clone()))
        };
        let vote: Option<Vote<NodeId>> = match decode_meta(KIND_VOTE)? {
            Some(b) => Some(bincode::deserialize(&b)?),
            None => None,
        };
        let committed: Option<LogId<NodeId>> = match decode_meta(KIND_COMMITTED)? {
            Some(b) => bincode::deserialize(&b)?,
            None => None,
        };
        let last_purged: Option<LogId<NodeId>> = match decode_meta(KIND_PURGE)? {
            Some(b) => Some(bincode::deserialize(&b)?),
            None => None,
        };

        let reader = wal.reader();
        let entries: BTreeMap<u64, Slot> = replay
            .entries
            .iter()
            .map(|(i, loc)| (*i, Slot { bytes: None, loc: Some(*loc) }))
            .collect();
        let progress = LogProgress::default();
        if let Some(v) = &vote {
            progress.note_vote(v);
        }
        let last_index = entries
            .keys()
            .next_back()
            .copied()
            .or(last_purged.map(|p| p.index));
        if let Some(l) = last_index {
            progress.last_durable.fetch_max(l + 1, Ordering::Release);
        }
        let committed_index = Arc::new(AtomicU64::new(0));
        if let Some(c) = &committed {
            committed_index.fetch_max(c.index, Ordering::Relaxed);
            progress.committed.fetch_max(c.index + 1, Ordering::Release);
        }
        let state = Arc::new(Mutex::new(State {
            entries,
            cached_bytes: 0,
            cache_limit: opts.cache_bytes,
            evict_cursor: 0,
            vote,
            committed,
            last_purged,
            pending_purge: None,
        }));
        let stats = Arc::new(WalStats::default());
        stats.segments.store(wal.segment_count() as u64, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel();
        {
            let state = state.clone();
            let stats = stats.clone();
            std::thread::Builder::new()
                .name("raft-wal".into())
                .spawn(move || writer(wal, rx, state, stats))?;
        }
        Ok(Self { state, tx, reader, engine, stats, committed_index, progress })
    }

    /// The store's progress, for [`crate::read_index::LocalReadIndex`].
    pub fn progress(&self) -> LogProgress {
        self.progress.clone()
    }

    /// The committed index, kept current as openraft saves it.
    pub fn committed_index(&self) -> Arc<AtomicU64> {
        self.committed_index.clone()
    }

    pub fn stats(&self) -> Arc<WalStats> {
        self.stats.clone()
    }

    fn send(&self, cmd: Cmd) -> io::Result<()> {
        self.tx.send(cmd).map_err(|mpsc::SendError(cmd)| {
            let e = || io::Error::new(io::ErrorKind::BrokenPipe, "raft WAL writer has stopped");
            if let Some(done) = cmd.done {
                done(Err(e()));
            }
            e()
        })
    }

    /// Write records and wait for their sync.
    async fn write_synced(&self, records: Vec<(u8, u64, Arc<Vec<u8>>)>, drop_upto: Option<u64>) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd {
            records,
            sync: true,
            drop_upto,
            done: Some(Box::new(move |r| {
                let _ = tx.send(r);
            })),
        })?;
        rx.await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "raft WAL writer has stopped"))?
    }

    /// Copy a value into the data file's `raft_meta` (non-durable; the
    /// next checkpoint persists it). Only a backup reads it.
    async fn mirror(&self, key: &[u8], value: &[u8]) {
        let mut batch = WriteBatch::new();
        batch.put(TABLE_META, key, value);
        if let Err(e) = self.engine.commit(batch, WriteOptions { sync: false }).await {
            tracing::warn!(error = %e, "mirroring raft metadata into the data file");
        }
    }

    /// A purge openraft asked for that the WAL has not recorded yet.
    pub fn pending_purge(&self) -> Option<LogId<NodeId>> {
        self.state.lock().unwrap().pending_purge
    }

    /// Record `purged` in the WAL and delete the segments it covers.
    /// Only once the data file is durable past `purged` (the
    /// checkpointer calls this right after a checkpoint).
    pub async fn complete_purge(&self, purged: LogId<NodeId>) -> io::Result<()> {
        let bytes = bincode::serialize(&purged)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.write_synced(vec![(KIND_PURGE, purged.index, Arc::new(bytes))], Some(purged.index))
            .await?;
        let mut st = self.state.lock().unwrap();
        if st.pending_purge.is_some_and(|p| p.index <= purged.index) {
            st.pending_purge = None;
        }
        Ok(())
    }

    fn decode(bytes: &[u8]) -> Result<Entry<TypeConfig>, StorageError<NodeId>> {
        bincode::deserialize(bytes).map_err(|e| io_err(ErrorVerb::Read, e))
    }

    /// The log id of the last entry, read back if it is not cached.
    async fn last_log_id(&self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let (bytes, loc) = {
            let st = self.state.lock().unwrap();
            match st.entries.iter().next_back() {
                None => return Ok(None),
                Some((_, slot)) => (slot.bytes.clone(), slot.loc),
            }
        };
        let entry = match (bytes, loc) {
            (Some(b), _) => Self::decode(&b)?,
            (None, Some(loc)) => {
                let reader = self.reader.clone();
                let b = tokio::task::spawn_blocking(move || reader.read(loc))
                    .await
                    .map_err(|e| io_err(ErrorVerb::Read, e))?
                    .map_err(|e| io_err(ErrorVerb::Read, e))?;
                Self::decode(&b)?
            }
            (None, None) => return Err(io_err(ErrorVerb::Read, "raft WAL entry has no data")),
        };
        Ok(Some(entry.log_id))
    }
}

impl RaftLogReader<TypeConfig> for WalLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        enum Src {
            Mem(Arc<Vec<u8>>),
            Disk(Loc),
        }
        let srcs: Vec<Src> = {
            let st = self.state.lock().unwrap();
            let mut v = Vec::new();
            for (_, slot) in st.entries.range(range) {
                match (&slot.bytes, slot.loc) {
                    (Some(b), _) => v.push(Src::Mem(b.clone())),
                    (None, Some(loc)) => v.push(Src::Disk(loc)),
                    (None, None) => {
                        return Err(io_err(ErrorVerb::Read, "raft WAL entry has no data"));
                    }
                }
            }
            v
        };
        if srcs.iter().all(|s| matches!(s, Src::Mem(_))) {
            return srcs
                .iter()
                .map(|s| match s {
                    Src::Mem(b) => Self::decode(b),
                    Src::Disk(_) => unreachable!(),
                })
                .collect();
        }
        let reader = self.reader.clone();
        tokio::task::spawn_blocking(move || {
            srcs.iter()
                .map(|s| match s {
                    Src::Mem(b) => Self::decode(b),
                    Src::Disk(loc) => {
                        let b = reader.read(*loc).map_err(|e| io_err(ErrorVerb::Read, e))?;
                        Self::decode(&b)
                    }
                })
                .collect()
        })
        .await
        .map_err(|e| io_err(ErrorVerb::Read, e))?
    }
}

impl RaftLogStorage<TypeConfig> for WalLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let last_purged = self.state.lock().unwrap().last_purged;
        let last = self.last_log_id().await?;
        Ok(LogState { last_purged_log_id: last_purged, last_log_id: last.or(last_purged) })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let bytes = bincode::serialize(vote).map_err(|e| io_err(ErrorVerb::Write, e))?;
        self.write_synced(vec![(KIND_VOTE, 0, Arc::new(bytes.clone()))], None)
            .await
            .map_err(|e| io_err(ErrorVerb::Write, e))?;
        self.state.lock().unwrap().vote = Some(*vote);
        // Before returning: openraft grants or acts on the vote only
        // after this returns.
        self.progress.note_vote(vote);
        self.mirror(META_VOTE, &bytes).await;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.state.lock().unwrap().vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        if let Some(c) = &committed {
            self.committed_index.fetch_max(c.index, Ordering::Relaxed);
            self.progress.committed.fetch_max(c.index + 1, Ordering::Release);
        }
        self.state.lock().unwrap().committed = committed;
        let bytes = bincode::serialize(&committed).map_err(|e| io_err(ErrorVerb::Write, e))?;
        // Not synced: it rides the next sync, and an older committed id
        // after a crash is one openraft accepts.
        self.send(Cmd {
            records: vec![(KIND_COMMITTED, 0, Arc::new(bytes))],
            sync: false,
            drop_upto: None,
            done: None,
        })
        .map_err(|e| io_err(ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.state.lock().unwrap().committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut records = Vec::new();
        let mut last = None;
        {
            let mut st = self.state.lock().unwrap();
            for entry in entries {
                if matches!(entry.payload, openraft::EntryPayload::Membership(_)) {
                    self.progress
                        .membership
                        .fetch_max(entry.log_id.index + 1, Ordering::Release);
                }
                {
                    let mut ts = self.progress.term_start.lock().unwrap();
                    if entry.log_id.leader_id.term > ts.0 {
                        *ts = (entry.log_id.leader_id.term, entry.log_id.index + 1);
                    }
                }
                let index = entry.log_id.index;
                let bytes =
                    Arc::new(bincode::serialize(&entry).map_err(|e| io_err(ErrorVerb::Write, e))?);
                // An entry replaces whatever was at its index and after.
                st.remove_from(index);
                st.cached_bytes += bytes.len();
                st.entries.insert(index, Slot { bytes: Some(bytes.clone()), loc: None });
                records.push((KIND_ENTRY, index, bytes));
                last = Some(index);
            }
            self.stats.cached_bytes.store(st.cached_bytes as u64, Ordering::Relaxed);
        }
        let progress = self.progress.clone();
        self.send(Cmd {
            records,
            sync: true,
            drop_upto: None,
            done: Some(Box::new(move |r: io::Result<()>| {
                if r.is_ok() {
                    if let Some(last) = last {
                        progress.last_durable.fetch_max(last + 1, Ordering::Release);
                    }
                }
                callback.log_io_completed(r);
            })),
        })
        .map_err(|e| io_err(ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.state.lock().unwrap().remove_from(log_id.index);
        self.write_synced(vec![(KIND_TRUNCATE, log_id.index, Arc::new(Vec::new()))], None)
            .await
            .map_err(|e| io_err(ErrorVerb::Write, e))?;
        // Only a follower truncates; the local read barrier is for a
        // leader, but keep the value true.
        self.progress
            .last_durable
            .fetch_min(log_id.index, Ordering::Release);
        {
            let mut ts = self.progress.term_start.lock().unwrap();
            if ts.1 > log_id.index {
                *ts = (0, 0);
            }
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let hole = {
            let mut st = self.state.lock().unwrap();
            let last = st
                .entries
                .keys()
                .next_back()
                .copied()
                .or(st.last_purged.map(|p| p.index));
            st.remove_upto(log_id.index);
            if st.last_purged.is_none_or(|p| p.index < log_id.index) {
                st.last_purged = Some(log_id);
            }
            if st.pending_purge.is_none_or(|p| p.index < log_id.index) {
                st.pending_purge = Some(log_id);
            }
            last.is_none_or(|l| l < log_id.index)
        };
        let bytes = bincode::serialize(&log_id).map_err(|e| io_err(ErrorVerb::Write, e))?;
        self.mirror(META_LAST_PURGED, &bytes).await;
        if hole {
            // Past the last entry: the next append would follow a hole
            // that only this purge explains (a follower that installed
            // a snapshot). Make the data file durable at the snapshot,
            // then the purge, before anything is appended after it.
            self.engine.sync().await.map_err(|e| io_err(ErrorVerb::Write, e))?;
            self.complete_purge(log_id)
                .await
                .map_err(|e| io_err(ErrorVerb::Write, e))?;
        }
        Ok(())
    }
}

/// When the checkpointer makes the data file durable.
#[derive(Debug, Clone, Copy)]
pub struct CheckpointConfig {
    /// At most this long between checkpoints while entries are applied.
    pub interval: Duration,
    /// Or as soon as this many entries were applied since the last one.
    pub entries: u64,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self { interval: Duration::from_millis(100), entries: 10_000 }
    }
}

/// Make the data file durable in the background and let the WAL drop
/// what it no longer needs (see the module docs). `applied` is the state
/// machine's applied watch (`index + 1`), `data_file` the redb file.
pub fn spawn_checkpointer(
    store: WalLogStore,
    applied: watch::Receiver<u64>,
    data_file: PathBuf,
    cfg: CheckpointConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut applied = applied;
        let mut ticker = tokio::time::interval(cfg.interval.max(Duration::from_millis(1)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut durable: Option<u64> = None;
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                r = applied.changed() => {
                    if r.is_err() {
                        break;
                    }
                    let now = *applied.borrow_and_update();
                    let since = now.saturating_sub(durable.unwrap_or(0));
                    if since < cfg.entries.max(1) {
                        continue;
                    }
                }
            }
            let target = *applied.borrow_and_update();
            let purge = store.pending_purge();
            if durable == Some(target) && purge.is_none() {
                continue;
            }
            let t = Instant::now();
            // Write back what the non-durable commits left in the page
            // cache without holding redb's writer lock; the commit's own
            // fsync then has little left, so applies wait less on it.
            let file = data_file.clone();
            let _ = tokio::task::spawn_blocking(move || {
                std::fs::File::open(file).and_then(|f| f.sync_data())
            })
            .await;
            let locked = Instant::now();
            if let Err(e) = store.engine.sync().await {
                store.stats.checkpoint_failures.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(error = %e, "checkpoint of the data file failed; retrying");
                continue;
            }
            durable = Some(target);
            let took = t.elapsed().as_nanos() as u64;
            store.stats.checkpoints.fetch_add(1, Ordering::Relaxed);
            store.stats.checkpoint_nanos.fetch_add(took, Ordering::Relaxed);
            store.stats.checkpoint_max_nanos.fetch_max(took, Ordering::Relaxed);
            let commit = locked.elapsed().as_nanos() as u64;
            store.stats.checkpoint_commit_nanos.fetch_add(commit, Ordering::Relaxed);
            store.stats.checkpoint_commit_max_nanos.fetch_max(commit, Ordering::Relaxed);
            store.stats.durable_applied.store(target, Ordering::Relaxed);
            // openraft purges only applied entries, so a purge seen
            // before this checkpoint's applied index is covered by it.
            if let Some(p) = purge {
                if p.index < target {
                    if let Err(e) = store.complete_purge(p).await {
                        tracing::warn!(error = %e, "recording a raft log purge in the WAL");
                    }
                }
            }
        }
    })
}
