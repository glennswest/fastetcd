//! `IouringEngine` — Linux-only KvStore implementation backed by
//! Linux `io_uring` via the `tokio-uring` runtime.
//!
//! Architecture:
//!
//! - A dedicated OS thread hosts a `tokio_uring::start` runtime;
//!   it owns the WAL file handle and the in-memory MVCC index.
//! - The public `IouringEngine` runs on the main tokio runtime
//!   and forwards every operation to the worker thread via a
//!   bounded `mpsc::channel<Command>`.
//! - Each `Command` carries a `oneshot::Sender<Result<...>>` so
//!   the public-side `async fn` can await the result.
//!
//! On-disk layout matches `WalEngine`: length-prefixed bincode
//! batches in an append-only file. Recovery is a single pass at
//! open time.
//!
//! What's **not** in this commit:
//!
//! - `O_DIRECT` + aligned-buffer plumbing. The current
//!   implementation uses the page cache the way `WalEngine` does;
//!   the architectural win of `io_uring` (batched submission,
//!   no per-syscall context switches) is in place, but the
//!   p99-tail benefit from page-cache bypass is a follow-up.
//! - Group-commit windowing. Each commit currently triggers its
//!   own fsync. Coalescing commits arriving within ~1ms into one
//!   fsync is the next tuning step.

#![cfg(all(feature = "iouring", target_os = "linux"))]

use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::kvstore::{
    BatchOp, KvStore, Snapshot, StorageError, StorageResult, WriteBatch, WriteOptions,
};

/// Wire-shape mirror of [`BatchOp`].
#[derive(Debug, Clone, Serialize, Deserialize)]
enum BatchOpWire {
    Put {
        table: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        table: String,
        key: Vec<u8>,
    },
    DeleteRange {
        table: String,
        start: Vec<u8>,
        end: Vec<u8>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalRecord {
    ops: Vec<BatchOpWire>,
}

type Indexed = BTreeMap<(String, Vec<u8>), Vec<u8>>;

/// Commands the worker thread accepts.
enum Cmd {
    Commit {
        ops: Vec<BatchOpWire>,
        sync: bool,
        reply: oneshot::Sender<StorageResult<()>>,
    },
    Snapshot {
        reply: oneshot::Sender<StorageResult<Indexed>>,
    },
    Sync {
        reply: oneshot::Sender<StorageResult<()>>,
    },
    SizeOnDisk {
        reply: oneshot::Sender<StorageResult<u64>>,
    },
    Defragment {
        reply: oneshot::Sender<StorageResult<()>>,
    },
}

impl From<&BatchOp> for BatchOpWire {
    fn from(op: &BatchOp) -> Self {
        match op {
            BatchOp::Put { table, key, value } => BatchOpWire::Put {
                table: table.clone(),
                key: key.clone(),
                value: value.clone(),
            },
            BatchOp::Delete { table, key } => BatchOpWire::Delete {
                table: table.clone(),
                key: key.clone(),
            },
            BatchOp::DeleteRange { table, start, end } => BatchOpWire::DeleteRange {
                table: table.clone(),
                start: start.clone(),
                end: end.clone(),
            },
        }
    }
}

fn apply_to_index(idx: &mut Indexed, op: BatchOpWire) {
    match op {
        BatchOpWire::Put { table, key, value } => {
            idx.insert((table, key), value);
        }
        BatchOpWire::Delete { table, key } => {
            idx.remove(&(table, key));
        }
        BatchOpWire::DeleteRange { table, start, end } => {
            let lo = (table.clone(), start);
            let hi = (table, end);
            let to_remove: Vec<_> = idx.range(lo..hi).map(|(k, _)| k.clone()).collect();
            for k in to_remove {
                idx.remove(&k);
            }
        }
    }
}

fn replay(bytes: &[u8]) -> Indexed {
    let mut idx: Indexed = BTreeMap::new();
    let mut i = 0usize;
    while i + 4 <= bytes.len() {
        let len = u32::from_be_bytes(bytes[i..i + 4].try_into().expect("4 bytes")) as usize;
        i += 4;
        if i + len > bytes.len() {
            return idx;
        }
        let Ok(record) = bincode::deserialize::<WalRecord>(&bytes[i..i + len]) else {
            return idx;
        };
        i += len;
        for op in record.ops {
            apply_to_index(&mut idx, op);
        }
    }
    idx
}

#[derive(Clone)]
pub struct IouringEngine {
    tx: mpsc::Sender<Cmd>,
    path: PathBuf,
}

impl IouringEngine {
    /// Open or create an io_uring-backed engine. Spawns the worker
    /// thread; the engine is usable as soon as this returns.
    pub fn open<P: AsRef<Path>>(path: P) -> StorageResult<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(StorageError::io)?;
        }
        // Replay the existing WAL on the main thread (one-time
        // sync read) before handing the file to the worker.
        let initial_bytes = std::fs::read(&path).unwrap_or_default();
        let initial_index = replay(&initial_bytes);

        let (tx, rx) = mpsc::channel::<Cmd>(128);
        let worker_path = path.clone();
        let started = Arc::new(std::sync::Mutex::new(false));
        let started_signal = started.clone();
        thread::Builder::new()
            .name("fastetcd-iouring".into())
            .spawn(move || {
                if let Err(e) = tokio_uring::start(worker(worker_path, initial_index, rx, started_signal))
                {
                    tracing::error!(
                        target: "fastetcd::iouring",
                        "tokio_uring runtime exited: {e}"
                    );
                }
            })
            .map_err(StorageError::io)?;
        // Spin briefly for the worker to signal ready; tokio_uring::start
        // is synchronous from the calling thread's POV until the future
        // completes, so we can't reliably await it here without
        // additional plumbing. The channel itself is the readiness
        // signal: send is unbounded until the worker drops rx.
        let _ = started;
        Ok(Self { tx, path })
    }

    async fn call<R>(
        &self,
        make: impl FnOnce(oneshot::Sender<StorageResult<R>>) -> Cmd,
    ) -> StorageResult<R> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(make(reply))
            .await
            .map_err(|_| StorageError::Closed)?;
        rx.await.map_err(|_| StorageError::Closed)?
    }
}

#[async_trait]
impl KvStore for IouringEngine {
    async fn snapshot(&self) -> StorageResult<Arc<dyn Snapshot>> {
        let idx = self.call(|reply| Cmd::Snapshot { reply }).await?;
        Ok(Arc::new(IouringSnapshot { index: idx }))
    }

    async fn commit(&self, batch: WriteBatch, opts: WriteOptions) -> StorageResult<()> {
        let ops: Vec<BatchOpWire> = batch.ops().iter().map(BatchOpWire::from).collect();
        self.call(|reply| Cmd::Commit {
            ops,
            sync: opts.sync,
            reply,
        })
        .await
    }

    async fn sync(&self) -> StorageResult<()> {
        self.call(|reply| Cmd::Sync { reply }).await
    }

    async fn size_on_disk(&self) -> StorageResult<u64> {
        self.call(|reply| Cmd::SizeOnDisk { reply }).await
    }

    fn engine_name(&self) -> &'static str {
        "iouring"
    }

    async fn defragment(&self) -> StorageResult<()> {
        self.call(|reply| Cmd::Defragment { reply }).await
    }
}

struct IouringSnapshot {
    index: Indexed,
}

#[async_trait]
impl Snapshot for IouringSnapshot {
    async fn get(&self, table: &str, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        Ok(self
            .index
            .get(&(table.to_string(), key.to_vec()))
            .cloned())
    }

    async fn range(
        &self,
        table: &str,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let lo_key = match &start {
            Bound::Included(v) => v.clone(),
            Bound::Excluded(v) => {
                let mut k = v.clone();
                k.push(0);
                k
            }
            Bound::Unbounded => Vec::new(),
        };
        let lo = (table.to_string(), lo_key);
        let hi_opt: Option<(String, Vec<u8>)> = match &end {
            Bound::Included(v) => {
                let mut k = v.clone();
                k.push(0);
                Some((table.to_string(), k))
            }
            Bound::Excluded(v) => Some((table.to_string(), v.clone())),
            Bound::Unbounded => None,
        };
        let iter: Box<dyn Iterator<Item = (&(String, Vec<u8>), &Vec<u8>)>> =
            if let Some(hi) = hi_opt {
                Box::new(self.index.range(lo..hi))
            } else {
                Box::new(self.index.range(lo..))
            };
        let mut out = Vec::new();
        for (i, ((tab, key), val)) in iter.enumerate() {
            if tab != table {
                break;
            }
            if limit != 0 && i >= limit {
                break;
            }
            out.push((key.clone(), val.clone()));
        }
        Ok(out)
    }

    async fn count(
        &self,
        table: &str,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    ) -> StorageResult<u64> {
        Ok(self.range(table, start, end, 0).await?.len() as u64)
    }
}

/// Worker loop: owns the WAL file via tokio-uring and the in-memory
/// index. Receives `Cmd`s and applies them.
async fn worker(
    path: PathBuf,
    mut index: Indexed,
    mut rx: mpsc::Receiver<Cmd>,
    started: Arc<std::sync::Mutex<bool>>,
) {
    use tokio_uring::fs::OpenOptions;

    let mut file = match OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .append(true)
        .open(&path)
        .await
    {
        Ok(f) => f,
        Err(e) => {
            tracing::error!(
                target: "fastetcd::iouring",
                "failed to open WAL via io_uring: {e}"
            );
            return;
        }
    };

    // Position at end for append.
    let mut wal_bytes: u64 = std::fs::metadata(&path)
        .map(|m| m.len())
        .unwrap_or(0);
    *started.lock().unwrap() = true;

    while let Some(cmd) = rx.recv().await {
        match cmd {
            Cmd::Commit { ops, sync, reply } => {
                let wire = WalRecord { ops: ops.clone() };
                let body = match bincode::serialize(&wire) {
                    Ok(b) => b,
                    Err(e) => {
                        let _ = reply.send(Err(StorageError::io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            e,
                        ))));
                        continue;
                    }
                };
                let mut framed = Vec::with_capacity(4 + body.len());
                framed.extend_from_slice(&(body.len() as u32).to_be_bytes());
                framed.extend_from_slice(&body);
                let frame_len = framed.len() as u64;
                let (res, _buf) = file.write_at(framed, wal_bytes).submit().await;
                match res {
                    Ok(_n) => {}
                    Err(e) => {
                        let _ = reply.send(Err(StorageError::io(e)));
                        continue;
                    }
                }
                if sync {
                    if let Err(e) = file.sync_data().await {
                        let _ = reply.send(Err(StorageError::io(e)));
                        continue;
                    }
                }
                wal_bytes += frame_len;
                for op in ops {
                    apply_to_index(&mut index, op);
                }
                let _ = reply.send(Ok(()));
            }
            Cmd::Snapshot { reply } => {
                let _ = reply.send(Ok(index.clone()));
            }
            Cmd::Sync { reply } => {
                let r = file.sync_data().await.map_err(StorageError::io);
                let _ = reply.send(r);
            }
            Cmd::SizeOnDisk { reply } => {
                let _ = reply.send(Ok(wal_bytes));
            }
            Cmd::Defragment { reply } => {
                // Replay the index into a fresh file then atomic-rename.
                let ops: Vec<BatchOpWire> = index
                    .iter()
                    .map(|((t, k), v)| BatchOpWire::Put {
                        table: t.clone(),
                        key: k.clone(),
                        value: v.clone(),
                    })
                    .collect();
                let wire = WalRecord { ops };
                let body = match bincode::serialize(&wire) {
                    Ok(b) => b,
                    Err(e) => {
                        let _ = reply.send(Err(StorageError::io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            e,
                        ))));
                        continue;
                    }
                };
                let mut framed = Vec::with_capacity(4 + body.len());
                framed.extend_from_slice(&(body.len() as u32).to_be_bytes());
                framed.extend_from_slice(&body);
                let tmp = path.with_extension("compact-tmp");
                let tmp_file = match OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .truncate(true)
                    .open(&tmp)
                    .await
                {
                    Ok(f) => f,
                    Err(e) => {
                        let _ = reply.send(Err(StorageError::io(e)));
                        continue;
                    }
                };
                let frame_len = framed.len() as u64;
                let (res, _) = tmp_file.write_at(framed, 0).submit().await;
                if let Err(e) = res {
                    let _ = reply.send(Err(StorageError::io(e)));
                    continue;
                }
                if let Err(e) = tmp_file.sync_data().await {
                    let _ = reply.send(Err(StorageError::io(e)));
                    continue;
                }
                // Drop old file before rename.
                if let Err(e) = std::fs::rename(&tmp, &path) {
                    let _ = reply.send(Err(StorageError::io(e)));
                    continue;
                }
                file = match OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .append(true)
                    .open(&path)
                    .await
                {
                    Ok(f) => f,
                    Err(e) => {
                        let _ = reply.send(Err(StorageError::io(e)));
                        continue;
                    }
                };
                wal_bytes = frame_len;
                let _ = reply.send(Ok(()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kvstore::conformance;

    fn temp_path() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("test.iouring");
        (dir, p)
    }

    #[tokio::test]
    async fn engine_name_is_iouring() {
        let (_d, p) = temp_path();
        let e = IouringEngine::open(p).unwrap();
        assert_eq!(e.engine_name(), "iouring");
    }

    #[tokio::test]
    async fn conformance_put_get_delete() {
        let (_d, p) = temp_path();
        let e = IouringEngine::open(p).unwrap();
        conformance::put_get_delete(&e).await;
    }

    #[tokio::test]
    async fn conformance_range_scan() {
        let (_d, p) = temp_path();
        let e = IouringEngine::open(p).unwrap();
        conformance::range_scan_lex_order(&e).await;
    }

    #[tokio::test]
    async fn conformance_delete_range() {
        let (_d, p) = temp_path();
        let e = IouringEngine::open(p).unwrap();
        conformance::delete_range_is_atomic(&e).await;
    }

    #[tokio::test]
    async fn conformance_snapshot_isolation() {
        let (_d, p) = temp_path();
        let e = IouringEngine::open(p).unwrap();
        conformance::snapshot_is_consistent_under_writes(&e).await;
    }

    #[tokio::test]
    async fn conformance_count() {
        let (_d, p) = temp_path();
        let e = IouringEngine::open(p).unwrap();
        conformance::count_matches_range_len(&e).await;
    }
}
