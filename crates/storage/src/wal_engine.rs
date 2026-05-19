//! `WalEngine` — second first-class KvStore implementation.
//!
//! Architecture:
//!
//! - **Append-only WAL** file holds the durable record of every
//!   batch. Each commit serializes its op list with bincode,
//!   length-prefixes it, writes to the WAL, and `fsync`s.
//! - **In-memory `BTreeMap<(table, key), value>`** is the index +
//!   value store. Reads serve from a cheap clone of this map.
//! - **On open**, the WAL is replayed in order to rebuild the
//!   in-memory state. Recovery is bounded by the WAL size; we add
//!   a checkpoint mechanism in a follow-up.
//!
//! ## Why this engine
//!
//! Single-writer semantics + group-committed WAL + in-memory index
//! is the architecture that delivers predictable p99: writes always
//! amount to "append a bounded blob + one fsync"; reads never touch
//! the disk. It's the architectural shape `io_uring` + `O_DIRECT`
//! + a per-core executor are useful for — the kernel I/O swap is a
//! future drop-in below this layer that does not change the
//! `KvStore` semantics.
//!
//! Behind cargo feature `wal-engine`. Default off; enable to
//! benchmark against redb side-by-side.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::kvstore::{
    BatchOp, KvStore, Snapshot, StorageError, StorageResult, WriteBatch, WriteOptions,
};

/// One persisted batch record on the WAL. Bincode-serialized,
/// length-prefixed (u32 BE).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalRecord {
    ops: Vec<BatchOpWire>,
}

/// Wire shape mirrors [`BatchOp`] but is decoupled so future on-disk
/// format changes don't require touching the public `WriteBatch` API.
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

/// In-memory store, keyed by `(table, user_key)`.
type Indexed = BTreeMap<(String, Vec<u8>), Vec<u8>>;

#[derive(Clone)]
pub struct WalEngine {
    inner: Arc<Inner>,
}

struct Inner {
    path: PathBuf,
    state: Mutex<State>,
}

struct State {
    /// In-memory index + values. Cloned per `snapshot()`.
    index: Indexed,
    /// Open WAL handle for append.
    wal: File,
    /// Approximate WAL byte size, for `size_on_disk`.
    wal_bytes: u64,
}

impl WalEngine {
    /// Open or create a WAL-backed engine at `path`. If `path`
    /// exists, replays the WAL to rebuild in-memory state.
    pub async fn open<P: AsRef<Path>>(path: P) -> StorageResult<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(StorageError::io)?;
        }
        // Open the WAL: create if missing.
        let mut wal = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .await
            .map_err(StorageError::io)?;

        // Read everything for replay.
        let metadata = wal.metadata().await.map_err(StorageError::io)?;
        let total = metadata.len();
        let mut buf: Vec<u8> = Vec::with_capacity(total as usize);
        // We need a separate read handle since `append` mode positions
        // writes at the end but we want to read from the start.
        let mut reader = OpenOptions::new()
            .read(true)
            .open(&path)
            .await
            .map_err(StorageError::io)?;
        reader.read_to_end(&mut buf).await.map_err(StorageError::io)?;
        drop(reader);

        let index = replay(&buf).map_err(|e| {
            StorageError::Misuse(format!("WAL replay failed at byte {e}"))
        })?;

        // Ensure we'll append fresh from current end.
        wal.seek(std::io::SeekFrom::End(0))
            .await
            .map_err(StorageError::io)?;

        Ok(Self {
            inner: Arc::new(Inner {
                path,
                state: Mutex::new(State {
                    index,
                    wal,
                    wal_bytes: total,
                }),
            }),
        })
    }
}

/// Replay the WAL bytes into an in-memory index. Returns the byte
/// offset where parsing failed (if any).
fn replay(bytes: &[u8]) -> Result<Indexed, usize> {
    let mut idx: Indexed = BTreeMap::new();
    let mut i = 0usize;
    while i + 4 <= bytes.len() {
        let len = u32::from_be_bytes(bytes[i..i + 4].try_into().expect("4 bytes")) as usize;
        i += 4;
        if i + len > bytes.len() {
            // Partial record (truncated). Stop replay here — accept
            // the bytes we have. A `fsync`'d engine never produces a
            // partial record at a clean shutdown, but a crash mid-
            // write can.
            return Ok(idx);
        }
        let record: WalRecord = match bincode::deserialize(&bytes[i..i + len]) {
            Ok(r) => r,
            Err(_) => return Err(i),
        };
        i += len;
        for op in record.ops {
            apply_to_index(&mut idx, op);
        }
    }
    Ok(idx)
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
            let to_remove: Vec<_> = idx
                .range(lo..hi)
                .map(|(k, _)| k.clone())
                .collect();
            for k in to_remove {
                idx.remove(&k);
            }
        }
    }
}

#[async_trait]
impl KvStore for WalEngine {
    async fn snapshot(&self) -> StorageResult<Arc<dyn Snapshot>> {
        let g = self.inner.state.lock().await;
        let snap = WalSnapshot {
            index: g.index.clone(),
        };
        Ok(Arc::new(snap))
    }

    async fn commit(&self, batch: WriteBatch, opts: WriteOptions) -> StorageResult<()> {
        let wire = WalRecord {
            ops: batch.ops().iter().map(BatchOpWire::from).collect(),
        };
        let body = bincode::serialize(&wire)
            .map_err(|e| StorageError::io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?;
        let mut framed = Vec::with_capacity(4 + body.len());
        framed.extend_from_slice(&(body.len() as u32).to_be_bytes());
        framed.extend_from_slice(&body);

        let mut g = self.inner.state.lock().await;
        g.wal.write_all(&framed).await.map_err(StorageError::io)?;
        if opts.sync {
            g.wal.sync_data().await.map_err(StorageError::io)?;
        }
        g.wal_bytes += framed.len() as u64;
        for op in batch.ops() {
            apply_to_index(&mut g.index, BatchOpWire::from(op));
        }
        Ok(())
    }

    async fn sync(&self) -> StorageResult<()> {
        let mut g = self.inner.state.lock().await;
        g.wal.sync_data().await.map_err(StorageError::io)?;
        Ok(())
    }

    async fn size_on_disk(&self) -> StorageResult<u64> {
        let g = self.inner.state.lock().await;
        Ok(g.wal_bytes)
    }

    fn engine_name(&self) -> &'static str {
        "wal"
    }

    async fn defragment(&self) -> StorageResult<()> {
        // Rewrite the WAL: replay the in-memory index into a fresh
        // file, then swap.
        let mut g = self.inner.state.lock().await;
        // Build a single WriteRecord containing one Put per current
        // (table, key, value).
        let ops: Vec<BatchOpWire> = g
            .index
            .iter()
            .map(|((t, k), v)| BatchOpWire::Put {
                table: t.clone(),
                key: k.clone(),
                value: v.clone(),
            })
            .collect();
        let wire = WalRecord { ops };
        let body = bincode::serialize(&wire).map_err(|e| {
            StorageError::io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })?;
        let mut framed = Vec::with_capacity(4 + body.len());
        framed.extend_from_slice(&(body.len() as u32).to_be_bytes());
        framed.extend_from_slice(&body);

        let tmp = self.inner.path.with_extension("compact-tmp");
        // Write the new file.
        let mut new_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&tmp)
            .await
            .map_err(StorageError::io)?;
        new_file
            .write_all(&framed)
            .await
            .map_err(StorageError::io)?;
        new_file.sync_data().await.map_err(StorageError::io)?;
        drop(new_file);

        // Atomically swap.
        tokio::fs::rename(&tmp, &self.inner.path)
            .await
            .map_err(StorageError::io)?;

        // Reopen the WAL handle pointing at the new file (append mode).
        let new_handle = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&self.inner.path)
            .await
            .map_err(StorageError::io)?;
        g.wal = new_handle;
        g.wal_bytes = framed.len() as u64;
        Ok(())
    }
}

struct WalSnapshot {
    index: Indexed,
}

#[async_trait]
impl Snapshot for WalSnapshot {
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
        let v = self.range(table, start, end, 0).await?;
        Ok(v.len() as u64)
    }
}

// Add the missing seek import via the AsyncSeekExt trait. Kept at
// the bottom so the rest of the file reads top-down.
use tokio::io::AsyncSeekExt;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kvstore::conformance;

    fn temp_wal() -> tempfile::TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    async fn open(dir: &tempfile::TempDir) -> WalEngine {
        WalEngine::open(dir.path().join("test.wal")).await.unwrap()
    }

    #[tokio::test]
    async fn engine_name_is_wal() {
        let d = temp_wal();
        let e = open(&d).await;
        assert_eq!(e.engine_name(), "wal");
    }

    #[tokio::test]
    async fn conformance_put_get_delete() {
        let d = temp_wal();
        let e = open(&d).await;
        conformance::put_get_delete(&e).await;
    }

    #[tokio::test]
    async fn conformance_range_scan() {
        let d = temp_wal();
        let e = open(&d).await;
        conformance::range_scan_lex_order(&e).await;
    }

    #[tokio::test]
    async fn conformance_delete_range() {
        let d = temp_wal();
        let e = open(&d).await;
        conformance::delete_range_is_atomic(&e).await;
    }

    #[tokio::test]
    async fn conformance_snapshot_isolation() {
        let d = temp_wal();
        let e = open(&d).await;
        conformance::snapshot_is_consistent_under_writes(&e).await;
    }

    #[tokio::test]
    async fn conformance_count() {
        let d = temp_wal();
        let e = open(&d).await;
        conformance::count_matches_range_len(&e).await;
    }

    #[tokio::test]
    async fn wal_persists_across_reopen() {
        let d = temp_wal();
        let path = d.path().join("persist.wal");
        {
            let e = WalEngine::open(&path).await.unwrap();
            let mut batch = WriteBatch::new();
            batch.put("kv", b"a", b"1");
            batch.put("kv", b"b", b"2");
            e.commit(batch, WriteOptions::default()).await.unwrap();
        }
        let e = WalEngine::open(&path).await.unwrap();
        let snap = e.snapshot().await.unwrap();
        assert_eq!(
            snap.get("kv", b"a").await.unwrap().as_deref(),
            Some(b"1".as_ref())
        );
        assert_eq!(
            snap.get("kv", b"b").await.unwrap().as_deref(),
            Some(b"2".as_ref())
        );
    }
}
