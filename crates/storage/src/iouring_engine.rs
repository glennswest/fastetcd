//! `io_uring` implementation of the [`KvStore`](crate::kvstore::KvStore)
//! trait, Linux-only, behind cargo feature `iouring`.
//!
//! Planned shape:
//!   - `glommio` thread-per-core runtime hosts a dedicated I/O reactor.
//!   - Group-committed WAL (append-only segments) holds the durable record
//!     of every write; writes batch into a single `O_DIRECT` write per
//!     group commit window.
//!   - In-memory MVCC index serves reads from a copy-on-write tree.
//!   - Compaction reclaims WAL space by writing condensed segments.
//!
//! This file currently contains the skeleton; calls into the engine
//! return [`StorageError::Misuse`] until the implementation lands.
//! The point of including it now is to keep the trait surface honest:
//! anything we add to [`KvStore`](crate::kvstore::KvStore) must be
//! implementable by both engines.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;

use crate::kvstore::{KvStore, Snapshot, StorageError, StorageResult, WriteBatch, WriteOptions};

/// io_uring-backed engine. Linux-only.
///
/// Behind cargo feature `iouring`; on non-Linux builds the feature is
/// not enabled and this module is not compiled.
#[derive(Clone)]
pub struct IoUringEngine {
    _inner: Arc<()>,
}

impl IoUringEngine {
    /// Open or create an io_uring-backed engine at `path`. Currently
    /// returns an error indicating the engine is not yet implemented.
    pub fn open<P: AsRef<Path>>(_path: P) -> StorageResult<Self> {
        Err(StorageError::Misuse(
            "iouring engine not yet implemented; tracked as task #15".into(),
        ))
    }
}

#[async_trait]
impl KvStore for IoUringEngine {
    async fn snapshot(&self) -> StorageResult<Arc<dyn Snapshot>> {
        Err(StorageError::Misuse("iouring engine not yet implemented".into()))
    }

    async fn commit(&self, _batch: WriteBatch, _opts: WriteOptions) -> StorageResult<()> {
        Err(StorageError::Misuse("iouring engine not yet implemented".into()))
    }

    async fn sync(&self) -> StorageResult<()> {
        Err(StorageError::Misuse("iouring engine not yet implemented".into()))
    }

    async fn size_on_disk(&self) -> StorageResult<u64> {
        Ok(0)
    }

    fn engine_name(&self) -> &'static str {
        "iouring"
    }
}
