//! `TypeConfig::SnapshotData`: a snapshot body that lives in a file.
//!
//! It used to be `Cursor<Vec<u8>>`, so every snapshot passed through
//! RAM whole: the leader read the entire `.snap` into a `Vec` to send
//! it, and a follower collected every chunk into a `Vec` before
//! installing it (fastetcd#30). A [`SnapshotFile`] is instead one of:
//!
//! - **Retained**: a retained `.snap`, opened read-only. openraft reads
//!   it chunk by chunk to send it. The file is immutable once written,
//!   so nothing is pinned: compaction and defragment never wait on a
//!   transfer. If roll-off deletes the file mid-transfer, the open
//!   handle keeps reading it; its blocks are freed when the transfer
//!   ends.
//! - **Incoming**: a temp file in the snapshot directory that a
//!   follower writes received chunks into. Install renames it into
//!   place as the retained snapshot. If the transfer is abandoned the
//!   file is deleted when the handle is dropped (and any leftover is
//!   reclaimed on the next start). The live database is never touched
//!   until install.
//! - **Memory**: the fallback, the same `Cursor<Vec<u8>>` as before. Used
//!   only when the disk cannot hold the snapshot, so no failure path
//!   gets worse than it was.
//!
//! File I/O here is synchronous inside the poll functions, one chunk
//! (openraft's `snapshot_max_chunk_size`, 3 MiB) at a time. That is
//! deliberate. `tokio::fs::File` completes a write in the background
//! and reports a failure on a *later* call, after openraft has already
//! counted the bytes as written, so a full disk could silently lose a
//! chunk. A synchronous write fails on the chunk it failed on, which is
//! what lets a receive fall back to memory without losing data.

use std::fmt;
use std::fs::File;
use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};

use crate::snapshot_store::{is_out_of_space, SnapshotStore};

/// A snapshot body: a file on the data volume, or bytes in memory when
/// the volume cannot hold one. See the module docs.
pub struct SnapshotFile {
    body: Body,
    /// Result of the last `start_seek`, returned by `poll_complete`.
    seek_result: Option<io::Result<u64>>,
}

enum Body {
    Retained(File),
    Incoming(Incoming),
    Memory(Cursor<Vec<u8>>),
}

/// A snapshot being received into a temp file.
struct Incoming {
    file: File,
    /// `None` once install has taken ownership of the file.
    path: Option<PathBuf>,
    store: SnapshotStore,
    /// Whether the retained snapshots have already been discarded to
    /// make room for this one.
    discarded: bool,
}

impl Drop for Incoming {
    fn drop(&mut self) {
        // An abandoned transfer: the leader stepped down, the stream was
        // replaced by a newer snapshot, or install failed. On a full
        // volume this file holds exactly the space the next attempt
        // needs.
        if let Some(path) = self.path.take() {
            let _ = std::fs::remove_file(&path);
        }
    }
}

/// What install gets from a received (or otherwise supplied) snapshot.
pub(crate) enum Content {
    /// A received temp file, now owned by the caller: renamed into place
    /// by install, or deleted.
    Incoming { file: File, path: PathBuf },
    /// A retained snapshot's file, which install must not move or delete.
    Retained(File),
    Memory(Vec<u8>),
}

impl SnapshotFile {
    /// An in-memory snapshot body.
    pub fn memory(bytes: Vec<u8>) -> Self {
        Self::from_body(Body::Memory(Cursor::new(bytes)))
    }

    /// A retained snapshot opened for reading.
    pub(crate) fn retained(file: File) -> Self {
        Self::from_body(Body::Retained(file))
    }

    /// A temp file to receive a snapshot into, created by the store.
    pub(crate) fn incoming(file: File, path: PathBuf, store: SnapshotStore) -> Self {
        Self::from_body(Body::Incoming(Incoming {
            file,
            path: Some(path),
            store,
            discarded: false,
        }))
    }

    fn from_body(body: Body) -> Self {
        Self {
            body,
            seek_result: None,
        }
    }

    /// True if the body is held in memory rather than in a file.
    pub fn is_in_memory(&self) -> bool {
        matches!(self.body, Body::Memory(_))
    }

    /// Hand the body to install.
    pub(crate) fn into_content(self) -> Content {
        match self.body {
            Body::Retained(file) => Content::Retained(file),
            Body::Memory(cursor) => Content::Memory(cursor.into_inner()),
            Body::Incoming(mut incoming) => {
                let path = incoming.path.take().expect("path is taken only here");
                // `Incoming` has a Drop impl, so the file cannot be moved
                // out; a second handle to the same open file is.
                match incoming.file.try_clone() {
                    Ok(file) => Content::Incoming { file, path },
                    Err(e) => {
                        // Put the path back so Drop cleans the file up,
                        // and hand install a body it will fail to read
                        // with a clear error.
                        incoming.path = Some(path);
                        tracing::error!(
                            target: "fastetcd::snapshot",
                            error = %e,
                            "could not reopen the received snapshot"
                        );
                        Content::Memory(Vec::new())
                    }
                }
            }
        }
    }

    /// A write into the incoming file failed. Make room and retry once,
    /// then fall back to memory. A receive that errors here becomes a
    /// `StorageError` in openraft, and turning a full volume into a
    /// storage error is the deadlock fastetcd#14 fixed.
    fn recover_write(&mut self, err: io::Error, pos: u64, data: &[u8]) -> io::Result<()> {
        let Body::Incoming(incoming) = &mut self.body else {
            return Err(err);
        };
        if is_out_of_space(&err) && !incoming.discarded {
            incoming.discarded = true;
            tracing::warn!(
                target: "fastetcd::snapshot",
                error = %err,
                "no space to receive the snapshot — discarding every retained \
                 snapshot and retrying"
            );
            incoming.store.discard_all();
            let retried = incoming
                .file
                .seek(SeekFrom::Start(pos))
                .and_then(|_| incoming.file.write_all(data));
            match retried {
                Ok(()) => return Ok(()),
                Err(e) => return self.fall_back_to_memory(e, pos, data),
            }
        }
        self.fall_back_to_memory(err, pos, data)
    }

    /// Move what has been received so far into memory and continue
    /// there. Writes are synchronous, so the file holds every chunk
    /// openraft was told was written.
    fn fall_back_to_memory(&mut self, err: io::Error, pos: u64, data: &[u8]) -> io::Result<()> {
        let Body::Incoming(incoming) = &mut self.body else {
            return Err(err);
        };
        let mut bytes = Vec::new();
        let read_back = incoming
            .file
            .seek(SeekFrom::Start(0))
            .and_then(|_| incoming.file.read_to_end(&mut bytes));
        if let Err(e) = read_back {
            tracing::error!(
                target: "fastetcd::snapshot",
                write_error = %err,
                read_error = %e,
                "could not write the incoming snapshot, nor read it back to \
                 continue in memory"
            );
            return Err(err);
        }
        tracing::warn!(
            target: "fastetcd::snapshot",
            error = %err,
            received_bytes = bytes.len(),
            "cannot write the incoming snapshot to disk — continuing in memory. \
             Free space on the data volume."
        );
        let mut cursor = Cursor::new(bytes);
        cursor.set_position(pos);
        cursor.write_all(data)?;
        // Dropping the incoming handle deletes the temp file, freeing
        // its space.
        self.body = Body::Memory(cursor);
        Ok(())
    }

    fn position(&mut self) -> io::Result<u64> {
        match &mut self.body {
            Body::Retained(f) => f.stream_position(),
            Body::Incoming(i) => i.file.stream_position(),
            Body::Memory(c) => Ok(c.position()),
        }
    }
}

impl fmt::Debug for SnapshotFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.body {
            Body::Retained(_) => f.write_str("SnapshotFile::Retained"),
            Body::Incoming(i) => write!(f, "SnapshotFile::Incoming({:?})", i.path),
            Body::Memory(c) => write!(f, "SnapshotFile::Memory({} bytes)", c.get_ref().len()),
        }
    }
}

fn read_sync(r: &mut impl Read, buf: &mut ReadBuf<'_>) -> io::Result<()> {
    loop {
        match r.read(buf.initialize_unfilled()) {
            Ok(n) => {
                buf.advance(n);
                return Ok(());
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
}

impl AsyncRead for SnapshotFile {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match &mut this.body {
            Body::Retained(f) => Poll::Ready(read_sync(f, buf)),
            Body::Incoming(i) => Poll::Ready(read_sync(&mut i.file, buf)),
            Body::Memory(c) => Pin::new(c).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for SnapshotFile {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match &mut this.body {
            Body::Memory(c) => Pin::new(c).poll_write(cx, data),
            Body::Retained(_) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "a retained snapshot is read-only",
            ))),
            Body::Incoming(i) => {
                let pos = match i.file.stream_position() {
                    Ok(p) => p,
                    Err(e) => return Poll::Ready(Err(e)),
                };
                let written = match i.file.write_all(data) {
                    Ok(()) => Ok(()),
                    Err(e) => this.recover_write(e, pos, data),
                };
                Poll::Ready(written.map(|()| data.len()))
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Poll::Ready(match &mut this.body {
            Body::Incoming(i) => i.file.flush(),
            Body::Retained(_) | Body::Memory(_) => Ok(()),
        })
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Durability (fsync) is install's job, once, before the rename.
        self.poll_flush(cx)
    }
}

impl AsyncSeek for SnapshotFile {
    fn start_seek(self: Pin<&mut Self>, pos: SeekFrom) -> io::Result<()> {
        let this = self.get_mut();
        let result = match &mut this.body {
            Body::Retained(f) => f.seek(pos),
            Body::Incoming(i) => i.file.seek(pos),
            Body::Memory(c) => Seek::seek(c, pos),
        };
        this.seek_result = Some(result);
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        let this = self.get_mut();
        Poll::Ready(match this.seek_result.take() {
            Some(result) => result,
            None => this.position(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

    /// A write the incoming file refuses must not fail the receive: what
    /// was received so far moves to memory and the transfer carries on,
    /// and the temp file is released.
    #[tokio::test]
    async fn a_failed_write_falls_back_to_memory_without_losing_data() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path(), 1).unwrap();
        let (mut file, path) = store.begin_incoming().unwrap();
        file.write_all(b"hello").unwrap();
        drop(file);
        // Reopen read-only, so the next write fails (EBADF), the same
        // way a write on a full volume does.
        let read_only = File::open(&path).unwrap();
        let mut body = SnapshotFile::incoming(read_only, path.clone(), store);

        body.seek(SeekFrom::Start(5)).await.unwrap();
        body.write_all(b" world").await.unwrap();
        assert!(body.is_in_memory());
        assert!(!path.exists(), "the temp file is released");

        let mut all = Vec::new();
        body.seek(SeekFrom::Start(0)).await.unwrap();
        body.read_to_end(&mut all).await.unwrap();
        assert_eq!(all, b"hello world");
    }

    #[tokio::test]
    async fn a_retained_snapshot_is_read_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.snap");
        std::fs::write(&path, b"abc").unwrap();
        let mut body = SnapshotFile::retained(File::open(&path).unwrap());
        assert!(body.write_all(b"x").await.is_err());
        let mut all = Vec::new();
        body.read_to_end(&mut all).await.unwrap();
        assert_eq!(all, b"abc");
    }
}
