//! A sequential, segmented write-ahead log of byte records (fastetcd#85).
//!
//! This is the raft log's durable form: `fastetcd-raft`'s
//! `WalLogStore` puts openraft entries, the vote, the committed id and
//! purges through it. Nothing here knows openraft's types; a record is
//! a kind, an index and opaque bytes.
//!
//! ## Layout
//!
//! `<dir>/<seq:016x>.wal`, one file per segment, numbered from 0.
//! A new segment is preallocated (`fallocate` on Linux) to
//! [`WalOptions::segment_bytes`], so an `fdatasync` after an append
//! writes the data and not the file's growing size. Every write lands
//! at the end of the last segment: on a spinning disk the fsync on a
//! client's path is a sequential write.
//!
//! A record is
//!
//! ```text
//! len u32 LE | crc32c u32 LE | kind u8 | index u64 LE | payload (len bytes)
//! ```
//!
//! The CRC covers kind, index and payload. A header of zeros (kind 0)
//! is the end of a segment's data: preallocated space reads as zeros.
//!
//! ## Meaning (applied in order on open, see [`Replay`])
//!
//! - [`KIND_ENTRY`] at `i`: the entry at `i`; any entry at `>= i` before
//!   it is gone (an append after a conflict replaces the tail).
//! - [`KIND_TRUNCATE`] at `i`: every entry at `>= i` is gone.
//! - [`KIND_PURGE`] at `i`: every entry at `<= i` is gone (payload kept).
//! - [`KIND_VOTE`], [`KIND_COMMITTED`]: the latest payload wins.
//!
//! Purge, vote and committed are *sticky*: each new segment starts with
//! their current values, so a prefix of segments can be deleted
//! ([`Wal::drop_segments_upto`]) without losing them.
//!
//! ## Damage
//!
//! A bad record (CRC, length, kind) in the **last** segment is a torn
//! tail from a crash in the middle of a write that was never
//! acknowledged: the segment is cut there and preallocated again, so a
//! later write can never be followed by a stale record. A bad record in
//! an earlier segment was synced before that segment was closed, so it
//! is real damage and [`Wal::open`] refuses.

use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// An openraft log entry.
pub const KIND_ENTRY: u8 = 1;
/// Remove every entry at `>= index`.
pub const KIND_TRUNCATE: u8 = 2;
/// Remove every entry at `<= index`; the payload is the purged log id.
pub const KIND_PURGE: u8 = 3;
/// The saved vote.
pub const KIND_VOTE: u8 = 4;
/// The committed log id.
pub const KIND_COMMITTED: u8 = 5;

const STICKY: [u8; 3] = [KIND_PURGE, KIND_VOTE, KIND_COMMITTED];

/// Bytes before a record's payload.
pub const HEADER_LEN: u64 = 4 + 4 + 1 + 8;
const MAX_PAYLOAD: u32 = 1 << 30;

/// Default segment size: small enough for the 512 MiB volumes small
/// clusters run on (#14), large enough that a roll is rare.
pub const DEFAULT_SEGMENT_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub struct WalOptions {
    /// Size each new segment is preallocated to; a segment past it is
    /// closed before the next write.
    pub segment_bytes: u64,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self { segment_bytes: DEFAULT_SEGMENT_BYTES }
    }
}

/// Where a record's payload is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Loc {
    pub segment: u64,
    /// Offset of the record's header in the segment.
    pub offset: u64,
    /// Payload length.
    pub len: u32,
}

/// What the log holds, from [`Wal::open`].
#[derive(Debug, Default)]
pub struct Replay {
    /// Entries left after truncations and purges, by index.
    pub entries: BTreeMap<u64, Loc>,
    /// Latest `(index, payload)` of each sticky kind present.
    pub meta: BTreeMap<u8, (u64, Vec<u8>)>,
    /// A torn tail was cut off the last segment.
    pub torn_tail: bool,
}

// ---- CRC-32C (Castagnoli), table-driven -------------------------------

const fn crc_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 { 0x82F6_3B78 ^ (c >> 1) } else { c >> 1 };
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
}

static CRC_TABLE: [u32; 256] = crc_table();

fn crc32c(parts: &[&[u8]]) -> u32 {
    let mut c = !0u32;
    for part in parts {
        for &b in *part {
            c = CRC_TABLE[((c ^ b as u32) & 0xff) as usize] ^ (c >> 8);
        }
    }
    !c
}

fn encode_record(out: &mut Vec<u8>, kind: u8, index: u64, payload: &[u8]) {
    let idx = index.to_le_bytes();
    let crc = crc32c(&[&[kind], &idx, payload]);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.push(kind);
    out.extend_from_slice(&idx);
    out.extend_from_slice(payload);
}

enum Parsed<'a> {
    Record { kind: u8, index: u64, payload: &'a [u8], next: usize },
    End,
    Bad(&'static str),
}

fn parse(buf: &[u8], at: usize) -> Parsed<'_> {
    let h = HEADER_LEN as usize;
    if at >= buf.len() {
        return Parsed::End;
    }
    if buf.len() - at < h {
        return if buf[at..].iter().all(|b| *b == 0) {
            Parsed::End
        } else {
            Parsed::Bad("short header")
        };
    }
    let header = &buf[at..at + h];
    if header.iter().all(|b| *b == 0) {
        return Parsed::End;
    }
    let len = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let crc = u32::from_le_bytes(header[4..8].try_into().unwrap());
    let kind = header[8];
    let index = u64::from_le_bytes(header[9..17].try_into().unwrap());
    if !(KIND_ENTRY..=KIND_COMMITTED).contains(&kind) {
        return Parsed::Bad("unknown record kind");
    }
    if len > MAX_PAYLOAD || buf.len() - at - h < len as usize {
        return Parsed::Bad("record runs past the end of the segment");
    }
    let payload = &buf[at + h..at + h + len as usize];
    if crc32c(&[&[kind], &index.to_le_bytes(), payload]) != crc {
        return Parsed::Bad("checksum mismatch");
    }
    Parsed::Record { kind, index, payload, next: at + h + len as usize }
}

// ---- files ------------------------------------------------------------

fn segment_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{seq:016x}.wal"))
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Reserve `[0, len)` of `file` so later writes allocate nothing. Best
/// effort: without it (a full disk, a filesystem without `fallocate`)
/// the segment grows as it is written, which is correct, just slower.
fn preallocate(file: &File, len: u64) {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        // SAFETY: a valid open descriptor and in-range lengths.
        let rc = unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, len as libc::off_t) };
        if rc != 0 {
            tracing::warn!(
                error = %io::Error::last_os_error(),
                "could not preallocate a WAL segment; it grows as it is written"
            );
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (file, len);
    }
}

fn create_segment(dir: &Path, seq: u64, bytes: u64) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(segment_path(dir, seq))?;
    preallocate(&file, bytes);
    file.sync_all()?;
    sync_dir(dir)?;
    Ok(file)
}

fn list_segments(dir: &Path) -> io::Result<Vec<u64>> {
    let mut seqs = Vec::new();
    for e in std::fs::read_dir(dir)? {
        let name = e?.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(stem) = name.strip_suffix(".wal") else { continue };
        if stem.len() == 16 {
            if let Ok(seq) = u64::from_str_radix(stem, 16) {
                seqs.push(seq);
            }
        }
    }
    seqs.sort_unstable();
    Ok(seqs)
}

/// Reads payloads by [`Loc`], from any thread, while the [`Wal`] writes.
pub struct WalReader {
    dir: PathBuf,
    files: Mutex<HashMap<u64, Arc<File>>>,
}

impl WalReader {
    fn new(dir: PathBuf) -> Self {
        Self { dir, files: Mutex::new(HashMap::new()) }
    }

    fn file(&self, seq: u64) -> io::Result<Arc<File>> {
        let mut files = self.files.lock().unwrap();
        if let Some(f) = files.get(&seq) {
            return Ok(f.clone());
        }
        let f = Arc::new(File::open(segment_path(&self.dir, seq))?);
        files.insert(seq, f.clone());
        Ok(f)
    }

    fn forget(&self, seq: u64) {
        self.files.lock().unwrap().remove(&seq);
    }

    /// The payload at `loc`, its checksum verified.
    pub fn read(&self, loc: Loc) -> io::Result<Vec<u8>> {
        let file = self.file(loc.segment)?;
        let mut buf = vec![0u8; HEADER_LEN as usize + loc.len as usize];
        file.read_exact_at(&mut buf, loc.offset)?;
        match parse(&buf, 0) {
            Parsed::Record { payload, .. } if payload.len() == loc.len as usize => {
                Ok(payload.to_vec())
            }
            Parsed::Bad(why) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("WAL segment {:016x} at {}: {why}", loc.segment, loc.offset),
            )),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("WAL segment {:016x} at {}: no record", loc.segment, loc.offset),
            )),
        }
    }
}

struct Segment {
    seq: u64,
    /// Highest entry index written to it, if any.
    max_entry: Option<u64>,
}

/// The writer. One owner (the log store's writer thread) appends,
/// syncs and drops segments; [`WalReader`]s read concurrently.
pub struct Wal {
    dir: PathBuf,
    opts: WalOptions,
    segments: Vec<Segment>,
    file: File,
    /// Write position in the last segment.
    offset: u64,
    /// Current value of each sticky kind, written at each new segment.
    sticky: BTreeMap<u8, (u64, Vec<u8>)>,
    reader: Arc<WalReader>,
}

impl Wal {
    /// Open (or create) the log in `dir` and replay it.
    pub fn open(dir: &Path, opts: WalOptions) -> io::Result<(Wal, Replay)> {
        let opts = WalOptions { segment_bytes: opts.segment_bytes.max(4096) };
        std::fs::create_dir_all(dir)?;
        let seqs = list_segments(dir)?;
        let reader = Arc::new(WalReader::new(dir.to_path_buf()));
        let mut replay = Replay::default();
        if seqs.is_empty() {
            let file = create_segment(dir, 0, opts.segment_bytes)?;
            let wal = Wal {
                dir: dir.to_path_buf(),
                opts,
                segments: vec![Segment { seq: 0, max_entry: None }],
                file,
                offset: 0,
                sticky: BTreeMap::new(),
                reader,
            };
            return Ok((wal, replay));
        }

        let mut segments = Vec::with_capacity(seqs.len());
        let mut end_of_last = 0u64;
        for (n, &seq) in seqs.iter().enumerate() {
            let last = n + 1 == seqs.len();
            let path = segment_path(dir, seq);
            let buf = std::fs::read(&path)?;
            let mut at = 0usize;
            let mut max_entry = None;
            loop {
                match parse(&buf, at) {
                    Parsed::Record { kind, index, payload, next } => {
                        let loc = Loc { segment: seq, offset: at as u64, len: payload.len() as u32 };
                        match kind {
                            KIND_ENTRY => {
                                let purged = replay.meta.get(&KIND_PURGE).map(|(i, _)| *i);
                                let _ = replay.entries.split_off(&index);
                                if purged.is_none_or(|p| index > p) {
                                    replay.entries.insert(index, loc);
                                }
                                max_entry = Some(max_entry.map_or(index, |m: u64| m.max(index)));
                            }
                            KIND_TRUNCATE => {
                                let _ = replay.entries.split_off(&index);
                            }
                            KIND_PURGE => {
                                replay.entries = replay.entries.split_off(&(index.saturating_add(1)));
                                if index == u64::MAX {
                                    replay.entries.clear();
                                }
                                replay.meta.insert(kind, (index, payload.to_vec()));
                            }
                            _ => {
                                replay.meta.insert(kind, (index, payload.to_vec()));
                            }
                        }
                        at = next;
                    }
                    Parsed::End => break,
                    Parsed::Bad(why) if last => {
                        tracing::warn!(
                            segment = %path.display(),
                            offset = at,
                            why,
                            "WAL: cutting a torn tail (a write the crash interrupted)"
                        );
                        let f = OpenOptions::new().write(true).open(&path)?;
                        f.set_len(at as u64)?;
                        preallocate(&f, opts.segment_bytes.max(at as u64));
                        f.sync_all()?;
                        replay.torn_tail = true;
                        break;
                    }
                    Parsed::Bad(why) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "WAL segment {} is damaged at offset {at} ({why}), and it is \
                                 not the last segment: this is not a torn write",
                                path.display()
                            ),
                        ));
                    }
                }
            }
            if last {
                end_of_last = at as u64;
            }
            segments.push(Segment { seq, max_entry });
        }
        let last_seq = segments.last().unwrap().seq;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(segment_path(dir, last_seq))?;
        let wal = Wal {
            dir: dir.to_path_buf(),
            opts,
            segments,
            file,
            offset: end_of_last,
            sticky: replay.meta.clone(),
            reader,
        };
        Ok((wal, replay))
    }

    /// A reader sharing this log's files.
    pub fn reader(&self) -> Arc<WalReader> {
        self.reader.clone()
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Close the last segment (synced) and start the next, beginning
    /// with the sticky values.
    fn roll(&mut self) -> io::Result<()> {
        self.file.sync_data()?;
        let seq = self.segments.last().unwrap().seq + 1;
        self.file = create_segment(&self.dir, seq, self.opts.segment_bytes)?;
        self.offset = 0;
        self.segments.push(Segment { seq, max_entry: None });
        let mut buf = Vec::new();
        for (kind, (index, payload)) in &self.sticky {
            encode_record(&mut buf, *kind, *index, payload);
        }
        if !buf.is_empty() {
            self.file.write_all_at(&buf, 0)?;
            self.offset = buf.len() as u64;
        }
        Ok(())
    }

    /// Append records (not synced: call [`Wal::sync`]). Returns where
    /// each one is.
    pub fn append(&mut self, records: &[(u8, u64, &[u8])]) -> io::Result<Vec<Loc>> {
        let mut locs = Vec::with_capacity(records.len());
        let mut buf: Vec<u8> = Vec::new();
        for &(kind, index, payload) in records {
            if !(KIND_ENTRY..=KIND_COMMITTED).contains(&kind) || payload.len() as u64 > MAX_PAYLOAD as u64 {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "bad WAL record"));
            }
            if self.offset + buf.len() as u64 >= self.opts.segment_bytes
                && self.offset + buf.len() as u64 > 0
            {
                if !buf.is_empty() {
                    self.file.write_all_at(&buf, self.offset)?;
                    self.offset += buf.len() as u64;
                    buf.clear();
                }
                self.roll()?;
            }
            let at = self.offset + buf.len() as u64;
            encode_record(&mut buf, kind, index, payload);
            let seg = self.segments.last_mut().unwrap();
            locs.push(Loc { segment: seg.seq, offset: at, len: payload.len() as u32 });
            if kind == KIND_ENTRY {
                seg.max_entry = Some(seg.max_entry.map_or(index, |m| m.max(index)));
            }
            if STICKY.contains(&kind) {
                self.sticky.insert(kind, (index, payload.to_vec()));
            }
        }
        if !buf.is_empty() {
            self.file.write_all_at(&buf, self.offset)?;
            self.offset += buf.len() as u64;
        }
        Ok(locs)
    }

    /// Make every appended record durable (`fdatasync`).
    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync_data()
    }

    /// Delete the oldest segments while every entry in them is at
    /// `<= index` (never the last one). The caller first makes a
    /// [`KIND_PURGE`] at `>= index` durable. Returns how many went.
    pub fn drop_segments_upto(&mut self, index: u64) -> io::Result<usize> {
        let mut dropped = 0;
        while self.segments.len() > 1
            && self.segments[0].max_entry.is_none_or(|m| m <= index)
        {
            let seg = self.segments.remove(0);
            self.reader.forget(seg.seq);
            std::fs::remove_file(segment_path(&self.dir, seg.seq))?;
            dropped += 1;
        }
        if dropped > 0 {
            sync_dir(&self.dir)?;
        }
        Ok(dropped)
    }

    /// Number of segment files.
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts() -> WalOptions {
        WalOptions { segment_bytes: 4096 }
    }

    fn entries(r: &Replay, reader: &WalReader) -> Vec<(u64, Vec<u8>)> {
        r.entries.iter().map(|(i, l)| (*i, reader.read(*l).unwrap())).collect()
    }

    #[test]
    fn crc32c_known_value() {
        // RFC 3720 test vector: 32 bytes of zeros.
        assert_eq!(crc32c(&[&[0u8; 32]]), 0x8A91_36AA);
    }

    #[test]
    fn append_replay_round_trip_across_segments() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut wal, r) = Wal::open(dir.path(), opts()).unwrap();
            assert!(r.entries.is_empty());
            wal.append(&[(KIND_VOTE, 0, b"vote1")]).unwrap();
            for i in 0..100u64 {
                let p = vec![i as u8; 100];
                wal.append(&[(KIND_ENTRY, i, &p)]).unwrap();
            }
            wal.sync().unwrap();
            assert!(wal.segment_count() > 1, "4 KiB segments must roll");
        }
        let (wal, r) = Wal::open(dir.path(), opts()).unwrap();
        let got = entries(&r, &wal.reader());
        assert_eq!(got.len(), 100);
        for (i, (idx, p)) in got.iter().enumerate() {
            assert_eq!(*idx, i as u64);
            assert_eq!(p, &vec![i as u8; 100]);
        }
        assert_eq!(r.meta.get(&KIND_VOTE).unwrap().1, b"vote1");
        assert!(!r.torn_tail);
    }

    #[test]
    fn truncate_and_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut wal, _) = Wal::open(dir.path(), opts()).unwrap();
            for i in 0..10u64 {
                wal.append(&[(KIND_ENTRY, i, b"a")]).unwrap();
            }
            wal.append(&[(KIND_TRUNCATE, 7, b"")]).unwrap();
            // An entry at 5 replaces 5.. even without a truncate record.
            wal.append(&[(KIND_ENTRY, 5, b"b")]).unwrap();
            wal.sync().unwrap();
        }
        let (wal, r) = Wal::open(dir.path(), opts()).unwrap();
        let got = entries(&r, &wal.reader());
        let idx: Vec<u64> = got.iter().map(|(i, _)| *i).collect();
        assert_eq!(idx, vec![0, 1, 2, 3, 4, 5]);
        assert_eq!(got[5].1, b"b");
    }

    #[test]
    fn purge_drops_prefix_segments_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut wal, _) = Wal::open(dir.path(), opts()).unwrap();
            wal.append(&[(KIND_VOTE, 0, b"v")]).unwrap();
            for i in 0..200u64 {
                wal.append(&[(KIND_ENTRY, i, &[1u8; 64])]).unwrap();
            }
            wal.append(&[(KIND_PURGE, 149, b"p149")]).unwrap();
            wal.sync().unwrap();
            let before = wal.segment_count();
            let dropped = wal.drop_segments_upto(149).unwrap();
            assert!(dropped > 0 && wal.segment_count() == before - dropped);
        }
        let (_wal, r) = Wal::open(dir.path(), opts()).unwrap();
        assert_eq!(*r.entries.keys().next().unwrap(), 150);
        assert_eq!(*r.entries.keys().last().unwrap(), 199);
        // Sticky values survive the deleted segments.
        assert_eq!(r.meta.get(&KIND_VOTE).unwrap().1, b"v");
        assert_eq!(r.meta.get(&KIND_PURGE).unwrap(), &(149, b"p149".to_vec()));
    }

    #[test]
    fn torn_tail_is_cut_and_never_resurrected() {
        let dir = tempfile::tempdir().unwrap();
        let tear_at;
        {
            let (mut wal, _) = Wal::open(dir.path(), WalOptions { segment_bytes: 1 << 20 }).unwrap();
            wal.append(&[(KIND_ENTRY, 0, b"zero")]).unwrap();
            let locs = wal
                .append(&[(KIND_ENTRY, 1, b"one-one-one"), (KIND_ENTRY, 2, b"two")])
                .unwrap();
            wal.sync().unwrap();
            tear_at = locs[0].offset + HEADER_LEN + 3;
        }
        // Damage record 1's payload: a torn write. Record 2 is intact
        // behind it, and must not come back.
        let path = segment_path(dir.path(), 0);
        let f = OpenOptions::new().write(true).open(&path).unwrap();
        f.write_all_at(b"XX", tear_at).unwrap();
        drop(f);
        {
            let (mut wal, r) = Wal::open(dir.path(), WalOptions { segment_bytes: 1 << 20 }).unwrap();
            assert!(r.torn_tail);
            assert_eq!(r.entries.keys().copied().collect::<Vec<_>>(), vec![0]);
            // A shorter new record at the cut, then reopen: the old
            // record 2 must not be parsed after it.
            wal.append(&[(KIND_ENTRY, 1, b"n")]).unwrap();
            wal.sync().unwrap();
        }
        let (wal, r) = Wal::open(dir.path(), WalOptions { segment_bytes: 1 << 20 }).unwrap();
        assert!(!r.torn_tail);
        let got = entries(&r, &wal.reader());
        assert_eq!(got, vec![(0, b"zero".to_vec()), (1, b"n".to_vec())]);
    }

    #[test]
    fn damage_in_an_older_segment_refuses() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut wal, _) = Wal::open(dir.path(), opts()).unwrap();
            for i in 0..100u64 {
                wal.append(&[(KIND_ENTRY, i, &[7u8; 100])]).unwrap();
            }
            wal.sync().unwrap();
        }
        let f = OpenOptions::new().write(true).open(segment_path(dir.path(), 0)).unwrap();
        f.write_all_at(b"\xff\xff", HEADER_LEN + 10).unwrap();
        drop(f);
        let err = Wal::open(dir.path(), opts()).err().expect("must refuse");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
