//! A read-only reader of BoltDB files, as etcd writes them (fastetcd#60).
//!
//! etcd's backend runs bbolt with `NoFreelistSync`: the freelist is not
//! written, and the meta page says so (`freelist = 0xFFFF_FFFF_FFFF_FFFF`).
//! Go bbolt rebuilds it by scanning when it opens such a file to write;
//! the bbolt-rs crate refuses to open one at all ("PGID_NO_FREE_LIST not
//! currently supported"), so no etcd snapshot could be read. Reading needs
//! no freelist, so this reads the format directly:
//!
//! - page `n` is at `n * page_size`, `(overflow + 1)` pages long; its
//!   16-byte header is `id u64, flags u16, count u16, overflow u32`
//!   (little-endian), flags 0x01 branch, 0x02 leaf, 0x04 meta;
//! - pages 0 and 1 are meta pages: `magic u32 (0xED0CDAED), version u32
//!   (2), page_size u32, flags u32, root {pgid u64, sequence u64},
//!   freelist u64, pgid u64, txid u64, checksum u64` (FNV-1a 64 of the
//!   fields before it); the valid one with the higher txid is current;
//! - branch elements: `pos u32, ksize u32, pgid u64`; leaf elements:
//!   `flags u32 (0x01 = nested bucket), pos u32, ksize u32, vsize u32`;
//!   `pos` is from the element's own address, key then value;
//! - a bucket's value is `root pgid u64, sequence u64`; root 0 means the
//!   bucket is inline: a leaf page follows in the value itself.
//!
//! Pages are read with positioned reads, so a snapshot is never held in
//! memory whole.

use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

const MAGIC: u32 = 0xED0C_DAED;
const VERSION: u32 = 2;
const PAGE_HEADER: usize = 16;
const ELEMENT: usize = 16;
const BRANCH: u16 = 0x01;
const LEAF: u16 = 0x02;
const BUCKET_LEAF: u32 = 0x01;

/// An open BoltDB file.
pub struct BoltFile {
    file: File,
    page_size: u64,
    root: u64,
}

/// A bucket: its root page, or its inline leaf page.
pub enum Bucket {
    Root(u64),
    Inline(Vec<u8>),
}

fn u16_at(b: &[u8], o: usize) -> u16 {
    u16::from_le_bytes(b[o..o + 2].try_into().unwrap())
}
fn u32_at(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}
fn u64_at(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}

fn fnv1a64(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &x in data {
        h ^= x as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// A meta page's (txid, page_size, root pgid), if it is valid.
fn meta(page: &[u8]) -> Option<(u64, u64, u64)> {
    let m = &page[PAGE_HEADER..PAGE_HEADER + 64];
    if u32_at(m, 0) != MAGIC || u32_at(m, 4) != VERSION || fnv1a64(&m[..56]) != u64_at(m, 56) {
        return None;
    }
    Some((u64_at(m, 48), u32_at(m, 8) as u64, u64_at(m, 16)))
}

impl BoltFile {
    pub fn open(path: &Path) -> anyhow::Result<BoltFile> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        // The first meta page is at 0; the second at page_size, which the
        // first (if valid) gives, else the common sizes are tried.
        let mut first = vec![0u8; 4096.min(len as usize)];
        file.read_exact_at(&mut first, 0)?;
        if first.len() < PAGE_HEADER + 64 {
            anyhow::bail!("{} is too short to be a BoltDB file", path.display());
        }
        let m0 = meta(&first);
        let mut best = m0;
        for ps in m0.map(|m| vec![m.1]).unwrap_or_else(|| vec![4096, 8192, 16384, 65536]) {
            if ps + (PAGE_HEADER as u64) + 64 > len {
                continue;
            }
            let mut second = vec![0u8; PAGE_HEADER + 64];
            file.read_exact_at(&mut second, ps)?;
            if let Some(m1) = meta(&second) {
                if best.is_none_or(|b| m1.0 > b.0) {
                    best = Some(m1);
                }
            }
        }
        let Some((_, page_size, root)) = best else {
            anyhow::bail!(
                "{} is not a BoltDB file etcd wrote: no valid meta page (magic 0xED0CDAED, version 2)",
                path.display()
            );
        };
        Ok(BoltFile { file, page_size, root })
    }

    fn page(&self, id: u64) -> anyhow::Result<Vec<u8>> {
        let at = id * self.page_size;
        let mut header = [0u8; PAGE_HEADER];
        self.file.read_exact_at(&mut header, at)?;
        let overflow = u32_at(&header, 12) as u64;
        let mut p = vec![0u8; ((overflow + 1) * self.page_size) as usize];
        self.file.read_exact_at(&mut p, at)?;
        Ok(p)
    }

    /// Visit every entry under the page `p`, in key order: `(key,
    /// value, is_bucket)`.
    fn walk(
        &self,
        p: &[u8],
        f: &mut dyn FnMut(&[u8], &[u8], bool) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let (flags, count) = (u16_at(p, 8), u16_at(p, 10) as usize);
        for i in 0..count {
            let e = PAGE_HEADER + i * ELEMENT;
            if flags & LEAF != 0 {
                let (eflags, pos, ksize, vsize) =
                    (u32_at(p, e), u32_at(p, e + 4) as usize, u32_at(p, e + 8) as usize, u32_at(p, e + 12) as usize);
                let k = e + pos;
                f(&p[k..k + ksize], &p[k + ksize..k + ksize + vsize], eflags & BUCKET_LEAF != 0)?;
            } else if flags & BRANCH != 0 {
                let child = u64_at(p, e + 8);
                self.walk(&self.page(child)?, f)?;
            } else {
                anyhow::bail!("page {} is neither a branch nor a leaf (flags {flags:#x})", u64_at(p, 0));
            }
        }
        Ok(())
    }

    fn bucket_page(&self, b: &Bucket) -> anyhow::Result<Vec<u8>> {
        Ok(match b {
            Bucket::Root(id) => self.page(*id)?,
            Bucket::Inline(page) => page.clone(),
        })
    }

    /// The top-level bucket `name`, if the file has one.
    pub fn bucket(&self, name: &[u8]) -> anyhow::Result<Option<Bucket>> {
        let mut found = None;
        self.walk(&self.page(self.root)?, &mut |k, v, is_bucket| {
            if is_bucket && k == name && found.is_none() {
                let root = u64_at(v, 0);
                found = Some(if root == 0 { Bucket::Inline(v[16..].to_vec()) } else { Bucket::Root(root) });
            }
            Ok(())
        })?;
        Ok(found)
    }

    /// Every key and value of `b`, in key order (nested buckets skipped).
    pub fn for_each(
        &self,
        b: &Bucket,
        f: &mut dyn FnMut(&[u8], &[u8]) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        self.walk(&self.bucket_page(b)?, &mut |k, v, is_bucket| if is_bucket { Ok(()) } else { f(k, v) })
    }

    /// The value of `key` in `b`.
    pub fn get(&self, b: &Bucket, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let mut out = None;
        self.for_each(b, &mut |k, v| {
            if k == key {
                out = Some(v.to_vec());
            }
            Ok(())
        })?;
        Ok(out)
    }
}
