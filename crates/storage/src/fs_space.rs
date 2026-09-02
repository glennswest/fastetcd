//! Filesystem free-space probe for the data directory.
//!
//! fastetcd is normally deployed onto a fixed-size volume, so the limit
//! that matters is not a configured quota but the filesystem itself. A
//! store that only watches its own file size cannot tell the difference
//! between "40 MB of data on a 2 TB disk" and "40 MB of data on a 64 MB
//! volume" — and the second one wedges the node when it hits the wall
//! (fastetcd#14). Everything that decides whether there is room to write
//! a snapshot needs the device's numbers, not just the file's.
//!
//! `statvfs` covers every unix target we build for (Linux and macOS).
//! On anything else the probe returns `None` and callers fall back to a
//! configured quota alone.

use std::path::Path;

/// Capacity of the filesystem holding a path, in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FsSpace {
    /// Total size of the filesystem.
    pub total: u64,
    /// Bytes available to this (unprivileged) process. Excludes the
    /// root-reserved margin, which is the number that matters: fastetcd
    /// runs as its own user, so the reserve is not ours to spend.
    pub available: u64,
}

impl FsSpace {
    /// Bytes in use on the filesystem, from the process's point of view.
    pub fn used(&self) -> u64 {
        self.total.saturating_sub(self.available)
    }
}

/// Probe the filesystem holding `path`. Returns `None` if the platform
/// has no probe or the syscall fails (a missing path, most often).
#[cfg(unix)]
pub fn probe(path: &Path) -> Option<FsSpace> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    // SAFETY: `statvfs` writes into a zeroed, correctly-sized struct and
    // reads a NUL-terminated path we own for the duration of the call.
    let stat = unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c_path.as_ptr(), &mut stat) != 0 {
            return None;
        }
        stat
    };
    // f_frsize is the fragment size — the unit f_blocks/f_bavail count
    // in. It is 0 on some filesystems, in which case f_bsize is the
    // right fallback.
    let unit = if stat.f_frsize > 0 {
        stat.f_frsize as u64
    } else {
        stat.f_bsize as u64
    };
    Some(FsSpace {
        total: (stat.f_blocks as u64).saturating_mul(unit),
        available: (stat.f_bavail as u64).saturating_mul(unit),
    })
}

#[cfg(not(unix))]
pub fn probe(_path: &Path) -> Option<FsSpace> {
    None
}

/// Total size of every regular file directly under `dir`, recursively.
/// Missing directories count as zero — a node that has never written a
/// snapshot has no snapshot directory.
pub fn dir_size(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut total = 0u64;
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else { continue };
        if meta.is_dir() {
            total = total.saturating_add(dir_size(&entry.path()));
        } else {
            total = total.saturating_add(meta.len());
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_reports_a_plausible_filesystem() {
        let dir = tempfile::tempdir().unwrap();
        let space = probe(dir.path()).expect("statvfs on a temp dir");
        assert!(space.total > 0, "filesystem reports zero total bytes");
        assert!(space.available <= space.total);
    }

    #[test]
    fn probe_of_a_missing_path_is_none() {
        assert!(probe(Path::new("/definitely/not/a/real/path/here")).is_none());
    }

    #[test]
    fn dir_size_sums_files_recursively() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a"), vec![0u8; 100]).unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub/b"), vec![0u8; 50]).unwrap();
        assert_eq!(dir_size(dir.path()), 150);
        assert_eq!(dir_size(&dir.path().join("nope")), 0);
    }
}
