# Backups and recovering from a corrupt data file

fastetcd's durability is only as good as the device under it. A drive,
or a virtual block device, that acknowledges an fsync and then loses the
write can leave the redb data file unopenable after a power cut:

```
Error: storage io: DB corrupted: Failed to repair database. All roots are corrupted
```

Before v1.4.0 that was the end of the store. The node exited on every
start and there was nothing to recover from (fastetcd#37). Now:

- fastetcd takes **periodic backups** to a directory you give it, on a
  separate volume;
- a **lone member** whose data file is found corrupt restores itself from
  the newest good backup, keeps the corrupt file, and raises an alarm
  saying how much was lost;
- a **member of a multi-node cluster** refuses to start and tells you how
  to replace it, because it must never forget its raft vote.

## Periodic backups

Off unless `--backup-dir` is set.

| Flag | Default | |
|---|---|---|
| `--backup-dir` | unset (no backups) | Where backups go. Use a **different volume** from `--data-dir`. |
| `--backup-interval-secs` | `900` | Take a backup at least this often, when anything has changed. |
| `--backup-every-revisions` | `10000` | Also take one once this many revisions have been written since the last. `0` turns this trigger off. |
| `--backup-retain` | `4` | Backups kept, newest first. |

A backup is also taken whenever the raft membership changes, because
whether a node may restore itself depends on whether it was alone.
Nothing is written while nothing changes.

**What is in a backup.** Every table of the store, read from one
point-in-time view: the MVCC data, leases, users and roles, node-local
metadata such as the cluster id, and a copy of the raft vote and purge
point. Since 1.12 the raft log itself lives in `wal/`, not in the data
file (#85), so a backup does not carry it; restoring one starts a new
WAL from the vote and purge point it does carry. The state is what the
store had applied at that instant.

**A live backup on demand.** `etcdctl snapshot save <file>` (or
`fastetcd-ctl snapshot-save <file>`) streams the same format from a
running member (since 1.15, #61; root only with auth on), built from
the same point-in-time view and encoded straight into the stream, so it
needs no room on the data volume. `fastetcd restore <file>` restores it
like a periodic backup. Before 1.15 that stream was a raft snapshot,
which nothing can restore.

**File format.** `fastetcd-backup-<unix-ms>-rev<revision>.fbak`: an
8-byte magic `FEBACKUP`, a format version, a header (version, time,
node id, cluster id, revision, raft members), the tables, and a SHA-256
of everything before it. Each is written to a temp file, fsynced,
renamed into place, and the directory fsynced, so a listed backup is
whole. On `ENOSPC` the oldest backup is dropped and the write retried
once. A failed backup is logged and retried on the next tick. It never
affects serving.

**Why a separate volume.** A device that loses writes can corrupt the
data file and any backup on the same volume at the same time. fastetcd
warns at startup when `--backup-dir` shares a device with `--data-dir`.
Size the backup volume for `--backup-retain` full copies of the store.
Backups deliberately don't live on the data volume by default: four more
copies there would break the sizing in [04-disk-space.md](04-disk-space.md).

**Cost.** Taking a backup reads every table into memory and writes one
file, much like building a raft snapshot. The read holds a redb read
transaction only for the scan itself, not for the disk write.

## When the data file is corrupt

Checked on every start. "Corrupt" means redb's own corruption error
(both commit slots' roots fail their checksums), an unreadable header,
a data file that exists but is empty, or redb panicking while it
repairs the file. redb 2.6.3 does the last rather than return an error
when a device has lost whole pages and they read back as zeros. Before
v1.4.0, redb silently started a fresh store over an empty file. A lock
or permission error is *not* corruption and fails as before.

`--on-corruption` (default `restore`):

| Situation | What happens |
|---|---|
| Lone member, `restore`, a backup of this node verifies | **Restored.** See below. |
| `--initial-cluster` names another member, or the newest backup records another raft member | **Refuses**, whatever the flag says. |
| `refuse` | Refuses. |
| No `--backup-dir`, or no backup of this node whose checksum verifies | Refuses. |

When fastetcd refuses, the data file is **left exactly as it was**, and
the error says why and what to do.

### Restoring a lone member

1. Backups in `--backup-dir` are tried newest first. One whose checksum
   fails, or that another node took, is skipped with a warning.
2. A new store is built from it beside the corrupt one
   (`fastetcd.redb.restored`), and the recovery is recorded in it. If
   that fails, nothing has moved.
3. The corrupt file is renamed to `fastetcd.redb.corrupt.<unix-ms>` and
   kept for inspection; **it is never deleted**. The retained raft
   snapshots and the raft WAL move with it (`snapshots.corrupt.<unix-ms>`,
   `wal.corrupt.<unix-ms>`), since they may be newer than the backup.
   The start then builds a new WAL from the backup.
4. The restored store takes its place.

A crash between steps 3 and 4 leaves no `fastetcd.redb` but a complete
`fastetcd.redb.restored`. The next start finishes the swap. It never
creates an empty store in its place.

The store is back at the backup's revision. **Everything written after
that backup is lost**, and fastetcd says so loudly:

- the log line `RECOVERED FROM BACKUP`, with the backup, its revision and
  its age;
- the **CORRUPT alarm**: `etcdctl alarm list` shows it, and
  `etcdctl endpoint status` lists `CORRUPT` under errors. It survives
  restarts until `etcdctl alarm disarm`;
- metrics: `fastetcd_recovered_from_backup_total` (restores over the
  life of the store) and `fastetcd_recovered_revision` (the revision
  restored to, while the alarm is raised).

Unlike etcd, where CORRUPT refuses every request, fastetcd keeps
serving under this alarm. The restored store is consistent, just older,
and the point of restoring it was to serve again. Clients that cached
revisions newer than the restored one (a kube-apiserver watch cache,
for example) see the revision go backwards and relist.

### Replacing a corrupt member of a cluster

A raft member must never forget its vote. Starting a member without its
vote, whether empty or from a backup holding an older one, could let it
vote twice in one term and help elect two leaders. So a corrupt member
of a multi-node cluster does not start. The other members keep serving
and have all the data. Replace it:

```
# On a healthy member:
etcdctl member remove <id>            # the id is in the error message (hex)

# On the corrupt node, with fastetcd stopped:
mv /var/lib/fastetcd /var/lib/fastetcd.corrupt

# Add it back, and start it empty so it catches up from the leader:
etcdctl member add <name> --peer-urls=<peer-url>
fastetcd ... --initial-cluster-state=existing
```

## Restoring by hand

`fastetcd restore <file>` (server stopped) accepts a periodic backup as
well as a raw copy made by `fastetcd backup`. It refuses to overwrite a
newer store without `--force`, and keeps the current file as
`fastetcd.redb.replaced-<ts>`. A current file too corrupt to read is kept
the same way. The raft snapshots and WAL that went with the replaced
file are moved to `snapshots.replaced-<ts>` and `wal.replaced-<ts>`.

`fastetcd backup` copies the data file only. The data file is durable as
of its last checkpoint (#85): a server stopped cleanly checkpoints on the
way out, but one that was killed has its last ~100 ms of applies only in
the WAL. Start and stop it once before an offline backup if it was not
stopped cleanly. Restoring onto a node with a different identity needs
`--force-new-cluster` on its first start, as with etcd.

## redb quick-repair commits (two-phase commit) since 1.36.2

Every durable redb commit (each checkpoint, `sync`, a snapshot install,
the commits around a defragment) is a redb **quick-repair** commit
(#176). It saves the allocator state with the commit, so a start after a
kill or a power cut loads that state instead of walking every page of
the data file. On server1 (X9, drive-managed SMR) that walk took 179 to
600+ s per start and was the main cause of its reboot failures
(stormcos#626); a quick-repair open reads about 0.1 MiB at any size
(`crates/storage/tests/repair_cost.rs`: 9–83 ms where the walk took
0.4–8 s on a build VM). The applies committed non-durably after the last
checkpoint are lost on a kill either way and replayed from the WAL. A
start says which it was: `data file was not closed cleanly ... quick
repair, no walk`, or `... redb is repairing it` (a full walk: a file
whose last durable commit was written before 1.36.2, or by redb itself
during a compaction). After a full walk the engine makes one
quick-repair commit at once, so the next kill is quick again.

**The trade-off (owner's decision on #176, 2026-10-28).** Quick-repair
forces redb's two-phase commit, which 1.4.0 (#37) had deliberately left
off. With it, redb trusts the newest commit slot and never falls back to
the previous one. So on a device that loses fsync'd writes (stormblock#171,
#334), a torn checkpoint makes the file **unopenable** where it used to
roll back one commit:

- a lone member restores its newest backup that verifies (losing the
  writes since it) and raises the CORRUPT alarm (above; needs
  `--backup-dir`);
- a member of a multi-member cluster refuses to start: remove it and add
  it back (above), and it is caught up by a raft snapshot.

Kills and warm resets happen on every install; a device that loses
fsync'd writes is the rarer failure (stormblock#334 is fixed, and
stormblock's health lists `ublk_stuck`), and a member hit by it still
recovers. It costs one extra fsync per checkpoint, not per write (#67
below).

**What it costs (#67).** `crates/storage/tests/two_phase_commit_cost.rs`
(500 one-key commits, off and on, interleaved; `--ignored`), sc-build on a
build VM, 2026-10-14: a QEMU disk on pve's LVM-thin pool (btrfs), three
rounds, off/on pairs alternating:

| | off | on |
|---|---|---|
| p50 per commit | 10.0–10.7 ms | 16.4–20.3 ms |
| p99 per commit | 22–29 ms (one pair 155 ms) | 37–60 ms |
| commits/s | 87–104 | 47–56 |
| tmpfs (bookkeeping only) | 9.4–9.7 µs | 9.9 µs (+3–5%) |

With a real fsync it roughly doubles a durable commit (6 pairs of 6).
Since #85 a client write's fsync is the raft WAL's, and redb commits
durably only at a checkpoint (at most every 100 ms, paced to 4x its own
duration), so 2PC would no longer be paid per write: it would lengthen
every checkpoint by about one fsync, and with the pacing, space them
further apart. On a spindle, where checkpoints already compete with the
WAL for the disk (#135), that is not free either; it is what 1.36.2
pays for a start after a kill that does not walk the file.
