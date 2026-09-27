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
point-in-time view: the MVCC data, the raft log and vote, leases, users
and roles, and node-local metadata such as the cluster id. It is exactly
the state a crash at that instant would have left. (The raft snapshot
that `etcdctl snapshot save` streams holds only the MVCC tables.
Restoring from that would lose leases, users and the cluster id.)

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
   snapshots move with it (`snapshots.corrupt.<unix-ms>`), since they
   may be newer than the backup.
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
the same way. Restoring onto a node with a different identity needs
`--force-new-cluster` on its first start, as with etcd.

## redb two-phase commit: deliberately off

redb can commit in two phases, with an extra fsync that flips the commit
slot only once the new one is durable. fastetcd does not use it. In
redb 2.6, a two-phase commit makes repair trust the primary slot and
never fall back to the previous commit. So on a device that loses
fsync'd writes, a torn last commit becomes an unopenable file, and a
full restore from backup loses up to the backup interval. Without it,
the same tear is rolled back to the previous commit, losing only that
write. Its protection is against a deliberate attacker who controls
flush order and crashes, and it costs a second fsync on every write. The
measured cost is in the changelog for v1.4.0.
