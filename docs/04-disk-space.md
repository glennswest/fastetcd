# Disk space: keeping a bounded volume bounded

fastetcd is normally deployed onto a fixed-size volume. This document
covers what makes the store grow, what fastetcd does about it on its
own, what you can tune, and how to dig out a volume that has already
filled.

Background: fastetcd#14, where a single-node cluster filled a 64 MB data
volume after about a day of ordinary control-plane traffic and then
wedged in both directions.

## Why a store grows

Four things accumulate, and only the first is your data:

| What | Grows because | Bounded by |
|---|---|---|
| MVCC history | every write keeps the previous revision of the key | compaction |
| Raft log | every proposal is appended | a snapshot, then purge |
| Raft snapshot | it is a full serialized copy of the database | retention (`--max-snapshots`) |
| Free pages inside the data file | a copy-on-write B-tree reuses freed pages but does not shrink the file | defragment |

A store can therefore be mostly empty and still occupy the whole volume:
deleting every key frees pages inside the file without returning a byte
to the filesystem. `dbSize` (the file) and `dbSizeInUse` (the live data
in it) are reported separately for exactly this reason, and the gap
between them is what a defragment would hand back.

## Why running out is a deadlock, not a slowdown

When the volume fills, the snapshot write fails with `ENOSPC`. openraft
surfaces that storage error on the linearizable read barrier *and* on
every proposal, so:

```
status: Unavailable, message: "linearizable read barrier: when Write Snapshot(...): No space left on device"
status: Unavailable, message: "raft client_write: when Write Snapshot(...): No space left on device"
```

Reads fail. Writes fail. And the one recovery a client could attempt —
deleting keys so the next snapshot fits — is refused for the same
reason. Every process is healthy and the cluster is down.

So the design goal is not to fail gracefully at 100%; it is to never
arrive there.

## What fastetcd does on its own

**Measures the real thing.** Occupancy is sampled every
`--space-check-interval-secs` (default 30): the data file, the retained
snapshots, and the filesystem's actual free space via `statvfs`. The
effective capacity is the *smaller* of `--quota-backend-bytes` and what
the store can really reach on the device. A 2 GiB quota on a 64 MB
volume is not a limit, it is a fiction — and it is that fiction that let
the store run into `ENOSPC` with every threshold reporting healthy.

**Rolls snapshots off before writing new ones.** Snapshots are retained
as a numbered set and pruned oldest-first *ahead* of the next write, so
writing snapshot N+1 never needs room for `--max-snapshots + 1` copies at
once. Leftover temp files and half-written pairs are reclaimed on
startup. If a snapshot write still hits `ENOSPC`, every retained
snapshot is discarded and the write is retried — openraft can always
rebuild one, and a node with no snapshot is worth more than a node that
cannot write one.

**Reclaims at the high-water mark** (default 80% of capacity), in the
order that actually frees bytes:

1. Compact MVCC history to `--space-reclaim-retention` revisions (1000
   by default). This runs even with `--auto-compaction-retention 0`: an
   unbounded history is the usual reason a bounded volume fills.
2. Trigger a raft snapshot, so openraft can purge the log it has
   already applied instead of waiting for the next `--snapshot-count`
   boundary.
3. Defragment, if at least a few MB would come back. This is the only
   step that returns space to the filesystem, and it pauses reads and
   writes while it runs — so it is skipped when the live data itself is
   what fills the volume.

**Alarms at the alarm mark** (default 95%), while the store still works.
`NOSPACE` is raised, and writes — `Put`, a `Txn` containing a put,
`LeaseGrant` — are refused with gRPC `ResourceExhausted` and etcd's
message, `etcdserver: mvcc: database space exceeded`. Reads, deletes,
compaction and defragment are deliberately **not** gated: refusing those
is what turns a full volume into an unrecoverable one. The alarm clears
itself once occupancy falls back below `--space-clear-percent` (70).

## Flags

| Flag | Default | What it does |
|---|---|---|
| `--quota-backend-bytes` | `0` | Ceiling on the footprint. `0` = the volume is the limit. |
| `--space-high-water-percent` | `80` | Start reclaiming here. |
| `--space-alarm-percent` | `95` | Raise `NOSPACE` and refuse writes here. |
| `--space-clear-percent` | `70` | Clear the alarm below here. |
| `--space-check-interval-secs` | `30` | Sampling cadence. `0` disables the monitor entirely. |
| `--space-reclaim-retention` | `1000` | Revisions kept when compacting under pressure. |
| `--auto-defrag` | `true` | Let the reclaim path defragment. |
| `--max-snapshots` | `1` | Snapshots retained on disk. |
| `--auto-compaction-retention` | `0` | Steady-state compaction (off; Kubernetes drives its own). |
| `--snapshot-count` | `5000` | Applied entries between raft snapshots. |
| `--max-in-snapshot-log-to-keep` | `1000` | Log entries kept after a purge. |

Each has an `ETCD_*` environment fallback where etcd has the same flag,
so an unmodified etcd `EnvironmentFile` keeps working.

`--max-snapshots` defaults to 1 rather than etcd's 5 because a fastetcd
snapshot is a full copy of the database: on a fixed-size volume each
extra retained copy is another whole database. Raise it only if you
have the room and want older copies as a safety net.

## Metrics

```
etcd_mvcc_db_total_size_in_bytes           # the file
etcd_mvcc_db_total_size_in_use_in_bytes    # live data inside it
etcd_server_quota_backend_bytes            # effective capacity
fastetcd_store_snapshot_size_in_bytes      # retained snapshots
fastetcd_disk_total_bytes                  # the volume
fastetcd_disk_available_bytes
fastetcd_store_space_used_ratio            # 0-1; 0.8 reclaims, 0.95 alarms
fastetcd_nospace_alarm_active              # 1 while writes are refused
```

A useful alert fires well before the alarm does:

```yaml
- alert: FastetcdDiskFillingUp
  expr: fastetcd_store_space_used_ratio > 0.7
  for: 15m
```

## Digging out

**While the server is running** — the alarm has fired but the store is
still serving:

```bash
fastetcd-ctl status                    # dbSize vs dbSizeInUse vs capacity
fastetcd-ctl compact <revision>        # accepted under NOSPACE
fastetcd-ctl del <prefix> --prefix     # accepted under NOSPACE
fastetcd-ctl defrag                    # returns the freed pages
fastetcd-ctl alarm                     # list
fastetcd-ctl alarm --disarm            # clear (re-raised if still over)
```

`etcdctl` works for all of these too — `endpoint status`, `compact`,
`defrag`, `alarm list`, `alarm disarm`.

**When the volume is already full** and the server can no longer do
anything, stop it and defragment offline:

```bash
systemctl stop fastetcd
fastetcd defrag --data-dir /var/lib/fastetcd
systemctl start fastetcd
```

The offline path needs no quorum, no read barrier and no snapshot write
— which is the point, since those are exactly what fails on a full
volume. It also needs no free space to work: redb's compaction moves
live pages toward the front of the existing file and truncates it.

If the offline defrag reports nothing reclaimable, the live data itself
is what fills the volume. Compact history (or set
`--auto-compaction-retention`) and defragment again, or give the volume
more room.

## Sizing

Budget roughly:

```
volume >= 2 x (live data + retained history) + raft log headroom
```

The factor of two is the snapshot: while one is being written the volume
holds the database and (briefly) a new full copy of it. On a volume
sized for a single copy, keep `--max-snapshots 1` and give compaction a
retention window it can actually hold.
