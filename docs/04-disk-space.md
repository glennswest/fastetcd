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
to the filesystem. `dbSize` (the file) and `dbSizeInUse` (the pages
actually held) are reported separately for exactly this reason.

The gap between them is an **upper bound** on what a defragment can
return, not a promise. The allocator can usually only give back whole
regions at the end of the file, so a store whose live pages are spread
across it frees less — sometimes nothing. Read the figure as "is a
defragment worth the pause", never as "I will get this many bytes".
Two related things to know:

- Deleting a lot of keys does not immediately move bytes from
  `dbSizeInUse` to the gap: the engine releases those pages on a later
  commit. The figure catches up within a commit or two, which is why the
  reclaim path compacts *before* it measures.
- If a defragment frees nothing, the live data itself is what fills the
  volume. Compact history to turn live pages into free ones, then
  defragment again.

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
etcd_mvcc_db_total_size_in_use_in_bytes    # pages actually held
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

A defragment on a busy node does not fail: it stops handing out new
read snapshots, waits up to 30 seconds for the in-flight ones to finish,
then compacts. Writes continue during that wait — blocking them would
deadlock, since an apply holds its read snapshot across its own commit.
If a scan or watch holds a snapshot open past the timeout, the error
says so and points at the offline path below.

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

Ask the binary rather than guess:

```bash
fastetcd sizing --nodes 100                 # or --pods-per-node 110 for a dense cluster
```

It prints the arithmetic, not just a number. The ladder at the default
30 pods/node:

| Nodes | Live data | DB file | Minimum volume | Provision |
|---:|---:|---:|---:|---:|
| 1 | 32 MiB | 67 MiB | 317 MiB | **512 MiB** |
| 10 | 35 MiB | 73 MiB | 343 MiB | **512 MiB** |
| 100 | 63 MiB | 130 MiB | 603 MiB | **1 GiB** |
| 500 | 185 MiB | 386 MiB | 1.7 GiB | **2 GiB** |
| 1000 | 339 MiB | 704 MiB | 3.1 GiB | **4 GiB** |
| 5000 | 1.5 GiB | 3.2 GiB | 14.4 GiB | **16 GiB** |

Two things in that table are worth understanding before trusting it.

**The volume is ~10x the live data, and that is not waste.** A 100-node
cluster stores 63 MiB of Kubernetes objects and wants a 1 GiB volume.
The multipliers, in order of size: MVCC history between compactions
(+30%), the engine's copy-on-write overhead (x1.6), a raft snapshot
(another *full copy* of the database), the raft log, and up to
`--upgrade-backup-retain` safety backups (a full copy each). Sizing for
the 63 MiB is how a volume fills.

**Small clusters all land in the same bucket.** 1 node and 10 nodes both
provision at 512 MiB, because below roughly 50 nodes the estimate is
fixed cluster overhead — CRDs, RBAC, ServiceAccounts, API
priority-and-fairness config, none of which scale with node count —
multiplied by those same terms. There is nothing to shave by counting
nodes at that size. Past ~100 nodes the pod term takes over and the
estimate tracks the cluster.

Pod density, not node count, is what actually drives the variable part:
100 nodes at 110 pods/node stores more than twice what 100 nodes at 30
does. If your clusters run dense, pass `--pods-per-node`.

### Letting the server check

```bash
fastetcd --expected-nodes 100 ...
```

At startup this computes the same estimate against the real volume and
says so in the log — while the store is still empty and the answer is
still actionable:

```
ERROR DATA VOLUME IS TOO SMALL for the declared cluster size
      volume=64.0 MiB needs=602.8 MiB provision=1.0 GiB
```

It warns rather than refuses: an operator who knows their cluster better
than the model should not be blocked from starting, and a control plane
that will not boot is worse than one that needs attention later. It also
enables MVCC auto-compaction if `--auto-compaction-retention` was left
at `0`, since an unbounded history is what fills a bounded volume.

### Substituting your own numbers

Every constant in the model is a named value in
`crates/server/src/sizing.rs` with the reasoning attached —
`PER_NODE_BYTES` (10 KiB: a `Node` with up to 50 reported images, its
`Lease`, its `CSINode`), `PER_POD_BYTES` (8 KiB), `FIXED_CLUSTER_BYTES`
(32 MiB), `HISTORY_FRACTION_PCT`, `COW_OVERHEAD_PCT`. If you have
measured your own cluster, those are the numbers to argue with.
