# A lone write after idle (fastetcd#102)

rustkube#190 saw a lone create on an idle single-member fastetcd v1.12.0
cost ~15–20 ms in the datastore, against ~3–5 ms for writes back to back,
and asked whether a timer or a parked thread waits before the first fsync.

## Setup

- `tests/idle_write.sh v1.12.0` on a build VM (sc-build, 2026-10-14):
  one member of each build, data on the VM's disk (btrfs) and then on
  tmpfs (`/dev/shm`, where an fdatasync costs nothing).
- `fastetcd-bench --mode idle-put`, one client after one warm-up put:
  30 puts, each after 2 s of quiet; then 30 puts back to back.
- Around each run the member's `fastetcd_wal_fsyncs_total` and
  `fastetcd_wal_fsync_seconds_total`: how many WAL fdatasyncs the puts
  took and how long they took on average.

## Result

| build | data | lone put after 2 s idle | back to back | WAL fsyncs (lone / b2b) | mean fsync, lone / b2b |
|---|---|---|---|---|---|
| v1.12.0 | disk | mean 16.1 ms, p50 8.8, max 121 | mean 6.2 ms, p50 5.6 | 31 / 31 | 15.1 / 5.7 ms |
| v1.12.0 | tmpfs | mean 0.87 ms | mean 0.14 ms | 31 / 31 | 0 / 0 |
| v1.32.1 | disk | mean 11.7 ms, p50 12.2, max 23 | mean 9.3 ms, p50 11.0 | 31 / 31 | 10.7 / 8.8 ms |
| v1.32.1 | tmpfs | mean 0.88 ms | mean 0.10 ms | 31 / 31 | 0 / 0 |

- **Every write costs one WAL fdatasync, idle or not** (30 puts plus the
  warm-up: 31). No write waits for a batch timer or a second fsync.
- **On a disk the difference is the fsync itself.** On v1.12.0 the lone
  puts' mean is 9.9 ms above the back-to-back mean, and their mean fsync
  is 9.4 ms above; on v1.32.1, 2.4 ms and 1.9 ms. The device takes
  longer to fdatasync after a quiet spell than in a stream.
- **fastetcd's own cold path is under a millisecond.** On tmpfs, where
  nothing waits on a disk, a lone put takes ~0.8 ms more than one in a
  stream, in both versions: the client's and the server's threads waking
  from idle (the gRPC connection, tokio's workers, the WAL writer
  thread), not a timer.
- The two builds' disk rows are one run each, the tree's after the
  baseline's on the same VM; their back-to-back difference (6.2 vs 9.3 ms)
  is not a comparison of the builds.

Raw output: `data/idle-write/`.
