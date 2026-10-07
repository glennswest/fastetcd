# The kubelet's status report on an X9 blade (fastetcd#101)

#95 (group commit on a slow disk, v1.13) needed its acceptance measured
on the target hardware: the kubelet's `report` step (the pod status write
through the apiserver into fastetcd) for 5 busybox pods started 3 s apart
on server3, with a target **p50 < 30 ms**. On v1.12 it was 236–1,040 ms.

## Setup

- **server3**: an X9 blade with one 7200 rpm disk under stormblock (its
  journal, the pods' sandboxes and fastetcd's data all on that spindle).
  Release 11.91, which carries **fastetcd v1.18.0** (it includes v1.13's
  group commit and checkpoint pacing). One member.
- 2026-10-07 18:14–18:25 UTC. The machine was leased (`stormcentral
  testhost lease`), so nothing else ran tests on it.
- `tests/bench/blade_pods.py`: 30 s idle, then 5 pods 3 s apart, each
  pod's `storm.io/start-timing` read once it runs, and fastetcd's
  `/metrics` (:2381, stormcos#242) sampled every 1–3 s. Six runs, 45 s
  apart. Raw output and samples: `data/blade-server3/`.

## Result

**Report, 30 pods:** p50 **39.5 ms**, p90 660 ms, min 6.5 ms, max 818 ms.
9 of 30 are under 30 ms and 21 are at or under 100 ms; 7 are 0.29–0.82 s.
The target p50 is not met, though the median is about 10x better than
v1.12's range. The tail is the disk:

| run | report, ms (t1..t5) | WAL fsyncs | avg fsync | proposals / fsync | checkpoint share of disk |
|---|---|---|---|---|---|
| r1 | 729, 722, 9.2, 11, 38 | 72 | 76 ms | 1.19 | 11% |
| r2 | 30, 45, 22, 38, 89 | 78 | 13 ms | 1.00 | 7% |
| r3 | 17, 37, 23, 40, 636 | 71 | 90 ms | 1.10 | 11% |
| r4 | 21, 52, 818, 6.5, 30 | 70 | 128 ms | 1.16 | 15% |
| r5 | 605, 660, 634, 290, 39 | 57 | 208 ms | 1.40 | 19% |
| r6 | 50, 60, 16, 19, 127 | 80 | 12 ms | 0.99 | 5% |

Idle between runs: ~1.8 fsyncs/s (one proposal each), 6–29 ms average.

- **fastetcd is not queueing.** At ~4 writes/s there are 1.0–1.4
  proposals per fsync, so a report waits for about one fsync, and runs
  where the fsync stays near 12 ms have no report over 130 ms.
- **The slow reports are slow fsyncs.** In the bad runs, 6–10 WAL fsyncs
  used 2.2–4.3 s of a ~3 s sample interval: single fsyncs of 300–550 ms.
- **Checkpoints are in most of those windows** (1.1–1.6 s each there,
  under 0.45 s in the clean runs), but one r5 window had 3.2 s of WAL
  fsync with no checkpoint ending in it. So the spindle also stalls on its
  own: stormblock's journal and the sandboxes share it. stormblock's
  `/metrics` needs a token, so its latency could not be read next to these.
- Since boot this member's slowest WAL fsync was 84.7 s and its slowest
  checkpoint 72.7 s.

How much of the stall is fastetcd's own checkpoint, and what to do about
it (smaller durable steps, I/O priority, a disk mode in the test
container to measure it on the blade): **#135**.

## Reads: apiserver GET with the RAM cache (#84)

#82 (v1.11) keeps every key's index in RAM and the latest value of hot
keys in a cache. Its blade acceptance: an apiserver single-object GET of
the cilium-operator lease under 20 ms p99 (on v1.10, server1's took
0.2–0.55 s). Measured on server3 at 20:25–20:28 UTC with
`READ_ONLY=1 tests/bench/blade_getlatency.py`: idle, 200 GETs of
`kube-system/cilium-operator-resource-lock`; then 40 clients GETting
kube-system's Leases in turn and listing them every fifth loop while the
probe keeps GETting. Read-only, because this member had written nothing
since 19:09:47Z (#138): the numbers are reads without concurrent writes.

| run | idle p50 / p99 | under load p50 / p99 / max | load GETs/s | value cache over the load |
|---|---|---|---|---|
| ro-r1 | 1.7 / 6.4 ms | 7.6 / 12.1 / 29.8 ms | 4 315 | 424 895 hits, 0 misses |
| ro-r2 | 1.5 / 3.8 ms | 7.6 / 9.4 / 17.6 ms | 4 340 | 427 635 hits, 0 misses |
| ro-r3 | 1.5 / 2.9 ms | 6.9 / 9.8 / 26.4 ms | 4 734 | 466 019 hits, 0 misses |

Over the member's day (12:48Z start, real cluster traffic plus #101's and
this run's), its own counters: value cache 1 056 199 hits to 1 873 misses
(99.8%); store time of a single-key read, hit: 99.5% under 1 ms, 99.9%
under 5 ms; miss (22): all under 10 ms, 21 under 5 ms.

RSS could not be read on the node: fastetcd did not export it before 1.24
(`process_resident_memory_bytes`, #84).
