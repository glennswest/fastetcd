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

## RSS and lease renewals over four hours (#84), pvetest2

No X9 blade could run fastetcd >= 1.24 on 2026-10-13 (X9's last good
release is 11.88; 11.99 and 12.14 did not install there), so the long
run was done first on **pvetest2**: a pve VM installed with release
12.21-flowsdn (**fastetcd 1.31.0**, one member, the node's real store,
kube-controller-manager, kube-scheduler and stormcos's own leases
renewing in kube-system). Leased, 2026-10-13 21:43Z–01:43Z.

- `tests/bench/blade_rss.py <node> 4 60`: fastetcd's `/metrics` (:2381)
  every 60 s, every kube-system Lease every second (renewals, gaps,
  holder changes).
- Every 30 min a `tests/bench/blade_getlatency.py` round with writes on:
  200 idle GETs of a Lease, then 120 s of 40 clients renewing Leases
  through the apiserver (~600–1 150 PUTs/s) while the probe GETs.
- Defaults: `--value-cache-bytes` 128 MiB, `--engine-cache-bytes`
  256 MiB. Raw data: `data/rss-pvetest2/`.

**RSS** (240 samples, one process start throughout, open fds 21–23):

| | min | max | mean, first hour | mean, last hour |
|---|---|---|---|---|
| RSS | 70.3 MiB | 381.8 MiB | 158.6 MiB | 202.9 MiB |
| write-behind layers | 0 | 65.7 MiB | 8.4 MiB | 17.7 MiB |
| key index | 0.7 MiB | 5.1 MiB | | |
| value cache | 0.5 MiB | 1.2 MiB | | |
| data file | 145 MiB | 290 MiB | | 290 MiB |

- RSS stays inside its budget (value cache 128 + engine cache 256 +
  index ~5 = ~389 MiB): the maximum, 381.8 MiB, is the closest it came.
- It is not flat minute to minute: it swings 100–250 MiB between the
  apiserver's compactions (it follows the key index, correlation 0.83:
  the history the load writes, dropped at each compaction) and with the
  write-behind layers (0.47).
- Its floor rose each hour: 70, 78, 88, 102 MiB. The data file stopped
  growing in the first hour (290 MiB), and redb's page cache, which fills
  with the pages read up to `--engine-cache-bytes`, is the likely cause,
  but redb does not report its cache size, so this run cannot tell a
  cache filling from a leak. Whether the floor levels off below the budget
  is what the blade's longer run has to show.

**Lease renewals** (kube-system, polled each second), no holder change,
no poll error:

| lease | renewals | longest gap | leaseDuration |
|---|---|---|---|
| kube-controller-manager | 7 111 | 9.0 s | 15 s |
| kube-scheduler | 7 114 | 3.2 s | 15 s |
| stormipmi | 2 866 | 5.6 s | 15 s |
| stormupdate | 1 440 | 10.1 s | 45 s |
| vmimages | 480 | 30.1 s | 90 s |

There is no cilium-operator on a flowsdn node; every lease that is there
was renewed in time throughout, the load rounds included.

**GETs with writes going on** (8 rounds): idle GET p99 8.2–43.9 ms
(median of rounds 13.5); under the renewal load GET p50 0.9–9.5 ms,
p99 18.6–53.9 ms, renewal PUT p50 19–38 ms, p99 62–196 ms, max 1.26 s.
This is a VM disk shared with other test machines, not the spinning
disk the acceptance is about; server3's read-only numbers above stay the
reference for the 20 ms target.

That run on an X9 blade follows.

## RSS, GETs and lease renewals over six hours (#84), X9 blade server1

**server1** (X9 blade, one 7200 rpm disk), 2026-10-29 14:47–20:47Z,
leased. Release 12.58's install had just failed on it (a flowsdn image,
not fastetcd), leaving the node serving with 12.58's fastetcd: commit
1579bfa (1.36.1 plus #176's quick-repair commits), one member, cilium,
the node's real store. Same tools as on pvetest2: `blade_rss.py <node> 6
60`, and a `blade_getlatency.py` round with writes on every 30 min (12
rounds; 40 clients renewing Leases, ~170–270 PUTs/s here). Defaults for
the caches. Raw data: `data/rss-server1/`.

**RSS** (360 samples, one process start, open fds 21, data file 145 MiB
throughout, key index ~440 keys / 0.6–1.0 MiB, value cache ~1 MiB):

| hour | min | median | max |
|---|---|---|---|
| 1 | 64 MiB | 96 MiB | 230 MiB |
| 2 | 86 MiB | 109 MiB | 179 MiB |
| 3 | 93 MiB | 116 MiB | 167 MiB |
| 4 | 93 MiB | 112 MiB | 174 MiB |
| 5 | 95 MiB | 121 MiB | 172 MiB |
| 6 | 98 MiB | 125 MiB | 209 MiB |

The floor rose in the first two hours and then held at 93–98 MiB (+5 MiB
over four hours), with the data file not growing: a cache filling, then
full, not a leak. The maximum, 230 MiB, is far inside the budget (value
cache 128 + engine cache 256 + index ~1 MiB). The swings follow the
write-behind layers (up to 67 MiB before a checkpoint).

**GETs** (single object, through the apiserver): idle p99 2.8–12.3 ms;
under the renewal load p50 1.8–2.0 ms, p99 14.6–18.5 ms (target < 20 ms:
met, now with writes going on), max 0.15–3.2 s; value cache 99.96% hits.

**Writes** are where the disk shows: in the load rounds a lease PUT took
p50 60–80 ms, p99 0.9–4.1 s, max 5.8–14.7 s (WAL fsyncs mean 40–72 ms,
~15 proposals per fsync, checkpoints using 6–30 s of each 120 s).

**Lease renewals** (kube-system, polled each second, no poll error):

| lease | renewals | longest gap | leaseDuration | gaps > 15 s (in load rounds) |
|---|---|---|---|---|
| cilium-operator-resource-lock | 6 945 | 377 s | 15 s | 53 (13) |
| kube-controller-manager | 8 155 | 49 s | 15 s | 40 (8) |
| kube-scheduler | 8 150 | 49 s | 15 s | 39 (8) |
| stormipmi | 3 643 | 48 s | 15 s | 42 (9) |
| snapshot-controller-leader | 5 263 | 151 s | 15 s | 21 (5) |
| stormupdate | 2 124 | 48 s | 45 s | |
| vmimages | 719 | 75 s | 90 s | |

Not on time: cilium-operator's lock changed holder 84 times. The late
renewals fall at the same moments for every lease, mostly outside the
load rounds, and at those moments the store took almost no writes (24
and 29 puts in the minutes at 17:14Z and 17:25Z, 500–800 around them):
store-wide write stalls of ~20–60 s on the node's ordinary traffic. Over
the process's life the longest WAL fsync was 17.8 s and the longest
checkpoint commit 96.6 s. Reads did not stall. Filed as #180 (with #135,
the checkpoint and the WAL sharing the spindle).

What #84 asked, on this blade: GET p99 < 20 ms on a spinning disk, yes
(also with writes); RSS flat, yes after the first two hours, inside its
budget; cilium-operator lease renewals on time, no, because of write
stalls (#180), not the read cache #82 added.
