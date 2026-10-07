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
