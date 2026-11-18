# Raft apply loop on a dedicated thread? (fastetcd#194)

**Question.** The raft apply loop (openraft's state-machine worker) runs
on the shared tokio runtime, with the proposer, RaftCore, watch fan-out
and every gRPC stream. Would moving it to a thread of its own improve
write latency under a busy watch load? The issue's rule: build it only
if the shared runtime shows up in write p99.

**Answer: no, not built.** Under 1000 watch streams the runtime's
scheduling delay does grow (2 ms mean, p99 between 10 and 25 ms, max
31–37 ms). But a dedicated apply thread removes only the apply task's
own wait, one of the several runtime hops a write takes (gRPC handler,
proposer, RaftCore, log flush, apply, response). The gain is bounded by
that one hop: about 2 ms of a 35–54 ms p50, and at most 10–25 ms of a
95–162 ms p99. The p99 is set by the WAL fsyncs. On the 3-member runs,
single fsync stalls of 1.2 s and 8.9 s on the build VM's disk set the
worst runs. The cost would be a second executor for the state machine
and channels both ways for every apply.

What remains is the probe:
`fastetcd_runtime_schedule_delay_seconds` (histogram) and
`fastetcd_runtime_schedule_delay_max_seconds`. If it later shows a tail
that is a real part of the write latency on a node, this question is
worth asking again with that node's numbers.

## How it was measured

- **Probe** (`crates/server/src/runtime_probe.rs`). A plain OS thread
  spawns an empty task on the runtime every 10 ms and records the time
  from the spawn to the task's first poll. That is the wait any woken
  task has, the apply task included.
- **Load.** `fastetcd-bench --mode put --conns 64 --total 20000 --watchers N`
  (256 B values on 10000 keys under `/bench/`). N watch streams on the
  prefix drain every event, a connection per 100 streams, opened before
  the puts. Every run delivered every event (N × 19968), and no watcher
  was cancelled or resynced.
- **Harness.** `tests/watch_load.sh` (through `tests/read_latency.sh`,
  `METRICS=1`). It ran on one sc-build VM (8 GB) at e1ed85d (1.39.1 +
  the probe), 2026-11-18, with data dirs on the VM's disk. The bench
  client runs on the same VM as the members, so its CPU, and that of
  1000 watch streams, competes with theirs. On 3 members the bench runs
  twice, against member 1's and member 2's client ports. The probe
  covers both runs and the start.
- One run of each. Treat differences of a few ms as noise.

## Results

Write latency (ms) and throughput, per bench run:

| Members | Watchers | Port | ops/s | p50 | p90 | p99 | max |
|---|---|---|---|---|---|---|---|
| 1 | 0 | 1 | 2500 | 23.1 | 37.6 | 67.6 | 94.7 |
| 1 | 100 | 1 | 1995 | 28.1 | 46.1 | 96.2 | 124.4 |
| 1 | 1000 | 1 | 1627 | 34.7 | 56.4 | 94.8 | 138.2 |
| 3 | 0 | 1 | 1267 | 42.6 | 77.2 | 110.5 | 386.8 |
| 3 | 0 | 2 | 275 | 28.7 | 65.5 | 3269 | 15618 (fsync stall 8.9 s) |
| 3 | 100 | 1 | 359 | 90.3 | 394.1 | 1122.7 | 1763 (fsync stall 1.2 s) |
| 3 | 100 | 2 | 1251 | 43.9 | 87.5 | 138.1 | 197.0 |
| 3 | 1000 | 1 | 1069 | 53.7 | 91.2 | 162.1 | 210.1 |
| 3 | 1000 | 2 | 1168 | 44.8 | 94.0 | 146.8 | 179.1 |

Runtime scheduling delay (the probe) over each case, per member:

| Members | Watchers | Member | samples | mean | ≤ 1 ms | ≤ 10 ms | ≤ 25 ms | max |
|---|---|---|---|---|---|---|---|---|
| 1 | 0 | 1 | 1185 | 0.13 ms | 98.9% | 99.9% | 100% | 13.2 ms |
| 1 | 100 | 1 | 1386 | 0.14 ms | 98.2% | 100% | 100% | 6.7 ms |
| 1 | 1000 | 1 | 1651 | 1.97 ms | 66.2% | 96.7% | 99.9% | 30.6 ms |
| 3 | 0 | 1 / 2 / 3 | ~9316 | 0.08 ms | 99.8% | 100% | 100% | 3.0 / 2.2 / 4.9 ms |
| 3 | 100 | 1 / 2 / 3 | ~7314 | 0.22–0.25 ms | 95.7–96.1% | 99.8–99.9% | 100% | 33.2 / 15.8 / 48.9 ms |
| 3 | 1000 | 1 / 2 | ~4083 | 1.40–1.44 ms | 79.7% | 95.8–95.9% | 99.9% | 36.5 / 34.8 ms |
| 3 | 1000 | 3 | 4086 | 0.13 ms | 97.4% | 100% | 100% | 6.8 ms |

In the 3-member, 1000-watcher case, member 3 served no watchers. Its
delay stayed at the idle level while members 1 and 2 (the ones the
watchers were on) rose to about 1.4 ms mean. So the delay comes from
the watch fan-out on the member that serves it. That points at the
fan-out's own cost, not at where the apply task runs.
