# Upstream etcd vs fastetcd v1.11 and v1.12

*fastetcd#90, measured 2026-10-03. Reproduce with `tests/bench/compare.sh v3.7.2`;
raw results in [`data/`](data/).*

The owner's question: *how does fastetcd compare with the real etcd, and does
it do better on a slow disk?* Three servers, the same machine and disk each
time, one at a time:

- **etcd v3.7.2**, the official `etcd-v3.7.2-linux-amd64.tar.gz` (sha256
  `3a3679bc51a4ee9d30bccea1da7cd4fe62c6fc1d2ca1255068d2c53bf3026135`, checked
  against the release's `SHA256SUMS`), default flags;
- **fastetcd v1.11.0** (5405914): the raft log in redb, RAM key index and value
  cache (#82);
- **fastetcd v1.12.0** (7851f73): the raft log in a sequential WAL, applies held
  in RAM and checkpointed into redb in the background (#85).

on three disks:

| disk | machine | filesystem | fsync rate (fio 4k write+fsync) |
|---|---|---|---|
| **dev-ssd** | the build box (16 cores, 63 GiB), shared with other builds | ext4 on LVM thin, SSD | not measured (SSD; fastetcd's own WAL fsyncs averaged ~3.5 ms) |
| **pve-ssd** | pve VM, 4 vCPU, 8 GiB | btrfs on local-lvm (SSD) | ~116/s, p50 16 ms |
| **pve-slow** | pve VM, 4 vCPU, 8 GiB | btrfs on sas-lvm-thin, throttled to 150 IOPS / 120 MB/s | **~6/s, p50 ~120 ms**, slower than a 7200 rpm disk; the X9 blades' spinning disks are closer to this than to the SSDs |

## The short answer

**For a Kubernetes control plane, fastetcd v1.12 is the better datastore on
every disk measured, and by the widest margin on the slow one.** With
rustkube's apiserver and 40 clients renewing Leases and listing (the shape
of kubelets and controllers), a single-object GET under load:

| apiserver GET of a Lease under load, p99 | etcd v3.7.2 | fastetcd v1.11.0 | fastetcd v1.12.0 |
|---|---:|---:|---:|
| dev-ssd | 49.8 ms | 75.0 ms | **4.5 ms** |
| pve-ssd | 178.7 ms | 43.2 ms | **1.4 ms** |
| pve-slow | 835.2 ms | 293.8 ms | **1.0 ms** |
| background renewal loops/s: dev-ssd | 961 | 669 | **1 802** |
| ... pve-ssd | **520** | 274 | 463 |
| ... pve-slow | **82** | 27 | 74 |

On the slow disk etcd's reads queue behind its fsyncs (most likely: a
linearizable read waits for the leader to apply up to its read index, and
applying waits on the disk): its own
linearizable Range p99 was 835 ms. fastetcd v1.12 answers the same read in
0.1 ms, from RAM, at nearly the same write rate (74 vs 82 renewal loops/s).

**For raw write throughput, etcd is still ahead, and the gap grows as the
disk slows.** etcd's `benchmark`, 1000 clients:

| puts/s, 1000 clients | etcd v3.7.2 | fastetcd v1.11.0 | fastetcd v1.12.0 |
|---|---:|---:|---:|
| dev-ssd, 1 member | 15 126 / 15 884 | 369 / 341 | 4 882 / 4 477 |
| pve-ssd, 1 member | 20 264 | 443 | 5 077 |
| pve-slow, 1 member | 8 786 | 94 | 1 514 |
| pve-slow, 3 members | 1 542 | 29 | 909 |

v1.12 is 10–30x v1.11, and matches or beats etcd on single-put
transactions in three of four SSD runs (txn-put, 1000 clients: dev-ssd
16 307 / 17 792 vs 13 679 / 17 718 per second, 3 members 9 985 vs 6 793,
pve-ssd 3 members 6 953 vs 6 414; but pve-ssd 1 member 12 294 vs 17 797).
On the slow disk etcd carries ~1 460 writes per fsync to fastetcd's ~250,
which matches the cap fastetcd's proposer puts on what one fsync can carry
(#94).

So, to the question *"will ours work better with a slower disk?"*:
**reads, yes, by orders of magnitude; writes, not yet.** By the numbers
the write ceiling on a slow disk is the proposer's cap (#94), not the WAL
or the RAM layer; raising it is the next step, and the measurement that
confirms or refutes that.

### Where fastetcd wins

- **Reads under write load**, any disk: linearizable and serializable reads
  are served from RAM (#82) and do not wait on the WAL (#85). Through the
  apiserver, 10–800x lower p99 under load (table above). Direct
  linearizable Range p99 with 1000 clients: 12–49 ms vs etcd's 29–104 ms.
  Range *throughput* is about even: etcd is ahead on dev-ssd's 16 cores
  (108–158k vs 79–97k/s), fastetcd on the 4-vCPU VMs (103–108k vs
  79–89k/s, 1 member).
- **Watch creation** p99: 1.1–3.7 ms vs etcd's 1.7–68 ms (etcd is ahead
  only on pve-ssd, 1 member).
- **Restart after a crash** on the fast disks: first linearizable read
  0.1–0.7 s after a SIGKILL vs etcd's 0.7–1.1 s (v1.12 once took 4.1 s).
  On the slow disk v1.12 is slower than etcd (1.3 / 3.4 s vs 1.1 / 2.0 s,
  1 / 3 members): it replays the applies its last checkpoint missed.
- **Memory at idle**: 14–16 MiB per member vs etcd's 28–30 MiB.
- **Bytes to disk per write vs v1.11**: 40–100x fewer (v1.12 2–23 MiB per
  1000 puts across all runs; v1.11 150–950 MiB).

### Where etcd wins

- **Write throughput** with many clients: 1.3–6x v1.12's puts depending on
  disk and cluster size; widest on the slow disk with 1 member (#94).
- **Lease keepalive**: 43–126k/s vs v1.12's 7–12k/s on the SSDs (28k vs
  0.8k on the slow disk, 1 member). fastetcd puts every renewal through Raft; etcd renews
  on the leader in memory (#92).
- **CPU per write**: 0.04–0.12 s per 1000 puts vs v1.12's 0.11–0.67.
- **Bytes to disk per write**: etcd 0.7–3.1 MiB per 1000 puts vs v1.12's
  2–23 MiB (the WAL's zero-filled segments and the background checkpoints
  are in that number).
- **Memory under load**: etcd's RSS stays under ~90 MiB per member;
  fastetcd fills its budgeted caches (redb page cache 256 MiB, value cache
  up to 128 MiB, write-behind 64 MiB, WAL cache 64 MiB) to 200–700 MiB per
  member. These are configurable; etcd leans on the kernel's page cache,
  which RSS does not count.

### The write-p99 tail

- **Single client**, every server: p99.9 of 0.1–2.5 s on dev-ssd, from
  fsync stalls of the shared build disk (seen as a
  `fastetcd_wal_fsync_max_seconds` of 1.47 s in a v1.12 run, and in etcd's
  own p99.9 of 1.7–2.5 s). Not a difference between the servers.
- **1000 clients**: with 1 member on the SSDs, v1.12's put p99 is 2–3x
  etcd's (359–525 ms vs 154–173 ms); with 3 members it is 1.5x on pve-ssd
  (429 vs 288 ms) and half etcd's on dev-ssd (439 vs 908 ms). Its txn-put
  p99 is lower than etcd's in four of five SSD runs (120–308 ms vs
  170–404 ms; pve-ssd 1 member 304 vs 170). Under sustained writes
  v1.12's write-behind can run into its budget and wait for a
  checkpoint's fsync (#93); its plain put is also oddly ~3.5x slower than
  its txn-put on dev-ssd (#93).
- **v1.11 under 1000 writers** is not usable: 5–60 s p99, and on 3 members
  it lost its leader under the load (twice on the slow disk).

### Durability

16 writers, every member SIGKILLed mid-write, then restarted:
**no server lost an acknowledged write**, in 21 crash-restarts (each
server, on every disk and cluster size, plus a second dev-ssd round).
This is a process crash: the kernel's page cache survives it. A power cut is not tested here; stormcentral's power-cut stage
on the X9 hardware is #89.

## Method

`tests/bench/compare.sh <etcd-version>` does everything, unprivileged:

1. downloads the etcd release tarball and its `SHA256SUMS`, checks the
   tarball;
2. downloads Go 1.26.8 (what etcd v3.7.2's `go.mod` asks for, sha256 from
   dl.google.com) and builds etcd's own `benchmark` tool from the
   `v3.7.2` tag (`go build ./tools/benchmark`);
3. builds fastetcd at each tag (`cargo build --release --locked`), and
   this tree's `fastetcd-bench`;
4. for each cluster size and contender, on a fresh data directory: starts
   the members on 127.0.0.1, waits for a linearizable read, runs the
   workloads below one after another on the same cluster, stops it;
5. writes `bench.csv`, `resources.csv`, `durability.csv`, `k8s.csv`.

`tests/bench/report.py` builds the tables below from the CSVs.

**etcd's `benchmark`** (`--precise`, `--endpoints` = every member):

| workload | command |
|---|---|
| put, 1 client | `--conns=1 --clients=1 put --key-size=8 --sequential-keys --total=2000 --val-size=256` |
| put, 1000 clients | `--conns=100 --clients=1000 put --key-size=8 --sequential-keys --total=20000 --val-size=256` |
| range, 1000 clients | `--conns=100 --clients=1000 range foo --consistency=l` (and `=s`) `--total=100000` |
| txn-put, 1000 clients | `--conns=100 --clients=1000 txn-put --txn-ops=1 --key-size=8 --val-size=256 --key-space-size=100000 --total=20000` |
| watch | `--conns=10 --clients=10 watch --streams=10 --watch-per-stream=100 --watched-key-total=100 --put-total=2000` |
| lease keepalive | `--conns=100 --clients=1000 lease-keepalive --total=50000` |

These follow etcd's own performance guide. On pve-slow the write sizes
were smaller (`PUT1C_TOTAL=200 PUT_TOTAL=10000 KEEPALIVE_TOTAL=10000
WATCH_PUTS=1000`) and, for 3 members, each workload was cut off after
300 s (`BENCH_TIMEOUT=300`): a rate written `~x` is what it reached in
that time. Rates are per second either way, so the disks compare.

The watch workload reports two things: the latency of creating the 1000
watches, and the rate events were delivered. The tool's per-event
"latency" times its own receive loop, not delivery, and is not reported.

**Resources** are measured around the 1000-client put, summed over the
members: RSS (`/proc/<pid>/status`, sampled every 0.5 s), CPU time
(`utime + stime`), `write_bytes` (bytes sent to the storage layer) and
`wchar` (bytes passed to write syscalls) from `/proc/<pid>/io`.

**Durability**: `fastetcd-bench --mode durability-write` (16 clients, each
putting `/dur/<client>/<n>` for n = 0, 1, ...; a put counts once its
response arrived) runs for 8 s, then every member gets SIGKILL; the
members restart on the same data and `--mode durability-check` reports the
time to the first linearizable read and how many acknowledged keys are
missing.

**Kubernetes-shaped**: rustkube 8123b3e's `test/e2e/get-latency.sh`, a
real kube-apiserver (release build) on each datastore (etcd behind a
wrapper that maps fastetcd's flags to its own): GET/LIST latency of a
Lease and a CRD for a ServiceAccount and for system:masters, idle and
while 40 identities renew their own Leases and list; the datastore's own
Range latency through the v3 JSON gateway; the background renewal rate.

### What these numbers are not

- **One machine, loopback.** Members of a "3-member cluster" share one
  host and one disk, so a 3-member run measures replication and fsyncs
  contending for the same device, not a network.
- **dev-ssd is a shared build box.** Other builds run beside the
  benchmark; two rounds of the 1-member run are shown to give the spread.
  The pve VMs were dedicated.
- **btrfs on the VMs**, ext4 on dev. Same for every contender on a disk.
- **Default flags** for each server. fastetcd's caches are sized by flags
  (`--engine-cache-bytes`, `--value-cache-bytes`, `--write-behind-bytes`,
  `--wal-cache-bytes`); etcd's `--snapshot-count` default (10000) differs
  from fastetcd's (5000).
- **The first workload after start** sometimes catches a disk stall (a
  1.4–3.1 s maximum on single-client puts, for etcd and fastetcd alike).

## Results

Generated by `tests/bench/report.py` from [`data/`](data/). "a / b" is two rounds of the same run; "–" is a value the tool did not print (no p99.9 under 1000 samples) or a run that failed.

## dev-ssd

### dev-ssd, 1 member

Throughput and p99 latency (etcd's `benchmark`):

| workload | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| put, 1 client | 103 / 102/s, p99 68.2 / 53.9 ms | 106 / 129/s, p99 61.7 / 55.3 ms | 201 / 175/s, p99 19.8 / 35.6 ms |
| put, 1000 clients | 15 126 / 15 884/s, p99 170.6 / 154.2 ms | 369 / 341/s, p99 5758.4 / 7474.7 ms | 4 882 / 4 477/s, p99 379.7 / 524.8 ms |
| txn-put, 1000 clients | 13 679 / 17 718/s, p99 225.6 / 195.4 ms | 1 664 / 1 503/s, p99 785.1 / 1290.4 ms | 16 307 / 17 792/s, p99 127.8 / 120.1 ms |
| range linearizable, 1000 clients | 107 685 / 157 989/s, p99 29.4 / 49.8 ms | 77 231 / 94 428/s, p99 16.7 / 13.1 ms | 79 417 / 96 533/s, p99 16.9 / 12.2 ms |
| range serializable, 1000 clients | 150 342 / 236 732/s, p99 26.4 / 19.3 ms | 74 611 / 90 414/s, p99 19.2 / 14.6 ms | 82 606 / 92 712/s, p99 15.7 / 13.4 ms |
| lease keepalive, 1000 clients | 125 772 / 122 621/s, p99 21.3 / 24.8 ms | 2 009 / 1 935/s, p99 904.9 / 708.5 ms | 12 441 / 11 724/s, p99 138.3 / 141.9 ms |
| watch create, 1000 watches | 34 368 / 36 831/s, p99 14.8 / 13.6 ms | 88 438 / 90 758/s, p99 1.0 / 1.8 ms | 84 418 / 67 829/s, p99 2.0 / 1.8 ms |
| watch events delivered | 710 360 / 936 646 ev/s | 305 618 / 328 912 ev/s | 594 205 / 736 780 ev/s |

Latency p50 / p99 / p99.9, ms (rounds separated by ·):

| workload | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| put, 1 client | 3.2 / 68.2 / 1695.4 · 3.2 / 53.9 / 2517.3 | 5.0 / 61.7 / 709.8 · 5.1 / 55.3 / 98.3 | 2.9 / 19.8 / 1463.0 · 3.1 / 35.6 / 874.0 |
| put, 1000 clients | 57.3 / 170.6 / 174.6 · 44.2 / 154.2 / 154.7 | 2366.0 / 5758.4 / 5759.4 · 2369.0 / 7474.7 / 7475.5 | 186.9 / 379.7 / 380.9 · 201.7 / 524.8 / 525.8 |
| txn-put, 1000 clients | 50.1 / 225.6 / 226.4 · 44.2 / 195.4 / 198.8 | 606.0 / 785.1 / 785.9 · 659.4 / 1290.4 / 1291.6 | 51.6 / 127.8 / 128.7 · 49.9 / 120.1 / 121.5 |
| range linearizable, 1000 clients | 7.3 / 29.4 / 41.7 · 4.8 / 49.8 / 57.2 | 12.8 / 16.7 / 19.4 · 10.5 / 13.1 / 14.2 | 12.4 / 16.9 / 19.6 · 10.2 / 12.2 / 14.4 |
| range serializable, 1000 clients | 3.7 / 26.4 / 39.2 · 2.2 / 19.3 / 24.4 | 13.1 / 19.2 / 22.1 · 10.8 / 14.6 / 17.5 | 12.0 / 15.7 / 17.8 · 10.6 / 13.4 / 14.6 |
| lease keepalive, 1000 clients | 5.1 / 21.3 / 23.4 · 4.3 / 24.8 / 29.4 | 476.9 / 904.9 / 925.9 · 508.2 / 708.5 / 728.7 | 78.3 / 138.3 / 144.7 · 83.3 / 141.9 / 163.4 |
| watch create, 1000 watches | 0.1 / 14.8 / 15.4 · 0.1 / 13.6 / 13.7 | 0.1 / 1.0 / 1.5 · 0.1 / 1.8 / 2.0 | 0.1 / 2.0 / 2.3 · 0.1 / 1.8 / 2.0 |

Resources (summed over members):

|  | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| RSS idle, MiB | 29 / 30 | 14 / 15 | 16 / 16 |
| RSS peak under the 1000-client put, MiB | 86 / 83 | 318 / 316 | 684 / 702 |
| CPU seconds per 1000 puts | 0.054 / 0.058 | 0.342 / 0.338 | 0.217 / 0.228 |
| bytes to storage per 1000 puts, KiB | 705 / 673 | 327 649 / 328 271 | 7 890 / 7 593 |
| bytes written by syscalls per 1000 puts, KiB | 763 / 737 | 318 885 / 318 885 | 2 840 / 2 786 |

Durability (every member SIGKILLed mid-write, 16 writers):

|  | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| acknowledged puts before SIGKILL | 6 467 / 8 163 | 3 499 / 3 537 | 7 010 / 4 207 |
| acknowledged puts missing after restart | 0 / 0 | 0 / 0 | 0 / 0 |
| restart to first linearizable read, ms | 903 / 750 | 154 / 104 | 309 / 4 056 |

### dev-ssd, 3 members

Throughput and p99 latency (etcd's `benchmark`):

| workload | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| put, 1 client | 94/s, p99 75.4 ms | 59/s, p99 89.1 ms | 55/s, p99 98.2 ms |
| put, 1000 clients | 4 904/s, p99 908.1 ms | 143/s, p99 16439.9 ms | 3 861/s, p99 439.3 ms |
| txn-put, 1000 clients | 6 793/s, p99 367.3 ms | 630/s, p99 5106.7 ms | 9 985/s, p99 284.8 ms |
| range linearizable, 1000 clients | 128 551/s, p99 71.6 ms | 66 069/s, p99 23.6 ms | 63 815/s, p99 24.4 ms |
| range serializable, 1000 clients | 199 366/s, p99 17.5 ms | 168 771/s, p99 18.2 ms | 153 970/s, p99 17.7 ms |
| lease keepalive, 1000 clients | 71 587/s, p99 49.9 ms | 1 139/s, p99 1665.2 ms | 11 566/s, p99 179.8 ms |
| watch create, 1000 watches | 95 640/s, p99 2.0 ms | 115 714/s, p99 2.1 ms | 84 358/s, p99 2.3 ms |
| watch events delivered | 377 450 ev/s | 125 656 ev/s | 443 266 ev/s |

Latency p50 / p99 / p99.9, ms (rounds separated by ·):

| workload | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| put, 1 client | 4.0 / 75.4 / 875.5 | 11.5 / 89.1 / 120.0 | 7.0 / 98.2 / 1821.9 |
| put, 1000 clients | 164.9 / 908.1 / 944.0 | 5989.4 / 16439.9 / 16442.0 | 240.0 / 439.3 / 444.2 |
| txn-put, 1000 clients | 140.0 / 367.3 / 391.3 | 1452.9 / 5106.7 / 5111.0 | 77.7 / 284.8 / 308.4 |
| range linearizable, 1000 clients | 5.2 / 71.6 / 105.4 | 14.9 / 23.6 / 29.3 | 15.5 / 24.4 / 33.5 |
| range serializable, 1000 clients | 2.5 / 17.5 / 27.1 | 4.8 / 18.2 / 24.6 | 5.3 / 17.7 / 32.9 |
| lease keepalive, 1000 clients | 7.6 / 49.9 / 63.0 | 838.1 / 1665.2 / 2038.7 | 82.1 / 179.8 / 185.9 |
| watch create, 1000 watches | 0.1 / 2.0 / 2.2 | 0.1 / 2.1 / 2.4 | 0.1 / 2.3 / 2.5 |

Resources (summed over members):

|  | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| RSS idle, MiB | 93 | 47 | 51 |
| RSS peak under the 1000-client put, MiB | 219 | 1 089 | 2 156 |
| CPU seconds per 1000 puts | 0.123 | 1.008 | 0.669 |
| bytes to storage per 1000 puts, KiB | 2 173 | 954 106 | 23 399 |
| bytes written by syscalls per 1000 puts, KiB | 3 898 | 940 208 | 9 481 |

Durability (every member SIGKILLed mid-write, 16 writers):

|  | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| acknowledged puts before SIGKILL | 2 026 | 1 710 | 3 589 |
| acknowledged puts missing after restart | 0 | 0 | 0 |
| restart to first linearizable read, ms | 953 | 156 | 670 |

### dev-ssd: Kubernetes (rustkube apiserver, 1 member)

p50 / p99 ms per request; the last row is the background load's rate:

|  | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| idle: sa GET lease | 0.3 / 1.3 | 0.3 / 1.2 | 0.2 / 1.4 |
| idle: sa LIST leases | 0.3 / 0.9 | 0.2 / 0.9 | 0.2 / 1.1 |
| idle: sa GET crd | 0.3 / 1.4 | 0.2 / 0.8 | 0.3 / 1.1 |
| idle: admin GET lease | 0.3 / 1.2 | 0.2 / 1.2 | 0.2 / 1.3 |
| idle: admin LIST leases | 0.3 / 1.2 | 0.2 / 1.3 | 0.2 / 0.9 |
| idle: store linearizable | 0.2 / 1.4 | 0.1 / 0.4 | 0.1 / 0.3 |
| idle: store serializable | 0.2 / 0.7 | 0.1 / 0.5 | 0.1 / 0.1 |
| load: sa GET lease | 9.1 / 49.8 | 0.4 / 75.0 | 0.3 / 4.5 |
| load: sa LIST leases | 9.2 / 104.8 | 0.2 / 8.8 | 0.3 / 5.2 |
| load: sa GET crd | 9.8 / 88.4 | 0.2 / 8.2 | 0.2 / 3.7 |
| load: admin GET lease | 8.1 / 191.4 | 0.3 / 5.6 | 0.2 / 0.6 |
| load: admin LIST leases | 10.1 / 74.2 | 0.3 / 7.4 | 0.2 / 5.5 |
| load: store linearizable | 8.3 / 99.0 | 0.1 / 0.3 | 0.1 / 2.5 |
| load: store serializable | 0.2 / 3.5 | 0.1 / 0.1 | 0.1 / 2.7 |
| load: 40 clients renew loops/s | 961 | 669 | 1 802 |

## pve-ssd

### pve-ssd, 1 member

Throughput and p99 latency (etcd's `benchmark`):

| workload | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| put, 1 client | 90/s, p99 81.4 ms | 76/s, p99 67.6 ms | 92/s, p99 78.3 ms |
| put, 1000 clients | 20 264/s, p99 173.2 ms | 443/s, p99 5571.9 ms | 5 077/s, p99 358.7 ms |
| txn-put, 1000 clients | 17 797/s, p99 170.3 ms | 1 497/s, p99 907.2 ms | 12 294/s, p99 304.2 ms |
| range linearizable, 1000 clients | 89 233/s, p99 32.2 ms | 104 912/s, p99 18.0 ms | 102 866/s, p99 17.8 ms |
| range serializable, 1000 clients | 98 575/s, p99 27.6 ms | 104 309/s, p99 18.5 ms | 104 464/s, p99 20.4 ms |
| lease keepalive, 1000 clients | 72 831/s, p99 30.5 ms | 1 815/s, p99 828.7 ms | 11 118/s, p99 204.1 ms |
| watch create, 1000 watches | 56 267/s, p99 1.7 ms | 67 989/s, p99 3.1 ms | 53 195/s, p99 3.7 ms |
| watch events delivered | 420 851 ev/s | 155 363 ev/s | 162 100 ev/s |

Latency p50 / p99 / p99.9, ms (rounds separated by ·):

| workload | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| put, 1 client | 6.8 / 81.4 / 382.6 | 9.2 / 67.6 / 129.1 | 7.5 / 78.3 / 175.7 |
| put, 1000 clients | 36.1 / 173.2 / 178.7 | 998.1 / 5571.9 / 5574.1 | 176.8 / 358.7 / 360.2 |
| txn-put, 1000 clients | 40.0 / 170.3 / 171.7 | 659.0 / 907.2 / 909.2 | 64.4 / 304.2 / 309.7 |
| range linearizable, 1000 clients | 9.3 / 32.2 / 38.5 | 9.1 / 18.0 / 26.1 | 9.3 / 17.8 / 21.6 |
| range serializable, 1000 clients | 7.9 / 27.6 / 37.3 | 9.2 / 18.5 / 22.9 | 9.1 / 20.4 / 25.6 |
| lease keepalive, 1000 clients | 8.4 / 30.5 / 51.3 | 540.3 / 828.7 / 838.6 | 77.4 / 204.1 / 257.8 |
| watch create, 1000 watches | 0.1 / 1.7 / 3.5 | 0.1 / 3.1 / 3.4 | 0.1 / 3.7 / 4.4 |

Resources (summed over members):

|  | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| RSS idle, MiB | 28 | 14 | 16 |
| RSS peak under the 1000-client put, MiB | 71 | 302 | 324 |
| CPU seconds per 1000 puts | 0.039 | 0.432 | 0.190 |
| bytes to storage per 1000 puts, KiB | 867 | 320 609 | 3 197 |
| bytes written by syscalls per 1000 puts, KiB | 760 | 318 885 | 2 743 |

Durability (every member SIGKILLed mid-write, 16 writers):

|  | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| acknowledged puts before SIGKILL | 4 948 | 1 821 | 2 019 |
| acknowledged puts missing after restart | 0 | 0 | 0 |
| restart to first linearizable read, ms | 1 048 | 103 | 104 |

### pve-ssd, 3 members

Throughput and p99 latency (etcd's `benchmark`):

| workload | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| put, 1 client | 42/s, p99 133.7 ms | 27/s, p99 144.0 ms | 29/s, p99 143.5 ms |
| put, 1000 clients | 7 570/s, p99 288.4 ms | 245/s, p99 8856.1 ms | 4 080/s, p99 428.5 ms |
| txn-put, 1000 clients | 6 414/s, p99 404.0 ms | 477/s, p99 3720.4 ms | 6 953/s, p99 307.6 ms |
| range linearizable, 1000 clients | 71 701/s, p99 47.3 ms | 62 241/s, p99 80.8 ms | 61 147/s, p99 36.8 ms |
| range serializable, 1000 clients | 94 998/s, p99 33.5 ms | 140 243/s, p99 20.0 ms | 129 521/s, p99 19.3 ms |
| lease keepalive, 1000 clients | 43 136/s, p99 56.5 ms | 655/s, p99 2633.0 ms | 7 066/s, p99 227.5 ms |
| watch create, 1000 watches | 32 915/s, p99 2.3 ms | 25 078/s, p99 1.6 ms | 71 982/s, p99 1.3 ms |
| watch events delivered | 103 714 ev/s | 54 208 ev/s | 98 222 ev/s |

Latency p50 / p99 / p99.9, ms (rounds separated by ·):

| workload | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| put, 1 client | 14.6 / 133.7 / 197.7 | 27.0 / 144.0 / 199.6 | 24.8 / 143.5 / 237.4 |
| put, 1000 clients | 121.6 / 288.4 / 296.1 | 4454.2 / 8856.1 / 8861.6 | 216.6 / 428.5 / 501.6 |
| txn-put, 1000 clients | 131.7 / 404.0 / 436.4 | 1887.1 / 3720.4 / 3785.1 | 130.5 / 307.6 / 317.1 |
| range linearizable, 1000 clients | 10.6 / 47.3 / 67.4 | 13.3 / 80.8 / 183.0 | 14.8 / 36.8 / 56.5 |
| range serializable, 1000 clients | 6.7 / 33.5 / 44.5 | 4.6 / 20.0 / 30.0 | 5.2 / 19.3 / 30.5 |
| lease keepalive, 1000 clients | 14.4 / 56.5 / 77.8 | 1505.2 / 2633.0 / 3153.6 | 132.3 / 227.5 / 248.1 |
| watch create, 1000 watches | 0.1 / 2.3 / 16.7 | 0.2 / 1.6 / 4.1 | 0.1 / 1.3 / 2.4 |

Resources (summed over members):

|  | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| RSS idle, MiB | 88 | 45 | 49 |
| RSS peak under the 1000-client put, MiB | 213 | 933 | 1 202 |
| CPU seconds per 1000 puts | 0.082 | 1.284 | 0.485 |
| bytes to storage per 1000 puts, KiB | 2 899 | 933 896 | 9 656 |
| bytes written by syscalls per 1000 puts, KiB | 3 867 | 932 652 | 9 093 |

Durability (every member SIGKILLed mid-write, 16 writers):

|  | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| acknowledged puts before SIGKILL | 1 330 | 922 | 984 |
| acknowledged puts missing after restart | 0 | 0 | 0 |
| restart to first linearizable read, ms | 678 | 205 | 267 |

### pve-ssd: Kubernetes (rustkube apiserver, 1 member)

p50 / p99 ms per request; the last row is the background load's rate:

|  | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| idle: sa GET lease | 0.3 / 1.5 | 0.2 / 1.0 | 0.3 / 1.0 |
| idle: sa LIST leases | 0.3 / 1.0 | 0.2 / 0.7 | 0.3 / 1.0 |
| idle: sa GET crd | 0.3 / 0.9 | 0.2 / 1.3 | 0.4 / 1.3 |
| idle: admin GET lease | 0.3 / 1.0 | 0.2 / 1.1 | 0.3 / 1.1 |
| idle: admin LIST leases | 0.3 / 0.9 | 0.2 / 0.8 | 0.2 / 1.2 |
| idle: store linearizable | 0.2 / 0.6 | 0.1 / 0.3 | 0.1 / 0.3 |
| idle: store serializable | 0.2 / 0.6 | 0.1 / 0.3 | 0.1 / 0.3 |
| load: sa GET lease | 14.5 / 178.7 | 0.2 / 43.2 | 0.2 / 1.4 |
| load: sa LIST leases | 13.8 / 115.3 | 0.2 / 53.7 | 0.2 / 5.3 |
| load: sa GET crd | 19.5 / 308.5 | 0.3 / 46.4 | 0.2 / 3.5 |
| load: admin GET lease | 17.4 / 138.4 | 0.2 / 42.0 | 0.3 / 1.1 |
| load: admin LIST leases | 15.6 / 113.8 | 0.2 / 45.1 | 0.2 / 4.4 |
| load: store linearizable | 12.3 / 123.6 | 0.0 / 10.5 | 0.1 / 0.2 |
| load: store serializable | 0.2 / 3.9 | 0.0 / 0.4 | 0.1 / 0.2 |
| load: 40 clients renew loops/s | 520 | 274 | 463 |

## pve-slow

### pve-slow, 1 member

Throughput and p99 latency (etcd's `benchmark`):

| workload | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| put, 1 client | 15/s, p99 763.7 ms | 8/s, p99 241.5 ms | 8/s, p99 507.0 ms |
| put, 1000 clients | 8 786/s, p99 206.6 ms | 94/s, p99 20114.5 ms | 1 514/s, p99 1845.2 ms |
| txn-put, 1000 clients | 4 554/s, p99 743.7 ms | 29/s, p99 64683.5 ms | 887/s, p99 1997.9 ms |
| range linearizable, 1000 clients | 78 513/s, p99 57.4 ms | 92 770/s, p99 21.3 ms | 108 193/s, p99 17.3 ms |
| range serializable, 1000 clients | 95 950/s, p99 29.5 ms | 94 023/s, p99 22.8 ms | 115 912/s, p99 18.0 ms |
| lease keepalive, 1000 clients | 27 893/s, p99 89.8 ms | 26/s, p99 53765.1 ms | 791/s, p99 1630.8 ms |
| watch create, 1000 watches | 12 006/s, p99 67.7 ms | 40 991/s, p99 10.4 ms | 88 208/s, p99 1.3 ms |
| watch events delivered | 95 425 ev/s | 7 347 ev/s | 13 538 ev/s |

Latency p50 / p99 / p99.9, ms (rounds separated by ·):

| workload | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| put, 1 client | 33.5 / 763.7 / – | 133.9 / 241.5 / – | 112.8 / 507.0 / – |
| put, 1000 clients | 89.3 / 206.6 / 213.1 | 9157.6 / 20114.5 / 20116.8 | 509.9 / 1845.2 / 1848.6 |
| txn-put, 1000 clients | 178.8 / 743.7 / 746.6 | 39665.3 / 64683.5 / 64684.7 | 1082.0 / 1997.9 / 2000.7 |
| range linearizable, 1000 clients | 10.5 / 57.4 / 69.1 | 10.3 / 21.3 / 26.7 | 8.8 / 17.3 / 21.1 |
| range serializable, 1000 clients | 8.1 / 29.5 / 41.1 | 10.1 / 22.8 / 27.5 | 8.2 / 18.0 / 22.7 |
| lease keepalive, 1000 clients | 10.6 / 89.8 / 94.2 | 36044.0 / 53765.1 / 53765.9 | 1033.9 / 1630.8 / 1634.0 |
| watch create, 1000 watches | 0.1 / 67.7 / 68.5 | 0.1 / 10.4 / 10.6 | 0.1 / 1.3 / 1.5 |

Resources (summed over members):

|  | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| RSS idle, MiB | 28 | 15 | 16 |
| RSS peak under the 1000-client put, MiB | 72 | 191 | 194 |
| CPU seconds per 1000 puts | 0.042 | 0.272 | 0.108 |
| bytes to storage per 1000 puts, KiB | 800 | 158 211 | 2 207 |
| bytes written by syscalls per 1000 puts, KiB | 758 | 157 429 | 1 685 |

Durability (every member SIGKILLed mid-write, 16 writers):

|  | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| acknowledged puts before SIGKILL | 938 | 172 | 159 |
| acknowledged puts missing after restart | 0 | 0 | 0 |
| restart to first linearizable read, ms | 1 056 | 416 | 1 295 |

### pve-slow, 3 members

Throughput and p99 latency (etcd's `benchmark`):

| workload | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| put, 1 client | 5/s, p99 713.8 ms | 2/s, p99 1396.7 ms | 4/s, p99 772.8 ms |
| put, 1000 clients | 1 542/s, p99 911.8 ms | 29/s, p99 53144.3 ms | 909/s, p99 2364.6 ms |
| txn-put, 1000 clients | 1 434/s, p99 1352.4 ms | 23/s, p99 44598.9 ms | 690/s, p99 2049.7 ms |
| range linearizable, 1000 clients | 68 970/s, p99 104.1 ms | 65 502/s, p99 34.3 ms | 60 401/s, p99 48.9 ms |
| range serializable, 1000 clients | 100 642/s, p99 28.8 ms | 133 813/s, p99 23.2 ms | 125 980/s, p99 22.4 ms |
| lease keepalive, 1000 clients | 8 264/s, p99 211.2 ms | ~1.8/s, p99 – ms | 658/s, p99 1860.9 ms |
| watch create, 1000 watches | 23 755/s, p99 7.2 ms | –/s, p99 – ms | 72 558/s, p99 1.1 ms |
| watch events delivered | 21 637 ev/s | – ev/s | 12 602 ev/s |

Latency p50 / p99 / p99.9, ms (rounds separated by ·):

| workload | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| put, 1 client | 167.0 / 713.8 / – | 445.9 / 1396.7 / – | 263.0 / 772.8 / – |
| put, 1000 clients | 608.5 / 911.8 / 1012.8 | 19741.0 / 53144.3 / 53151.5 | 987.0 / 2364.6 / 2368.4 |
| txn-put, 1000 clients | 595.9 / 1352.4 / 1800.9 | 1707.3 / 44598.9 / 44601.1 | 1370.4 / 2049.7 / 2052.9 |
| range linearizable, 1000 clients | 10.4 / 104.1 / 133.9 | 13.5 / 34.3 / 69.8 | 14.0 / 48.9 / 64.4 |
| range serializable, 1000 clients | 6.6 / 28.8 / 45.8 | 4.9 / 23.2 / 41.1 | 5.1 / 22.4 / 35.9 |
| lease keepalive, 1000 clients | 16.2 / 211.2 / 229.7 | – / – / – | 1416.6 / 1860.9 / 1865.8 |
| watch create, 1000 watches | 0.1 / 7.2 / 36.5 | – | 0.1 / 1.1 / 2.6 |

Resources (summed over members):

|  | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| RSS idle, MiB | 88 | 46 | 50 |
| RSS peak under the 1000-client put, MiB | 197 | 671 | 696 |
| CPU seconds per 1000 puts | 0.096 | 0.602 | 0.303 |
| bytes to storage per 1000 puts, KiB | 3 143 | 221 874 | 5 148 |
| bytes written by syscalls per 1000 puts, KiB | 4 210 | 242 989 | 5 447 |

Durability (every member SIGKILLed mid-write, 16 writers):

|  | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| acknowledged puts before SIGKILL | 282 | 84 | 143 |
| acknowledged puts missing after restart | 0 | 0 | 0 |
| restart to first linearizable read, ms | 2 038 | 1 109 | 3 389 |

### pve-slow: Kubernetes (rustkube apiserver, 1 member)

p50 / p99 ms per request; the last row is the background load's rate:

|  | etcd-v3.7.2 | fastetcd-v1.11.0 | fastetcd-v1.12.0 |
|---|---:|---:|---:|
| idle: sa GET lease | 0.3 / 41.4 | 0.4 / 1.7 | 0.2 / 1.0 |
| idle: sa LIST leases | 0.3 / 1.2 | 0.4 / 1.2 | 0.2 / 0.9 |
| idle: sa GET crd | 0.3 / 0.8 | 0.2 / 0.9 | 0.2 / 0.7 |
| idle: admin GET lease | 0.3 / 1.2 | 0.2 / 0.8 | 0.2 / 0.7 |
| idle: admin LIST leases | 0.3 / 0.9 | 0.2 / 0.9 | 0.2 / 0.8 |
| idle: store linearizable | 0.2 / 0.5 | 0.1 / 0.1 | 0.1 / 0.3 |
| idle: store serializable | 0.2 / 0.9 | 0.1 / 0.3 | 0.1 / 0.3 |
| load: sa GET lease | 145.0 / 835.2 | 0.3 / 293.8 | 0.2 / 1.0 |
| load: sa LIST leases | 136.4 / 1298.2 | 0.2 / 1.0 | 0.4 / 1.1 |
| load: sa GET crd | 132.6 / 660.5 | 0.2 / 0.9 | 0.2 / 0.9 |
| load: admin GET lease | 131.3 / 1007.7 | 0.2 / 125.2 | 0.2 / 1.2 |
| load: admin LIST leases | 118.0 / 455.5 | 0.2 / 1.1 | 0.2 / 0.8 |
| load: store linearizable | 71.8 / 835.1 | 0.0 / 0.3 | 0.1 / 0.1 |
| load: store serializable | 0.2 / 0.8 | 0.0 / 0.0 | 0.1 / 0.4 |
| load: 40 clients renew loops/s | 82 | 27 | 74 |

On pve-slow with 3 members, fastetcd v1.11.0's watch run failed: its
leader changed mid-run and etcd's `benchmark` panics on the first failed
put. Its lease keepalive was cut off at 300 s (~1.8/s).

## Follow-ups filed from this

- **#94 (P1)**: on a slow disk the proposer (3 writes in flight x 256
  proposals) caps what one fsync carries; etcd carries ~6x more. This is
  the write gap on the X9 blades' kind of disk.
- **#92**: `LeaseKeepAlive` goes through Raft; etcd renews in memory.
- **#93**: v1.12's write-behind back-pressure waits behind the
  checkpoint's fsync under sustained writes; and its plain put runs ~3.5x
  slower than its one-put txn on dev-ssd.
- **#89**: the X9 acceptance and stormcentral's power-cut stage for v1.12.

On a real X9 blade (server3, fastetcd v1.18.0), the kubelet's status
report: [blade-server3.md](blade-server3.md) (#101).

## Reproducing

```
# On any Linux box with cargo, gcc, git, curl, python3 and protoc:
MEMBERS="1 3" PARTS="bench durability k8s" DISK=my-disk tests/bench/compare.sh v3.7.2
# A very slow disk:
PUT1C_TOTAL=200 PUT_TOTAL=10000 KEEPALIVE_TOTAL=10000 WATCH_PUTS=1000 BENCH_TIMEOUT=300 ...
# Through sc-build (the build box's disk); the CSVs are printed at the end:
sc-build 'MEMBERS=1 tests/bench/compare.sh v3.7.2'
tests/bench/extract.py <log> docs/benchmarks/data/<name>   # CSVs + raw summaries
tests/bench/report.py docs/benchmarks/data/<name> ...       # these tables
```

Data sets: `dev-ssd-1member`, `dev-ssd-1member-round2`, `dev-ssd-3members`,
`dev-ssd-k8s` (sc-build on the build box; harness at 877cba8, fcd3477,
cfb6e6e, cfb6e6e), `pve-ssd`, `pve-slow-1member`, `pve-slow-3members` (the
pve VMs; harness at e9a3aea, e9a3aea, 2671979). The harness changed only in
reporting between those commits; the servers measured are always the
release binaries above. Each `raw.txt` has the machine, the etcd
checksum, every workload's progress line, fastetcd's WAL metrics where
dumped, and each `benchmark` summary as printed.
