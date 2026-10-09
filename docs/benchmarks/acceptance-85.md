# #85's acceptance: an X9 blade, a power cut, no SSD regression (fastetcd#89)

#85 (v1.12.0: the raft log is a sequential WAL, applies are held in RAM by a
write-behind layer, and redb is checkpointed in the background) was first
measured on dev.g8.lo's SSD only. #89 asks for its acceptance on the hardware
that motivated it, plus a hard power cut and no regression on SSD. The data
is in `data/acceptance-85/`.

## SSD: no regression against v1.11.0

`tests/read_latency.sh v1.11.0` on a fresh sc-build VM (pve, 8 cores, 7 GB,
2026-10-08), one member, data dirs on the VM's disk. Each round builds
v1.11.0 and this tree (b064120, 1.29.0) and runs them back to back with the
same bench (40 clients looping GET + CAS Txn, a prefix Range every fifth
loop, and a probe of 200 linearizable then 200 serializable Ranges).

| round | version | writes/s | write p50 / p99 / max | linearizable p99 | serializable p99 |
|---|---|---|---|---|---|
| 1 | v1.11.0 | 545 | 52.5 / 141 / 205 ms | 16.2 ms | 27.5 ms |
| 1 | 1.29.0 | 1 718 | 14.5 / 92.8 / 164 ms | 1.04 ms | 2.37 ms |
| 2 | v1.11.0 | 627 | 41.7 / 127 / 163 ms | 13.0 ms | 14.1 ms |
| 2 | 1.29.0 | 2 776 | 9.6 / 55.6 / 98 ms | 0.58 ms | 1.24 ms |
| 3 | v1.11.0 | 744 | 35.8 / 118 / 180 ms | 14.5 ms | 20.7 ms |
| 3 | 1.29.0 | 2 150 | 10.5 / 82.8 / 543 ms | 0.92 ms | 1.11 ms |

1.29.0 is ahead on every row: 2.9–4.4x the writes, a lower write p50 and
p99, and linearizable reads ~15x faster at p99. The 543 ms maximum in round
3 was a single write; that round's p99 was still 83 ms.

The same rig on a test node: rustkube's get-latency / lease-churn load
(`tests/bench/blade_getlatency.py`) against pvetest1. That is a pve VM on
sas-lvm-thin running release 11.95, whose fastetcd lies between 1.24 and
1.26 (it exports `process_*` metrics but not the 1.27 in-flight gauges). The
load runs through its apiserver: 40 clients renew their own Lease and list
the Leases every fifth loop, while a probe GETs one Lease. There are only
after numbers here: a node runs its release's golden, so v1.11 cannot run
on it.

| run | GET idle p50 / p99 | GET under load p50 / p99 | renewal p50 / p99 | renewals/s | mean WAL fsync | proposals per fsync | back-pressure |
|---|---|---|---|---|---|---|---|
| r1 | 1.1 / 5.4 ms | 5.0 / 14.8 ms | 14.3 / 34.9 ms | 1 721 | 1.6 ms | 5.9 | 0 |
| r2 | 0.5 / 1.1 ms | 5.8 / 25.7 ms | 19.6 / 84.6 ms | 1 184 | 2.2 ms | 6.1 | 2 |
| r3 | 1.0 / 3.6 ms | 5.4 / 28.9 ms | 18.4 / 91.5 ms | 1 229 | 1.9 ms | 6.4 | 29 |

- Since the node started: the slowest WAL fsync took 0.179 s and the
  slowest checkpoint commit 0.225 s.
- The value cache served 99.94–99.96% of reads.
- The apiserver GET p99 was 15–29 ms under 1 200–1 700 renewals/s.
  rustkube#187's target of 20 ms is for the blade's own get-latency load,
  measured below.

## Power cut: every committed object survives

stormcentral's install stage "durability" writes 300 objects through the
apiserver, then cuts the power and checks each object after the reboot.
stormcentral#262 is still open: the stage passes vacuously if the writes
fail. So what matters is that the logs record real writes:

- pvetest1, release 11.95, 2026-10-08 01:33Z: "durability: 301 of 300
  committed objects survived the power cut".
- pvetest2, release 11.88-flowsdn, 2026-10-07 09:29Z: the same, 301 of 300.

On a pve machine the cut is `qm stop`, which kills QEMU at once and gives
the guest no shutdown (stormcentral `testhosts.rs` `bmc_boot`, `Cmd::Cycle`,
at 11518e9). Both releases carry fastetcd 1.12 or later, so after the cut
the WAL replayed whatever the last checkpoint had missed. What this does not
cover: a host losing power with writes still in its page cache. That would
need a blade. Blades get a warm reset by design and are not power-cut by
this stage.

## The X9 blade (server3)

server3: an X9 blade with one 7200 rpm disk under ublk. It runs release
11.91, with fastetcd 1.18.0, which carries #85 but not #93's checkpoint
presync (1.24.1). Measured 2026-10-09 11:56–12:01Z with the same rig, 3
rounds of 60 s, after #138's stall had been cleared by the overnight power
cycle (stormblock#334).

| run | GET idle p50 / p99 | GET under load p50 / p99 / max | renewal p50 / p99 / max | renewals/s | WAL fsyncs, mean | proposals per fsync | checkpoints, disk time | back-pressure |
|---|---|---|---|---|---|---|---|---|
| r1 | 1.3 / 2.1 ms | 1.4 / 8.3 / 462 ms | 32.5 ms / 4.04 s / 13.5 s | 278 | 1 286, 37.9 ms | 13.0 | 20, 28.4 s | 0 |
| r2 | 1.4 / 2.0 ms | 1.4 / 9.7 / 427 ms | 36.8 ms / 1.49 s / 2.49 s | 465 | 1 870, 22.6 ms | 15.0 | 13, 4.0 s | 3 |
| r3 | 1.4 / 2.5 ms | 1.4 / 8.1 / 272 ms | 32.1 ms / 2.54 s / 6.00 s | 329 | 1 351, 36.9 ms | 14.8 | 8, 11.9 s | 5 |

Since start (the node booted at ~11:00Z and ran other tests first), the
slowest WAL fsync took 27.8 s, during r2, and the slowest checkpoint commit
22.5 s.

- **Reads meet the target.** An apiserver GET under the lease-churn load
  has p99 8–10 ms, under rustkube#187's 20 ms (server1 on v1.10 was about
  45 ms, rustkube#177), and the value cache serves 99.99%.
- **The write median is the disk's.** A renewal's p50 is 32–37 ms, which is
  a WAL fsync (22–38 ms mean) plus the apiserver. Group commit carries 13–15
  proposals per fsync, and write-behind back-pressure fires 0–5 times a
  minute.
- **The write tail is long: p99 1.5–4 s, maximum 13.5 s.** It follows the
  disk. In r1 the checkpoints took 28.4 s of the 60 s and the mean WAL fsync
  was 38 ms; in r2 they took 4 s and it was 23 ms. Individual WAL fsyncs ran
  to tens of seconds. The checkpoint's random writes and the WAL's
  sequential fsyncs share one spindle (with stormblock's journal, the
  sandboxes and other volumes). That is #135, which now has these numbers.
- **No before.** v1.11 cannot run on a blade: a node runs its release's
  golden, and the oldest release on these blades already carries #85. The
  nearest blade reference for writes is #101 (the kubelet's status report on
  server3: p50 39.5 ms, slow ones 0.3–0.8 s at ~4 writes/s). For reads it is
  server1 on v1.10 (GET p99 about 45 ms).

## Verdict

- **Met:**
  - reads on the blade (GET p99 < 20 ms);
  - no SSD regression (better on every measure);
  - a hard cut of a VM keeps every committed object;
  - group commit and write-behind keep the write median at one disk fsync.
- **Not met:** the write tail on a spinning disk, which goes to #135, along
  with re-measuring on a release that carries #93 (1.24.1).
