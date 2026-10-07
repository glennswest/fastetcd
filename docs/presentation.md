---
marp: true
title: fastetcd
description: The StormCOS datastore — purpose and functionality
paginate: true
---

# fastetcd

**etcd v3, reimplemented in Rust**

Wire-compatible with unmodified etcd v3 clients (`etcdctl`, client-go,
kube-apiserver). Raft from day one, linearizable reads by default.
The datastore under StormCOS's Kubernetes control plane.

v1.13.0 · `npx @marp-team/marp-cli docs/presentation.md`

---

## The problem

A Kubernetes control plane needs etcd, and etcd on small or slow
hardware is where control planes get slow: Go's garbage collector on
the apply path, and reads that wait on the disk behind writes.

fastetcd keeps etcd's **wire protocol and semantics** (revisions,
watches, leases, Txn, Raft, linearizable reads) and changes the
implementation: Rust (no GC), every key's index and hot values in RAM,
the raft log a sequential WAL, and a store that manages its own disk
footprint on a fixed-size volume. A client cannot tell the difference;
the operator should only see the latency.

**Not a fork of etcd's behaviour.** Where it differs, it is listed as a
difference (README § Compatibility boundary), with an issue.

---

<style scoped>pre { font-size: 0.5em; } table, ul { font-size: 0.7em; }</style>

## Where it sits in StormCOS

stormcentral's graph (`stormcentral check`): fastetcd **depends on
nothing**. What depends on it:

| Who | How |
|---|---|
| **rustkube** | its apiserver stores every object in fastetcd (ready probe on :2379) |
| **irondirectory** | its directory lives in a dedicated fastetcd cluster |
| stormcos | ships it: golden `fastetcd` + volume `fastetcd-data` |

Also readers, not in the graph: **stormconsole** (`/health`, `/metrics`,
the v3 JSON gateway: status, members, keyspace, compact / defrag /
disarm) and **flowsdn**'s ClusterMesh store (planned deployment; its
peer-port policy is in flowsdn `deploy/clustermesh/`).

---

<style scoped>pre { font-size: 0.5em; } table, ul { font-size: 0.7em; }</style>

## How it works

```
 client :2379  gRPC KV/Watch/Lease/Cluster/Maintenance/Auth, grpc.health.v1,
               /health /livez /readyz, v3 JSON gateway POST /v3/...
      │  AuthInterceptor (token or client-cert CN, per-key RBAC)
      │  writes ──► Proposer: queued writes go as one batch entry
      │  reads  ──► read index: ConfirmLeader quorum, not RaftCore's queue
      ▼
 openraft 0.9 ── peers :2380 (RaftPeer: AppendEntries, snapshots,
      │                       ForwardWrite/Read, ConfirmLeader, AuthSync)
      │  WalLogStore: <data-dir>/wal/, one fdatasync per group
      ▼
 MVCC store: revisions · generations · leases · auth · events · Txn
      │  RAM: every key's index + latest-value LRU (--value-cache-bytes)
      │  write-behind layer, checkpointed into redb every 100 ms (paced)
      ▼
 redb  <data-dir>/fastetcd.redb      raft snapshots  <data-dir>/snapshots/
                                     metrics :2381   /metrics (Prometheus)
```

A write is acknowledged after its WAL fsync and its apply; redb is made
durable in the background, and the WAL is purged only behind it.

---

<style scoped>pre { font-size: 0.5em; } table, ul { font-size: 0.7em; }</style>

## What it does today (1/2) — the etcd API

From `docs/00-design.md` § Wire-compat surface, each tested with the
third-party `etcd-client` crate (`etcd_client_compat.rs`):

| Service | Works |
|---|---|
| KV | Range, Put, DeleteRange, Txn (a Range sees the txn's own writes), Compact |
| Watch | ranges, `prev_kv`, filters, history from `start_revision`, progress notify, compaction cancel; never skips a revision |
| Lease | Grant, Revoke, KeepAlive, TimeToLive, Leases; a Put naming a missing lease is refused |
| Cluster | MemberAdd / Remove / Update / List / Promote, forwarded to the leader |
| Maintenance | Status, Alarm (get, disarm), Defragment, Hash, HashKV, Snapshot |
| Auth | every RPC; replicated through Raft; token or TLS client-cert CN; admin RPCs root-only |

Same methods as etcd's v3 JSON gateway (`POST /v3/...`), etcd's routes
and JSON. Single- and multi-member; a follower forwards writes and
linearizable reads to the leader.

---

<style scoped>pre { font-size: 0.5em; } table, ul { font-size: 0.7em; }</style>

## What it does today (2/2) — operating it

- **Bounded disk.** Occupancy measured against the real volume; reclaim
  at 80% (compact → snapshot → purge → defragment); etcd's `NOSPACE`
  alarm at 95% refuses writes but never reads, deletes, compaction or
  defrag. `fastetcd sizing --nodes N` shows its arithmetic.
- **Backups and a corrupt file.** `--backup-dir`: checksummed backups
  (15 min / 10 000 revisions, keep 4). A lone member whose file is
  corrupt restores the newest good one and raises `CORRUPT`; a cluster
  member refuses and prints the remedy.
- **Offline tools.** `fastetcd backup | restore | fsck [--repair] |
  defrag`; automatic copy of the data file before a version change.
- **TLS** on client and peer ports, each with its own identity and CA.
- **Import from etcd**: `fastetcd-migrate` reads a BoltDB snapshot
  with its leases, users and roles (#60).
- **Upgrades are in place**: every on-disk change converts itself at
  start (1.12: raft log moved from redb into the WAL; one-way).

---

<style scoped>pre { font-size: 0.5em; } table, ul { font-size: 0.7em; }</style>

## Performance, measured

`docs/benchmarks/etcd-vs-fastetcd.md` (#90), etcd v3.7.2 vs fastetcd on
three disks, with rustkube's apiserver and 40 clients renewing Leases:

| apiserver GET of a Lease, p99 | etcd v3.7.2 | fastetcd v1.12.0 |
|---|---:|---:|
| build box SSD | 49.8 ms | **4.5 ms** |
| VM on SSD | 178.7 ms | **1.4 ms** |
| VM on a throttled disk (~6 fsync/s) | 835.2 ms | **1.0 ms** |

Reads under write load stay in RAM; etcd's wait on the disk. etcd still
has more **raw write throughput** on a slow disk and faster lease
keepalives. v1.13.0 (#95) narrowed the write gap on the slow disk
(20 clients: 26 → 133–186 puts/s, etcd 89/s; 1000 clients: etcd still
ahead, #94).

---

<style scoped>pre { font-size: 0.5em; } table, ul { font-size: 0.7em; }</style>

## Interfaces

| Port (default bind 127.0.0.1) | Serves |
|---|---|
| 2379 client | etcd gRPC, `grpc.health.v1`, `/health` `/livez` `/readyz`, `/v3/...` |
| 2380 peer | `fastetcd.raft.RaftPeer` between members (mTLS with `--peer-*`) |
| 2381 metrics | Prometheus `/metrics`, etcd's metric names |

- **Config**: etcd's flags (`--name`, `--initial-cluster`, `--data-dir`,
  TLS, `--quota-backend-bytes`, …) plus fastetcd's own (`--wal-*`,
  `--value-cache-bytes`, `--backup-dir`, …). Flags read `FASTETCD_*`
  variables, and etcd's own flags fall back to their `ETCD_*` names, so
  an etcd config drops in. Reference: `docs/01-configuration.md`.
- **Metrics**: `grpc_server_handled_total`, `etcd_debugging_mvcc_*`,
  `etcd_server_is_leader`, proposals, occupancy, WAL fsync / checkpoint
  times, cache hits (`docs/03-deploy.md` § Metrics).
- **CLIs**: `etcdctl` works as is; `fastetcd-ctl` (put/get/del, status,
  defrag, compact, alarm, auth members/adopt); `fastetcd-bench`.

---

<style scoped>pre { font-size: 0.5em; } table, ul { font-size: 0.7em; }</style>

## How it ships and runs

- **A special golden**, built by stormcos's `deploy/build-goldens.sh` in
  stage mode: `stormcentral component stage fastetcd` builds the pushed
  commit with `cargo build --release --locked --target
  x86_64-unknown-linux-musl` (static binaries, so `Cargo.lock` must be
  current).
- **Golden `fastetcd`** (64M, stormdbase): `fastetcd`, `fastetcd-ctl`,
  `fastetcd-migrate` in `/usr/bin`. PID 1 is **stormd** (API :9081): it
  starts fastetcd on `0.0.0.0:2379/2380/2381` advertising `${NODE_IP}`,
  restarts it on exit, TCP-probes :2379.
- **Golden `fastetcd-data`** (1G blank) is `/data/fastetcd`.
- **Starts**: stormpump's `30-kube` manifest, first, before the
  apiserver (sno / storage / bastion profiles). A worker carries the
  golden unstarted, so promoting it to a master is a container start.
- **Updated** by a stormcos release that carries a new golden; the data
  directory converts itself in place on the new version's first start.
- Elsewhere: build your own container image, Helm chart, rpm/deb,
  systemd unit (`docs/03-deploy.md`). No published image.

---

<style scoped>pre { font-size: 0.5em; } table, ul { font-size: 0.7em; }</style>

## Status — open issues that matter

| | Issue |
|---|---|
| P1 | #94 write throughput capped on a slow disk · #36 test containers per the stormcos test standard |
| P2 | #69 `/health` always healthy · #97 3 members: a lone write pays two fsyncs in series · #103 leadership lost under heavy load (fixed election timeouts) · #92 keepalive through Raft · #105 unknown-token error text breaks clientv3 re-auth · #84 #89 #101 verify on an X9 spinning disk |

Auth: no known authorization gap; tokens never expire (#46).
`gh issue list` for everything.

---

<style scoped>pre { font-size: 0.5em; } table, ul { font-size: 0.7em; }</style>

## Planned (not in the code)

- **Correctness first**: the lease-not-found check at apply as etcd
  does it, once members' lease tables are known to agree (#115).
- **etcd parity gaps**: MoveLeader / Downgrade (#57),
  watch fragmentation (#58), token TTL (#46), Lock / Election services
  (#64), WebSocket gateway streams (#66), a real `/health` (#69),
  `--auto-compaction-mode` (#21, an owner decision).
- **Writes on slow disks and 3 members**: more writes per fsync (#94),
  replicate before the leader's own fsync (#97),
  (#92), configurable election timeouts (#103).
- **Operations**: a rolling-upgrade test (#65), test containers on the
  test machines (#36).

---

## Where to read more

| | |
|---|---|
| `README.md` | status, ports, compatibility boundary, quick start |
| `docs/00-design.md` | design, read and write paths, wire-compat table |
| `docs/01-configuration.md` | every flag, env var and subcommand |
| `docs/02-testing.md` | the tests and `sc-build` |
| `docs/03-deploy.md` | goldens, TLS, auth, storage, memory, metrics |
| `docs/04-disk-space.md` | sizing, reclaim, alarms |
| `docs/05-backup-and-recovery.md` | backups and corruption recovery |
| `docs/benchmarks/` | etcd vs fastetcd, with the data |
| `CHANGELOG.md` | what changed, release by release |
