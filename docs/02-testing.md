# 02 — Testing strategy

fastetcd's correctness story rests on three concentric rings, each
cheaper-to-run and faster-to-fail than the next outer ring.

## How tests run

GitHub Actions is off for this repo. On the StormCOS setup nothing is
built on the session VM: every push is built and tested with `sc-build`,
each job on a fresh build VM (dev.g8.lo until its retirement on
2026-10-07): `cargo build && cargo test`, or any command
(`sc-build 'cargo test -p fastetcd-server --test gateway'`). Each job gets
its own scratch drive, deleted afterwards. The golden build compiles with
`--locked`, so `sc-build 'cargo build --locked --workspace'` checks the
lock too. The build is warning-free (#78); keep it so with
`sc-build 'RUSTFLAGS="-D warnings" cargo build --locked --all-targets'`
before a release. It is not the default, so a lint a newer compiler adds
cannot stop the golden build.

## Ring 1 — Workspace unit + integration tests

Run with: `cargo test --workspace` (337 tests in 44 binaries at v1.12.0;
one is `#[ignore]`d).

- **Unit tests** in each crate: MVCC (revisions, txn, compaction, range
  revisions, op counts), engines, auth tables, snapshot files, TLS and
  flag rules, sizing, the gateway's JSON and status mapping.
- **raft** (`crates/raft/tests/`): a single-node cluster, restart and
  membership recovery, log-state at scale, snapshot build/install and
  transfer through files; `read_barrier.rs`: a sole voter's read
  barrier does not wait behind queued writes on a slow-fsync engine
  (openraft's does, as the control) and still sees every acknowledged
  write (#71); `batch_apply.rs`: a batched entry applies each proposal
  at its own revision, a replay skips exactly the part already on disk,
  and the proposer turns 40 concurrent proposals into a few entries
  (#75); `wal_log_store.rs`: openraft's own log-store checks
  (`openraft::testing::Suite`) over the WAL and the write-behind layer,
  restart, the upgrade from a log in redb, and a node that snapshots,
  drops WAL segments and restarts with everything (#85).
- **storage** unit tests include `raft_wal` (round trip across
  segments, truncate, purge, a torn tail cut and never resurrected,
  damage in an older segment refused) and `write_behind` (engine
  conformance, and 3000 random batches checked against a plain map
  through flushes and syncs) (#85).
- **server** (`crates/server/tests/`), through real tonic clients and,
  where it matters, a client port built as `main.rs` builds it:
  - KV, watch (incl. forced lag), lease and lease expiry, cluster and
    maintenance, health, metrics, the v3 JSON gateway;
  - auth: RBAC, Txn, Watch and admin authorization, client-cert CN,
    replication across 3 members;
  - 3-member clusters over the real peer transport
    (`multinode_grpc.rs`: replication, forwarding, linearizable reads,
    LIST→WATCH revision contract), peer TLS, snapshot catch-up of a
    late learner;
  - space alarm, corruption recovery from backups, `cluster_id`;
  - `apply_replay.rs`: SIGKILL the real binary after acknowledged puts;
    the unsynced applies are missing from the file, and a restart
    replays them from the WAL (#71, #85);
  - `wal_upgrade.rs`: a data directory whose raft log is in redb (as
    every version before 1.12 wrote it) starts on the real binary: the
    log moves into `wal/`, every key is there, redb's copy is cleared
    (#85);
  - `multinode_fastpath.rs`: 3 members, writers on the leader and a
    follower, linearizable readers on both checked against every
    acknowledged write; reads confirmed by quorum, writes batched; with a
    member older than `ConfirmLeader`, nothing is batched (#75).
- **migrate** (`crates/migrate/tests/round_trip.rs`): etcd snapshot
  import.
- `crates/storage/tests/two_phase_commit_cost.rs`: a measurement, not a
  pass/fail check.

`snapshot_transfer_grpc` was flaky until 1.20.1 (#45: an abandoned
resend rolled off the installed snapshot).

`fastetcd-ctl` over TLS (#59): `sc-build tests/ctl_tls.sh` makes a CA
and certificates with openssl, starts this tree's `fastetcd` with
`--cert-file`, then with `--client-cert-auth` and auth on, and runs every
ctl command over TLS: CN identity, `--user`, and the refusals (no or the
wrong CA, plaintext against TLS, mismatched options).

A real rolling upgrade (#65): `sc-build 'tests/rolling_upgrade.sh
<from-ref> [<to-ref>]'` (`tree` names this tree either side, so `tree
v1.24.1` is a downgrade) builds both, starts three member processes on
`from` with auth (root; alice on /app/ only), a 600 s lease with a key
and four writers putting unique keys through all endpoints, replaces the
members one at a time with `to` (SIGTERM, same data dir and flags), and
checks every acknowledged write is there, the longest write outage (15 s
bound), auth and RBAC, a lease keep-alive at each step and the lease's
key at the end, each replaced member's pre-version backup, no batched
log entry while a member is older than 1.10 and batching after, and that
every member applied the same last index. etcd's etcdctl v3.5.17 is the
client. Run as v1.9.0 → tree (crosses 1.10's batching, 1.12's WAL
migration and the later version gates), v1.24.1 → tree, and tree →
v1.24.1 when a release changes anything that crosses the wire or the
disk. The other scripts: `tests/clientv3_reauth.sh` (etcd's Go client
across a member restart, #105), `tests/clientv3_fragment.sh` (etcd's Go
client joins watch fragments, #58), `tests/chart_args.sh`,
`tests/migrate_e2e.sh`.

`CHURN=1 MEMBERS=3 tests/read_latency.sh` restarts the leader a few
seconds into each bench (`CHURN_AFTER`, 5), so the numbers include a
leader change; the bench counts and retries the failed RPCs (#104).

Read latency under write load, on a real disk (#71):
`sc-build 'tests/read_latency.sh v1.8.0'` runs `fastetcd-bench --mode
read-under-load` (40 clients looping GET + CAS Txn, a prefix Range every
fifth loop, a probe of 200 linearizable and 200 serializable Ranges)
against this tree and, if given, a baseline ref; `MEMBERS=3` runs a
local three-member cluster and measures through two members' client
ports (#75). Each run prints when its five slowest writes (>= 200 ms)
started, in UTC as the members' logs print it; `METRICS=1` adds each
member's WAL, checkpoint, leader-change and proposal metrics and their
election and `slow raft WAL fdatasync` log lines, so a stall can be
put down to the disk, an election or fastetcd (#83). `DATA_ROOT=/dev/shm`
puts the data dirs on tmpfs, where no fsync stalls.

**On a running node: the test container (#36).** Per stormcentral's
test standard, `test/` builds one image (`test/build.sh` stages a static
test binary and the commit's static `fastetcd`; `test/Containerfile`
packages them `FROM scratch`) that stormcentral runs on its test machines
as `/test short|medium|long` (`stormcentral test run fastetcd short`).
`short` checks the node's fastetcd without changing anything cluster-wide
(health, status and alarms, KV, watch and lease round trips under
`/storm-test/<run id>/`, removed) and that the commit's binary serves;
`medium` runs features and failure paths (compaction, leases, RBAC,
NOSPACE, snapshot restore, SIGKILL, leader loss) on members it starts
itself; `long` runs night waves with a trend. `test/README.md` lists every
check. `sc-build tests/test_container.sh` runs all three in a build job,
against a local member standing in for the node. On a test machine
(`stormcentral test run fastetcd short --tag pvetest2 --commit <sha>`;
blades only outside their 19:00–06:00 Chicago power-off window), run
3070765391 passed on 2026-10-08: `commit-binary-serves` ran, and the five
node checks skip, saying why, while the node's client port is mutual TLS
(stormcos#146) and the Job gets no client pair (stormcentral#555).

Against upstream etcd (#90): `tests/bench/compare.sh <etcd-version>`
downloads the etcd release (sha256-checked), builds etcd's own
`benchmark` tool from the same tag with a downloaded Go, builds
fastetcd at each tag in `FASTETCD_TAGS`, and runs etcd's put / range /
txn-put / watch / lease-keepalive workloads, a SIGKILL durability test
(`fastetcd-bench --mode durability-write|durability-check`), resource
sampling and, with `PARTS=k8s`, rustkube's apiserver rig on each. It
writes CSVs; `tests/bench/extract.py` pulls them out of an sc-build log
and `tests/bench/report.py` turns them into the tables of
`docs/benchmarks/etcd-vs-fastetcd.md`.

## Ring 2 — Third-party Rust client compatibility

Run with: `cargo test -p fastetcd-server --test etcd_client_compat`

Uses the `etcd-client` Rust crate (shares no code with fastetcd) to
drive put / get / range / delete-range / txn / lease / watch /
member-list / status. If a client that knows nothing about fastetcd's
internals works against it, the wire protocol is right.

## Ring 3 — Upstream etcd test suites

Out-of-tree, optional. Run when paranoia (or a release) demands it.

### 3a. `etcdctl` smoke

```
./tests/etcdctl_smoke.sh
ETCDCTL=/path/to/etcdctl ./tests/etcdctl_smoke.sh
```

Needs an `etcdctl` binary and builds fastetcd in release, so run it
through `sc-build` where the build box has one. It boots fastetcd, runs a
sequence of `etcdctl`
commands (put / get / del / range / txn / lease / member / endpoint
status), shuts down. Failure on any step indicates a wire-protocol
or behavioral incompatibility with the canonical client. Requires a
recent `etcdctl` binary (`go install
go.etcd.io/etcd/etcdctl/v3@latest`).

### 3b. etcd robustness suite

[etcd-io/etcd/tests/robustness/](https://github.com/etcd-io/etcd/tree/main/tests/robustness)
is black-box correctness testing maintained by the etcd team:
randomized client workloads, fault injection (kill / partition /
restart), and a linearizability checker
([Porcupine](https://github.com/anishathalye/porcupine)). It expects
to start and supervise an etcd-compatible server process. Adapting
it to fastetcd is a follow-up effort and the single highest-ROI
correctness investment beyond the rings above.

Adapter outline (not yet implemented):
1. Provide a binary path the harness can `exec` — `fastetcd` already
   accepts the etcd-compatible flag shape.
2. Tell the harness our snapshot file location matches their
   conventions (or implement a shim).
3. Run their suite; triage failures.

### 3c. Jepsen for etcd

[aphyr/jepsen.etcd](https://github.com/jepsen-io/jepsen/tree/main/etcd)
exercises linearizability under network partitions. Heaviest lift to
set up; the gold-standard correctness signal for consensus systems.

### 3d. Kubernetes e2e

Boot a `kind`/`k3d` cluster with `--etcd-servers=$FASTETCD_ENDPOINT`
and run Kubernetes' upstream e2e tests. The most demanding etcd
consumer in the wild; if K8s works against fastetcd, fastetcd is
production-credible.

## When to run what

| Trigger | Run |
|---|---|
| Every push | Ring 1 + Ring 2 (`sc-build`) |
| Pre-release | Ring 1 + Ring 2, `cargo build --locked`, Ring 3a where `etcdctl` is available |
| API contract change | Also Ring 3b-3d, when set up (not yet) |

Test containers per the stormcos test standard (short, medium, long
suites run as Jobs on the test machines) are not built yet: #36.
