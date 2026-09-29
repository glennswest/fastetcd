# 02 — Testing strategy

fastetcd's correctness story rests on three concentric rings, each
cheaper-to-run and faster-to-fail than the next outer ring.

## How tests run

GitHub Actions is off for this repo. On the StormCOS setup nothing is
built on the session VM: every push is built and tested on `dev.g8.lo`
with `sc-build` (`cargo build && cargo test`, or any command:
`sc-build 'cargo test -p fastetcd-server --test gateway'`). Each job gets
its own scratch drive, deleted afterwards. The golden build compiles with
`--locked`, so `sc-build 'cargo build --locked --workspace'` checks the
lock too.

## Ring 1 — Workspace unit + integration tests

Run with: `cargo test --workspace` (271 tests in 34 binaries at v1.8.0;
one is `#[ignore]`d).

- **Unit tests** in each crate: MVCC (revisions, txn, compaction, range
  revisions, op counts), engines, auth tables, snapshot files, TLS and
  flag rules, sizing, the gateway's JSON and status mapping.
- **raft** (`crates/raft/tests/`): a single-node cluster, restart and
  membership recovery, log-state at scale, snapshot build/install and
  transfer through files.
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
  - space alarm, corruption recovery from backups, `cluster_id`.
- **migrate** (`crates/migrate/tests/round_trip.rs`): etcd snapshot
  import.
- `crates/storage/tests/two_phase_commit_cost.rs`: a measurement, not a
  pass/fail check.

Known flake: `snapshot_transfer_grpc` (#45), about 1 run in 12.

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
