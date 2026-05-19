# 02 — Testing strategy

fastetcd's correctness story rests on three concentric rings, each
cheaper-to-run and faster-to-fail than the next outer ring.

## Ring 1 — Workspace unit + integration tests

Run with: `cargo test --workspace`

- **Storage / MVCC unit tests** (~50): per-engine conformance suite +
  MVCC-specific tests (compact, txn, historical reads, ...).
- **Raft glue tests** (1): single-node openraft cluster that proposes
  an entry and observes it apply against `MvccStore`.
- **gRPC service tests** through real tonic clients (~30): KV, Watch,
  Lease, Cluster, Maintenance, plus the auto-expiry ticker, plus a
  3-node cluster exercising the gRPC peer transport.

Total: ~85 tests, runs in under 15 seconds. **This is the test set
the project relies on day-to-day.**

## Ring 2 — Third-party Rust client compatibility

Run with: `cargo test -p fastetcd-server --test etcd_client_compat`

Uses the `etcd-client` Rust crate (the most-widely-used Rust etcd v3
client; shares no code with fastetcd) to drive put / get / range /
delete-range / txn / lease / watch / member-list / status. If a
third-party client that knows nothing about fastetcd's internals
works against it, the wire protocol is right.

8 tests, runs in ~0.5s. **This is the strongest in-repo wire-compat
signal.**

## Ring 3 — Upstream etcd test suites

Out-of-tree, optional. Run when paranoia (or a release) demands it.

### 3a. `etcdctl` smoke

```
./tests/etcdctl_smoke.sh
ETCDCTL=/path/to/etcdctl ./tests/etcdctl_smoke.sh
```

Builds fastetcd in release, boots it, runs a sequence of `etcdctl`
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
| Pre-commit | Ring 1 |
| PR CI | Ring 1 + Ring 2 |
| Pre-release | Ring 1 + Ring 2 + Ring 3a |
| Major release | Ring 1 + Ring 2 + Ring 3a + Ring 3b |
| API contract change | All four rings |
