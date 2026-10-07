# fastetcd-test — fastetcd's test container

fastetcd's tests of a **running** fastetcd, per stormcentral's
[`docs/test-standard.md`](https://github.com/glennswest/stormcentral/blob/main/docs/test-standard.md)
(#36). Unit and integration tests stay in `cargo test` (docs/02-testing.md).

## How it runs

stormcentral builds the image from this repo at a commit (`test/build.sh`
stages two static musl binaries, the test and the commit's `fastetcd`;
`test/Containerfile` packages them `FROM scratch`), pushes it to the test
machine's registry, and runs it as a Job:

```bash
stormcentral test run fastetcd short     # or medium, long; --tag <machine>
```

in a namespace of its own, unprivileged, with no host path, host network or
cluster read (`test/requires.toml`). The node's fastetcd is rustkube's live
datastore, so **no suite ever changes its cluster-wide state**: no
compaction, defragment, alarm, auth or membership change. Everything of that
kind runs against members the suite starts itself, as child processes of
the test, from the image's `/fastetcd` (the commit under test), on loopback,
with their data under `/results` (or `$TMPDIR`).

Output: one JSON object per test on stdout —
`{"test": …, "status": "pass|fail|skip", "ms": …, "detail": …}` — then
`{"summary": {"pass": n, "fail": n, "skip": n}}`. Exit 0 all passed, 1 a
test failed, 2 the suite could not run (reported as a `could-not-run` skip:
the node's client port unreachable, no writable directory, no binary).

Environment, beyond the runner's `STORM_*`: `FASTETCD_TEST_NODE_PORT` (the
node's client port, 2379), `FASTETCD_TEST_BIN` (`/fastetcd`),
`FASTETCD_TEST_DIR` (where members keep their data), and for `long`
`FASTETCD_TEST_LONG_SCALE` (1.0), `FASTETCD_TEST_LONG_HOLD_SECS` (120) and
`FASTETCD_TEST_LONG_WAVES` (until the budget). `tests/test_container.sh`
runs all three suites in an sc-build job, against a local member standing
in for the node.

## The suites

**short** (< 2 min) — fastetcd is up on the node and does its main job.
Against `$STORM_NODE:2379`, everything it writes under
`/storm-test/<run id>/` and removed:

| test | what it proves |
|---|---|
| `node-health` | `GET /health` answers `{"health":"true"}` |
| `node-status` | `Maintenance.Status` names a leader; no alarm is raised (a NOSPACE or CORRUPT alarm fails it) |
| `node-kv-roundtrip` | put, linearizable get, a CAS on `mod_revision` (current succeeds, stale refused), a prefix range and delete |
| `node-watch` | a prefix watch sees a PUT and a DELETE, in order |
| `node-lease` | a lease granted, a key attached, a keep-alive, its keys listed, revoked: the key is gone |
| `node-cleanup` | nothing is left under the run's prefix |
| `commit-binary-serves` | the commit's `/fastetcd` starts on this machine and serves a write and a read |

A client port that does not answer plain HTTP (mutual TLS, stormcos#146: the
runner gives the test no client certificate) makes the node checks skip,
saying so; `commit-binary-serves` still runs.

**medium** (~3–10 min) — features and failure paths, end to end, on the
suite's own members:

| test | what it proves |
|---|---|
| `history-watch-compaction` | a watch from an old revision replays every PUT with `prev_kv`; after compaction a read below it is `OutOfRange` and a watch below it is cancelled with `compact_revision` |
| `lease-expiry-and-keepalive` | a key on a 2 s lease goes; one kept alive stays; a keep-alive of the lapsed lease answers TTL 0 |
| `auth-rbac` | with auth on, a user scoped to `/app/` writes there and nowhere else, no token is refused, and revoking a lease holding a key it cannot write is denied (#47) |
| `nospace-alarm-and-recovery` | on a 24 MiB quota, writes are refused with NOSPACE while reads and deletes work; after compact, defragment and disarm, writes work (#14) |
| `snapshot-save-restore` | a live snapshot restores with `fastetcd restore`: keys, the lease and its key (#61, #41) |
| `sigkill-keeps-acknowledged-writes` | 16 writers, SIGKILL mid-load: every acknowledged write is there after restart |
| `three-members-leader-loss` | writes through a follower, a linearizable read on another, the leader SIGKILLed, a new leader, nothing acknowledged lost, the old leader rejoins and catches up |

**long** (the night window) — waves of fastetcd's main workload on one
long-lived member, sized from the pod's CPUs and cgroup memory limit: each
wave ramps (writes its keys), holds (updates, linearizable reads, four
watches, lease churn) and drains (delete, compact, defragment). One line per
wave (`wave-<n>`) with its ramp throughput and p99 and the member's residue
after the drain (RSS, file descriptors, data file size, leases left), then
`wave-trend`: a full-size wave under 70% of the first's ramp, or a residue
grown past wave 1's, fails it, naming the first wave that regressed. (The
standard's container and VM waves are cluster workloads; fastetcd's own
workload is the datastore's.)
