# CLAUDE.md — fastetcd

Project-specific context. Cross-project rules live in `../CLAUDE.md`.

## Project summary

Rust implementation of the etcd v3 wire protocol. Wire-compatible gRPC.
Multi-node Raft from day one. Importer for existing etcd BoltDB data.
Focused on low resource overhead and predictable latency.

## Version

**`1.1.0`** — Bounded on-disk footprint (#14). A fixed-size data volume
must stay bounded and must never wedge the store. On the reported
node a 64 MB volume filled after ~a day of ordinary control-plane
traffic and then deadlocked in both directions: the snapshot write
failed with ENOSPC, openraft surfaced that storage error on the
linearizable read barrier *and* on every proposal, so even deleting
keys to make room was refused — every process healthy, cluster down.
Four changes:

- **Snapshots are a retained set with roll-off.** They used to be one
  file overwritten in place, which still needed room for two copies
  during the write. `--max-snapshots` (default 1) is now enforced
  oldest-first *before* the new snapshot is written, so a write never
  needs room for `retain + 1`. Temp files and half-written pairs are
  reclaimed on startup; the pre-1.1 `current.snap` layout migrates in
  place. An ENOSPC on the write discards every retained snapshot and
  retries — openraft rebuilds one, and a node with no snapshot beats a
  node that cannot write one.
- **Occupancy is measured against the device, not a quota.** The
  monitor samples the data file, the retained snapshots and the
  filesystem's real free space (`statvfs`); effective capacity is the
  smaller of `--quota-backend-bytes` and what the volume can actually
  hold. A 2 GiB quota on a 64 MB volume is a fiction, and it was that
  fiction that let every threshold report healthy right up to ENOSPC.
- **Reclaim at a high-water mark** (80%): compact MVCC history to
  `--space-reclaim-retention` (even with auto-compaction off), trigger
  a raft snapshot so the log can be purged, then defragment — the only
  step that actually returns pages to the filesystem, since a
  copy-on-write B-tree never shrinks its file on its own.
- **NOSPACE alarm at 95%, raised while the store still works.** Writes
  (`Put`, a `Txn` containing a put, `LeaseGrant`) are refused with
  `ResourceExhausted` / `etcdserver: mvcc: database space exceeded`;
  reads, deletes, compaction and defragment are deliberately not gated,
  because refusing those is what makes a full volume unrecoverable.
  Clears itself below 70% or via `etcdctl alarm disarm`.

Also: `Maintenance.Alarm`/`Status` report real alarms, `dbSizeInUse`
and `dbSizeQuota`; seven occupancy metrics on `/metrics`; offline
`fastetcd defrag` (no quorum, no read barrier, no snapshot write —
works on an already-full volume); `fastetcd-ctl status/defrag/compact/
alarm`; `docs/04-disk-space.md`. Changelog backfilled for 0.8.2–1.0.7,
which had drifted.

Previous: **`1.0.7`** — Tooling release: ships `fastetcd-bench`, a small concurrent
gRPC load generator (put / linearizable-get / serializable-get,
throughput + latency percentiles), now bundled in the release tarball
(work-plan #14). No server/behavior change from 1.0.6. Reference numbers
on 8-core/15G/SSD, single node, 256B values: put ~5.2k ops/s (p50 11ms,
fsync-bound), linearizable get ~61k ops/s (p50 0.9ms), serializable get
~57k ops/s; RSS flat at ~274 MB. Follower serializable reads ~64k ops/s.

Previous: **`1.0.6`** — Bounded memory + log-size management (#13, the sustaining
half). Four changes so a node's memory and raft log stay bounded 24/7
regardless of data size, and snapshot+purge keeps up instead of
stalling:
- **Snapshots live on disk, not RAM.** The state machine used to hold
  the entire serialized database as a `Vec` in `current_snapshot` (why
  an idle node sat at gigabytes) and lost it on restart (why purge
  stalled). Now the body is written to `<data-dir>/snapshots/current.snap`
  and only the small `SnapshotMeta` is kept in memory; it's reloaded on
  restart so openraft can purge the log immediately.
- **Bounded log purge.** `purge`/`truncate` used a range read that loaded
  the whole purged prefix (key+value) into RAM — a large part of the
  2.1 GB. They now use a `delete_range` (keys only).
- **Auto-compaction** (opt-in, `--auto-compaction-retention <revs>`,
  revision mode): a leader-side ticker proposes `Compact` through Raft
  to bound MVCC history, and therefore snapshot cost. Off by default —
  under Kubernetes the apiserver drives compaction itself.
- **Tunable snapshot policy**: `--snapshot-count` (default 5000) and
  `--max-in-snapshot-log-to-keep` (default 1000) now configure openraft's
  snapshot/purge, matching etcd's `--snapshot-count`.

Previous: **`1.0.5`** — Startup no longer loads the whole raft log into RAM (#13,
the acute half). On the g8 3-master cluster every member hung right
after the "starting" line — the peer port (:2380) never opened, so no
election, no quorum, control plane down. Cause: `get_log_state` (called
by openraft on every startup) did `range(Unbounded, Unbounded)` over
`raft_log`, materializing all ~93k un-purged entries (2.1 GB) just to
read the last one. Added `Snapshot::last` (an O(log n) reverse B-tree
lookup in redb; a slow default for other engines) and used it in
`get_log_state`. Also gated + windowed the #11 membership-recovery log
scan so a healthy cluster never scans the log on startup and a legacy
recovery scan is bounded to the last 20k entries. Startup is now bounded
regardless of log size. NOTE: this fixes *restart*; it does not stop the
log from growing — that's the snapshot/purge work below (still needed
for 24/7 operation, since fastetcd's whole-DB in-RAM snapshot build
stalls at scale, which is why the log reached 93k).

Previous: **`1.0.4`** — Startup-robustness fix. In v1.0.3 the automatic
pre-version safety backup was fatal: `backup_before_version(...).await?`
meant that if the backup copy failed — no disk space for a second copy
of a large db, an unwritable/misowned `backups/` dir — fastetcd exited
and systemd crash-looped it, so a *failed safety net* could take a
control-plane node down (the opposite of its purpose). The backup is now
best-effort: on failure it logs an error with the remedy (free space /
fix permissions / `FASTETCD_UPGRADE_BACKUP=false`) and the node starts
anyway. Nothing else about the backup changed.

Previous: **`1.0.3`** — Data-directory safety toolkit, so an upgrade can never
be a one-way door. (1) **Automatic pre-version backup**: the fastetcd
version that last opened a data dir is recorded in `mvcc_meta`; on
startup, if the running binary is a different version and there is data,
a full copy of `fastetcd.redb` is written to `<data-dir>/backups/`
*before* any recovery/conversion writes (on by default;
`--upgrade-backup`/`--upgrade-backup-dir`). (2) New offline
subcommands (server must be stopped; each opens the redb file
exclusively and refuses if it's locked): `fastetcd backup --out <path>`
(consistent single-file copy), `fastetcd restore <backup> [--force]`
(refuses to overwrite a newer dir without `--force`, keeps the
pre-restore file as `fastetcd.redb.replaced-*`), and `fastetcd fsck
[--repair]` (checks structural integrity, `format_version`, raft
membership / `last_applied`, and MVCC counter sanity; `--repair` reuses
the #11 recovery path — deep MVCC key-index damage is reported, not
auto-fixed, so restore from a backup). The #9/#11 recovery logic is now
shared between server startup and `fsck --repair`. Restoring a backup
onto a *new* node identity needs `--force-new-cluster` on first start,
as with etcd.

Previous: **`1.0.2`** — Makes the #11 upgrade a faithful in-place conversion. The
v1.0.1 recovery rebuilt the voter set from `--initial-cluster`, which is
only a guess if the live membership had diverged from bootstrap. v1.0.2
first recovers the *actual* membership from the retained raft log (scans
for the newest `Membership` entry — the true voter set as of the last
on-disk membership change), and falls back to `--initial-cluster` only
when the log has been purged clean of it. The startup log states which
source was used. Standing rule going forward: any on-disk-incompatible
change ships an automatic in-place upgrade; never tell an operator to
wipe/rebuild.

Previous: **`1.0.1`** — Critical upgrade fix (#11). A rolling upgrade of a
long-running cluster from v0.8.2 → v1.0.0 could strand it: every member
came up with an empty voter set and never elected a leader (MVCC data
survived; only membership/leadership was lost). Root cause was the #9
recovery path: v0.8.2 never persisted raft membership, so once its log
had been purged the membership entry (at the head of the log) was gone
from everywhere, and `recover_applied_floor` adopted the purge floor as
`last_applied` — telling openraft not to replay — without recovering
the membership. (v0.8.2 was itself already latently crash-looping on
any restart-after-purge, the #9 bug; the upgrade just forced the
restart.) Fix: detect the pre-v1.0.1 on-disk format (no `format_version`
marker in `mvcc_meta`) and, when the restored membership is empty but
data is present, rebuild the voter set from `--initial-cluster` and
persist it durably before Raft starts — an in-place upgrade. Added a
`--force-new-cluster` escape hatch (etcd parity) that rebuilds
single-node membership from self while preserving the redb data, for
arbitrarily stranded dirs. New `mvcc_meta` `format_version` marker
(FORMAT_VERSION = 1) suppresses re-recovery. Note: downgrading below
v1.0.1 after it (or v0.8.2 after v1.0.0) is unsafe — the old binary
crash-loops on the purged log; treat 1.0.1 as a one-way upgrade from
0.8.x.

Previous: **`1.0.0`** — First stable release. The wire-compat bar is met end to
end: unmodified etcd v3 clients (etcdctl, client-go, kubeadm) drive
KV/Watch/Lease/Cluster/Maintenance/Auth through Raft, single- and
multi-node, with linearizable reads by default. Shipped as the #10 fix
below closes the last known correctness gap surfaced by the rustkube
soak tests.

Headline fix — linearizable reads (#10). A single-client
read-modify-write loop (GET `mod_revision` → txn
`Compare::mod_revision(Equal)` + put) saw ~25% spurious CAS failures on
a cluster with no concurrent writer. Root cause: fastetcd never
implemented linearizable reads — `serve_range` ignored `serializable`
(`let _ = req.serializable;`) and always read local state, so a
follower whose state machine lags, or a leader that has silently lost
leadership, answered a default read with stale data; the client's next
GET then returned a `mod_revision` older than committed, and the
following CAS compared against it and failed. Fix: a default
(non-serializable) Range now runs a read barrier first — on the leader
`Raft::ensure_linearizable` (ReadIndex + wait-for-apply), on a follower
forward the whole Range to the leader over a new `RaftPeer.ForwardRead`
RPC. Cluster-of-one satisfies the barrier trivially, so single-node
reads stay a direct local read; `serializable=true` still opts out.
The single-machine test harness applies too fast to show the
staleness, so the regression test asserts the barrier itself: a node
that can't confirm leadership must *fail* a linearizable read rather
than serve stale local state (verified it fails without the fix).

Previous: **`0.8.3`** — Three durability/compat fixes found by the rustkube
cluster tests.

`#9` — a single node couldn't survive a reboot. `FastetcdStateMachine`
rebuilt `last_applied_log_id`/`last_membership` as `None` on *every*
open (`main.rs` called `new(mvcc)`, which had no restore path), so
openraft saw an empty state machine next to a populated MVCC store and
replayed the log from index 0. That crash-looped once a snapshot had
purged the early entries ("expected index [0, N), got [None, None)")
and, when the log happened to be intact, silently double-applied every
mutation — the second failure mode wasn't in the report. Both fields
now persist into `mvcc_meta`, staged before an entry is dispatched and
folded into that entry's own `WriteBatch`, so the mutation and the log
id describing it commit in one fsync. Note this was never log
corruption: the raft log was fine, the state machine just didn't know
where it was.

Data dirs already in the bad state have no applied position, so
startup adopts the log's `last_purged_log_id` as a floor (openraft
only purges what it has applied *and* snapshotted). **This re-applies
a bounded tail and can advance the revision past what clients
previously observed** — logged loudly at `warn`. Strictly better than
the old workaround (wiping the data dir), but not lossless.

`#8` — a rejoined member stayed at an old MVCC revision. Not a
snapshot-transfer problem: `rebuild_mvcc` replaces every MVCC table by
writing straight through the engine, but `MvccStore` caches
`current_rev`/`compact_rev`/`next_lease_id` from open. The snapshot
landed on disk while the handle served the stale counters — reads
clamp to `current_rev` (hiding every key above it, the reported "504
of 1004") and new writes allocate from `current_rev + 1`, colliding
with the snapshot's revisions. Added `MvccStore::reload_write_state`.
v0.8.2 fixed the *leader* side of this path (torn snapshot build);
this is the follower side, which is why #8 stayed open.

`#7` — `etcdctl member add/remove` against a follower returned
openraft's raw `ForwardToLeader` error. Membership changes go through
openraft's own APIs rather than the replicated log, so they can't ride
on the `ForwardWrite` RPC added for #4; added a sibling
`RaftPeer.ForwardMembership` over the same peer channel plus
`ServerState::propose_add_learner`/`propose_set_voters`.

Each regression test was confirmed to fail without its fix, not merely
pass with it.

Previous: **`0.8.2`** — Atomic snapshot build: capture the MVCC handle
and `last_applied` together under the state-machine lock, so a
concurrent apply can't produce a snapshot whose data predates its own
`last_applied_log_id`.

Previous: **`0.8.1`** — Security fix (#6): `--client-cert-auth` was never
actually enforced. The flag had no `env` binding and no entry in the
`ETCD_*`→`FASTETCD_*` compat shim, so `ETCD_CLIENT_CERT_AUTH=true`
(the documented drop-in config) was silently ignored — the flag
stayed `false`, TLS was built with no client-CA verifier, and any
client could complete the handshake and read/write anonymously on
both the gRPC port and the `/health` route. Wired the `env` binding +
compat pair (same for `peer-client-cert-auth`) and set
`client_auth_optional(false)` explicitly. Verified end to end:
certless TLS clients are now rejected at the handshake; CA-signed
clients still succeed.

Previous: **`0.8.0`** — Found and fixed the real reason releases had stalled:
GitHub Actions is disabled at the repo-settings level (confirmed off
since 2026-05-24, not a workflow bug or billing issue) — every push
and tag since then, including `v0.6.0`/`v0.7.0`, silently ran
nothing. Decision: leave Actions off; `dev.g8.lo` is now the sole
build+test path, scripted via `deploy/packaging/run-tests.sh` and
`deploy/packaging/build-release.sh` (the now-dead
`.github/workflows/ci.yml` was deleted). Published the missing
`v0.7.0` GitHub Release from `dev.g8.lo`, and verified its rpm/deb
end to end there (systemd unit, `fastetcd-ctl put`/`get` through
Raft) — noting a Fedora + SELinux `Enforcing` quirk with foreign
`dpkg` maintainer scripts that isn't a package defect.

Previous: **`0.7.0`** — Fixed a multi-node blocker (#4): client writes sent to
a non-leader now actually forward to the leader over the existing
peer channel (new `RaftPeer.ForwardWrite` RPC / `WriteForwarder` in
`crates/raft/src/network.rs`), instead of relaying openraft's
`ForwardToLeader` error straight to the client with an empty
address. `raft.initialize()` also now carries real peer-URL
`BasicNode` addresses. Added etcd-compat `GET /health` (+ `/livez`,
`/readyz`) on the client gRPC port itself via tonic 0.12's
`Routes`↔`axum::Router` interop (#5), so LB/k8s health probes work
without a second port.

Earlier: **`0.6.0`** — `ETCD_*` env var drop-in compat (falls back to etcd's
env names when `FASTETCD_*` is unset), a systemd unit
(`deploy/systemd/fastetcd.service`, autostart + unconditional
auto-restart) and rpm/deb packaging (`crates/server/Cargo.toml`
`[package.metadata.deb]` / `[package.metadata.generate-rpm]`) that
bundle all three binaries as static `x86_64-unknown-linux-musl`
builds — avoids baking in a glibc-version requirement from the
packaging host. Verified end to end on dev.g8.lo (Fedora 43):
`dnf install` / `dpkg -i` both create the `fastetcd` system user,
enable, and start the service automatically.

Older: **`0.5.0`** — Kubernetes-ready: grpc.health.v1.Health for
service-mesh probes, a Helm chart at `deploy/charts/fastetcd`,
real `fastetcd-ctl` client (put/get/del/snapshot-save), README
rewrite. Only remaining gap from the v0.1.0 era is the openraft
0.10 upgrade for real `MoveLeader`.

Version locations (keep in sync):
- `Cargo.toml` workspace `[workspace.package] version`
- This file (the line above)
- Tags `vX.Y.Z`

## Architecture pillars

- **Storage**: trait-first abstraction (`KvStore`) with two first-class
  engines selectable at runtime: `redb` (default, cross-platform) and
  `iouring` (Linux, `glommio` + `O_DIRECT` + custom WAL; behind cargo
  feature `iouring`).
- **Consensus**: `openraft` — core, not optional. Single-node is a
  cluster-of-one.
- **gRPC**: `tonic` + `prost`, generated from vendored etcd `.proto` files.
- **Async runtime**: `tokio` for the gRPC frontend and Raft node;
  `glommio` runtime is internal to the iouring engine.
- **Logging/tracing**: `tracing` + `tracing-subscriber`.

## Repo layout

```
crates/
  proto/      # tonic-generated etcd v3 stubs
  storage/    # MVCC state machine over redb
  raft/       # openraft glue: log storage, state machine adapter, transport
  server/     # binary: gRPC frontend + raft node + lifecycle
  migrate/    # binary: read etcd BoltDB → write into fastetcd
  ctl/        # binary: minimal etcdctl-compat smoke client
docs/
  00-design.md
benches/
```

## Work plan

Tracked live in the Claude task system. Snapshot of the order:

1. Project scaffolding (done)
2. Cargo workspace + crate skeletons
3. Vendor etcd protos, wire up tonic codegen (done)
4. Design `KvStore` trait + implement redb engine
5. MVCC state machine over `KvStore`
6. openraft integration (log storage trait + redb impl, state machine adapter)
7. Raft peer transport (gRPC) + discovery
8. KV gRPC service
9. Watch service
10. Lease service
11. Maintenance + Cluster services
12. iouring engine implementing `KvStore` (Linux-only, cargo feature)
13. Migration tool from etcd BoltDB
14. Benchmarks: redb engine vs iouring engine vs upstream etcd
15. **Bounded on-disk footprint (#14) — done, shipped in v1.1.0.** A bounded data
    volume must never wedge the store. Work items:
    - [x] `KvStore::usage()` (file bytes / in-use bytes / fragmented
      bytes) + filesystem free-space probe.
    - [x] Space monitor task: high-water reclaim (compact → snapshot →
      purge → defragment) and a NOSPACE alarm at the alarm water mark.
    - [x] Writes rejected with `ResourceExhausted` under the alarm while
      reads/deletes/compaction/defrag stay available (etcd parity).
    - [x] Snapshot writes survive a full disk: retained-set roll-off
      before each write, stale `.tmp`/orphan cleanup on open, and an
      ENOSPC retry that frees every retained snapshot first.
    - [x] `Maintenance.Alarm` returns real alarms; `Status` reports
      `dbSizeInUse` and `dbSizeQuota`.
    - [x] Occupancy metrics on `/metrics`.
    - [x] Offline `fastetcd defrag` escape hatch that works on a full
      volume, plus `fastetcd-ctl defrag/compact/alarm`.

## Constraints & rules

- **Wire compatibility is the bar.** If unmodified etcd v3 clients don't
  work, the feature isn't done.
- **No v2 HTTP API.** Deprecated upstream; not in scope.
- **Linearizable reads by default** (read-index), serializable opt-in.
  Match etcd's semantics, not "close enough."
- **All writes go through Raft.** No direct state-machine writes from gRPC
  handlers, even for "internal" things like lease expiry — those go
  through Raft too so they survive failover.
- **Compaction-correctness is non-negotiable.** Revision history truncation
  must coordinate with watchers and raft log compaction.
