# Changelog

## [Unreleased]

## [v1.1.0] — 2026-09-02

### Added
- **space (#14):** disk-space accounting, pressure reclaim and etcd's
  `NOSPACE` alarm, so a bounded data volume stays bounded. Occupancy —
  data file, retained snapshots, and the filesystem's real free space
  via `statvfs` — is sampled every `--space-check-interval-secs`
  (default 30). Effective capacity is the smaller of
  `--quota-backend-bytes` and what the volume can actually hold, so an
  oversized quota can no longer hide a small volume. Above
  `--space-high-water-percent` (80) the store reclaims: compact MVCC
  history to `--space-reclaim-retention` revisions (even with
  auto-compaction off), trigger a raft snapshot so the log can be
  purged, then defragment if enough would come back.
- **space (#14):** raft snapshots are now a retained set with roll-off.
  `--max-snapshots` (default 1, etcd's flag name) is enforced
  oldest-first *before* a new snapshot is written, so a write never
  needs room for `retain + 1` copies. Leftover temp files and
  half-written pairs are reclaimed on startup, and the pre-v1.1
  single-file `current.snap` layout is migrated in place.
- **admin (#14):** `fastetcd defrag --data-dir <dir>` — an offline
  escape hatch that works on an already-full volume. It needs no
  quorum, no read barrier and no snapshot write, which is exactly what
  fails when the volume is full, and redb's compaction shrinks the file
  in place rather than needing free space.
- **maintenance (#14):** `Alarm` returns the real alarm set and honours
  `DEACTIVATE`; `Status` reports `dbSizeInUse` (live bytes, measured
  behind a 60s cache) and `dbSizeQuota` (effective capacity), and lists
  `NOSPACE` under `errors`.
- **metrics (#14):** `etcd_mvcc_db_total_size_in_use_in_bytes`,
  `etcd_server_quota_backend_bytes`,
  `fastetcd_store_snapshot_size_in_bytes`, `fastetcd_disk_total_bytes`,
  `fastetcd_disk_available_bytes`, `fastetcd_store_space_used_ratio`,
  `fastetcd_nospace_alarm_active`.
- **storage (#14):** `KvStore::usage()` reports file / live /
  fragmented bytes (redb via an aborted write-txn stats walk), and
  `fs_space::probe()` reads the data directory's real free space.
- **ctl (#14):** `fastetcd-ctl status`, `defrag`, `compact <rev>`,
  `alarm [--disarm]`.
- **docs:** `docs/04-disk-space.md` — what grows, what fastetcd does
  about it, every flag and metric, and the dig-out procedures.

### Changed
- **BEHAVIOR (#14):** above `--space-alarm-percent` (95) the `NOSPACE`
  alarm is raised and writes — `Put`, a `Txn` containing a put, and
  `LeaseGrant` — are refused with gRPC `ResourceExhausted` and etcd's
  message `etcdserver: mvcc: database space exceeded`. Reads, deletes,
  compaction and defragment are deliberately **not** gated: refusing
  those is what turns a full volume into an unrecoverable one. The
  alarm clears itself below `--space-clear-percent` (70), or via
  `etcdctl alarm disarm`.
- **raft (#14):** a snapshot write that still hits `ENOSPC` now
  discards every retained snapshot and retries, instead of returning a
  storage error that openraft surfaces on every subsequent read and
  write. openraft rebuilds a snapshot on its next tick; a node with no
  snapshot is worth more than a node that cannot write one.
- `--quota-backend-bytes` is no longer a no-op compat flag.

### Fixed
- **#14:** a full data volume wedged the store in both directions —
  every read failed at the linearizable barrier behind a pending
  snapshot write, every write failed at the same place, and the one
  recovery a client could attempt (deleting keys so the next snapshot
  fits) was refused for the same reason. Every process healthy, cluster
  down. The store now refuses writes early, while it still works.

## [v1.0.7] — 2026-07-21

### Added
- **ctl:** `fastetcd-bench`, a concurrent gRPC load generator (put /
  linearizable get / serializable get; throughput and latency
  percentiles), bundled in the release tarball. No server change.

## [v1.0.6] — 2026-07-20

### Changed
- **raft (#13):** bounded memory and log size. Snapshot bodies live on
  disk instead of RAM (only `SnapshotMeta` is kept in memory) and are
  reloaded on restart so purge is not stalled; `purge`/`truncate` use a
  keys-only `delete_range` instead of loading the purged prefix;
  `--snapshot-count` and `--max-in-snapshot-log-to-keep` are now real
  flags.

### Added
- **server:** `--auto-compaction-retention` (revision mode) and
  `--auto-compaction-interval-secs`; a leader-side ticker proposes
  `Compact` through Raft. Off by default.

## [v1.0.5] — 2026-07-20

### Fixed
- **raft (#13):** startup no longer loads the whole raft log into RAM.
  `get_log_state` did an unbounded range over `raft_log` just to read
  the last entry, which hung every member of a 3-master cluster before
  it could bind its peer port. Added `Snapshot::last` (an O(log n)
  reverse B-tree lookup in redb) and gated/windowed the #11
  membership-recovery scan so a healthy cluster never scans on startup.

## [v1.0.4] — 2026-07-20

### Fixed
- **server:** the automatic pre-version safety backup was fatal on
  failure, so a full disk or an unwritable `backups/` directory
  crash-looped the node — a failed safety net taking down the control
  plane. It is now best-effort: log the error with the remedy and start.

## [v1.0.3] — 2026-07-19

### Added
- **admin:** data-directory safety toolkit. Automatic pre-version
  backup before any recovery/conversion write, plus offline `fastetcd
  backup`, `restore` and `fsck [--repair]` (each opens the redb file
  exclusively and refuses if the server holds it).

## [v1.0.2] — 2026-07-19

### Fixed
- **server (#11):** the upgrade recovery now recovers the *actual*
  membership from the retained raft log (newest `Membership` entry) and
  falls back to `--initial-cluster` only when the log has been purged
  clean of it. The startup log states which source was used.

## [v1.0.1] — 2026-07-19

### Fixed
- **server (#11):** a rolling upgrade from v0.8.2 could strand a
  cluster — every member came up with an empty voter set and never
  elected a leader. Detects the pre-v1.0.1 on-disk format and rebuilds
  the voter set in place, persisting it before Raft starts. Added
  `--force-new-cluster` (etcd parity) and a `format_version` marker.

## [v1.0.0] — 2026-07-19

### Fixed
- **kv (#10):** linearizable reads. `serve_range` ignored
  `serializable` and always read local state, so a lagging follower or
  a leader that had silently lost leadership answered a default read
  with stale data — surfacing as ~25% spurious CAS failures in
  read-modify-write loops. A default Range now runs a read barrier
  first: `ensure_linearizable` on the leader, `RaftPeer.ForwardRead` to
  the leader on a follower. `serializable=true` still opts out.

## [v0.8.3] — 2026-07-18

### Fixed
- **raft (#9):** a single node could not survive a reboot —
  `last_applied_log_id` / `last_membership` were rebuilt as `None` on
  every open, so openraft replayed from index 0 (crash-looping once a
  snapshot had purged the early entries, or silently double-applying
  every mutation). Both now persist into `mvcc_meta`, folded into the
  same `WriteBatch` as the mutation they describe.
- **raft (#8):** a rejoined member stayed at an old MVCC revision:
  `rebuild_mvcc` wrote through the engine while `MvccStore` kept
  serving the counters it cached at open. Added
  `MvccStore::reload_write_state`.
- **cluster (#7):** `member add/remove` against a follower returned
  openraft's raw `ForwardToLeader`; added `RaftPeer.ForwardMembership`.

## [v0.8.2] — 2026-07-16

### Fixed
- **raft (#8):** atomic snapshot build — the MVCC handle and
  `last_applied` are captured together under the state-machine lock, so
  a concurrent apply cannot produce a snapshot whose data predates its
  own `last_applied_log_id`.

## [v0.8.1] — 2026-07-06

### Fixed
- **security (#6):** `--client-cert-auth` is now actually enforced.
  The flag had no `env` binding, and the `ETCD_*`→`FASTETCD_*` compat
  shim had no entry for it, so `ETCD_CLIENT_CERT_AUTH=true` (and
  `FASTETCD_CLIENT_CERT_AUTH`) were silently ignored — the flag stayed
  `false`, TLS was built with no client-CA verifier, and any client
  could complete the handshake and read/write anonymously. Wired
  `env = "FASTETCD_CLIENT_CERT_AUTH"` onto the flag, added the
  `ETCD_CLIENT_CERT_AUTH` compat pair (same fix for
  `peer-client-cert-auth`), and set `client_auth_optional(false)`
  explicitly so mandatory client auth doesn't depend on a tonic
  default. Verified: certless TLS clients are now rejected at the
  handshake on both the gRPC port and the `/health` route; clients
  presenting a CA-signed cert still succeed.

## [v0.8.0] — 2026-07-06

### Changed
- **ci:** Root cause of the stalled release pipeline found: GitHub
  Actions is disabled at the repo-settings level
  (`repos/.../actions/permissions` → `enabled: false`), not broken —
  confirmed off since 2026-05-24, so the `v0.6.0`/`v0.7.0` tag
  pushes never ran anything and `v0.7.0` shipped no GitHub Release.
  Decision: leave Actions disabled; `dev.g8.lo` is now the sole
  build+test path. Deleted `.github/workflows/ci.yml`. Added
  `deploy/packaging/run-tests.sh` (workspace + `wal-engine` +
  `iouring` feature tests) and `deploy/packaging/build-release.sh`
  (rpm/deb/tarball) to script that path. Published the missing
  `v0.7.0` GitHub Release from `dev.g8.lo`.

### Documentation
- **test:** Verified the v0.7.0 rpm and deb end-to-end on
  `dev.g8.lo`: both create the `fastetcd` system user, enable, and
  start the service; `fastetcd-ctl put`/`get` round-trips through
  Raft. Documented a Fedora-specific caveat — `dpkg`'s maintainer
  scripts fail under SELinux `Enforcing` on Fedora (missing SELinux
  context for dpkg scripts, not a package defect); works under
  `setenforce 0` and is a non-issue on real Debian/Ubuntu.

## [v0.7.0] — 2026-07-01

### Fixed
- **fix(raft):** Multi-node client writes sent to a non-leader no
  longer fail. `raft.initialize()` was defaulting every member's
  `BasicNode.addr` to empty (a bare `BTreeSet<NodeId>` instead of a
  `BTreeMap<NodeId, BasicNode>`), and — more fundamentally — fastetcd
  never actually forwarded a write to the leader; it just relayed
  openraft's `ForwardToLeader` error (with that empty address baked
  in) straight to the client. Added a `RaftPeer.ForwardWrite` RPC
  (`crates/raft/src/network.rs`'s new `WriteForwarder`) that hands
  the write off to the leader over the same peer channel already
  used for AppendEntries/Vote/InstallSnapshot, and a single shared
  `ServerState::propose()` that every write path (KV, Lease grant/
  revoke/keepalive) now goes through. Regression test: a `PUT` sent
  to a follower in the 3-node integration test now succeeds and
  replicates. Closes #4.

### Added
- **feat(server):** Serve etcd's plain-HTTP `GET /health` (and
  `/livez`, `/readyz`) on the client gRPC port itself, so load
  balancers and Kubernetes httpGet probes already pointed at etcd's
  client port work unchanged against fastetcd — no second port
  needed. Built on tonic 0.12's `Routes`↔`axum::Router` interop
  (`tonic::service::Routes::builder()...routes().into_axum_router()`),
  preserving TLS. Closes #5.

### 2026-07-01
- **ci:** Add a `build-packages` job that builds the rpm/deb/tarball
  (static `x86_64-unknown-linux-musl`) on every `v*` tag push and
  publishes them straight to the GitHub Release via
  `softprops/action-gh-release`, mirroring the manual v0.6.0 release
  process. Future tags no longer need a manual build-and-upload
  pass.

### 2026-06-30
- **docs:** Replace the stale hand-written systemd unit template in
  `docs/03-deploy.md` with the real `deploy/systemd/fastetcd.service`
  shipped in the rpm/deb, document the rpm/deb/tarball install paths
  released to GitHub Releases, and update the `README.md` deployment
  quick-paths list to match.

## [v0.6.0] — 2026-06-30

### Added
- **feat(server):** Read `ETCD_*` environment variables as a fallback
  for every `FASTETCD_*`-prefixed CLI arg (name, data-dir, listen/
  advertise URLs, initial-cluster*, TLS cert/key/CA paths, metrics
  URL, snapshot-count, quota-backend-bytes, max-request-bytes,
  log-level). An unmodified etcd `EnvironmentFile` (systemd,
  container, Kubernetes) now boots a fastetcd cluster identically —
  `FASTETCD_*` still takes precedence when both are set. Closes #2.
- **feat(deploy):** Add a systemd unit (`deploy/systemd/fastetcd.service`)
  with `Restart=always` and an unbounded restart-rate limit
  (`StartLimitIntervalSec=0` in `[Unit]`) so the service both
  autostarts (`WantedBy=multi-user.target`) and keeps retrying after
  any exit, plus an example `EnvironmentFile` at
  `deploy/systemd/fastetcd.conf.example`. Closes #3.
- **feat(deploy):** rpm (`cargo-generate-rpm`) and deb (`cargo-deb`)
  packaging metadata in `crates/server/Cargo.toml`, bundling
  `fastetcd` / `fastetcd-ctl` / `fastetcd-migrate` into a single
  `fastetcd` package built statically against
  `x86_64-unknown-linux-musl` — avoids baking in a glibc-version
  requirement tied to whichever host built the package. Maintainer/
  post-install scripts (`deploy/packaging/debian/`, inline rpm
  scriptlets) create the `fastetcd` system user, own
  `/var/lib/fastetcd`, and `enable --now` the unit on install.
  Verified end to end on dev.g8.lo (Fedora 43): both `dnf install`
  and `dpkg -i` bring up a working single-node server with
  `fastetcd-ctl put`/`get` round-tripping through it.

### 2026-06-29
- **docs:** Add a detailed "fastetcd vs etcd" comparison section to
  `README.md` covering runtime (Go/GC vs Rust/no-GC), storage engines
  (BoltDB vs pluggable redb/iouring), latency profile, the v3-only
  compatibility boundary, migration, and a decision guide. v3 clients
  (etcdctl, kube-apiserver) use gRPC natively, so no HTTP gateway is
  in scope; only the deprecated v2 HTTP API is called out as out of
  scope.

### 2026-05-24
- **fix(storage):** iouring tests now skip gracefully when
  io_uring is blocked in the test environment (most commonly
  docker's default seccomp profile rejecting `io_uring_setup`
  with EPERM) rather than panicking. Tests use a new
  `iouring_available()` probe at entry. Closes #1.

### 2026-05-23
- **chore:** Note that fastetcd is now mirrored on a local Forgejo
  instance (`forcicd.g8.lo:3000`) for faster CI feedback on the LAN.
  Github Actions remain authoritative; the mirror runs the same
  `.github/workflows/ci.yml` unmodified. Local runner uses
  forgejo/runner:7 (supports node24-based actions like
  Swatinem/rust-cache@v2).

### 2026-05-19
- **feat(server):** etcd-compat CLI flag aliases. fastetcd now
  accepts etcd's plural URL forms (`--listen-client-urls`,
  `--listen-peer-urls`, `--advertise-client-urls`,
  `--initial-advertise-peer-urls`) — comma-separated lists with
  the singular forms kept as aliases. Each is parsed for the
  first entry to bind. Also accepts the no-op flags etcd's e2e /
  robustness harness passes when launching the binary
  (`--snapshot-count`, `--quota-backend-bytes`,
  `--max-request-bytes`, `--log-level`, `--log-outputs`,
  `--logger`, `--metrics`, `--enable-pprof`,
  `--initial-cluster-token`, peer-TLS flags
  `--peer-cert-file`/`--peer-key-file`/`--peer-trusted-ca-file`/
  `--peer-client-cert-auth`); their values are logged at debug
  level but otherwise ignored. This makes fastetcd a drop-in
  binary for any tooling that already knows how to launch etcd
  (including downstream robustness suites).

## [v0.5.0] — 2026-05-19

Kubernetes-ready additions on top of v0.4.0's production hardening.

### Added

- **gRPC health service** (`grpc.health.v1.Health`) on the client
  port. All fastetcd services (KV / Cluster / Maintenance / Watch
  / Lease / Auth) report `SERVING` at startup. Service meshes and
  k8s `readinessProbe.grpc` / `livenessProbe.grpc` work out of
  the box.
- **Helm chart** at `deploy/charts/fastetcd/`. StatefulSet of N
  replicas with stable peer DNS via a headless Service;
  auto-generates `--initial-cluster` from the replica set
  (override-able). Persistent `volumeClaimTemplates` per replica.
  Optional TLS via a referenced Secret. Optional `ServiceMonitor`
  for the Prometheus operator.
- **Real `fastetcd-ctl`** with subcommands `put`, `get [--prefix]`,
  `del [--prefix]`, `snapshot-save <path>` (streams
  `Maintenance.Snapshot` to a local file). Useful for end-to-end
  smoke without needing the Go etcdctl binary.

### Changed

- **README rewrite** for v0.4/v0.5 reality. Architecture diagram
  includes AuthInterceptor + per-key authz; quick-start uses
  both etcdctl and fastetcd-ctl; testing section cross-references
  the three-ring strategy doc; deployment section points at the
  Kubernetes / systemd / container paths.

### Remaining gap

- **openraft 0.10 upgrade** (waiting on its stable release) for
  real `MoveLeader.transfer_leader()`.

## [v0.4.0] — 2026-05-19

### 2026-05-19
- **feat(ctl):** `fastetcd-ctl` is now a real (small) client.
  Subcommands: `put`, `get [--prefix]`, `del [--prefix]`,
  `snapshot-save <path>` (streams `Maintenance.Snapshot` to a local
  file). Useful for smoke-testing fastetcd without the Go
  toolchain.
- **docs:** README rewrite for v0.4.0. Updated architecture
  diagram includes the AuthInterceptor + per-key authz; quick
  start uses both `etcdctl` and `fastetcd-ctl`; testing section
  cross-references `docs/02-testing.md`'s three rings; deployment
  section pointers at `docs/03-deploy.md`.
- **feat(server):** gRPC health service. Adds the standard
  `grpc.health.v1.Health` to the client port so service meshes
  and k8s gRPC probes work out of the box. All fastetcd services
  (KV / Cluster / Maintenance / Watch / Lease / Auth) are
  reported as `SERVING` at startup.
- **chore(deploy):** Helm chart at `deploy/charts/fastetcd`.
  StatefulSet of N replicas with stable peer DNS via a headless
  Service; auto-generates `--initial-cluster` from the replica
  set (override with `cluster.override`). Persistent
  volumeClaimTemplates per replica. Optional TLS via a referenced
  Secret. Optional `ServiceMonitor` for the Prometheus operator.
  k8s-native readiness/liveness via the new gRPC health service.

## [v0.4.0] — 2026-05-19

Production-grade hardening since v0.3.0: TLS, full Auth enforcement
(Phase 2 + Phase 3), Prometheus metrics, GitHub Actions CI,
distroless container, deployment guide.

### Added

- **TLS** for the client and peer gRPC listeners
  (`--cert-file` / `--key-file` / `--trusted-ca-file` /
  `--client-cert-auth`, matching etcd's flag shape).
- **Auth Phase 2** — per-request token enforcement.
  `AuthInterceptor` wraps every non-Auth gRPC service via
  `with_interceptor`; when auth is enabled, requests must carry a
  valid `token` metadata field. `AuthState` is now backed by
  `std::sync` primitives so the sync tonic interceptor can read
  live state. 4 enforcement tests.
- **Auth Phase 3** — per-key permission enforcement. New `authz`
  module checks every KV request against the authenticated user's
  roles + permissions. `Range` requires Read; `Put` /
  `DeleteRange` require Write. `root` user / `root` role bypass.
  3 enforcement tests.
- **Prometheus `/metrics`** endpoint on a side port
  (`--listen-metrics-url`, default `127.0.0.1:2381`). Exports
  etcd-compatible names: `etcd_server_has_leader`,
  `etcd_server_leader_changes_seen_total`,
  `etcd_mvcc_db_total_size_in_bytes`,
  `etcd_debugging_mvcc_current_revision`,
  `etcd_debugging_mvcc_compact_revision`. Lazy-refreshed on each
  scrape; no background task.
- **GitHub Actions CI** at `.github/workflows/ci.yml`. Matrix:
  Linux + macOS × default features, plus Linux-only `wal-engine`
  + `iouring` jobs. On tag push: builds release Linux binary and
  pushes a container image to `ghcr.io/glennswest/fastetcd`.
- **Container** images: `Dockerfile` (build-from-source) and
  `Dockerfile.ci` (consume pre-built artifact). Distroless
  runtime; runs as `nonroot`; exposes 2379 (client) + 2380 (peer)
  + 2381 (metrics).
- **`docs/03-deploy.md`** — production deployment guide covering
  container, systemd unit, TLS, Auth bootstrap, multi-node config,
  backups, and migration from upstream etcd.

### Remaining gap

- **openraft 0.10 upgrade** (waiting on its stable release) for
  real `MoveLeader.transfer_leader()`.

## [v0.3.0] — 2026-05-19

### 2026-05-19
- **feat(server):** Prometheus `/metrics` endpoint on a side port
  (`--listen-metrics-url`, default `127.0.0.1:2381`). Exports
  metric names that match etcd's where they map directly so
  existing dashboards and alerts work unchanged:
  `etcd_server_has_leader`,
  `etcd_server_leader_changes_seen_total`,
  `etcd_mvcc_db_total_size_in_bytes`,
  `etcd_debugging_mvcc_current_revision`,
  `etcd_debugging_mvcc_compact_revision`. Lazy-refreshed on every
  scrape — no background task — so the endpoint always reports
  current truth. Served via a minimal `hyper` HTTP/1 handler.
- **chore(ci):** GitHub Actions workflow at `.github/workflows/ci.yml`.
  Test matrix: Linux + macOS × default features, plus a
  Linux-only `wal-engine` job and a Linux-only `iouring` job
  (since `tokio-uring` is target-gated to Linux). On tag pushes
  (`vX.Y.Z`), builds a release Linux binary, pushes it to
  `ghcr.io/glennswest/fastetcd:tag` and `:latest` using
  `Dockerfile.ci` (which expects the binary as build context).
- **chore(container):** Two-Dockerfile setup. `Dockerfile` builds
  fastetcd from source in `rust:1.82-slim` then copies the binary
  into `gcr.io/distroless/cc-debian12`. `Dockerfile.ci` skips the
  build stage and copies a pre-built binary (the artifact from
  the CI job) for faster image production. Distroless gives us
  glibc without the full Debian userland; image runs as
  `nonroot`.
- **docs:** New `docs/03-deploy.md` covers container, systemd
  unit, TLS, Auth bootstrap, multi-node config, backups, and
  migration from upstream etcd.
- **feat(server):** Auth Phase 3 — per-key permission enforcement.
  New `authz` module checks every KV request against the
  authenticated user's roles. `Range` requires Read, `Put` and
  `DeleteRange` require Write. `root` user / `root` role bypass.
  Permission ranges follow etcd semantics (single key, prefix,
  `[key, range_end)`). The `AuthInterceptor` now inserts a typed
  `UserIdentity` into request extensions on every authenticated
  call; KV handlers read it and call `authz::authorize(...)`. 3
  new tests pass: out-of-range write is denied; in-range write is
  allowed; read-only role can't write.
- **feat(server):** TLS support for the client and peer gRPC
  listeners. New flags `--cert-file`, `--key-file`,
  `--trusted-ca-file`, `--client-cert-auth` matching etcd's
  shape. When `--cert-file` + `--key-file` are both set, the
  server listens over TLS (via tonic's `rustls` backend);
  `--client-cert-auth` additionally requires every client to
  present a TLS certificate signed by `--trusted-ca-file`. The
  same identity is used on both the client port and the peer
  port. Flag-parsing rejects the asymmetric case
  (`--cert-file` without `--key-file` etc.) with a clear error.
- **feat(server):** Auth Phase 2 — per-request token enforcement.
  `AuthState` refactored to use std::sync primitives (`AtomicBool`
  for the enabled flag, `std::sync::Mutex` for the token registry)
  so the sync tonic interceptor can read live state without an
  async runtime. `AuthInterceptor` now wraps every non-Auth gRPC
  service via `with_interceptor`; when auth is enabled it requires
  a valid `token` metadata field on every request and returns
  `Unauthenticated` otherwise. The Auth service itself stays
  unauthenticated so clients can still call `Authenticate`. 4 new
  enforcement tests pass: rejected-without-token,
  succeeds-with-valid-token, rejected-with-invalid-token,
  disable-restores-passthrough.

## [v0.3.0] — 2026-05-19

Closes Auth, Defragment, and the real iouring engine from the v0.2.0
"remaining known gaps" list. The only gap from v0.1.0 still open is
the openraft 0.10 upgrade (waiting on its stable release).

### Added

- **Auth gRPC service — Phase 1**: full surface of every Auth RPC
  (`AuthEnable` / `Disable` / `Status`, `Authenticate`, complete
  User / Role CRUD plus grant/revoke). Passwords hashed with
  `argon2`. `Authenticate` returns a 32-byte hex-encoded token
  tracked in an in-memory session registry. `AuthEnable` refuses
  unless a `root` user exists (matches etcd). `RoleDelete`
  cascade-drops the role from every user that referenced it.
  Persisted in dedicated `auth_state` / `auth_users` / `auth_roles`
  tables outside the MVCC revisioned space.
- **Real `Maintenance.Defragment`**: new `KvStore::defragment()`
  trait method (default no-op). redb wraps its `Database` in a
  `tokio::sync::RwLock` and runs `compact()` under the write
  lock; commits/reads share the read lock so defragmentation
  serializes against writers without starving readers.
  WAL-engine compacts by replaying its in-memory index to a tmp
  WAL and atomic-renaming. Tested end-to-end via the
  `Maintenance.Defragment` gRPC handler.
- **Real `IouringEngine`** (Linux-only, behind cargo feature
  `iouring`). A dedicated OS thread (`fastetcd-iouring`) hosts a
  `tokio_uring::start` runtime that owns the WAL file and the
  in-memory MVCC index. The public engine forwards every
  operation through a bounded `mpsc` channel and awaits results
  on per-call oneshots. Same on-disk format as `WalEngine` so a
  file written by one can be read by the other. The conformance
  suite ports verbatim (6 tests, gated `cfg(target_os = "linux")`).
  Non-Linux builds with `--features iouring` compile cleanly
  because `tokio-uring` is target-gated to Linux in `Cargo.toml`.

### Changed

- **`Maintenance.MoveLeader`** continues to return `Unimplemented`
  pending the openraft 0.10 upgrade (alpha-only, not safe to
  depend on yet). It now validates the target is a current voter
  before bailing.

### Known gaps remaining

- **openraft 0.10 upgrade** (waiting on stable release) — unlocks
  real `MoveLeader` via `Trigger::transfer_leader`.
- **iouring tuning**: `O_DIRECT` + aligned buffers (page-cache
  bypass) and group-commit windowing are follow-ups; the current
  iouring engine delivers the io_uring submission path but not
  yet the tail-latency wins those features unlock.
- **Auth Phase 2**: per-request token enforcement. tonic 0.12's
  sync interceptor signature can't `.await` the async token
  registry; the right shape is a `tower::Layer` wrapper, which
  is the next follow-up.

## [v0.2.0] — 2026-05-19

### 2026-05-19
- **feat(storage):** Real `IouringEngine` implementation
  (Linux-only, behind cargo feature `iouring`). A dedicated OS
  thread hosts a `tokio_uring::start` runtime that owns the WAL
  file and the in-memory MVCC index; the public engine forwards
  every operation through a bounded `mpsc::channel<Command>` and
  awaits results on per-call oneshots. Same on-disk format as
  `WalEngine` (length-prefixed bincode batches) so the two are
  binary-compatible. Conformance suite ports verbatim — six
  tests gated by `cfg(target_os = "linux")` exercise the engine
  on Linux CI. macOS/Windows builds with `--features iouring`
  compile cleanly because the `tokio-uring` dep is also
  target-gated to Linux. **Known gaps** vs a full production
  io_uring engine: `O_DIRECT` + aligned-buffer plumbing
  (page-cache bypass) and group-commit windowing (coalescing
  commits within ~1ms into one fsync) are both follow-ups; the
  current commit lands the io_uring submission path, not yet
  the tail-latency wins those features deliver.
- **feat(storage + server):** `Maintenance.Defragment` is real.
  New `KvStore::defragment()` trait method (default no-op for
  engines that don't compact). redb engine wraps its
  `Database` in a `tokio::sync::RwLock` and runs `compact()`
  under the write lock; commit/snapshot take the read lock so
  defrag serializes against writes without starving reads. WAL
  engine compacts by replaying its in-memory index into a fresh
  WAL file and atomically renaming. `Maintenance.Defragment`
  delegates to the active engine's implementation.
  Test: write+overwrite 50 keys, defragment, verify the latest
  value is still readable.
- **feat(server):** Auth gRPC service — Phase 1. Implements every
  Auth RPC (`AuthEnable` / `Disable` / `Status`, `Authenticate`,
  full User / Role CRUD, grant / revoke). Passwords hashed with
  `argon2` (default cost). Authenticate validates the stored hash
  and returns a 32-byte hex-encoded random token; tokens live in
  an in-memory registry on the local node. AuthEnable refuses
  unless a `root` user has been added (matches etcd's
  precondition). RoleDelete cascade-drops the role from every
  user that referenced it. New `mvcc/auth.rs` defines persisted
  `StoredUser` / `StoredRole` / `StoredPermission` types; auth
  data lives in dedicated tables outside the MVCC revisioned
  space (matching etcd's split). 6 new gRPC tests pass.
- **Phase 2 (not in this commit):** per-request token enforcement
  via a tower::Layer wrapper. The Phase 1 `AuthInterceptor`
  placeholder is wired but accepts every request — tonic 0.12's
  sync interceptor signature can't await the token registry
  cleanly; switching to a tower::Layer is the right shape.

## [v0.2.0] — 2026-05-19

Closes every known gap from v0.1.0 except the iouring kernel-bypass
backend (which remains genuinely multi-week follow-up work). 84
tests pass workspace-wide (+9 since v0.1.0). The `etcd-client`
third-party Rust client successfully drives a fastetcd server
through put / get / range / delete / txn / lease / watch /
member-list / status without modification — the strongest
wire-compat signal we can ship in CI.

### Added

- **Lease auto-expiry ticker** (`crates/server/src/lease_expiry.rs`).
  Background task runs on the leader only, once per second walks
  the persisted lease set, and proposes `LeaseRevoke` entries for
  any whose `deadline_unix_secs < now`. Cascade-delete fires through
  the same path explicit revokes use, so watchers see the delete
  events normally. Closes the v0.1.0 "no auto-expiry" gap.
- **Real cluster membership handlers** (`crates/server/src/cluster.rs`).
  `MemberAdd` derives a stable node id from the peer URL, registers
  the URL with the live `PeerEndpoints` map, calls openraft's
  `add_learner`, and (for non-learner) `change_membership` to
  promote. `MemberRemove` rebuilds the voter set minus the target
  and drops peer/directory entries (refuses self-removal).
  `MemberUpdate` rewrites the peer URL. `MemberPromote` lifts a
  learner to voter. `MemberList` returns the live directory with
  name + URLs + learner state. Replaces the v0.1.0 `Unimplemented`
  stubs.
- **Revision-preserving migration**
  (`MvccStore::bulk_load_records`, `MigrationMode::PreserveRevisions`).
  `fastetcd-migrate --preserve-revisions` writes `mvcc_kv` +
  `mvcc_idx` (+ `lease_keys`) directly so source records retain
  their original `create_revision`, `mod_revision`, `version`, and
  `lease`. After migration with the flag, `Range(rev)` and
  `Watch(start_rev)` behave identically to the source server's
  history.
- **Third-party Rust client compatibility tests**
  (`crates/server/tests/etcd_client_compat.rs`). Uses the
  `etcd-client` crate (etcdv3/etcd-client) — which shares no code
  with fastetcd — to drive fastetcd through put / get / range with
  prefix / delete-range with prev_kv / Txn compare-and-set / lease
  grant+attach+revoke with cascade-delete / Watch / MemberList /
  Status. 8 tests, ~0.5s, runs in normal CI without needing the
  Go toolchain.
- **`tests/etcdctl_smoke.sh`** — optional out-of-tree wire-compat
  smoke using the canonical `etcdctl` client. Boots a release-mode
  fastetcd, runs a representative command set, tears down. Skips
  with a clear message when etcdctl isn't installed.
- **`docs/02-testing.md`** — three-ring testing strategy: workspace
  tests, third-party client, upstream etcd suites (with concrete
  pointers at the robustness suite + Jepsen + K8s e2e for higher
  rings).

### Changed

- `Maintenance.MoveLeader` now validates the target is a current
  voter (`FailedPrecondition` if not — matches etcd) and returns a
  typed `Unimplemented` with a precise explanation of the
  openraft-0.9 limitation. Real leader transfer arrives with an
  openraft 0.10 upgrade.

### Remaining known gaps

- **iouring kernel-bypass file I/O.** `WalEngine` ships the
  architecture; swapping the file-I/O layer for glommio + `O_DIRECT`
  remains the right next step for sub-ms p99 on Linux.
- **openraft 0.10 upgrade** — needed for `MoveLeader`'s real
  implementation and potentially other primitives.
- **Auth gRPC service**.
- **`Defragment` / `Downgrade`**.

## [v0.1.0] — 2026-05-19

### 2026-05-19
- **test(server):** Third-party Rust client compatibility tests.
  New `tests/etcd_client_compat.rs` uses the widely-used
  `etcd-client` crate (etcdv3/etcd-client) — which shares no code
  with fastetcd — to validate the wire protocol end-to-end: put /
  get / range with prefix / delete-range with prev_kv / Txn
  compare-and-set / Lease grant+attach+revoke with cascade-delete /
  Watch (drop create-ack + progress notifies) / MemberList /
  Status. 8 tests, runs in ~0.5s. This is the strongest in-repo
  wire-compat signal.
- **test(harness):** `tests/etcdctl_smoke.sh` — optional shell
  harness that boots a release-build fastetcd and runs `etcdctl`
  through a representative command set (put/get/del/range/txn/
  lease/member/endpoint-status). Skipped when etcdctl isn't on
  PATH.
- **docs:** New `docs/02-testing.md` lays out the three-ring
  testing strategy (workspace tests → third-party Rust client →
  upstream etcd robustness / etcdctl / Jepsen / Kubernetes e2e)
  and points future correctness work at etcd's robustness suite
  as the highest-ROI investment.
- **feat(storage + migrate):** Revision-preserving migration. New
  `MvccStore::bulk_load_records(BulkKey[], next_rev)` writes
  `mvcc_kv` + `mvcc_idx` (and `lease_keys`) directly so source
  records retain their original `create_revision`, `mod_revision`,
  `version`, `lease`, and tombstone state. `fastetcd-migrate`
  gains `--preserve-revisions` (and a corresponding
  `MigrationMode::PreserveRevisions` library API). After migration
  with the new flag, `Range(rev)` and `Watch(start_rev)` behave
  identically to the source server's history. Multi-generation
  per-key is collapsed to "everything up to the most recent
  tombstone + its generation"; multi-generation history support
  is a follow-up.
- **feat(server):** `Maintenance.MoveLeader` now validates the
  target is a current voter and returns a typed
  `FailedPrecondition` for invalid targets; for valid ones it
  surfaces openraft 0.9's lack of an explicit `transfer_leader`
  primitive via `Unimplemented` with a precise message. Real
  transfer arrives with the openraft 0.10 upgrade.
- **feat(server):** Real cluster membership handlers. `MemberAdd`
  derives a stable node id from the peer URL (FNV-1a hash, masked
  to 63 bits), registers the URL with the live `PeerEndpoints` map
  used by `GrpcNetworkFactory` (so the leader can dial the new node
  immediately), and calls openraft's `add_learner`; voting members
  additionally call `change_membership` to promote. `MemberRemove`
  removes from voters via `change_membership(remaining, false)` and
  drops the peer/directory entries (refuses to remove self).
  `MemberUpdate` rewrites the peer URL entry; `MemberPromote` lifts
  a learner to voter. `MemberList` returns the live directory which
  also tracks `name`, `client_urls`, and `is_learner` per member.
  The directory is seeded from `--initial-cluster` on bootstrap.
  Replaces the previous `Unimplemented` stubs. New test verifies
  MemberAdd-as-learner appears in MemberList.
- **feat(server):** Lease auto-expiry ticker. Background task runs
  on the leader only (followers are no-ops) and once per second
  walks the persisted lease set; any lease with
  `deadline_unix_secs < now` is auto-revoked via the same
  `FastetcdLogEntry::LeaseRevoke` path explicit revokes use, so
  attached keys cascade-delete through Raft. Leadership transitions
  hand the work to the new leader on its next tick. Closes a known
  gap from v0.1.0. End-to-end test (`lease_expiry_grpc.rs`) grants
  a 1-second lease, attaches two keys, waits 2.5s, and verifies the
  lease is gone and both keys are deleted.

## [v0.1.0] — 2026-05-19

Initial usable release. fastetcd boots a single-node or multi-node
cluster, serves the full etcd v3 KV / Watch / Lease / Cluster /
Maintenance gRPC surface, and persists state through openraft.

### Added

- **Project scaffold and Cargo workspace** with six crates: `proto`,
  `storage`, `raft`, `server`, `migrate`, `ctl`. Apache 2.0 license.
- **Vendored etcd v3.6.11 `.proto` files** (`rpc.proto`, `kv.proto`,
  `auth.proto`) with a reproducible `vendor.sh` pipeline that strips
  gogoproto / grpc-gateway / versionpb annotations. `tonic-build`
  codegen emits stubs under `fastetcd_proto::{etcdserverpb, mvccpb,
  authpb}`.
- **Engine-agnostic `KvStore` trait** with concrete `WriteBatch`
  value, `Snapshot` trait, `WriteOptions`, and a conformance test
  suite reusable by any backend.
- **`redb` storage engine** (default; cross-platform). Single-file
  ACID B-tree. Passes the conformance suite (6 tests).
- **`WalEngine` storage engine** (opt-in via `wal-engine` feature).
  Append-only WAL + in-memory `BTreeMap` index, replay-on-open.
  Passes the conformance suite + persistence-across-reopen.
- **MVCC state machine** over `KvStore`: revisions
  (`Revision { main, sub }` packed 16-byte BE), per-key
  generation index, revision-keyed value table, atomic
  multi-mutation `apply`. `Range` supports current/historical
  reads, `limit` (with `more`), `keys_only`, `count_only`.
  `Compact(rev)` walks `KeyIndex` and prunes generations whose
  tombstone is `<= rev`; preserves the floor record per key for
  exact-rev historical reads. `Txn` with `Compare`
  (Version/CreateRev/ModRev/Value/Lease × Equal/NotEqual/Greater/
  Less, single-key or range) and atomic success/failure branches
  sharing one revision. `range_events()` backs Watch historical
  replay.
- **Lease layer** on `MvccStore`: lease records persisted in the
  `lease` table, `lease_keys` reverse index updated on every
  Put/DeleteRange with non-zero lease, `apply_lease_grant`,
  `apply_lease_revoke` cascade-deletes attached keys, `lease_ttl`
  computes remaining seconds, `lease_list`.
- **openraft integration**: `FastetcdLogEntry` (Apply / Txn /
  Compact / LeaseGrant / LeaseRevoke / LeaseKeepAlive / Noop),
  `FastetcdLogResponse`, `FastetcdStateMachine` wrapping
  `MvccStore`, `KvLogStore` (persistent `RaftLogStorage` over
  `KvStore`), `FastetcdSnapshotBuilder`.
- **Multi-node Raft peer transport**: internal proto
  `fastetcd.raft.RaftPeer` (AppendEntries / Vote / InstallSnapshot)
  carrying bincode-serialized openraft messages.
  `GrpcNetworkFactory` lazily dials peers over tonic;
  `RaftPeerService` handles inbound RPCs.
- **KV gRPC service**: Range, Put, DeleteRange, Txn, Compact.
  Routes mutations through Raft; reads served from `MvccStore`
  (serializable; on single-node this is linearizable since there
  are no replicas). `OutOfRange` on compacted-rev reads.
- **Watch gRPC service** (Phase 1 + 2): live event fan-out via a
  `broadcast::channel` on `MvccStore` plus historical replay from
  `start_revision`. Range filters, NOPUT/NODELETE filters,
  `prev_kv`, progress-notify timer, compacted-rev cancellation.
- **Lease gRPC service**: Grant, Revoke, KeepAlive (bidi stream),
  TimeToLive (with optional attached-keys list), Leases.
- **Cluster gRPC service**: `MemberList` returns self;
  Add/Remove/Update/Promote return `Unimplemented` (membership
  changes via openraft are tracked as follow-up work).
- **Maintenance gRPC service**: Status (real raft + MVCC values),
  Hash / HashKV (SHA-256 folded to u32), Snapshot (streaming),
  Alarm, Defragment (no-op), MoveLeader / Downgrade unimplemented.
- **`fastetcd-migrate` tool**: reads an etcd v3 BoltDB snapshot
  (via `bbolt-rs`), keeps the latest record per user-key, bulk-
  applies into a fresh `MvccStore`. Tombstones detected via the
  trailing `b't'` on bolt keys.
- **`fastetcd` server binary** with etcd-shaped flags:
  `--name`, `--node-id`, `--cluster-id`, `--data-dir`,
  `--listen-client-url`, `--listen-peer-url`, `--initial-cluster`,
  `--initial-cluster-state`. Multi-port serving: client services
  on one socket, RaftPeer service on the peer socket. Single
  `redb` file in `--data-dir` backs both MVCC state and Raft log.
- **Criterion benchmark harness** for the MVCC layer: put-single,
  range-single, batched put, delete-range, comparable across
  engines (`redb` vs `wal`).
- **75 tests pass workspace-wide.** All gRPC services have
  end-to-end tests through real tonic clients; a 3-node multinode
  test validates Raft replication over the gRPC peer transport.

### Known gaps

These are documented and tracked as follow-up work:

- **iouring kernel-bypass file I/O.** The `iouring` cargo feature
  exists and depends on `wal-engine`, but the glommio + `O_DIRECT`
  file-I/O layer that delivers the actual sub-ms p99 is not yet
  implemented. `WalEngine` provides the architectural shape that
  swap will live under.
- **Lease auto-expiry.** Deadlines are persisted and
  `LeaseTimeToLive` returns correct remaining seconds, but no
  leader-side ticker auto-revokes expired leases. Clients must
  explicitly revoke for cascade-delete to fire.
- **Cluster membership changes** (`MemberAdd` / `Remove` / `Update`
  / `Promote`). The peer transport is in; the gRPC handlers
  currently return `Unimplemented`. Wiring through openraft's
  `change_membership` API is straightforward follow-up.
- **Auth gRPC service** is deferred. Common deployments use TLS
  client-cert authentication outside the etcd Auth subsystem.
- **`MoveLeader` / `Defragment` / `Downgrade`** return
  `Unimplemented`.
- **Migration tool revision history**: currently imports only the
  latest record per user-key. Revision-preserving import is a
  follow-up (it requires direct mvcc_kv / mvcc_idx writes
  bypassing the public `apply` path).

### Storage architecture (recap)

```
┌──────────────────────────────────────────────────────────────────┐
│ tonic gRPC: KV / Watch / Lease / Cluster / Maintenance           │
└──────────────────────────────┬───────────────────────────────────┘
                               │ (writes proposed through Raft)
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│ openraft — consensus, log replication, membership, snapshots     │
│ KvLogStore over KvStore   ·   FastetcdStateMachine over MvccStore │
└──────────────────────────────┬───────────────────────────────────┘
                               │ (apply)
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│ MVCC state machine: revisions, generations, leases, events       │
└──────────────────────────────┬───────────────────────────────────┘
                               │ (KvStore trait, runtime-selectable)
                               ▼
                ┌──────────────┴──────────────┐
                │                             │
            redb engine                  wal-engine
        (default, B-tree)         (append-only WAL +
                                   in-memory BTreeMap;
                                   iouring backend below
                                   this is future work)
```

## [v0.0.0] — pre-release

See git history for individual commits prior to v0.1.0.

