# 03 — Deployment

This document covers running fastetcd in production. For development
runs see `README.md`.

## How fastetcd reaches a StormCOS node

On the StormCOS platform fastetcd is a **golden**, built from source by
stormcos's `deploy/build-goldens.sh`. That script and its document,
[stormcos `docs/goldens.md`](https://github.com/glennswest/stormcos/blob/main/docs/goldens.md),
are the one authority for the process. What it does with this repo
(section "control plane" of the script, as of 2026-09-29):

- **Source.** The checkout on the build box (`/root/fastetcd` on
  `dev.g8.lo`, or `FASTETCD_SRC`), fast-forwarded to `origin/main`. The
  golden is built from whatever `main` is at that moment. Nothing is
  downloaded from a GitHub release or a registry (owner, 2026-09-28).
- **Compile.** `cargo build --release --locked --target
  x86_64-unknown-linux-musl`, and it must produce `fastetcd`,
  `fastetcd-ctl` and `fastetcd-migrate`. fastetcd is a *required*
  component: if it does not build, the image build stops.
- **Golden `fastetcd`** (64M, a stormd base): the three binaries in
  `/usr/bin`, run and restarted by stormd (its API on port 9081), with a
  TCP liveness probe on 2379, as
  `fastetcd --data-dir /data/fastetcd --listen-client-urls
  http://0.0.0.0:2379 --listen-peer-urls http://0.0.0.0:2380
  --advertise-client-urls http://${NODE_IP}:2379
  --initial-advertise-peer-urls http://${NODE_IP}:2380`.
- **Golden `fastetcd-data`** (1G, blank): the `/data/fastetcd` volume.
  1G is headroom on top of fastetcd's own bounded growth (see
  [Disk space](#disk-space)).

What that asks of this repo:

- **`main` is what ships.** A change is in the next image as soon as it
  is pushed to `main`. A tag marks a version; it is not what the image
  build picks.
- **`Cargo.lock` must be committed and current.** `--locked` fails
  rather than update it, so a dependency added without its lock entry
  breaks the golden (as #52 did). Check with `sc-build 'cargo build
  --locked --workspace'`.
- **Every binary must build for musl** (no glibc-only dependency).
- fastetcd has no stormcentral component entry, so `stormcentral
  component build fastetcd` is refused. Its golden comes from the stormcos
  image build.

## Container images

No container image is published, and **GHCR is deliberately not used**
on this platform. For your own use, the repo ships a multi-stage
`Dockerfile` (binary built in the image) and a `Dockerfile.ci` (binary
built outside it, then copied in). Both produce a distroless image with
`fastetcd` at `/usr/local/bin/fastetcd`. Build one and push it to a
registry you run:

```
docker build -t fastetcd:dev .
docker run --rm -p 2379:2379 -p 2380:2380 \
    -v fastetcd-data:/var/lib/fastetcd \
    fastetcd:dev
```

GitHub Actions is disabled for this repo (a repo-level setting, off
since 2026-05-24). Builds and tests run on `dev.g8.lo` through
`sc-build`. Packages are built there by hand (below).

## Helm chart

`deploy/charts/fastetcd` runs a StatefulSet (3 replicas by default), a
headless service for the peers, a client service and an optional
ServiceMonitor, with `tls`, `peerTls` and `space` blocks mapped onto the
flags. It is for running fastetcd *inside* a Kubernetes cluster; a
StormCOS control plane does not use it (it runs the golden above).

- **Image:** none is published; set `image.repository` to one you built.
- **Storage:** each replica gets a PVC of `persistence.size` (10Gi;
  `fastetcd sizing` says what the cluster needs). On a StormCOS cluster
  a PVC is a stormblock volume, provisioned by the kubelet's built-in
  stormblock driver (class `stormblock`, the default class), so leave
  `persistence.storageClass` empty.
- **Before 1.14.1** the template's `--auto-defrag=true` was refused at
  argument parsing and no pod started (#53); from 1.14.1 boolean flags
  take `=true`/`=false`. `tests/chart_args.sh` renders the chart and
  starts the rendered command.

## Linux packages (rpm / deb)

For hosts outside StormCOS (the platform does not use these):
`deploy/packaging/build-release.sh vX.Y.Z` builds
`fastetcd-vX.Y.Z-1.x86_64.rpm`, `fastetcd_vX.Y.Z-1_amd64.deb` and a
plain `fastetcd-vX.Y.Z-x86_64-linux-musl.tar.gz` for distros that use
neither package manager. [GitHub
Releases](https://github.com/glennswest/fastetcd/releases) carry them
up to v1.2.0; later versions are tags only, so build the packages
from the tag you want (below). All three bundle `fastetcd` /
`fastetcd-ctl` / `fastetcd-migrate`, built statically against
`x86_64-unknown-linux-musl` — no glibc-version dependency on the
install target.

```
# Fedora / RHEL
sudo dnf install ./fastetcd-vX.Y.Z-1.x86_64.rpm

# Debian / Ubuntu
sudo dpkg -i ./fastetcd_vX.Y.Z-1_amd64.deb
```

Either install creates a `fastetcd` system user, owns
`/var/lib/fastetcd`, ships the systemd unit below to the platform's
unit dir, and runs `systemctl enable --now fastetcd` automatically
— no separate unit-file step needed. Multi-node and TLS flags go in
`/etc/fastetcd/fastetcd.conf` (see
`/usr/share/doc/fastetcd/fastetcd.conf.example`), loaded via the
unit's `EnvironmentFile=`.

Verified end-to-end on `dev.g8.lo` (Fedora 43) for v0.7.0: both the
rpm and the deb create the system user, enable, and start the
service, and a `put`/`get` through `fastetcd-ctl` round-trips. The
one caveat is testing the **deb on Fedora specifically**: Fedora's
`dpkg` build calls `setexeccon()` before running maintainer
scripts, and Fedora's SELinux policy has no context for dpkg's
scripts (only rpm's), so `postinst`/`prerm` fail under `Enforcing`
with "cannot set security execution context for maintainer script:
Invalid argument". This is a Fedora+foreign-`dpkg` limitation, not
a defect in the package — it installs and runs cleanly under
`setenforce 0`, and is a non-issue on real Debian/Ubuntu, where
`dpkg` is native.

Building the packages: `deploy/packaging/build-release.sh vX.Y.Z`
on a host with rustup (`x86_64-unknown-linux-musl` target),
`cargo-deb`, `cargo-generate-rpm`, `protoc`, and `musl-gcc`
installed. It builds the workspace, runs `cargo deb` / `cargo
generate-rpm`, bundles the tarball, and writes everything plus
`SHA256SUMS.txt` to `dist/`. Publish with `gh release create vX.Y.Z
dist/* --generate-notes`.

## systemd unit (manual install)

The unit fastetcd ships is `deploy/systemd/fastetcd.service` — it
autostarts (`WantedBy=multi-user.target`) and restarts
unconditionally on any exit (`Restart=always`, unbounded
restart-rate limit). To install it by hand instead of via the rpm/
deb above:

```
sudo useradd --system --home-dir /var/lib/fastetcd \
    --no-create-home --shell /sbin/nologin fastetcd
sudo mkdir -p /var/lib/fastetcd /etc/fastetcd
sudo chown fastetcd:fastetcd /var/lib/fastetcd
sudo cp deploy/systemd/fastetcd.service /etc/systemd/system/
sudo cp deploy/systemd/fastetcd.conf.example /etc/fastetcd/fastetcd.conf
# edit /etc/fastetcd/fastetcd.conf with your --name / --initial-cluster / etc.
sudo systemctl daemon-reload
sudo systemctl enable --now fastetcd
```

## TLS

Standard etcd-shaped flags. The client port and the raft peer port
are configured **independently**, as in etcd, so the peer port can
require a different, usually narrower, CA than the client port:

```
# Client port (KV / Watch / Lease / ... and /health)
--cert-file=/etc/fastetcd/cert.pem
--key-file=/etc/fastetcd/key.pem
--trusted-ca-file=/etc/fastetcd/ca.pem    # needed for --client-cert-auth
--client-cert-auth                         # require client certs

# Raft peer port
--peer-cert-file=/etc/fastetcd/peer.pem
--peer-key-file=/etc/fastetcd/peer-key.pem
--peer-trusted-ca-file=/etc/fastetcd/peer-ca.pem   # required with peer TLS
--peer-client-cert-auth                            # require peer certs
```

`--cert-file` never applies to the peer port. Without the `--peer-*`
flags the peer port is plaintext and unauthenticated, and if client TLS
is on fastetcd logs a warning saying so at startup. Anything that can
reach an unprotected peer port can take part in raft, so when members
are on different hosts or pods, turn peer TLS on.

With peer TLS on:

- members dial each other over TLS, verify the other member's
  certificate against `--peer-trusted-ca-file`, and present their own
  `--peer-cert-file` as their client certificate. A peer certificate
  therefore needs both the `serverAuth` and `clientAuth` usages (or
  none), and must be valid for the host in that member's peer URL;
- `--peer-client-cert-auth` makes the peer port refuse any caller that
  does not present a certificate the peer CA signed. A certificate the
  client CA signed is not enough. Set it on every member;
- every peer URL (`--listen-peer-urls`, `--initial-advertise-peer-urls`,
  `--initial-cluster`) must be `https://`. Without peer TLS they must be
  `http://`. A mismatch is a startup error naming the flag and URL, and
  a member added later with the wrong scheme fails to dial with an error
  naming its URL. A listen URL with no scheme (`0.0.0.0:2380`) is
  accepted either way.

Before v1.4.1, `--cert-file` was also served on the peer port and the
`--peer-*` flags were parsed and ignored. Members still dialled each
other in plaintext, so a multi-member cluster with `--cert-file` set
could not form (fastetcd#23).

The Helm chart has a separate `peerTls` block for this (see
`values.yaml`). `fastetcd-ctl` reaches a TLS client port with etcdctl's options
(since 1.21, #59): `--endpoint https://m1:2379 --cacert ca.pem`, plus
`--cert`/`--key` under `--client-cert-auth`.

Every flag above is also settable via its env var — `FASTETCD_*` or,
as a drop-in for existing etcd config, `ETCD_*` (e.g.
`ETCD_CLIENT_CERT_AUTH=true`, `ETCD_TRUSTED_CA_FILE=...`). With
`--client-cert-auth` (or `ETCD_CLIENT_CERT_AUTH=true`) set, client
authentication is **mandatory**: a client that presents no certificate
signed by `--trusted-ca-file` fails the TLS handshake outright, on both
the gRPC surface and the `/health` HTTP route.

## Auth

```
# 1. Add a root user (required to enable auth).
etcdctl user add root --no-password=false
etcdctl role add root
etcdctl user grant-role root root

# 2. Add per-user roles + permissions.
etcdctl role add app-readers
etcdctl role grant-permission app-readers read /app/ /app0
etcdctl user add reader --no-password=false
etcdctl user grant-role reader app-readers

# 3. Enable.
etcdctl auth enable
```

Clients then run with `--user=reader:password` (etcdctl) or send a
`token` metadata field after calling `Authenticate`.

### Users by client certificate

With `--client-cert-auth` on, a client that sends no `token` is the
user named by the Common Name (CN) of the client certificate the TLS
handshake verified, as in etcd. It needs no `Authenticate` call and no
password. Create such users with `--no-password`, and scope them with
roles as usual:

```
etcdctl user add cluster-b --no-password
etcdctl role add cluster-b-read
etcdctl role grant-permission cluster-b-read read /cilium/cluster-b/ /cilium/cluster-b0
etcdctl user grant-role cluster-b cluster-b-read
# cluster-b connects with a certificate whose CN is "cluster-b"
```

- A certificate whose CN names no user gets `PermissionDenied`.
- A certificate with no CN identifies no one (`Unauthenticated`).
- A `token` wins over the certificate: a client that authenticates
  acts as the user it logged in as.
- An invalid token is refused. The call is not retried as the
  certificate's user.
- Everything a user can do is scoped this way: KV, Txn, Watch, and
  the root-only calls, for which a certificate with CN `root` counts
  as root.
- Without `--client-cert-auth` the certificate names no one.

### Auth on a multi-member cluster

Auth state is replicated through Raft, as in etcd. Every change (users,
roles, permissions, `auth enable`/`disable`) is a log entry that every
member applies, so it can be made against any member and is in effect
on all of them once it commits. It also survives failover, restart and
a raft snapshot, which carries the auth tables. `Authenticate` is
replicated too: a token issued by one member is accepted by every
member. Tokens are held in memory only (etcd's "simple" tokens). A
restarted member has none, and clients authenticate again, as they do
with etcd. Reads (`user list`, `role get`, `auth status`) answer from
the member's own applied state.

Two checks run before the first auth change after a member starts, and
again after a membership change:

- **Every member must run fastetcd 1.5.0 or later.** An older member
  cannot apply an auth entry: it would stop receiving the log, and with
  two such members out of three the cluster would lose quorum. So auth
  changes, including `Authenticate`, are refused with `Unavailable`,
  naming the member, until every member is upgraded and reachable.
  During a rolling upgrade, log in before you start and keep the
  token. Tokens issued before the upgrade are lost when the member
  that issued them restarts.
- **Every member must hold the same auth state.** Before 1.5.0 each
  member kept whatever auth calls it served itself, so members
  upgraded from 1.4.x may differ. If they do, auth changes are refused
  with `FailedPrecondition`, and `fastetcd_auth_diverged` is 1 on
  `/metrics`. Nothing is chosen for you. Look at what each member
  holds, then replicate the one that is right to all of them:

  ```
  fastetcd-ctl --endpoint http://m1:2379 auth members
  fastetcd-ctl --endpoint http://m1:2379 --user root:pw auth adopt m2   # name or hex ID
  ```

  `auth adopt` replaces every member's users, roles and enabled flag
  with the chosen member's. Both commands need root while auth is on.
  `Authenticate` still works while members differ. A member only
  accepts a login its own tables agree with, and the result returned is
  the leader's. So if the members' root passwords differ, send the
  login to the leader.

If every member is empty (auth never used) or already identical, both
checks pass silently and nothing changes.

### What a role's permissions cover

With auth enabled, a non-root user's roles are checked on every KV
operation, the same way etcd checks them:

| Operation | Needs |
|---|---|
| `Range` | read on `[key, range_end)` |
| `Put` | write on `key`; plus read if `prev_kv` |
| `DeleteRange` | write on `[key, range_end)`; plus read if `prev_kv` |
| `Txn` | read on every compare target, and the above for **every** op in **both** the success and failure branches (nested txns included) |
| `Watch` (each create) | read on `[key, range_end)` |

A `Txn` is authorized as a whole before anything is proposed: one
access the user's roles do not cover fails it with `PermissionDenied`,
and nothing in it is applied. Both branches are checked because which
one runs is only decided when the txn is applied. A compare is a read,
because it reveals whether a key exists and what it holds. A
write-only role therefore cannot run a guarded write (`compare` +
`put`) on its own keys.

A watch create the user may not read is refused as etcd refuses it: the
response has `created` and `canceled` set, `watch_id` -1 and
`cancel_reason` `etcdserver: permission denied`. Nothing is registered
or replayed, and the stream stays open for other watches. As in etcd,
the check is made once, at create: revoking a permission does not
cancel a watch that is already running; it keeps delivering until it
or its stream ends (restart the client after narrowing its role).

### What needs root

While auth is on, these calls need the caller to be `root` or to hold
the `root` role. The rules are etcd's (`needAdminPermission`, the
maintenance and membership checks); a non-root caller gets
`PermissionDenied`, a caller with no valid token `Unauthenticated`:

| Service | Root only | Any logged-in user |
|---|---|---|
| Auth | every change (`auth enable`/`disable`, user and role add / delete / grant / revoke, **any** password change, including your own), `AuthStatus`, `UserList`, `RoleList`, `UserGet` of another user, `RoleGet` of a role you do not hold | `UserGet` of yourself, `RoleGet` of a role you hold; `Authenticate` needs no login |
| Maintenance | `Snapshot` (the whole keyspace), `Defragment`, `Hash`, `HashKV`, `MoveLeader`, `Downgrade`, `Alarm` activate / disarm | `Status`, `Alarm` list |
| Cluster | `MemberAdd`, `MemberRemove`, `MemberUpdate`, `MemberPromote` | `MemberList` |
| KV | none | `Compact`, as in etcd |
| fastetcd admin | `auth members`, `auth adopt` | none |

With auth off, every call is open, as in etcd. The check runs on the
member serving the call, against its applied auth state, as etcd checks
membership changes.

### Lease calls

A lease's keys are deleted when it is revoked or lapses, so the lease
calls are checked against the keys attached to the lease when the call
arrives, as in etcd (`checkLeasePuts`, `checkLeaseRenew`,
`checkLeaseTimeToLive`, `checkLeaseLeases`; #47). Root is exempt; a lease
with no keys needs only a login.

| Call | Needs, on every key attached to the lease |
|---|---|
| `LeaseRevoke` | write |
| `LeaseKeepAlive` | write (denied, the keep-alive stream ends with `PermissionDenied`) |
| `LeaseTimeToLive` | read, with `keys: true` only; the TTL alone needs a login |
| `LeaseLeases` | read, on every key of every lease |
| `Put` naming a lease, and every put in either branch of a `Txn` | write, besides the put's own key |
| `LeaseGrant` | a login |

**Limits.** No authorization gap is known, but: tokens never expire
(#46), and every check runs on the member serving the call against its
applied auth state at that moment (etcd checks a put and a revoke again
at apply), so a permission revoked a moment earlier can still be honoured
by a member a moment behind.

### Auth errors

Auth errors are etcd's, code and text (since 1.23.1, #105, #127): clientv3
recognises its typed errors by the text, and re-authenticates by itself
only on `etcdserver: invalid auth token` (a token this member does not
know: it restarted, or has not applied that `Authenticate` yet, tokens
not being persisted, as in etcd) and `etcdserver: user name is empty` (no
token). A denied call answers `PermissionDenied` `etcdserver: permission
denied` (the detail is logged at debug, target `fastetcd::authz`); a bad
name or password `InvalidArgument` `etcdserver: authentication failed,
invalid user ID or password`; `Authenticate` with auth off
`FailedPrecondition` `etcdserver: authentication is not enabled`; user and
role not found / already existing, and a missing root user on
`AuthEnable`, etcd's `FailedPrecondition` texts. `tests/clientv3_reauth.sh`
checks etcd's own Go client through a member restart.

## Storage engine

A data directory holds:

| Path | What |
|---|---|
| `fastetcd.redb` | The data file: one redb ACID B-tree with the MVCC data, leases, auth and node metadata. |
| `wal/` | The raft log and vote: a sequential write-ahead log in segments written full of zeros up front (`--wal-segment-bytes`, 16 MiB), plus `spare.wal.tmp`, the next one, made ready in the background. |
| `snapshots/` | Retained raft snapshots. |

There is no engine choice to make: `fastetcd-storage` also has `wal`
and `iouring` key-value engines, but the server cannot use them (#55);
`wal/` is the raft log, not one of those.

### Writes: one sequential fsync (#85)

A client's write is acknowledged after its raft entry is durable in the
WAL (on a quorum) and applied. The fsync on that path is an append at
the end of the newest segment: a sequential write, which is what a
spinning disk or a network block device does best. Concurrent writes
share one `fdatasync` (group commit, #95): while one write (or batch)
is being made durable and applied, the writes that arrive queue on the
leader, and the moment it is answered they are proposed together as
one raft entry, written and synced once. A
burst of writes pays about one fsync, not one each; a lone write pays
one. (openraft 0.9 appends one entry at a time and waits for its fsync,
so the batch is where writes are grouped.) A batch takes everything that
queued, up to 4096 writes or 512 KiB, so with a thousand clients waiting
one fsync carries hundreds of writes, not at most 256 as before 1.17
(#94). A follower behind is sent the log in AppendEntries of at most
~1 MiB of entries (each must be sent and fsynced by the follower within
a heartbeat interval, openraft's timeout), and the peer port accepts
messages up to 16 MiB.

Applying an entry does not touch the data file: its writes are held in
RAM (the write-behind layer, up to `--write-behind-bytes`, 64 MiB) and
in the RAM index and value cache ([Memory](#memory)), and reads see
them at once. A **checkpoint** then writes them into the data file and
makes it durable, in the background: at most
`--wal-checkpoint-interval-ms` (100) after an apply, or after
`--wal-checkpoint-entries` (10000) entries. On a slow disk, where a
checkpoint (the data file's page writes and fsync) takes about as long
as that interval, the next one waits at least four times as long as the
last took, so checkpoints use at most a fifth of the disk's time and
the WAL's fsyncs, which clients wait on, are not queued behind them
(#95). An apply never waits on the data file's fsync; only if more than
`--write-behind-bytes` (or 1024 batches) is held does it write them out
first, and since 1.24.1 that waits only for the write-out: the
checkpoint fsyncs the data file with no lock held, then makes its
durable commit, which has little left to write (#93).

After a crash the data file is as of its last checkpoint, and the WAL
replays the rest: a member re-applies up to the committed index it
recorded, and the leader re-commits anything after it. Nothing
acknowledged is lost. A torn write at the end of the WAL (a crash during
a write that was never acknowledged) is cut off at startup; damage
anywhere else in it stops the member, which then needs replacing
(member remove / add) like a member with a corrupt data file.

The raft log is purged after each snapshot (`--snapshot-count`), but a
WAL segment is deleted only once a checkpoint has made the data file
durable past every entry in it.

An fdatasync of the WAL that takes 1 s or more (etcd's threshold) is
logged at WARN as `slow raft WAL fdatasync` with `took_ms`: a write
stall at that time was the disk's.

Metrics: `fastetcd_wal_fsyncs_total`, `fastetcd_wal_fsync_seconds_total`,
`fastetcd_wal_bytes_appended_total`, `fastetcd_wal_segments`,
`fastetcd_wal_cached_bytes`, `fastetcd_checkpoints_total`,
`fastetcd_checkpoint_seconds_total`, `fastetcd_checkpoint_failures_total`,
`fastetcd_checkpoint_commit_seconds_total`, `fastetcd_checkpoint_durable_applied_index`,
`fastetcd_wal_fsync_max_seconds`, `fastetcd_checkpoint_max_seconds`,
`fastetcd_checkpoint_commit_max_seconds`,
`fastetcd_checkpoint_pace_seconds` (the least time before the next
checkpoint, 4x the last one's duration); group commit (#95):
`fastetcd_wal_entries_synced_total` and
`fastetcd_wal_proposals_synced_total`, the raft entries and the client
proposals in them made durable by the fsyncs; the write-behind layer:
`fastetcd_write_behind_bytes`, `fastetcd_write_behind_batches`,
`fastetcd_write_behind_flushes_total`,
`fastetcd_write_behind_flush_seconds_total`,
`fastetcd_write_behind_backpressure_total` and
`fastetcd_write_behind_backpressure_seconds_total` (applies that had to
write the layers out, and how long they waited),
`fastetcd_write_behind_presync_seconds_total` (the checkpoints' fsyncs
of the data file outside the lock). Average fsync time is
`rate(fastetcd_wal_fsync_seconds_total) / rate(fastetcd_wal_fsyncs_total)`;
fsyncs per second is `rate(fastetcd_wal_fsyncs_total)`, and writes per
fsync is
`rate(fastetcd_wal_proposals_synced_total) / rate(fastetcd_wal_fsyncs_total)`.

**Upgrading to 1.12** moves the raft log out of the data file on the
first start: `wal/` is built from it (in `wal.tmp/`, then renamed) and
the data file's copy is cleared. It is one-way: **do not downgrade a
member below 1.12** once it has started on 1.12, since an older binary
would find no raft log in the data file. Upgrade members one at a time,
as usual; a 1.12 member and an older one replicate normally (the peer
protocol did not change).

## Memory

A read should not wait on the disk (#82). Like etcd, fastetcd keeps
**every key's index in RAM** (etcd's `treeIndex`): key → the revisions
of each of its generations. It is loaded from the data file at startup
and changed only after the write it describes is in the engine, so it
is never ahead of the disk. A read finds the revision it needs there;
compaction walks it too, instead of reading the whole index table.

On top of it, a **latest-value cache** holds the newest record of
recently used keys, least recently used evicted first, within
`--value-cache-bytes`. Every write puts what it wrote in it, a delete
removes the key, and a read of the latest revision that missed fills
it. An entry answers a read only when it is the exact revision the
index names for that read, so the cache can make a read faster but
never different: a hot single-object GET reads nothing from the disk,
history and cold keys read the engine as before. Linearizable reads
still wait for the read index first; the cache changes where a read's
bytes come from, not when it may read. Values over
`--value-cache-max-entry-bytes` are never cached.

Memory to expect, beyond the process's own few tens of MiB:

| Part | Size |
|---|---|
| Key index | ~(key length + 64 B) per key, plus 16 B per revision kept (history since the last compaction) and ~40 B per generation. Tens of MiB for a large Kubernetes cluster. |
| Value cache | Up to `--value-cache-bytes` (default min(128 MiB, 5% of memory)). |
| Engine cache | Up to `--engine-cache-bytes` (default 256 MiB), redb's page cache, filled as pages are read. |
| Raft log cache | Up to `--wal-cache-bytes` (default 64 MiB): recent raft entries, for replication. |
| Write-behind | Up to `--write-behind-bytes` (default 64 MiB): applied writes not yet in the data file (about 100 ms worth). |

The kernel's page cache comes on top and is reclaimable. The startup log
line `RAM cache:` gives the configured budgets and the index's size; the
metrics below give them live.

| Metric | Type | Meaning |
|---|---|---|
| `fastetcd_value_cache_hits_total` / `…_misses_total` | counter | Record lookups the value cache answered, and those that read the engine. |
| `fastetcd_value_cache_evictions_total` | counter | Entries evicted to stay in budget. Climbing fast with a low hit ratio: the budget is smaller than the hot set. |
| `fastetcd_value_cache_bytes` / `…_entries` / `…_budget_bytes` | gauge | What the cache holds (entry overhead included), and its budget. |
| `fastetcd_key_index_keys` / `fastetcd_key_index_bytes` | gauge | Keys in the resident index, and its approximate size. |
| `fastetcd_mvcc_get_duration_seconds{cache}` | histogram | The store's time for each single-key Range (after the read barrier): `cache="hit"` read nothing from the engine, `miss` did. |
| `process_resident_memory_bytes` / `process_virtual_memory_bytes` | gauge | The process's RSS and virtual size (since 1.24, #84; etcd's names, from `/proc/self/status`). RSS flat over a long run, near the budgets above, is the check. |
| `process_cpu_seconds_total`, `process_start_time_seconds`, `process_open_fds`, `process_max_fds` | counter / gauge | etcd's other process metrics. |

On server3 (X9 blade, spinning disk, fastetcd 1.18) over a 7.5 h day the
value cache answered 99.8% of lookups; 99.5% of hits took under 1 ms in
the store, and apiserver GETs of the cilium-operator lease stayed under
13 ms p99 at 4 300 GETs/s (`docs/benchmarks/blade-server3.md`).

## Multi-node

```
# On node-a:
fastetcd --name=node-a --data-dir=/var/lib/fastetcd-a \
    --listen-client-urls=http://127.0.0.1:23791 \
    --listen-peer-urls=http://127.0.0.1:23801 \
    --initial-cluster-token=my-cluster \
    --initial-cluster=node-a=http://127.0.0.1:23801,node-b=http://127.0.0.1:23802,node-c=http://127.0.0.1:23803

# Same flags on node-b/c with their own data dirs and listen URLs.
```

With `--initial-cluster-state=new` (the default) each node calls
`raft.initialize` with the same member set; openraft elects a leader
among them. After bootstrap, `MemberAdd` adds new nodes (start them
with `--initial-cluster-state=existing`); `MemberRemove` removes them.
On separate hosts, listen on a routable address and set
`--advertise-client-urls` / `--initial-advertise-peer-urls` to it.

**Tested rolling upgrades** (`tests/rolling_upgrade.sh`, #65, at 1.25.1):
v1.9.0 → 1.25.1 and v1.24.1 → 1.25.1 one member process at a time, and
1.25.1 → v1.24.1 back, under writes, with auth and a lease: nothing
acknowledged lost, no write outage over a few seconds, auth, RBAC and
the lease intact, a pre-version backup on every member, batching only
once no member is older than 1.10. Do the same: one member at a time,
each back and applying before the next.

**Upgrading to 1.10, and versions after it.** From 1.10 a leader batches
concurrent writes into one log entry (`FastetcdLogEntry::Batch`), which
a member older than 1.10 cannot decode. It does so only once every
member, learners included, answers the 1.10 `ConfirmLeader` peer RPC,
so a rolling upgrade is safe: writes go one per entry, as before, until
the last member is upgraded. After that:

- **Do not add a member older than 1.10**: an older one stops at the
  first batch in the log. Since 1.20 a member records that its log has
  held a batch (`fastetcd_log_has_batched` = 1, never reset, carried by
  snapshots), and once it has, `MemberAdd` asks the new member which
  version it runs and refuses one older than 1.10. A member that does not
  answer yet (`member add` before it is started, the usual order) is
  added as a learner, but `MemberPromote` (or adding it as a voter) needs
  it to answer as 1.10 or later (#77).
- **Do not downgrade a member below 1.10** once the cluster has batched:
  it cannot read its own log. A downgraded binary cannot be made to
  refuse, so the leader checks every member every 30 s and logs an error
  naming one that answers as older; `fastetcd_members_unable_to_read_batches`
  counts them (on the leader). Upgrade it again, or remove it and add it
  back empty.

**A txn inside a txn (1.22, #56).** A nested txn is a new kind of log
entry content that a member older than 1.22 cannot decode. The member a
txn that nests arrives at asks every other member its version
(`ConfirmLeader`, once per membership) and refuses the txn until all run
1.22 or later: `FailedPrecondition` naming an older member, `Unavailable`
while one does not answer. Flat txns are never held back, so a rolling
upgrade is safe. Once a nested txn has been applied, **do not add a
member older than 1.22 or downgrade one below it**: it stops at that
entry. `MemberAdd` does not check for this (it refuses only members
older than 1.10).

**Lease keep-alives (1.23, #92).** As in etcd, the leader renews a lease
in RAM: a keep-alive is not logged, so it costs no fsync (on a follower it
is forwarded to the leader, and TimeToLive is answered by the leader).
The leader confirms it still leads before renewing. Expiry is still a
`LeaseRevoke` through Raft, proposed by the leader for a lease whose
deadline, the later of the persisted one and its RAM renewal, has
passed. A renewal lives only in the leader's RAM, so **a new leader, or a
restarted single member, gives every lease a full TTL from the moment it
leads** (etcd's `Promote`; etcd's optional lease checkpointing is not
implemented). A member older than 1.23 that became leader would read only
the persisted deadlines and expire every lease kept alive that way, so
RAM renewal is used only while every member answers `ConfirmLeader` with
1.23 or later; until then keep-alives go through Raft as before
(`fastetcd_lease_renewals_total{path="raft"}`). **Do not downgrade a
member below 1.23**, or add one, while keep-alives are renewed in RAM.

The same holds for replicated auth entries since 1.5.0.

**Leases in raft snapshots (1.16, #41).** Before 1.16 a raft snapshot
carried the MVCC and auth tables but not the lease tables, so a member
caught up by one (a new member or learner, or one that fell behind the
purged log) had no record of leases granted before that snapshot, and
kept any stale ones of its own. Its keys on those leases never expire
there, and when the leader revokes such a lease the other members delete
its keys and that member does not. From 1.16 a snapshot carries both
lease tables and an install replaces them, so this cannot start again.
A member that was caught up by a snapshot before 1.16 keeps whatever it
has until its next snapshot install: once every member runs 1.16,
remove that member and add it back empty (`member remove`, wipe its
data dir, `member add`), so it is caught up by a new snapshot. If you
cannot tell which members were, do it for each in turn. A 1.16 leader's
snapshots install on 1.5–1.15 members as before (the lease tables are
ignored there); a snapshot from an older leader leaves a 1.16 member's
lease tables as they are.

**A request that cannot apply (1.14, #49).** A put with `ignore_value`
or `ignore_lease` on a missing key, a `Compact` outside the store's
revisions, a keep-alive of a lease that is gone: from 1.14 the state
machine refuses such an entry (nothing applied) and keeps going; before
1.14 it stopped, on every member. A 1.14 leader also refuses them before
proposing, so they rarely reach the log, but one can (a Txn that deletes
a key and then puts it with `ignore_value`), and a member older than
1.14 stops on it, so finish a rolling upgrade to 1.14 before relying on
it. During the upgrade a write sent to an older follower that the 1.14
leader refuses comes back `Unavailable` instead of etcd's error (the
follower cannot decode the refusal); the write is refused either way.

### Timeouts and slow disks

A leader writes every log entry to its WAL and waits for the fsync before
it does anything else, heartbeats included (openraft 0.9 sends them from
the same loop, #103). If one fsync takes longer than the followers'
election timeout, plus openraft's leader lease, they elect a new leader.
With the defaults (`--heartbeat-interval 250`, `--election-timeout 1000`)
that is a stall of about 3–4 s. Each election pauses writes for a few
seconds. etcd behaves the same way and has the same remedy:

- Read the longest fsync each member has seen, `fastetcd_wal_fsync_max_seconds`.
  Look for the `slow raft WAL fdatasync` warnings, which name the
  election timeout when an fsync outlasted it.
- Set `--election-timeout` above the longest stall, on every member
  (`ETCD_ELECTION_TIMEOUT` works too). A follower then waits longer before
  it notices a leader that is really gone: that is the trade.
- Leave `--heartbeat-interval` alone unless the followers' fsync is fast.
  In fastetcd it is also how long one AppendEntries may take to be sent and
  fsynced by a follower (#94); below that, replication times out and
  retries forever.

The election timeout must be at least twice the heartbeat interval and at
most 50 s.

### Cluster id

Every response header carries a `cluster_id`. Clients that talk to
more than one cluster pin it: they record the id an endpoint first
reported and treat a change as "this endpoint now points at a
different store", draining and reconnecting instead of mixing two
clusters' data. So **give every deployment its own id**, and every
member of one deployment the same one:

- `--cluster-id=<n>` (`FASTETCD_CLUSTER_ID`) sets it explicitly. `0`
  is rejected.
- Otherwise, a new store derives it from `--initial-cluster-token`
  (`ETCD_INITIAL_CLUSTER_TOKEN` works too): FNV-1a-64 of the token,
  masked to 63 bits, never 0. Pass the same token to every member,
  including ones added later with `--initial-cluster-state=existing`.
- With neither, the id is `1`, the same as every other unconfigured
  deployment, and fastetcd warns at startup.

The id is chosen on a store's first start and persisted in its data
directory. Later starts use the persisted id whatever the token says,
so a restart or an upgrade never changes it. A store created before
v1.3.0 keeps reporting `1`. The one way to change a store's id is to
pass `--cluster-id`, and clients pinned to the old id will reconnect.
Members do not exchange their ids, so a member whose flags differ from
the others' reports a different id (#38).

## Disk space

fastetcd sizes itself against the volume it is on, not against a
configured quota: it samples the data directory and the filesystem's
real free space, reclaims at a high-water mark (compact → raft snapshot
→ log purge → defragment) and raises etcd's `NOSPACE` alarm at a higher
mark, where writes are refused but reads, deletes, compaction and
defragment keep working.

The defaults need no configuration on a normally-sized volume. On a
small one (a few hundred MB or less), lower `--space-high-water-percent`
so reclaim starts earlier, and keep `--max-snapshots 1` — a snapshot is
a full copy of the database.

If a volume has already filled and the server can no longer do anything,
stop it and defragment offline:

```
systemctl stop fastetcd
fastetcd defrag --data-dir /var/lib/fastetcd
systemctl start fastetcd
```

Full details, flags, metrics and recovery procedures:
`docs/04-disk-space.md`.

## Backups

Backups that restore:

- **Periodic backups** with `--backup-dir` on another volume, restored
  with `fastetcd restore <file>` (or automatically, on a lone member
  whose data file is corrupt). See `docs/05-backup-and-recovery.md`.
- **`fastetcd backup --out <file>`** with the server stopped, restored
  with `fastetcd restore <file>`.

- **A live snapshot**, `etcdctl snapshot save <file>` or `fastetcd-ctl
  snapshot-save <file>` against a running member (root only with auth
  on), restored with `fastetcd restore <file>` on a stopped member
  (1.15; #61). It is the same backup format as `--backup-dir` writes:
  every table, leases and users included, from one point in time, with
  a SHA-256 that `fastetcd-ctl snapshot-save` and `fastetcd restore`
  check. It is not a BoltDB file, so `etcdctl snapshot status` and
  `etcdctl snapshot restore` cannot read it. A snapshot saved by 1.14.1
  or earlier held a raft snapshot that nothing restores; take a new one.

A restored data dir keeps the identity and raft membership of the
member the backup came from. To start it as a different member, or as
the first member of a new cluster, start it once with
`--force-new-cluster` and add the others with `member add`, as with
etcd's `snapshot restore`.

## v3 JSON gateway

The client port also serves etcd's v3 JSON gateway, as etcd does, so
HTTP tools need no gRPC client. It is etcd's own surface (v3.6), so
`curl` recipes and tools written for etcd work unchanged:

```bash
# Keys and values are base64: "foo" = Zm9v, "bar" = YmFy.
curl -s -X POST http://127.0.0.1:2379/v3/kv/put -d '{"key":"Zm9v","value":"YmFy"}'
curl -s -X POST http://127.0.0.1:2379/v3/kv/range -d '{"key":"Zm9v"}'
# {"header":{"cluster_id":"…","member_id":"…","revision":"1","raft_term":"1"},
#  "kvs":[{"key":"Zm9v","create_revision":"1","mod_revision":"1","version":"1","value":"YmFy"}],"count":"1"}
curl -s -X POST http://127.0.0.1:2379/v3/maintenance/status
curl -s -X POST http://127.0.0.1:2379/v3/cluster/member/list
# Everything under /registry/, keys only:
curl -s -X POST http://127.0.0.1:2379/v3/kv/range \
  -d '{"key":"L3JlZ2lzdHJ5Lw==","range_end":"L3JlZ2lzdHJ5MA==","keys_only":true}'
# A watch: the body holds requests, each response is a line.
curl -sN -X POST http://127.0.0.1:2379/v3/watch -d '{"create_request":{"key":"Zm9v"}}'
```

- **Routes:** `POST` only, the body is the request message as JSON, and an
  empty body is the default request. KV: `/v3/kv/range`, `put`,
  `deleterange`, `txn`, `compaction`. Lease: `/v3/lease/grant`, `revoke`,
  `timetolive`, `leases` (also under `/v3/kv/lease/`), `keepalive`.
  Watch: `/v3/watch`. Cluster: `/v3/cluster/member/add`, `remove`,
  `update`, `list`, `promote`. Maintenance: `/v3/maintenance/alarm`,
  `status`, `defragment`, `hash`, `hashkv`, `snapshot`,
  `transfer-leadership`, `downgrade`. Auth: `/v3/auth/enable`, `disable`,
  `status`, `authenticate`, `user/{add,get,list,delete,changepw,grant,revoke}`,
  `role/{add,get,list,delete,grant,revoke}`.
- **JSON:** as etcd's gateway writes it: proto field names (`raft_term`,
  `dbSize`, `ID`, `peerURLs`), 64-bit numbers as strings, keys and values
  base64, enums by name (`"action":"GET"`, `"alarm":"NOSPACE"`), fields
  at their default value left out. Requests accept either field
  spelling, numbers or strings, and ignore unknown fields.
- **Errors:** the HTTP status grpc-gateway maps the gRPC code to (400
  invalid argument / out of range, 401 unauthenticated, 403 permission
  denied, 404 not found, 429 NOSPACE, 503 unavailable, …), body
  `{"code":<grpc code>,"message":"…"}`.
- **Streams** (`snapshot`, `watch`, `keepalive`): one JSON object per
  line, `{"result":{…}}` or `{"error":{…}}`. For `watch` and `keepalive`
  the body is a sequence of JSON requests. A watch keeps streaming after
  the body ends, until the client disconnects; a keepalive stream ends
  with its requests. WebSocket upgrades are not supported.
- **Auth:** the same as gRPC. Send the token from `/v3/auth/authenticate`
  as the `Authorization` header (no `Bearer` prefix, as with etcd). Under
  `--client-cert-auth` a client certificate's CN names the user, as over
  gRPC. Writes and admin verbs are served, as etcd serves them; RBAC and
  the root-only rules apply to them exactly as to gRPC calls.
- Gateway calls count in `grpc_server_*_total` under the method they call.
- `--enable-grpc-gateway=false` (or `ETCD_ENABLE_GRPC_GATEWAY=false`)
  turns it off. gRPC and `/health` are unaffected.

## Health checks

On the client port, as etcd's (since 1.25, #69; before that they always
answered healthy):

| Route | Healthy | Not (503) |
|---|---|---|
| `GET /health` | `{"health":"true","reason":""}` | `{"health":"false","reason":…}`: `ALARM NOSPACE` / `ALARM CORRUPT` (this member's alarm), `RAFT NO LEADER`, `RANGE ERROR:…` (a linearizable keys-only read failed or took longer than 5 s + 2x the election timeout) |
| `GET /health?serializable=true` | as above, without the leader check and with a local read: is this process serving | |
| `GET /livez` | `ok` | `serializable_read` failed |
| `GET /readyz` | `ok` | `data_corruption` (CORRUPT alarm), `serializable_read`, `linearizable_read`, `non_learner` |

`?exclude=NOSPACE` / `?exclude=CORRUPT` on `/health`, and
`?exclude=<check>` on `/livez` / `/readyz`, skip one; `?verbose` lists
every check's `[+]name ok` / `[-]name failed: …` line, which a failure
always shows. Each check also has its own route (`/readyz/linearizable_read`).
`grpc.health.v1` follows `/readyz`: every service, and `""`, is SERVING
while it passes (checked every second). Counters
`etcd_server_health_success_total` / `etcd_server_health_failures_total`.

Point a **liveness** probe at `/livez` (it never fails on quorum loss,
so a partition does not get members restarted) and **readiness** at
`/readyz`; the Helm chart does. Alarms are this member's: fastetcd raises
NOSPACE and CORRUPT per member rather than cluster-wide. CORRUPT means
the store restored itself from a backup (#37): the member serves, but
`/health` and `/readyz` say false until `etcdctl alarm disarm`, as
etcd's do. A probe that should keep routing to it excludes the alarm.

## Metrics

Prometheus text on `GET /metrics` at `--listen-metrics-url`, refreshed
on each scrape. Names are etcd's where one exists, so dashboards written
for etcd work. Counters start at 0 when the process starts; compute rates
from deltas between scrapes.

**Traffic** (#29):

| Metric | Type | Meaning |
|---|---|---|
| `grpc_server_started_total{grpc_type,grpc_service,grpc_method}` | counter | Calls started on the client port. |
| `grpc_server_handled_total{…,grpc_code}` | counter | Calls finished, by status code name (`OK`, `NotFound`, …). A call whose client went away before its status was sent (how every `Watch` ends) is `Canceled`. |
| `etcd_debugging_mvcc_put_total`, `…_delete_total` | counter | Puts and deletes this member applied. Every member counts every write, including those in a txn and the deletes a lease revoke causes. |
| `etcd_debugging_mvcc_range_total` | counter | Ranges this member served, including ranges in a txn. |
| `etcd_debugging_mvcc_txn_total` | counter | Txns this member applied. |
| `etcd_debugging_mvcc_watch_stream_total` | gauge | Open `Watch` streams. |
| `etcd_debugging_mvcc_watcher_total` | gauge | Watchers on those streams. |
| `etcd_debugging_mvcc_slow_watcher_total` | gauge | Watchers behind the head: being caught up from history, or on a stream whose client is not reading. A watcher that falls behind is resynced rather than losing events (#16), so this is the sign that one is behind. |
| `fastetcd_watch_resyncs_total`, `fastetcd_watch_lag_cancels_total` | counter | Watchers caught up from history; watchers cancelled because that history was compacted. |

**Raft and membership:**

| Metric | Type | Meaning |
|---|---|---|
| `etcd_server_has_leader` | gauge | 1 if this member knows a leader. |
| `etcd_server_is_leader` | gauge | 1 if this member *is* the leader. |
| `etcd_server_id{server_id="<hex>"}` | gauge | Always 1; the label is this member's id, to match a scrape to a member. |
| `etcd_server_leader_changes_seen_total` | counter | Leader changes seen. |
| `etcd_server_proposals_committed_total` | gauge | The raft committed index (a gauge, as in etcd). |
| `etcd_server_proposals_applied_total` | gauge | The raft applied index. Committed minus applied is apply lag. |
| `etcd_server_proposals_pending` | gauge | Proposals this member is waiting on. |
| `fastetcd_read_index_total{path}` | counter | Linearizable read barriers this member served, by path: `sole_voter` (the only voter, from local state), `quorum` (leadership confirmed over `ConfirmLeader`), `raft` (openraft's read-index, which queues behind writes). Mostly `raft` on a leader means the fast path is not being taken: an older or unreachable member, or leadership changing. |
| `fastetcd_proposal_batches_total` | counter | Batched log entries this member proposed (group commit). |
| `fastetcd_proposals_batched_total` / `fastetcd_proposals_single_total` | counter | Proposals that went in batches, and alone. Stays all `single` while any member is older than 1.10. |
| `fastetcd_lease_renewals_total{path}` | counter | Keep-alives this member renewed as the leader: `ram`, or `raft` while a member is older than 1.23 (#92). |
| `fastetcd_lease_promotions_total` | counter | Terms in which this member, as the new leader, gave every lease a full TTL. |
| `fastetcd_log_has_batched` | gauge | 1 once this member has applied a batched log entry; never back to 0. From then on a member older than 1.10 cannot read the log (#77). |
| `fastetcd_members_unable_to_read_batches` | gauge | On the leader: members answering as older than 1.10 while the log holds batches. Non-zero: upgrade or replace them. |

**Store:** `etcd_debugging_mvcc_current_revision`,
`etcd_debugging_mvcc_compact_revision`,
`etcd_mvcc_db_total_size_in_bytes`,
`etcd_mvcc_db_total_size_in_use_in_bytes`,
`etcd_server_quota_backend_bytes`, `fastetcd_engine_info{engine}`
(`redb`, `wal` or `iouring`), the RAM cache metrics in
[Memory](#memory), and the disk and recovery metrics in
[04-disk-space](04-disk-space.md) and
[05-backup-and-recovery](05-backup-and-recovery.md).

## Migration from etcd

```
# Take a snapshot from a running etcd cluster (or use a pre-existing
# snapshot file).
etcdctl --endpoints=$ETCD_ENDPOINT snapshot save etcd-snap.db

# Replay into a fresh fastetcd data dir.
fastetcd-migrate --from etcd-snap.db --to /var/lib/fastetcd \
    --preserve-revisions
```

Without `--preserve-revisions`, only the latest live value per key
is imported. Use the flag if you have clients that rely on
historical reads or watch reconnections from past revisions.
