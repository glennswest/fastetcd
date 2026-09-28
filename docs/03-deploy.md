# 03 — Deployment

This document covers running fastetcd in production. For development
runs see `README.md`.

## Container

The repo ships a multi-stage `Dockerfile` (binary built in the
image) and a `Dockerfile.ci` (binary pre-built outside, faster
image build in CI). Both produce a distroless image with the
`fastetcd` binary at `/usr/local/bin/fastetcd`.

```
docker build -t fastetcd:dev .
docker run --rm -p 2379:2379 -p 2380:2380 \
    -v fastetcd-data:/var/lib/fastetcd \
    fastetcd:dev
```

GitHub Actions is disabled for this repo (repo-level setting,
confirmed off since 2026-05-24 — not a workflow or billing issue,
just switched off). All testing, building, and packaging happens
by hand on a Linux box with the musl target and packaging tools
installed — `dev.g8.lo` — via `deploy/packaging/run-tests.sh` and
`deploy/packaging/build-release.sh`.

```
podman build -t ghcr.io/glennswest/fastetcd:vX.Y.Z -f Dockerfile.ci .
podman push ghcr.io/glennswest/fastetcd:vX.Y.Z
```

## Linux packages (rpm / deb)

Releases publish `fastetcd-vX.Y.Z-1.x86_64.rpm` and
`fastetcd_vX.Y.Z-1_amd64.deb` to [GitHub
Releases](https://github.com/glennswest/fastetcd/releases), plus a
plain `fastetcd-vX.Y.Z-x86_64-linux-musl.tar.gz` for distros that
use neither package manager. All three bundle `fastetcd` /
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

Before v1.5.0, `--cert-file` was also served on the peer port and the
`--peer-*` flags were parsed and ignored. Members still dialled each
other in plaintext, so a multi-member cluster with `--cert-file` set
could not form (fastetcd#23).

The Helm chart has a separate `peerTls` block for this (see
`values.yaml`).

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

**Auth is not yet a security boundary.** One gap remains, tracked: the
lease RPCs are not authorized, so any user can revoke any lease and
delete the keys attached to it (#47). Until that lands, treat auth as
protection against mistakes by trusted clients, not against a hostile
one: scope untrusted clients with mTLS and a proxy.

## Storage engine

fastetcd ships two engines:

- `redb` (default): cross-platform, ACID single-file B-tree. Good
  for development, smaller deployments, and any environment where
  single-file ops simplicity matters.
- `iouring` (Linux-only, opt-in at build time with
  `--features iouring`): tokio-uring-backed append-only WAL +
  in-memory index. Architectural fit for high write throughput
  with predictable p99 once the O_DIRECT + group-commit tuning
  lands (tracked).

Switch at runtime by recompiling with the right features and
pointing `--data-dir` at a fresh directory; cross-engine migration
is not currently supported in place.

## Multi-node

```
# On node-a:
fastetcd --name=node-a --data-dir=/var/lib/fastetcd-a \
    --listen-client-url=127.0.0.1:23791 \
    --listen-peer-url=127.0.0.1:23801 \
    --initial-cluster=node-a=http://127.0.0.1:23801,node-b=http://127.0.0.1:23802,node-c=http://127.0.0.1:23803

# Same flags on node-b/c with their own data dirs and listen URLs.
```

Each node calls `raft.initialize` with the same member set;
openraft elects a leader among them. After bootstrap, `MemberAdd`
adds new nodes; `MemberRemove` removes them.

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

```
etcdctl --endpoints=$ENDPOINT snapshot save snapshot.db
```

`Maintenance.Snapshot` streams the entire engine state to the
client in 64 KiB chunks, read from the snapshot file on disk rather
than buffered in memory. The snapshot file can be re-imported on a
fresh fastetcd via `fastetcd-migrate --from=snapshot.db
--to=/var/lib/fastetcd-new`.

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
