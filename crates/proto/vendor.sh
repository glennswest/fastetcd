#!/usr/bin/env bash
# Vendor etcd v3 .proto files into crates/proto/protos/.
#
# Re-running this script regenerates the vendored protos from upstream
# tags. It strips annotations that prost/tonic cannot resolve without
# additional plugin proto vendoring (see strip_annotations.py for details).
#
# The stripped protos retain the full gRPC service + message definitions,
# which is everything tonic-build needs to emit Rust stubs.
#
# Usage: ./vendor.sh                    # uses default upstream tag
#        ETCD_TAG=v3.6.11 ./vendor.sh   # pin a specific tag

set -euo pipefail

ETCD_TAG="${ETCD_TAG:-v3.6.11}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STRIP="${SCRIPT_DIR}/strip_annotations.py"
DEST="${SCRIPT_DIR}/protos/etcd/api"

mkdir -p "${DEST}/etcdserverpb" "${DEST}/mvccpb" "${DEST}/authpb"

# versionpb is intentionally not vendored — it only declares min-version
# metadata via `extend google.protobuf.*` blocks that require
# descriptor.proto. We strip its uses from the other protos instead.

echo "fetching etcd ${ETCD_TAG} protos..."

for path in etcdserverpb/rpc.proto mvccpb/kv.proto authpb/auth.proto; do
    tmp="$(mktemp)"
    curl --silent --show-error --fail --location \
        "https://raw.githubusercontent.com/etcd-io/etcd/${ETCD_TAG}/api/${path}" \
        > "${tmp}"
    python3 "${STRIP}" "${tmp}" > "${DEST}/${path}"
    rm -f "${tmp}"
    echo "  vendored ${path}"
done

echo "done. vendored protos under ${DEST}/"
