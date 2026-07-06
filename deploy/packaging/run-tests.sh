#!/usr/bin/env bash
# Runs the full test matrix. GitHub Actions is intentionally disabled for
# this repo (repo-level setting, confirmed 2026-07-06) — dev.g8.lo is the
# sole build+test path. Run this before build-release.sh on any tagged
# version, and any time you want a pre-push check.
#
# Usage: deploy/packaging/run-tests.sh
set -euo pipefail
cd "$(dirname "$0")/../.."

echo "=== cargo test --workspace ==="
cargo test --workspace --no-fail-fast

echo "=== cargo test -p fastetcd-storage --features wal-engine ==="
cargo test -p fastetcd-storage --features wal-engine --no-fail-fast

echo "=== cargo test -p fastetcd-storage --features iouring ==="
cargo test -p fastetcd-storage --features iouring --no-fail-fast

echo "All tests passed."
