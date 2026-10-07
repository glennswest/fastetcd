#!/bin/sh
# Build fastetcd's test image context for the commit checked out (#36).
#
#   test/build.sh [target]        default x86_64-unknown-linux-musl
#
# Per stormcentral docs/test-standard.md this runs first, in the checkout
# on the build box, with cargo: it builds the static test binary and the
# commit's static fastetcd (the medium and long suites, and short's last
# check, run it as their own members) and stages both in test/.stage/,
# which test/Containerfile (context: the repo root) packages FROM scratch.
set -eu
target=${1:-x86_64-unknown-linux-musl}
root=$(cd "$(dirname "$0")/.." && pwd)
cargo build --release --locked --target "$target" -p fastetcd-test -p fastetcd-server \
    --manifest-path "$root/Cargo.toml"
tdir=${CARGO_TARGET_DIR:-$root/target}
stage="$root/test/.stage"
rm -rf "$stage"
mkdir -p "$stage"
cp "$tdir/$target/release/fastetcd-test" "$tdir/$target/release/fastetcd" "$stage/"
echo "staged: $(ls "$stage" | tr '\n' ' ')"
