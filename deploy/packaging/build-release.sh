#!/usr/bin/env bash
# Builds the rpm/deb/tarball release artifacts for a tagged version.
# Run on a Linux host with rustup (x86_64-unknown-linux-musl target),
# cargo-deb, cargo-generate-rpm, protoc, and musl-gcc installed —
# e.g. dev.g8.lo. Not run in CI; GitHub Actions only runs `cargo test`.
#
# Usage: deploy/packaging/build-release.sh vX.Y.Z
set -euo pipefail

ver="${1:?usage: build-release.sh vX.Y.Z}"
cd "$(dirname "$0")/../.."

cargo build --release --workspace --target x86_64-unknown-linux-musl

mkdir -p target/debian target/generate-rpm
cargo deb -p fastetcd-server --no-build --target x86_64-unknown-linux-musl -o target/debian/
cargo generate-rpm -p crates/server -o target/generate-rpm/

dir="fastetcd-${ver}-x86_64-linux"
rm -rf dist
mkdir -p "dist/$dir"
cp target/x86_64-unknown-linux-musl/release/fastetcd \
   target/x86_64-unknown-linux-musl/release/fastetcd-ctl \
   target/x86_64-unknown-linux-musl/release/fastetcd-migrate \
   target/x86_64-unknown-linux-musl/release/fastetcd-bench \
   deploy/systemd/fastetcd.service \
   deploy/systemd/fastetcd.conf.example \
   README.md \
   "dist/$dir/"
tar czf "dist/fastetcd-${ver}-x86_64-linux-musl.tar.gz" -C dist "$dir"

cd dist
mv ../target/debian/*.deb ../target/generate-rpm/*.rpm .
sha256sum *.deb *.rpm *.tar.gz > SHA256SUMS.txt

echo "Built in dist/:"
ls -la
echo
echo "Publish with: gh release create ${ver} dist/*.deb dist/*.rpm dist/*.tar.gz dist/SHA256SUMS.txt --generate-notes"
