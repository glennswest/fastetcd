#!/usr/bin/env bash
# Build the fastetcd golden tree with deploy/build-golden.sh (the entry point
# stormcentral runs) and check what is in it (#257).
#
#   deploy/check-image.sh [STORMCOS_CHECKOUT]
#
# With a stormcos checkout, also: the stage's config is rebuilt from that
# checkout's deploy/build-goldens.sh §fastetcd — its heredoc, its
# env_default_flag and membership edits and its log limits, the recipe's own
# text run as the stage runs it — and must equal this golden's config value
# for value. Then stormcos's gen-static-pod.py (stormcos#559) makes the static
# pod from each, with stormcos's etcd.yaml, and the two must be identical.
# Run on a build VM (sc-build); writes only under $TMPDIR.
set -euo pipefail
cd "$(dirname "$0")/.."
stormcos=${1:-}
work=$(mktemp -d "${TMPDIR:-/tmp}/check-image.XXXXXX")
fail=0
bad() { echo "FAIL: $*"; fail=1; }
out="$work/fastetcd"
echo "== fastetcd"
bash deploy/build-golden.sh fastetcd "$out"
for b in fastetcd fastetcd-ctl fastetcd-migrate; do
    [ -x "$out/usr/bin/$b" ] || bad "no usr/bin/$b"
done
[ "$(readlink "$out/stormd")" = .stormd/bin/stormd ] || bad "/stormd is not the link .stormd/bin/stormd"
for d in bin usr/bin sbin usr/sbin etc/stormd data dev proc sys run tmp var/log/stormd .stormd; do
    [ -d "$out/$d" ] || bad "no $d/"
done
[ "$(stat -c %a "$out/tmp")" = 1777 ] || bad "tmp is not 1777"
[ "$(readlink "$out/bin/ls")" = ../stormd ] && [ "$(readlink "$out/usr/bin/grep")" = ../../stormd ] || bad "applets are not links to ../stormd"
[ ! -e "$out/etc/kubernetes" ] || bad "carries etc/kubernetes (stormcos's static pod, #257)"
files=$(cd "$out" && find . -type f | sort)
echo "   files: $(echo "$files" | tr '\n' ' ')"
[ "$(echo "$files" | wc -l)" = 4 ] || bad "expected 4 files (three binaries, the config), got: $files"
"$out/usr/bin/fastetcd" --version
# A build is reproducible: the same commit, the same bytes.
bash deploy/build-golden.sh fastetcd "$work/again" >/dev/null
for f in usr/bin/fastetcd usr/bin/fastetcd-ctl usr/bin/fastetcd-migrate etc/stormd/config.toml; do
    cmp -s "$out/$f" "$work/again/$f" || bad "a second build of the same commit gave another $f"
done
# Unknown names and architectures are refused.
if bash deploy/build-golden.sh rustkube-apiserver "$work/x" 2>/dev/null; then bad "rustkube-apiserver was accepted"; fi
if GOLDEN_ARCH=aarch64 bash deploy/build-golden.sh fastetcd "$work/y" 2>/dev/null; then bad "GOLDEN_ARCH=aarch64 was accepted"; fi

if [ -n "$stormcos" ]; then
    echo "== against stormcos $(git -C "$stormcos" rev-parse --short HEAD): the stage's config and static pod"
    stage="$work/stage"
    cp -a "$out" "$stage"
    python3 - "$stormcos/deploy/build-goldens.sh" "$stage/etc/stormd/config.toml" "$work" << 'PY' || fail=1
import re, subprocess, sys
path, cfg, work = sys.argv[1:]
src = open(path).read()
def between(start, end="\n"):
    i = src.index(start) + len(start)
    j = src.index(end, i)
    return src[i:j]
# The stage's heredoc, as written to the stage's config.toml.
open(cfg, "w").write(between("cat > \"$FE_STAGE/etc/stormd/config.toml\" << 'CONF'\n", "\nCONF\n") + "\n")
# env_default_flag's edit, run as the stage runs it.
edf = between("python3 - \"$cfg\" \"$proc\" \"$flag\" \"$var\" << 'PY' || return 1\n", "\nPY\n")
subprocess.run([sys.executable, "-c", edf, cfg, "fastetcd", "--data-dir", "FASTETCD_DATA_DIR"], check=True)
# The membership and timing defaults.
mem = between("python3 - \"$FE_STAGE/etc/stormd/config.toml\" << 'PY' || exit 1\n", "\nPY\n")
subprocess.run([sys.executable, "-c", mem, cfg], check=True)
# golden_stormd's stormd_log_limits.
lim = between("stormd_log_limits() {\n    cat >> \"$1/etc/stormd/config.toml\" << 'CONF'\n", "\nCONF\n")
open(cfg, "a").write(lim + "\n")
PY
    python3 - "$stage/etc/stormd/config.toml" "$out/etc/stormd/config.toml" << 'PY' || fail=1
import sys, tomllib
want, got = (tomllib.load(open(p, "rb")) for p in sys.argv[1:])
if want != got:
    print(f"FAIL: the config is not the stage's\n  stage: {want}\n  here:  {got}")
    sys.exit(1)
print(f"   config: the stage's, value for value ({len(got['process'][0]['args'])} args, env_default {sorted(got['process'][0]['env_default'])})")
PY
    for t in "$stage" "$out"; do
        python3 "$stormcos/deploy/static-pods/gen-static-pod.py" "$t" fastetcd "$stormcos/deploy/static-pods/etcd.yaml" || bad "gen-static-pod.py refused $t"
    done
    for f in etcd.json etcd.env; do
        if cmp -s "$stage/etc/kubernetes/static/$f" "$out/etc/kubernetes/static/$f"; then
            echo "   static pod $f: identical to the stage's"
        else
            bad "static pod $f differs from the stage's"; diff "$stage/etc/kubernetes/static/$f" "$out/etc/kubernetes/static/$f" || true
        fi
    done
fi
rm -rf "$work"
[ "$fail" = 0 ] && echo "check-image: all passed" || { echo "check-image: FAILED"; exit 1; }
