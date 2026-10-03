#!/usr/bin/env python3
"""Pull compare.sh's results out of its log (fastetcd#90).

    tests/bench/extract.py <compare.sh log> <out-dir>

A run through sc-build loses its files with its drive; its log keeps the
CSVs (printed at the end) and the raw benchmark summaries. This writes
<out-dir>/<name>.csv for each CSV and <out-dir>/raw.txt with everything
from "== raw benchmark summaries" on, plus the run's header lines.
"""
import os
import re
import sys

log, out = sys.argv[1], sys.argv[2]
os.makedirs(out, exist_ok=True)
lines = open(log, errors="replace").read().splitlines()
header = [l for l in lines if l.startswith(("== host", "== data volume", "== etcd", "== rustkube", "==> fastetcd main@"))]
csvs, cur = {}, None
for l in lines:
    m = re.match(r"^--- (\w+)\.csv$", l)
    if m:
        cur = m.group(1)
        csvs[cur] = []
        continue
    if cur is not None:
        if l.startswith(("==", "@@", "EXIT", "--- ")) or not l.strip():
            cur = None
            m = re.match(r"^--- (\w+)\.csv$", l)
            if m:
                cur = m.group(1)
                csvs[cur] = []
            continue
        csvs[cur].append(l)
for name, rows in csvs.items():
    with open(os.path.join(out, name + ".csv"), "w") as f:
        f.write("\n".join(rows) + "\n")
try:
    start = next(i for i, l in enumerate(lines) if l.startswith("== raw benchmark summaries"))
    end = next(i for i, l in enumerate(lines) if l.startswith("== results in"))
    raw = lines[start:end]
except StopIteration:
    raw = []
# The per-workload progress lines (incl. fastetcd's WAL metrics) too.
prog = [l for l in lines if l.startswith(("-- ", "   "))]
with open(os.path.join(out, "raw.txt"), "w") as f:
    f.write("\n".join(header + ["", "== progress"] + prog + [""] + raw) + "\n")
print(f"{out}: " + ", ".join(f"{k}.csv ({len(v) - 1} rows)" for k, v in csvs.items()))
