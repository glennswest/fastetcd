#!/usr/bin/env python3
"""
Strip annotations from etcd .proto files that require external proto
plugins which we don't vendor:

  - gogoproto.*          (Go code-gen hints — not useful for prost/tonic)
  - google.api.http      (grpc-gateway HTTP transcoding)
  - protoc-gen-openapiv2 (grpc-gateway swagger metadata)
  - versionpb.*          (etcd-internal min-version metadata; informational)

The stripped output retains the full gRPC service + message definitions,
which is everything tonic-build needs.

Usage: strip_annotations.py <input.proto>      writes to stdout
"""
import re
import sys


def strip_braced_option(text: str, option_name_re: str) -> str:
    """Remove `option (<name>) = { ... };` blocks with balanced braces."""
    pattern = re.compile(
        r"(^[ \t]*option \(" + option_name_re + r"\)\s*=\s*\{)",
        flags=re.MULTILINE,
    )
    out = []
    pos = 0
    for m in pattern.finditer(text):
        start = m.start()
        i = m.end() - 1  # position of opening `{`
        depth = 1
        i += 1
        while i < len(text) and depth > 0:
            c = text[i]
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
            i += 1
        # Skip trailing whitespace + `;`.
        while i < len(text) and text[i] in " \t":
            i += 1
        if i < len(text) and text[i] == ";":
            i += 1
        if i < len(text) and text[i] == "\n":
            i += 1
        out.append(text[pos:start])
        pos = i
    out.append(text[pos:])
    return "".join(out)


def strip_field_annotations(text: str) -> str:
    """
    Drop field-level annotations whose entries are all gogoproto/versionpb
    options. If an annotation list mixes them with other options, the
    other entries are preserved.
    """
    pattern = re.compile(r"\[((?:[^\[\]]|\[[^\]]*\])*)\]")

    def is_strippable(p: str) -> bool:
        return p.startswith("(gogoproto.") or p.startswith("(versionpb.")

    def repl(m: "re.Match[str]") -> str:
        inner = m.group(1)
        parts = [p.strip() for p in inner.split(",") if p.strip()]
        kept = [p for p in parts if not is_strippable(p)]
        if not kept:
            return ""
        return "[" + ", ".join(kept) + "]"

    return pattern.sub(repl, text)


def strip_proto(src: str) -> str:
    # 1. Drop imports we are intentionally not vendoring.
    drop_imports = [
        r'gogoproto/gogo\.proto',
        r'google/api/annotations\.proto',
        r'protoc-gen-openapiv2/options/annotations\.proto',
        r'google/protobuf/descriptor\.proto',
        r'etcd/api/versionpb/version\.proto',
    ]
    for imp in drop_imports:
        src = re.sub(
            rf'^import "{imp}";\s*\n',
            "",
            src,
            flags=re.MULTILINE,
        )
    # The leading comment `// for grpc-gateway` becomes orphaned once those
    # imports are gone; drop it.
    src = re.sub(r"^// for grpc-gateway\s*\n", "", src, flags=re.MULTILINE)

    # 2. Drop file-level gogoproto/versionpb options. The pattern matches both
    #    free-floating file options and `option (...) = "...";` lines that
    #    appear at the top of a message body.
    for prefix in ("gogoproto", "versionpb"):
        src = re.sub(
            rf"^(\s*)option \({prefix}\.[^)]+\) = [^;]+;\s*\n",
            "",
            src,
            flags=re.MULTILINE,
        )

    # 3. Drop multi-line `option (...) = { ... };` blocks for grpc-gateway.
    src = strip_braced_option(
        src, r"grpc\.gateway\.protoc_gen_openapiv2\.options\.openapiv2_swagger"
    )
    src = strip_braced_option(src, r"google\.api\.http")

    # 4. Drop the `extend google.protobuf.FileOptions { ... }` block that
    #    versionpb/version.proto uses for a Go-only file option. We don't
    #    re-use it anywhere, so dropping the whole block is safe.
    src = strip_braced_option(src, r"google\.protobuf\.FileOptions")  # no-op for these
    src = re.sub(
        r"^extend google\.protobuf\.FileOptions \{[^}]*\}\s*\n",
        "",
        src,
        flags=re.MULTILINE | re.DOTALL,
    )

    # 5. Field-level gogoproto annotations.
    src = strip_field_annotations(src)

    # 6. Collapse 3+ blank lines down to 2.
    src = re.sub(r"\n{3,}", "\n\n", src)

    return src


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: strip_annotations.py <input.proto>", file=sys.stderr)
        return 2
    with open(sys.argv[1], "r") as f:
        src = f.read()
    sys.stdout.write(strip_proto(src))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
