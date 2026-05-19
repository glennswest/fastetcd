# Multi-stage build for the fastetcd server binary.
#
# Build:  docker build -t fastetcd:dev .
# Run:    docker run --rm -p 2379:2379 -p 2380:2380 fastetcd:dev
#
# CI uses Dockerfile.ci which expects the binary to already be built
# (faster, reuses the build-linux-binary job's artifact).

# ---- stage 1: build ---------------------------------------------------------
FROM rust:1.82-slim AS builder
WORKDIR /src
COPY . .
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config protobuf-compiler ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN cargo build --release -p fastetcd-server --bin fastetcd

# ---- stage 2: runtime -------------------------------------------------------
# Distroless gives us a minimal image with the dynamic libc fastetcd
# needs (glibc) without the full Debian userland.
FROM gcr.io/distroless/cc-debian12
COPY --from=builder /src/target/release/fastetcd /usr/local/bin/fastetcd
EXPOSE 2379 2380
VOLUME ["/var/lib/fastetcd"]
USER nonroot
ENTRYPOINT ["/usr/local/bin/fastetcd"]
CMD ["--data-dir=/var/lib/fastetcd"]
