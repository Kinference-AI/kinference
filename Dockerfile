# syntax=docker/dockerfile:1.7
#
# The image ships no config; mount one and pass --config, e.g.
#
#   docker run --rm -p 50051:50051 \
#       -v $(pwd)/config.json:/etc/kinference/config.json:ro \
#       kinference --config /etc/kinference/config.json

# Match rust-toolchain.toml, or rustup downloads that toolchain on every build.
FROM --platform=$BUILDPLATFORM rust:1.95-slim-bookworm AS builder
ARG TARGETARCH

# The `zmq` crate compiles a bundled libzmq with g++; cargo needs git for the
# `dynamo-kv-router` dep.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        git \
        g++ \
        g++-x86-64-linux-gnu libc6-dev-amd64-cross \
        g++-aarch64-linux-gnu libc6-dev-arm64-cross \
    && rm -rf /var/lib/apt/lists/*
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc
RUN rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu

WORKDIR /src

COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY src ./src
COPY crates ./crates

# Read by `option_env!` in both binaries; unset falls back to
# `CARGO_PKG_VERSION`.
ARG KINFERENCE_VERSION
ENV KINFERENCE_VERSION=${KINFERENCE_VERSION}

# The target dir is a cache mount, so the binaries are copied out to /out
# for the runtime stage.
RUN --mount=type=cache,id=cargo-registry-$TARGETARCH,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-target-$TARGETARCH,target=/src/target \
    target=$(case "$TARGETARCH" in \
        amd64) echo x86_64-unknown-linux-gnu ;; \
        arm64) echo aarch64-unknown-linux-gnu ;; \
        *) echo "unsupported architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac) \
    && cargo build --locked --release --bins --target "$target" \
    && mkdir -p /out/home \
    && cp "target/$target/release/kinference" /out/ \
    && cp "target/$target/release/kinfctl" /out/ \
    && cp "target/$target/release/compute_hash" /out/

FROM debian:bookworm-slim

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/kinference         /usr/local/bin/kinference
#   docker run --rm -i --entrypoint kinfctl <image> check - < policy.yaml
COPY --from=builder /out/kinfctl       /usr/local/bin/kinfctl
COPY --from=builder /out/compute_hash  /usr/local/bin/compute_hash
# The HuggingFace Hub tokenizer fallback caches under $HOME.
COPY --from=builder --chown=10001:10001 /out/home /home/app
ENV HOME=/home/app

USER 10001
EXPOSE 50051

ENV RUST_LOG=info

ENTRYPOINT ["/usr/local/bin/kinference"]
