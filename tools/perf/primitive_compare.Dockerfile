# syntax=docker/dockerfile:1
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
FROM rust:1.98.1-bookworm AS build
ARG TARGET_CACHE_ID=cellar-tikv-pr6aff-linux-primitives-perf-target
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY vendor ./vendor
COPY examples ./examples
COPY tools ./tools
RUN --mount=type=cache,id=cellar-linux-daemon-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=${TARGET_CACHE_ID},target=/src/target \
    find Cargo.toml Cargo.lock crates examples tools vendor -type f -exec touch {} + && \
    CARGO_INCREMENTAL=0 cargo build -j 1 --locked --profile lab -p cellar \
        --bin cellar-perf-raft --bin cellar-perf-baseline && \
    mkdir /out && cp target/lab/cellar-perf-raft target/lab/cellar-perf-baseline /out/ && \
    cp tools/perf/primitive_load.py /out/ && \
    rustc -vV > /out/raft-rustc.txt && \
    sha256sum /out/cellar-perf-* /out/primitive_load.py > /out/executables.sha256
RUN find Cargo.toml Cargo.lock crates vendor examples tools \
    \( -name .git -o -name .cellar -o -name node_modules -o -name __pycache__ \) -prune -o \
    \( -type f -o -type l \) ! -name '*.pyc' ! -name .DS_Store -print0 | \
    LC_ALL=C sort -z | xargs -0 sha256sum > /out/raft-source.sha256
FROM debian:bookworm-slim
ARG SOURCE_REVISION=unspecified
LABEL org.opencontainers.image.revision=${SOURCE_REVISION}
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates python3 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ /usr/local/bin/
COPY LICENSE NOTICE /usr/share/doc/cellar/
WORKDIR /data
ENTRYPOINT ["/usr/local/bin/cellar-perf-raft"]
