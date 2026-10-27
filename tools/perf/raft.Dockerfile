# syntax=docker/dockerfile:1
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

FROM rust:1.98.1-bookworm AS build
ARG BUILD_JOBS=2
ARG TARGET_CACHE_ID=cellar-ios-7da4ddb5-raft-target
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY vendor ./vendor
COPY examples ./examples
COPY tools/perf/*.rs ./tools/perf/
COPY tools/xtask ./tools/xtask
# Archived source can predate cached artifacts; force Cargo to check this snapshot.
RUN --mount=type=cache,id=cellar-perf-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=${TARGET_CACHE_ID},target=/src/target \
    find Cargo.toml Cargo.lock crates examples tools vendor -type f -exec touch {} + && \
    cargo build -j ${BUILD_JOBS} --profile lab --locked -p cellar --bin cellar-perf-raft --bin cellar-perf-baseline && \
    mkdir /out && cp target/lab/cellar-perf-raft target/lab/cellar-perf-baseline /out/
COPY tools/perf ./tools/perf
# Include embedded SQL, protobuf, C, assets and build recipes as well as Rust.
# Match source_manifest() and the context's node_modules/.cellar exclusions.
RUN find Cargo.toml Cargo.lock crates vendor examples tools/perf tools/xtask \
    \( -name .git -o -name .cellar -o -name node_modules -o -name __pycache__ \) -prune -o \
    \( -type f -o -type l \) ! -name '*.pyc' ! -name .DS_Store -print0 | \
    LC_ALL=C sort -z | xargs -0 sha256sum > /out/raft-source.sha256
FROM debian:bookworm-slim
ARG SOURCE_REVISION=unspecified
LABEL org.opencontainers.image.revision=${SOURCE_REVISION}
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ /usr/local/bin/
COPY LICENSE NOTICE /usr/share/doc/cellar/
WORKDIR /data
ENTRYPOINT ["/usr/local/bin/cellar-perf-raft"]
