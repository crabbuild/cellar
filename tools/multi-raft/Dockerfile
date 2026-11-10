# syntax=docker/dockerfile:1
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

FROM rust:1.98.1-bookworm AS build
ARG BUILD_JOBS=1
ARG TARGET_CACHE_ID=cellar-3617-cell-group-dr-target
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY vendor ./vendor
COPY examples ./examples
COPY tools ./tools
# Archived sources can predate cached artifacts. Recheck the exact snapshot.
RUN --mount=type=cache,id=cellar-3617-cell-group-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=${TARGET_CACHE_ID},target=/src/target \
    find Cargo.toml Cargo.lock crates examples tools vendor -type f -exec touch {} + && \
    CARGO_PROFILE_DEV_DEBUG=0 cargo build -j ${BUILD_JOBS} --locked -p cellar --bin cellar-perf-cell-group && \
    mkdir /out && cp target/debug/cellar-perf-cell-group /out/
# Match source_manifest(): include build recipes and embedded non-Rust inputs.
RUN find Cargo.toml Cargo.lock crates vendor examples tools \
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
ENTRYPOINT ["/usr/local/bin/cellar-perf-cell-group"]
