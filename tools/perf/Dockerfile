# syntax=docker/dockerfile:1
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

FROM rust:1.98.1-bookworm AS build
ARG TARGET_CACHE_ID=cellar-embedded-perf-target
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY vendor ./vendor
COPY examples ./examples
COPY tools/perf ./tools/perf
COPY tools/xtask ./tools/xtask
# Archived source can predate cached artifacts; force Cargo to check this snapshot.
RUN --mount=type=cache,id=cellar-perf-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=${TARGET_CACHE_ID},target=/src/target \
    find Cargo.toml Cargo.lock crates examples tools vendor -type f -exec touch {} + && \
    cargo build --release --locked -p cellar --bin cellar-perf-cells --bin cellar-perf-load --bin cellar-perf-fault-cells && \
    mkdir /out && cp target/release/cellar-perf-cells target/release/cellar-perf-load target/release/cellar-perf-fault-cells /out/
FROM debian:bookworm-slim
ARG SOURCE_REVISION=unspecified
LABEL org.opencontainers.image.revision=${SOURCE_REVISION}
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ /usr/local/bin/
COPY LICENSE NOTICE /usr/share/doc/cellar/
WORKDIR /data
ENTRYPOINT ["/usr/local/bin/cellar-perf-cells"]
