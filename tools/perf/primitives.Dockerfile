# syntax=docker/dockerfile:1
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

FROM rust:1.98.1-bookworm AS build
ARG TARGET_CACHE_ID=cellar-linux-primitives-perf-target
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY vendor ./vendor
COPY examples ./examples
COPY tools ./tools
RUN --mount=type=cache,id=cellar-linux-daemon-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=${TARGET_CACHE_ID},target=/src/target \
    find Cargo.toml Cargo.lock crates examples tools vendor -type f -exec touch {} + && \
    CARGO_INCREMENTAL=0 CARGO_TERM_COLOR=never \
    cargo test -j 1 --locked --profile lab -p cellar --test native_cell_groups_primitives \
      --no-run --message-format=json > /tmp/build.jsonl && \
    fixture=$(python3 -c 'import json; rows=[json.loads(s) for s in open("/tmp/build.jsonl")]; print(next(r["executable"] for r in rows if r.get("reason")=="compiler-artifact" and r["target"]["name"]=="native_cell_groups_primitives" and r.get("executable")))') && \
    mkdir /out && cp "$fixture" /out/cellar-perf-primitives && \
    rustc -vV > /out/compiler.txt && \
    sha256sum /out/cellar-perf-primitives > /out/executable.sha256
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
ENTRYPOINT ["/usr/local/bin/cellar-perf-primitives"]
