# syntax=docker/dockerfile:1
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

FROM rust:1.98.1-bookworm AS build
ARG BUILD_JOBS=1
ARG TARGET_CACHE_ID=cellar-linux-daemon-target
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY vendor ./vendor
COPY examples ./examples
COPY tools ./tools
RUN --mount=type=cache,id=cellar-linux-daemon-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=${TARGET_CACHE_ID},target=/src/target \
    find Cargo.toml Cargo.lock crates examples tools vendor -type f -exec touch {} + && \
    (CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TERM_COLOR=never \
      cargo test -j "${BUILD_JOBS}" --locked -p cellar --test native_raft_daemon --no-run >/tmp/build.log 2>&1 \
      || (cat /tmp/build.log && exit 1)) && \
    fixture=$(sed -n 's/^ *Executable tests\/native_raft_daemon.rs (\(.*\))$/\1/p' /tmp/build.log) && \
    test -x "$fixture" && mkdir /out && cp "$fixture" /out/cellar-raft-daemon && cp target/debug/cellar /out/
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
ENTRYPOINT ["/usr/local/bin/cellar-raft-daemon"]
