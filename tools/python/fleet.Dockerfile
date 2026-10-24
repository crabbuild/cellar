# syntax=docker/dockerfile:1
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

FROM ghcr.io/astral-sh/uv:0.11.19 AS uv
FROM rust:1.98.1-bookworm AS build
ARG TARGET_CACHE_ID=cellar-python-fleet-target
ARG PYTHON_RUNTIME=3.14.5
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/cellar-python PYTHONDONTWRITEBYTECODE=1 CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0
RUN uv python install "$PYTHON_RUNTIME" --install-dir /opt/cellar-python --no-bin
RUN uv python find --managed-python "$PYTHON_RUNTIME" > /python-executable
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY vendor ./vendor
COPY examples ./examples
COPY tools ./tools
# Archived source timestamps must not reuse another revision's executables.
RUN --mount=type=cache,id=cellar-python-fleet-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=${TARGET_CACHE_ID},target=/src/target \
    find Cargo.toml Cargo.lock crates examples tools vendor -type f -exec touch {} + && \
    cargo build --release --locked -p cellar --bin cellar-perf-cells --bin cellar-perf-load && \
    mkdir /out && cp target/release/cellar-perf-cells target/release/cellar-perf-load /out/ && \
    PYO3_PYTHON="$(uv python find --managed-python "$PYTHON_RUNTIME")" \
    cargo build --release --locked -p cellar-python --example python-fleet --features embed && \
    cp target/release/examples/python-fleet /out/
FROM debian:bookworm-slim
ARG SOURCE_REVISION=unspecified
ARG PYTHON_RUNTIME=3.14.5
LABEL org.opencontainers.image.revision=${SOURCE_REVISION}
LABEL cellar.python.runtime=${PYTHON_RUNTIME}
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /opt/cellar-python /opt/cellar-python
COPY --from=build /python-executable /usr/local/share/cellar/python-executable
# PyO3 links libpython by soname. Its managed prefix is not a default loader
# directory, so provisioning the files alone is insufficient for the host.
RUN find /opt/cellar-python -name 'libpython3.14*.so.1.0' -printf '%h\n' | sort -u > /etc/ld.so.conf.d/cellar-python.conf
RUN test -s /etc/ld.so.conf.d/cellar-python.conf && ldconfig && \
    "$(cat /usr/local/share/cellar/python-executable)" -I -S -B -c 'import ssl, sqlite3, sys; assert sys.version_info[:2] == (3, 14)'
COPY --from=build /out/ /usr/local/bin/
RUN ldd /usr/local/bin/python-fleet > /tmp/cellar-linkage && \
    cat /tmp/cellar-linkage && ! grep -q 'not found' /tmp/cellar-linkage && rm /tmp/cellar-linkage
COPY LICENSE NOTICE /usr/share/doc/cellar/
WORKDIR /data
ENTRYPOINT ["/usr/local/bin/python-fleet"]
