# syntax=docker/dockerfile:1
FROM rust:1.98.1-bookworm AS build
ARG TARGET_CACHE_ID
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY examples ./examples
COPY tools/perf ./tools/perf
COPY tools/xtask ./tools/xtask
RUN --mount=type=cache,id=cellar-perf-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=${TARGET_CACHE_ID},target=/src/target \
    cargo build --release --locked -p fleetcare --bins && \
    mkdir /out && cp target/release/fleetcare-* /out/
FROM debian:bookworm-slim AS toolkit
ARG SOURCE_DIGEST
LABEL io.cellar.fleetcare.source-digest=${SOURCE_DIGEST}
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates python3 openssl nftables && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/fleetcare-embedded /out/fleetcare-daemon /out/fleetcare-load /usr/local/bin/
COPY LICENSE NOTICE /usr/share/doc/cellar/
COPY tools/perf/fleetcare-simulator.py /usr/local/bin/
WORKDIR /data
FROM toolkit AS sandbox
ENTRYPOINT []
CMD ["python3", "-m", "http.server", "8080"]
FROM toolkit AS node
ENTRYPOINT ["fleetcare-embedded"]
