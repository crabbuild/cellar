# syntax=docker/dockerfile:1
FROM rust:1.98.1-bookworm AS build
ARG TARGET_CACHE_ID
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY vendor ./vendor
COPY examples ./examples
COPY tools ./tools
# Archived source can predate cached artifacts; force Cargo to check this snapshot.
RUN --mount=type=cache,id=cellar-console-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=${TARGET_CACHE_ID},target=/src/target \
    find Cargo.toml Cargo.lock crates examples tools vendor -type f -exec touch {} + && \
    cargo build --release --locked --jobs 2 -p fleetcare --bin fleetcare-daemon --bin fleetcare-load --bin fleetcare-publish -p cellar-admin --bin cellar-dashboard && \
    mkdir /out && cp target/release/fleetcare-* target/release/cellar-dashboard /out/

FROM node:24-bookworm-slim AS browser
WORKDIR /app
COPY packages/console/package.json ./
RUN npm install --no-audit --no-fund
COPY packages/console ./
RUN npm run build

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates python3 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ /usr/local/bin/
COPY --from=browser /app/dist/ /opt/cellar/console/
COPY tools/perf/fleetcare-simulator.py /usr/local/bin/
COPY LICENSE NOTICE /usr/share/doc/cellar/
WORKDIR /data
STOPSIGNAL SIGINT
ENTRYPOINT ["fleetcare-daemon"]
