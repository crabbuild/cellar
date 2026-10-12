# syntax=docker/dockerfile:1
FROM rust:1.98.1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY examples ./examples
COPY tools/perf ./tools/perf
RUN --mount=type=cache,id=cellar-perf-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cellar-perf-target,target=/src/target \
    cargo build --release --locked -p cellar --bin cellar-perf-cells --bin cellar-perf-load && \
    mkdir /out && cp target/release/cellar-perf-cells target/release/cellar-perf-load /out/
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ /usr/local/bin/
COPY LICENSE NOTICE /usr/share/doc/cellar/
WORKDIR /data
ENTRYPOINT ["/usr/local/bin/cellar-perf-cells"]
