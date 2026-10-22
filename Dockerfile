# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1.98.1
ARG CELLAR_COMMIT=unknown

FROM rust:${RUST_VERSION}-bookworm AS build
ARG TARGETARCH
# `release` for shipped artifacts; a fast-loop caller passes `lab` to skip
# the fat-LTO relink and keep incremental state in the target cache.
ARG CELLAR_PROFILE=release
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY vendor ./vendor
COPY examples ./examples
COPY tools/perf ./tools/perf
COPY tools/xtask ./tools/xtask
RUN --mount=type=cache,id=cellar-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cellar-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=cellar-target-${TARGETARCH},target=/src/target,sharing=locked \
    mkdir -p /out && \
    cargo build -p cellar --bin cellar --profile "${CELLAR_PROFILE}" --locked -p cellar && \
    install -m 755 "target/${CELLAR_PROFILE}/cellar" /out/cellar

# The final image depends on this stage, so a break in the framework's tests or
# lints stops the build.
FROM build AS test
ARG TARGETARCH
RUN rustup component add clippy
# The ltx fault-injection oracle diffs databases with the sqlite3 CLI.
RUN apt-get update && \
    apt-get install -y --no-install-recommends sqlite3 && \
    rm -rf /var/lib/apt/lists/*
RUN --mount=type=cache,id=cellar-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cellar-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=cellar-target-${TARGETARCH},target=/src/target,sharing=locked \
    cargo test --profile "${CELLAR_PROFILE}" --locked && \
    cargo clippy --profile "${CELLAR_PROFILE}" --all-targets --locked -- -D warnings

FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
ARG CELLAR_COMMIT
ARG CELLAR_VERSION=unknown
LABEL org.opencontainers.image.title="cellar" \
      org.opencontainers.image.revision="${CELLAR_COMMIT}" \
      org.opencontainers.image.version="${CELLAR_VERSION}"
COPY LICENSE NOTICE /usr/share/doc/cellar/
COPY --from=test /out/cellar /usr/local/bin/cellar
ENTRYPOINT ["/usr/local/bin/cellar"]
