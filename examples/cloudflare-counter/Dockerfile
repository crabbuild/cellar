# syntax=docker/dockerfile:1.7
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
FROM --platform=$BUILDPLATFORM rust:1.98.1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++-x86-64-linux-gnu && rm -rf /var/lib/apt/lists/* && rustup target add x86_64-unknown-linux-gnu
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY vendor ./vendor
COPY examples ./examples
COPY tools ./tools
ARG CELLAR_COMMIT=development
ENV CELLAR_COMMIT=$CELLAR_COMMIT CARGO_BUILD_JOBS=2 CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc CXX_x86_64_unknown_linux_gnu=x86_64-linux-gnu-g++
RUN --mount=type=cache,id=cellar-cloudflare-counter-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cellar-cloudflare-counter-linux-target,target=/src/target \
    cargo build --locked --profile lab --target x86_64-unknown-linux-gnu -p cellar --bin cellar-cloudflare-counter && mkdir /out && cp target/x86_64-unknown-linux-gnu/lab/cellar-cloudflare-counter /out/
FROM debian:bookworm-slim
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/cellar-cloudflare-counter /usr/local/bin/
COPY examples/cloudflare-counter/entrypoint.sh /usr/local/bin/counter-entrypoint
COPY LICENSE NOTICE /usr/share/doc/cellar/
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["/bin/sh", "/usr/local/bin/counter-entrypoint"]
