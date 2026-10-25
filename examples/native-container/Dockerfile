# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

FROM rust:1.98.1-alpine AS build
WORKDIR /build
COPY server.rs /build/server.rs
RUN rustc --edition=2021 -O -C target-feature=+crt-static server.rs -o server

FROM alpine:3.23
WORKDIR /app
COPY --from=build /build/server /app/server
EXPOSE 8080
CMD ["/app/server"]
