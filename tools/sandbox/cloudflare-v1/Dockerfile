# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
FROM node@sha256:85a395c77b811fa7f5b5e4aa69cd6eb4c3b80c7f1a8e34704dc0ce061e5b404e
COPY --from=docker.io/cloudflare/sandbox@sha256:fc1740c3cba26461f0ab1771287c84018ab2cca43a70ebb823087c85d780aaec /usr/local/bin/sandbox-shim /usr/local/bin/sandbox-shim
LABEL cellar.sandbox.sdk="1.0.3" cellar.sandbox.helper="sha256:fc1740c3cba26461f0ab1771287c84018ab2cca43a70ebb823087c85d780aaec"
RUN mkdir /workspace
WORKDIR /workspace
CMD ["sleep", "infinity"]
