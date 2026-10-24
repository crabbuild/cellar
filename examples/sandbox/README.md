<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Bounded native tool execution

Execute a fixed Rust tool in a cell-owned container and interpret its completion.

## How it works

The [Rust handler](../native-container/main.rs) resolves a named `Sandbox` cell,
starts its container once, waits for port 8080 and forwards HTTP. For `/exec`
it runs `/app/server --tool` and bounds collected output to 4096 bytes.
The [Rust peer](../native-container/server.rs) and [Dockerfile](../native-container/Dockerfile)
are shared with the [container host guide](../native-container/README.md).

## Run end to end

Prerequisites: Rust 1.94.1 or newer, Cargo, native C/C++ build tools, curl and
a running Linux Docker/Podman engine with network/iptables support. Run from
the repository root. Base-image builds require network access. Follow
[AGENTS.md](../../AGENTS.md) for the target directory. With Docker/Colima:

```sh
docker info
export DOCKER_HOST="$(docker context inspect --format '{{.Endpoints.docker.Host}}')"
```

Cellar needs a reachable `unix://` socket. For Podman, set `DOCKER_HOST` to its
running service socket. In terminal 1:

```sh
cargo run -p cellar --bin cellar --locked -- \
  dev examples/native-container/project.json --no-watch --clean
```

Wait for `ready`. This builds the Rust host, peer image and fence image.
`--clean` resets only this project's local tutorial state. In terminal 2:

```sh
curl -sS -w '\n' -X POST http://127.0.0.1:9876/sandboxes/alice/exec
curl -sS -i http://127.0.0.1:9876/sandboxes/alice/exec
```

## Expected output and interpretation

POST returns HTTP 200 with:

```json
{"exit_code":0,"stdout":"{\"tool\":\"native-rust\",\"ok\":true}\n","stderr":""}
```

The exit code proves completion, stdout contains the tool's result and stderr
is empty. GET returns HTTP 405 with `use POST`. The command is fixed rather
than caller-provided. This ports the container/tool pattern; it does not
provide Cloudflare Sandbox SDK compatibility or a complete job service.

## Stop, restart and cleanup

Press Ctrl-C and let the host release its containers. State is under
`examples/native-container/.cellar`; omit `--clean` on restart to retain cell state.
Container disks remain ephemeral: store durable business data in the cell or
a binding. Add `--clean` on the next start to reset this project. Built images
remain cached; remove only your own images if needed. Do not prune shared
engine resources. The [roadmap](../ROADMAP.md) describes a fuller build-job service.
