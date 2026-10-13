# Native container cells and Rust tools

Run one isolated Rust HTTP process per named business cell, then execute a
fixed tool inside that cell's container.

## How it works

[main.rs](main.rs) routes `/sandboxes/NAME/PATH`
through `SANDBOXES` to a `Sandbox` cell. A startup lock avoids concurrent
starts. The SDK starts the container, waits for port 8080, and forwards the
request. [server.rs](server.rs) is a dependency-free Rust HTTP peer. Its response
includes `CELLAR_CELL_ID`, injected by the container host.

POST `/sandboxes/NAME/exec` runs the fixed command `/app/server --tool`. The SDK
collects at most 4096 output bytes and reports exit status, stdout and stderr.
The caller does not supply an arbitrary shell command. The [Dockerfile](Dockerfile)
compiles Rust and stages the peer in Alpine. The [project](project.json)
packages the application image and network fence image with the host executable.

## Prerequisites

Run from the repository root with Rust 1.94.1 or newer, Cargo, native C/C++
build tools, curl, and a running Linux Docker or Podman engine. Image builds
need network access for Rust/Alpine base images. For a Docker context such as
Colima, make its engine socket visible to Cellar:

```sh
docker info
export DOCKER_HOST="$(docker context inspect --format '{{.Endpoints.docker.Host}}')"
```

Use a reachable `unix://` socket. For Podman, provide its running service socket
as `DOCKER_HOST`. The fence needs the engine's Linux network/iptables support.
Follow [AGENTS.md](../../AGENTS.md) for the Cargo target directory. No cloud
bucket is required for this walkthrough.

## Run end to end

In terminal 1:

```sh
cargo run -p cellar --bin cellar --locked -- \
  dev examples/native-container/project.json --no-watch --clean
```

This builds the native application, peer image and fence image. Wait for the
`ready` URL. `--clean` resets this project's local tutorial state; omit it for
a preserving restart. In terminal 2:

```sh
curl -sS -w '\n' http://127.0.0.1:9876/sandboxes/alice/hello
curl -sS -w '\n' http://127.0.0.1:9876/sandboxes/bob/hello
curl -sS -w '\n' http://127.0.0.1:9876/sandboxes/alice/hello
curl -sS -w '\n' -X POST http://127.0.0.1:9876/sandboxes/alice/exec
curl -sS -i http://127.0.0.1:9876/sandboxes/alice/exec
```

## Expected output and interpretation

Hello returns HTTP 200 with `{"cell":"<cell identity>","path":"/hello"}`.
Alice's identity stays the same across both calls and differs from bob's.
This proves named routing and separate processes. The first request for each
cell can take longer because it starts the container.

The tool response is:

```json
{"exit_code":0,"stdout":"{\"tool\":\"native-rust\",\"ok\":true}\n","stderr":""}
```

Exit code 0 means successful tool completion. `stdout` is a JSON string containing
the tool's JSON and trailing newline; `stderr` is empty. GET `/exec` returns
HTTP 405 with `use POST`. This is a native lifecycle/exec pattern, not Cloudflare
Sandbox SDK compatibility.

## Stop, restart and cleanup

Press Ctrl-C and wait for graceful shutdown; the host releases its managed
containers. State lives under `examples/native-container/.cellar/native-dev`.
Add `--clean` on the next start to reset this project. Images remain in the engine's cache;
remove only your walkthrough's images if desired, without pruning shared resources.

Container disks are ephemeral. Put durable job state in `Context::storage` or
a binding. A restart can start a fresh process with the same business identity;
it does not prove the old container disk was preserved. See the [catalog](../README.md)
and [application roadmap](../ROADMAP.md).
