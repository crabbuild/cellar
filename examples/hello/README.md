# Stateless HTTP service

Serve the smallest compiled Rust HTTP application.

## How it works

GET /hello returns a JSON greeting. GET /health checks that the application can
dispatch.

Read the [Rust source](main.rs) and [project configuration](project.json). The
handler is a module compiled into `cellar-native-examples` by
[main.rs](../main.rs), rather than a standalone `rustc` input.

## Run end to end

Prerequisites: Rust 1.94.1 or newer, Cargo, the workspace's native C/C++ build tools,
and curl. Run commands from the repository root. Follow [AGENTS.md](../../AGENTS.md)
when selecting a target directory for this checkout. The local walkthrough needs no
external service or cloud credential.

In terminal 1:

```sh
cargo run -p cellar-main --bin cellar --locked -- \
  dev examples/hello/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' http://127.0.0.1:9876/hello
curl -sS -i -X POST http://127.0.0.1:9876/hello
```

## Expected output and interpretation

GET returns HTTP 200 with `{"hello":"Cellar","language":"Rust"}`. POST returns HTTP 405
with an `error` naming the allowed method. This verifies native HTTP dispatch and client
error handling. `/health` uses the same handler; it is a teaching liveness endpoint, not
a fleet readiness probe.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/hello/.cellar`. Restart with the same command without `--clean` to
retain it; add `--clean` on the next start to reset this example. Stop before running
another example on port 9876, or choose another `--port` and change the request URLs.
JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
