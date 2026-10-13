# Durable counter

Model durable business entities and verify their state survives a restart.

## How it works

GET reads; POST increments. Each name selects its own SQLite cell. Concurrent writes
serialize through the cell and commit atomically.

Read the [Rust source](main.rs) and [project configuration](project.json). The
handler is a module compiled into `cellar-native-examples` by
[native_examples.rs](../native_examples.rs), rather than a standalone `rustc` input.

## Run end to end

Prerequisites: Rust 1.94.1 or newer, Cargo, the workspace's native C/C++ build tools,
and curl. Run commands from the repository root. Follow [AGENTS.md](../../AGENTS.md)
when selecting a target directory for this checkout. The local walkthrough needs no
external service or cloud credential.

In terminal 1:

```sh
cargo run -p cellar --bin cellar --locked -- \
  dev examples/counter/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' http://127.0.0.1:9876/counters/alpha
curl -sS -w '\n' -X POST http://127.0.0.1:9876/counters/alpha
curl -sS -w '\n' -X POST http://127.0.0.1:9876/counters/alpha
curl -sS -w '\n' http://127.0.0.1:9876/counters/beta
```

## Expected output and interpretation

The four responses are `{"count":0}`, `{"count":1}`, `{"count":2}` and `{"count":0}`.
Alpha and beta are separate business identities. POST increments in a SQLite
transaction; GET does not change state. The framework output gate settles required
writes before releasing the response.

Stop with Ctrl-C, rerun the start command **without `--clean`**, then GET
`/counters/alpha`: it returns `{"count":2}`. This proves local restart persistence. The
fleet recipe adds distributed recovery checks.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/counter/.cellar/native-dev`. Restart with the same command without `--clean`
to retain it; add `--clean` on the next start to reset this example. Stop before running
another example on port 9876, or choose another `--port` and change the request URLs.
JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
