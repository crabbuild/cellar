# Structured Rust RPC

Call business methods through structured native cell RPC.

## How it works

The HTTP facade constructs a structured argument array and calls the counter with
Stub::call. Results use the host RPC channel and durability gate.

Read the [Rust source](../counter/main.rs) and [project configuration](project.json). The
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
  dev examples/rpc/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' -X POST http://127.0.0.1:9876/rpc/alpha
curl -sS -w '\n' http://127.0.0.1:9876/rpc/alpha
curl -sS -w '\n' http://127.0.0.1:9876/counters/alpha
```

## Expected output and interpretation

All three responses are `{"count":1}`. POST invokes `increment` through a structured RPC
call; GET invokes `get`. The final HTTP read reaches the same cell storage. The Counter
source also exposes `increment_once`, which queue and cron consumers use to deduplicate
effects.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/rpc/.cellar`. Restart with the same command without `--clean` to
retain it; add `--clean` on the next start to reset this example. Stop before running
another example on port 9876, or choose another `--port` and change the request URLs.
JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
