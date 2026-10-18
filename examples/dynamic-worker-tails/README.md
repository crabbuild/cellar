# Compiled loader and background trace

Compose trusted compiled code with an explicit environment and background trace.

## How it works

Loads a compiled Rust application with an explicit copied environment and denied
outbound network capability. The response identifies the environment; an event-owned
trace increments loaded-calls. This is the native composition equivalent of the upstream
loader/tail pattern. It does not execute supplied source or implement Cloudflare Tail
Worker events. Compiled code remains trusted in-process code; Outbound::Deny is a
framework capability limit, not an OS sandbox.

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
  dev examples/dynamic-worker-tails/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' http://127.0.0.1:9876/loaded
curl -sS -w '\n' http://127.0.0.1:9876/counters/loaded-calls
```

## Expected output and interpretation

The child returns `{"loaded":"compiled Rust","environment":{"team":"examples"}}`. The
trace counter eventually returns `{"count":1}`. These verify the explicit child
environment and completed background trace separately. A second call eventually raises
the counter to 2. Pending traces have event-drain semantics rather than durable job
recovery.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/dynamic-worker-tails/.cellar`. Restart with the same command
without `--clean` to retain it; add `--clean` on the next start to reset this example.
Stop before running another example on port 9876, or choose another `--port` and change
the request URLs. JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
