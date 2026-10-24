<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Event-owned background work

Return a response while short work remains owned by the request event.

## How it works

POST responds while wait_until retains a timer and a storage sync. GET observes
completed work. Graceful shutdown drains the event. Process failure can lose this work;
use alarms, queues or workflows for durable jobs.

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
cargo run -p cellar --bin cellar --locked -- \
  dev examples/async/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' -X POST http://127.0.0.1:9876/async/demo
sleep 1
curl -sS -w '\n' http://127.0.0.1:9876/async/demo
```

## Expected output and interpretation

POST usually returns `{"completed":0}`; it may already show 1 if background work wins
the race. GET eventually returns `{"completed":1}`. A second POST eventually raises it
to 2. This separates response completion from background completion. The finished count
persists, but the pending in-memory timer is not a durable job.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/async/.cellar`. Restart with the same command without `--clean` to
retain it; add `--clean` on the next start to reset this example. Stop before running
another example on port 9876, or choose another `--port` and change the request URLs.
JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
