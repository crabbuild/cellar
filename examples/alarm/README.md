<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Durable alarms

Schedule durable cell work and commit an occurrence effect with its deduplication
marker.

## How it works

POST persists a pending status and a wake in one transaction; after 500 ms GET returns
fired. DELETE cancels the wake. Effects and occurrence deduplication commit together.

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
  dev examples/alarm/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' -X POST http://127.0.0.1:9876/alarms/demo
sleep 1
curl -sS -w '\n' http://127.0.0.1:9876/alarms/demo
```

## Expected output and interpretation

POST normally returns `{"alarm":<epoch milliseconds>,"status":"pending"}`. After the
wake, GET returns `{"alarm":null,"status":"fired"}`. Timestamps vary. If a busy machine
delays the wake, repeat GET until `fired`.

For cancellation, POST to `/alarms/cancel`, immediately DELETE the same URL, wait one
second and GET it. If DELETE arrives before the 500 ms wake, `alarm` is null and
`status` stays `pending`. The stored business status is not proof that a wake still
exists. The persisted alarm survives eviction/restart; this walkthrough observes its
normal delivery.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/alarm/.cellar`. Restart with the same command without `--clean` to
retain it; add `--clean` on the next start to reset this example. Stop before running
another example on port 9876, or choose another `--port` and change the request URLs.
JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
