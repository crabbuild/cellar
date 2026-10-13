# Hibernating WebSocket room

Attach realtime clients to a durable room and observe persisted message effects.

## How it works

A named room accepts a hibernating socket and persists its message count before echoing
text or binary. GET /rooms/demo/state reads durable state. /wsclient is an HTTP
self-check that consumes a cell upgrade, sends text and reads its echo. Hibernation
preserves an activation socket; a node failure still requires clients to reconnect.

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
  dev examples/wsecho/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' http://127.0.0.1:9876/wsclient
curl -sS -w '\n' http://127.0.0.1:9876/rooms/client/state
curl -sS -i http://127.0.0.1:9876/rooms/demo
```

## Expected output and interpretation

The self-check returns `{"echo":"native Rust hello"}`, then state returns
`{"messages":1}`. A second self-check raises the count to 2. Ordinary HTTP to
`/rooms/demo` returns HTTP 426 because that route requires an upgrade.

For an optional network client, open a browser console:

```javascript
const ws = new WebSocket('ws://127.0.0.1:9876/rooms/demo');
ws.onmessage = event => console.log(event.data);
ws.onopen = () => ws.send('hello');
// After receiving the echo:
// ws.close(1000, 'done');
```

The console prints `hello`, and GET `/rooms/demo/state` then returns `{"messages":1}`.
JavaScript is only an optional browser client; the server and self-check are Rust.
Hibernation retains a live activation socket; node failure requires reconnecting. Close
the browser socket before stopping.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/wsecho/.cellar/native-dev`. Restart with the same command without `--clean` to
retain it; add `--clean` on the next start to reset this example. Stop before running
another example on port 9876, or choose another `--port` and change the request URLs.
JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
