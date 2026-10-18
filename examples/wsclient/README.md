# Native WebSocket client

Consume a bound cell upgrade and exercise a real outbound network handshake.

## How it works

GET /wsclient consumes a local room upgrade with no external service. For a real
outbound TCP/WebSocket handshake, start wsecho on port 9876, set
WEBSOCKET_PEER=ws://127.0.0.1:9876/rooms/outbound in examples/wsclient/.dev.vars, start
this project with --port 9877, and request /wsclient/outbound. The older
[native-websocket/main.rs](../native-websocket/main.rs) additionally demonstrates
persistent cell-owned outbound sockets and
declined upgrades.

Read the [Rust source](../wsecho/main.rs) and [project configuration](project.json). The
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
  dev examples/wsclient/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' http://127.0.0.1:9876/wsclient
curl -sS -w '\n' http://127.0.0.1:9876/rooms/client/state
```

## Expected output and interpretation

The local responses are `{"echo":"native Rust hello"}` and `{"messages":1}`. Repeating
the self-check raises the durable count. The outbound walkthrough below uses separate
processes and a TCP/WebSocket handshake.

## Outbound handshake with a second Rust process

Stop the local self-check host first. In terminal 1, start the echo server:

```sh
cargo run -p cellar-main --bin cellar --locked -- \
  dev examples/wsecho/project.json --no-watch --clean --port 9876
```

In terminal 2, create the client's local variable file and start the client:

```sh
cat > examples/wsclient/.dev.vars <<'VARS'
WEBSOCKET_PEER=ws://127.0.0.1:9876/rooms/outbound
VARS
cargo run -p cellar-main --bin cellar --locked -- \
  dev examples/wsclient/project.json --no-watch --clean --port 9877
```

If a `.dev.vars` already exists, edit its `WEBSOCKET_PEER` line instead of
replacing the file. After both hosts report ready, use terminal 3:

```sh
curl -sS -w '\n' http://127.0.0.1:9877/wsclient/outbound
curl -sS -w '\n' http://127.0.0.1:9876/rooms/outbound/state
```

The responses are `{"echo":"native Rust hello"}` and `{"messages":1}`.
The client performed a real network upgrade and the peer committed its message
effect. Without the variable, the outbound route returns HTTP 400. Stop both
hosts and remove the variable file you created, or restore its previous value.
The variable file is ignored by Git.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/wsclient/.cellar`. Restart with the same command without `--clean`
to retain it; add `--clean` on the next start to reset this example. Stop before running
another example on port 9876, or choose another `--port` and change the request URLs.
JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
