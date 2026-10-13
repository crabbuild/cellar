# Standalone WebSocket ownership host

Compare event-owned and cell-owned socket handling using the original Rust
WebSocket host, with a local self-check requiring no external WebSocket service.

## How it works

[native_websocket.rs](../native_websocket.rs) registers a Worker handler,
hibernating EchoRoom cells and persistent outbound Peer cells. `/bind/worker`
consumes a Worker upgrade; `/bind/cell` consumes a room upgrade. Both send text
and non-UTF-8 binary bytes, observe the replies and record the close event.
The source also demonstrates declined upgrades and cell-owned outbound events.

## Run end to end

Prerequisites: Rust 1.94.1 or newer, Cargo, native C/C++ build tools and curl.
Run from the repository root and follow [AGENTS.md](../../AGENTS.md) for the
target directory. In terminal 1:

```sh
CELLAR_WEBSOCKET_PEER=ws://127.0.0.1:9876 \
  cargo run -p cellar --bin cellar --locked -- \
  dev examples/native-websocket/project.json --no-watch --clean
```

The environment variable is required by this host's startup even when using
its local routes. The local self-checks below do not contact that peer URL.
Wait for `ready`, then in terminal 2:

```sh
curl -sS -w '\n' http://127.0.0.1:9876/bind/worker
curl -sS -w '\n' http://127.0.0.1:9876/bind/cell
curl -sS -w '\n' http://127.0.0.1:9876/room-state
```

## Expected output and interpretation

The Worker response contains `messages` with `"worker greeting"`,
`"reply:client hello"` and `[0,128,255]`; the cell response uses
`"room greeting"` instead. Both include `close:[4001,"complete",true]`.
The room state contains `received:[0,128,255]` and its owner identifier;
the close field appears after the close event is processed. These demonstrate
upgrade consumption, binary preservation, event-owned background work and
persisted cell effects. JSON key order and generated owner values vary.

For a separate-process outbound handshake, follow the
[wsclient guide](../wsclient/README.md). For this older host's `/echo` and
`/cells/NAME` paths, set `CELLAR_WEBSOCKET_PEER` to the origin of a peer that
provides `/echo`; declined-upgrade tests additionally need its `/decline`
endpoint. The local walkthrough above exercises real sockets without those
additional peer contracts.

## Stop, restart and cleanup

Press Ctrl-C and let the host drain. State is under
`examples/native-websocket/.cellar/native-dev`. Restart without `--clean` and
GET `/room-state` to observe the persisted received bytes. A closed network
connection does not survive restart. Add `--clean` on the next start to reset
this example, or use another `--port` when another host occupies 9876.
See the [catalog](../README.md) for the complete example set.
