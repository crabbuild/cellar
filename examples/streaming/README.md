<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# WebSocket and SSE in Rust, Python, JavaScript and TypeScript

Run the same small application on Cellar's native transport with your preferred
language. No external services, API credentials, or npm socket packages are
needed. Node 24+ is used for the shared client, and Python requires the adapter's
CPython 3.14 build prerequisites. Use the repository Rust toolchain and a unique
`CARGO_TARGET_DIR` as described in [AGENTS.md](../../AGENTS.md).

## What to try

| Route | Behavior |
| --- | --- |
| `GET /` | Browser client with live WebSocket and SSE message logs. |
| `GET /events` | Three SSE `progress` events, then `done`; the response finishes. |
| `GET /ws` | Worker-owned WebSocket that echoes text and binary frames. |
| `GET /rooms/demo/ws` | Hibernating cell socket with a durable message count. |
| `GET /rooms/demo/state` | JSON containing the room's persisted message count. |

Room names are 1–64 ASCII letters, digits, underscores or hyphens. Each name
owns separate state. HTTP requests to a socket route without an upgrade receive
426. The example's endpoints are intended for local learning.

SSE is a streaming HTTP response with `content-type: text/event-stream`. Each
record ends with a blank line. An event may span several network chunks, or one
chunk may contain several events. Producers yield individual records; Cellar
awaits each native write for backpressure. This finite example emits its events
as work becomes available, without an artificial delay. Replace the progress
producer with your own awaited work for model tokens or job updates.

WebSocket begins with an HTTP upgrade, then either side can send text or binary
frames. A worker keeps a receive loop running in its opening invocation. A
hibernating cell instead returns the upgrade and handles later messages through
fresh callbacks. The socket can remain open while the language cell object is
retired on the same live host. Store durable application data with `ctx.storage`;
do not retain a context or socket facade across callbacks.

Rust owns connections, cell routing and output gates for every backend. Python
and JavaScript call the Rust transport through invocation-scoped capabilities;
TypeScript compiles to JavaScript. The native gate holds output behind the
configured durability proof for writes it can reveal. A slow consumer applies
backpressure, and a disconnect releases the associated stream work. The cell
example saves its counter before echoing each message.

## Choose a server

Run commands from the repository root. Each command starts a local development
host on port 9876 with private local storage. Run one backend at a time; use
`--port` to choose another port. Ctrl-C drains the host. `--no-watch` builds once.

### Native Rust

The [Rust application](main.rs) shows `IdentityTransformStream`,
`wait_until_byte_stream`, `write_stream_chunk`, worker receive loops, and
`Invocation::WebSocketMessage` callbacks.

```sh
cargo run --locked -p cellar --bin cellar -- \
  dev examples/streaming/project-rust.json --no-watch
```

### JavaScript on Node

The [plain JavaScript application](app.js) uses `sseResponse`, `sseEvent`, and
`webSocketResponse`. The [Node host](node.rs) explicitly registers the room's
WebSocket callbacks. Node owns the language process; Rust owns the SDK sockets.

```sh
npm ci --prefix examples/streaming --ignore-scripts
CELLAR_NODE_ENTRY="$PWD/examples/streaming/app.js" \
CELLAR_NODE_EXECUTABLE="$(command -v node)" \
  cargo run --locked -p cellar --bin cellar -- \
  dev examples/streaming/project-node.json --no-watch
```

### TypeScript on Node or embedded V8

The [TypeScript application](app.ts) gives the same handlers explicit SDK types.
The build typechecks it, emits `dist/app.js` for Node, and creates a closed
`app.bundle.js` for embedded V8. Build tooling requires Node; an embedded server
runs the bundle inside the Rust process.

```sh
npm ci --prefix tools/javascript --ignore-scripts
npm ci --prefix examples/streaming --ignore-scripts
npm run --prefix examples/streaming build

# Choose Node:
CELLAR_NODE_ENTRY="$PWD/examples/streaming/dist/app.js" \
CELLAR_NODE_EXECUTABLE="$(command -v node)" \
  cargo run --locked -p cellar --bin cellar -- \
  dev examples/streaming/project-node.json --no-watch

# Or stop Node and choose embedded V8:
cargo run --locked -p cellar --bin cellar -- \
  dev examples/streaming/project-javascript.json --no-watch
```

The project's `javascript: true` explicitly permits the embedded V8 adapter.
Rebuild the bundle before running `dev` after changing application code; Cargo
embeds the prepared bundle. The plain JS module can also be bundled for V8 by
selecting `app.js` in `javascript.json`.

### Python

The [Python application](app.py) uses async generators, `StreamingResponse.sse`,
and `WebSocketResponse`. It imports only the standard library and the embedded
`cellar` facade. No pip dependencies are needed. The [Python host](python.rs)
registers callbacks and checks the combined shutdown report.

```sh
# Select an installed CPython 3.14 with its matching shared library and stdlib:
export PYO3_PYTHON="$(command -v python3.14)"
cargo run --locked -p cellar --bin cellar -- \
  dev examples/streaming/project-python.json --no-watch
```

See the [Python setup guide](../python/README.md#install-and-run) for interpreter
installation and ABI requirements.

## Connect a client

In another terminal, watch the raw SSE records with curl:

```sh
curl -N http://127.0.0.1:9876/events
```

The response contains three records like the following, followed by an
`event: done` record with `data: complete`:

```text
event: progress
id: 1
data: {"step":1}

```

Run the [shared Node client](check.mjs) against any backend:

```sh
node examples/streaming/check.mjs http://127.0.0.1:9876
curl http://127.0.0.1:9876/rooms/demo/state
```

It verifies browser asset delivery, SSE framing, worker and cell handshakes,
Unicode text, binary bytes
`[0, 128, 255]`, graceful close and the persisted message count. On fresh state,
the count becomes 2. Running it again adds 2. It prints:

```text
PASS SSE progress, worker/cell text and binary echo, durable message count
```

Open [the browser client](http://127.0.0.1:9876/). Choose a cell
or worker socket, click **Connect**, type a message and click **Send**. The log
shows the outgoing message and its incoming echo with arrival times. **Send binary
sample** demonstrates a non-UTF-8 frame. Cell echoes update the stored room count;
**Refresh count** also picks up messages sent by other clients. Each connection
echoes its own messages; the example does not broadcast between connections.

Click **Start SSE feed** to receive the three named `progress` events and final
`done` event. Each is logged as the browser receives it. This finite sample emits
quickly; the UI does not add a display delay. It closes EventSource on `done` so
EOF does not trigger a reconnect. Start the feed again to run another response.

The page uses the browser's `WebSocket` and `EventSource` APIs, with no framework
or client dependencies. [client.js](assets/client.js) shows the actual event
listeners. Log entries use text nodes and retain at most 100 records per protocol.
The four projects serve the same [static assets](assets/index.html) through the
native asset pipeline; `/events`, `/ws` and `/rooms/*` still enter the application.

## Restart and verification

Stop the server, start the same project without `--clean`, and read
`/rooms/demo/state`. The count survives. Existing network connections close;
clients must reconnect. Add `--clean` to a later `dev` command to reset that
project's teaching state. Each project keeps its state under `.cellar` beside
its configuration; Node's JS and TS variants share the same project.

After preparing the language dependencies above, this script launches servers
in temporary private directories, verifies both socket types and SSE, restarts
them, and checks recovered counts and clean shutdown:

```sh
cargo build --locked -p cellar --bin cellar
python3 examples/streaming/smoke.py \
  --cellar "$CARGO_TARGET_DIR/debug/cellar" \
  --backend rust --backend node-js --backend node-ts --backend v8 --backend python
```

Select only installed backends by omitting their flags. The V8 smoke check
expects `npm run --prefix examples/streaming build` to have prepared the bundle.
JavaScript CI runs the Rust, Node JS/TS and V8 variants; Python example CI
runs the Python variant with its configured CPython runtimes.

SSE event IDs here label one response; there is no durable event log or
`Last-Event-ID` replay. Hibernation preserves sockets only on the live host;
connections are not replicated across process restart or fleet failover.
Long-lived streams reserve invocation capacity and consume operation budgets.
See [streaming contracts and limits](../../docs/27-streaming-and-websockets.md)
for forwarding, outbound sockets, cancellation and durability details.
