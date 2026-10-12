# Native Rust examples

These applications use `cellar::sdk` and compile into native host executables.
Run the commands below from the repository root. See the [SDK guide](../docs/sdk.md)
for the supported primitives and their boundaries.

| Example | Demonstrates |
| --- | --- |
| [Counter](native_counter.rs) | Named business cells, durable storage, HTTP routing and static assets |
| [WebSockets](native_websocket.rs) | Outbound WebSockets, service upgrades and hibernating cell sockets |
| [Container cells](native-container/README.md) | One container per named cell, port readiness and HTTP forwarding |

## Counter and static assets

```sh
cargo run -p cellar --bin cellar --locked -- dev examples/native-counter-project.json
curl -X POST http://127.0.0.1:9876/counters/alpha
curl http://127.0.0.1:9876/counters/alpha
curl http://127.0.0.1:9876/
```

Each name selects an independent durable counter. `dev` watches the application
and preserves local state under `examples/.cellar/native-dev` between builds.

Build a deployable package with the same project configuration:

```sh
cargo run -p cellar --bin cellar --locked -- \
  build examples/native-counter-project.json --output counter-package
```

## WebSockets

Set `CELLAR_WEBSOCKET_PEER` to a WebSocket peer's `ws://` or `wss://` origin,
then run the compiled application with your node settings:

```sh
cargo run -p cellar --bin cellar-native-websocket --locked -- --help
```

## Container cells

A local Docker or Podman engine is required. The Rust application starts the
Python HTTP server contained in `native-container/`:

```sh
cargo run -p cellar --bin cellar --locked -- dev examples/native-container-project.json
```

See the [container example guide](native-container/README.md) for requests,
packaging and storage behavior.
