# Native Rust examples

These applications use `cellar::sdk` and compile into native host executables.
Run the commands below from the repository root.

| Example | Demonstrates |
| --- | --- |
| [Counter](native_counter.rs) | Named business cells, durable storage, HTTP routing and static assets |
| [WebSockets](native_websocket.rs) | Outbound WebSockets, service upgrades and hibernating cell sockets |
| [Container cells](native-container/README.md) | One container per named cell, port readiness and HTTP forwarding |
| [Embedded node](../tools/perf/embedded.rs) | Caller-owned executors, listeners, storage and peer clients, and returned shutdown proofs |

## Embedding in your application

Start an engine with `cellar::sdk::Node::start`. The
[complete measuring host](../tools/perf/embedded.rs) constructs `NodeConfig`,
`NodeServices`, a deployment graph and compiled application factories. It owns
the host and core runtimes, binds the listeners, supplies authenticated peer
and storage clients, captures its configuration and selects its stop future.
`Node::run_until` returns the outcome after joining the node's work. Keep both
executors alive until it returns. The host checks that both still accept work
and reports each shutdown phase; the harness requires a complete proof before
marking its run successful. A stopped outcome can contain unfinished phases
when a shutdown budget expires, so inspect the returned `ShutdownReport`.

The [SDK guide](../crates/cellar/sdk.rs) documents the public contracts and
minimal startup pattern. The [embedded fleet guide](../tools/perf/README.md)
provides the image build and local RustFS recovery commands for this host.

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
