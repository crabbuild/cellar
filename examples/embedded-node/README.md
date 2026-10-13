# Embed a Node in a Rust application

Own executors, listeners, clients and limits from a Rust application, then
inspect the node's preserving shutdown outcome.

## How it works

[main.rs](main.rs) creates two Tokio runtimes, binds listeners, generates peer
authentication, supplies explicit `NodeConfig`/`NodeServices`, registers counters
and starts `sdk::Node`. Public ingress is fixed to `127.0.0.1:9876`; the internal
listener uses an available local port. POST writes a named cell's SQLite count.

Ctrl-C or SIGTERM selects `ShutdownMode::Preserve`. The host checks ingress,
accepted application work, cells, pending dispatches and timeout before returning.
Both caller-owned executors outlive the node's stop and join.

## Run end to end

Prerequisites: Rust 1.94.1 or newer, Cargo, native C/C++ build tools and curl.
Run from the repository root and follow [AGENTS.md](../../AGENTS.md) for the
target directory. Stop other examples using port 9876. In terminal 1:

```sh
cargo run -p cellar --bin cellar-example-embedded --locked -- \
  examples/.cellar/embedded-walkthrough
```

Use a fresh directory for the initial values below. Wait for
`embedded example ready at http://127.0.0.1:9876`. In terminal 2:

```sh
curl -sS -w '\n' http://127.0.0.1:9876/counters/alpha
curl -sS -w '\n' -X POST http://127.0.0.1:9876/counters/alpha
curl -sS -w '\n' http://127.0.0.1:9876/counters/alpha
```

## Expected output and interpretation

The responses are `{"count":0}`, `{"count":1}` and `{"count":1}`. Stop with
Ctrl-C in terminal 1. The final report contains:

```json
{"outcome":"stopped","ingress_drained":true,"application_work_drained":true,"cells_drained":true}
```

These fields prove accepted work drained. The code also checks zero pending
dispatches and no drain timeout; a missing/failed report is not graceful proof.
Restart with the same command and GET alpha: it returns `{"count":1}`.
This verifies local SQLite persistence through a preserving stop.

## Stop and cleanup

Stop before deleting the tutorial directory. To reset only this walkthrough:

```sh
rm -rf examples/.cellar/embedded-walkthrough
```

Never share a directory between running nodes. This host supplies no shared
bucket or fleet replication. Its generated peer key belongs to one process;
a fleet needs securely supplied shared credentials and per-node signing
identities. The [fleet recipe](../distributed-fleet/README.md) shows caller-owned
storage and authenticated transports. Forced termination provides no graceful
proof. See the [catalog](../README.md) for the complete set.
