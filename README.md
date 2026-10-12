# Cellar

Cellar is a Rust framework for applications built from durable business cells.
It provides SQLite storage, bucket ownership, LTX replication, routing, alarms,
and fleet infrastructure. Cellar has no V8 or other JavaScript engine dependency,
including in default and all-feature builds.

Cellar uses a hard cutover: applications start with new Cellar configuration and
state. Original Celld aliases and persisted-state migration are unsupported.
All framework-owned SQLite tables use the reserved `__sys_` prefix.

Use `cellar::sdk` to define cells and services. A cell owns its durable state;
Cellar routes calls to its owner and gates externally visible results on the
required durability proof. Nodes sharing a bucket form a fleet.

The SDK provides storage and transactions, structured RPC, SQL, KV, queues,
workflows, cron, Blob, HTTP and bodies, assets, caches, HTML rewriting,
WebSockets, TCP/TLS, cell-owned containers, facets, and compiled application loaders. Owned values
include URLs, headers, forms, blobs/files, events, cancellation, byte streams,
encoding, compression, and cryptography. See the
[native Rust examples](examples/README.md).

Library hosts create an `ExecutionDomain` with their own Tokio handle and set
`ActorOptions`, `RuntimePolicy`, and container infrastructure through its services
before building application generations. `RuntimePolicy` controls worker and
request limits, admission waits, and handler budgets independently for each host;
library defaults are bounded and do not read environment variables. The CLI
resolves its environment settings at startup. The production actor can run on a
separate caller-owned core executor. A complete library bootstrap and graceful
durability drain API are still being extracted from the process runner.

## Build and run

```sh
cargo build -p cellar --bin cellar --locked
./target/debug/cellar --help
./target/debug/cellar dev examples/native-counter-project.json
```

The example serves `http://127.0.0.1:9876/counters/NAME`; POST increments its
persistent counter. `dev` builds the Rust application, watches its project,
and keeps durable state in `.cellar/native-dev`. Use `--no-watch` for a single
build, `--logs` for node logs, and `--clean` to reset the project's local state.

`cellar build PROJECT.json --output DIRECTORY` produces a verified application
package. `cellar deploy DIRECTORY --bucket NAME` publishes it; `cellar install`
downloads and verifies the executable. Run that compiled application executable
with your node's bucket, listeners, and peer settings. The existing
`cellar-native` executable is an alias for the same CLI.

Rust application code is compiled into its host executable. Source JavaScript,
Node.js modules, and the previous JavaScript-based Python/Wasm wrappers are no
longer executable in Cellar. Use the [native Rust examples](examples/README.md)
for current application development.

## Fleet and deployment

A conditional bucket write grants cell ownership to one node. LTX captures
committed SQLite writes. One node waits for bucket upload to prove durability;
a fleet can prove it through peer log replication before bucket upload.
Ownership fencing, restore, durable wakes, and shutdown use the retained host.
Broader fleet rollout and recovery validation remain follow-up work.

The default Docker image contains the Rust CLI:

```sh
docker build -t cellar .
docker run --rm cellar --version
```

Application images should copy their compiled executable into a compatible
runtime image and set it as the entry point. Pass fleet credentials and mount
persistent node storage when running that application. The generic CLI image
has no application factories compiled into it.

## Verification

```sh
python3 tools/check_native_dependencies.py
cargo test --workspace --all-features --lib --tests --locked
cargo test --workspace --all-features --doc --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```

Cellar, Cellar Logic, Cellar LTX, and Cellar Native are Apache-2.0 licensed.
See [LICENSE](LICENSE) and [NOTICE](NOTICE) for the license and attribution.
