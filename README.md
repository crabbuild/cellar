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

Library hosts call `sdk::Node::start` with `NodeConfig`, `NodeServices`, a
`DeploymentGraph` and `CompiledApplications`. The application supplies bound
listeners, host and core executors, HTTP clients, peer authentication and a
per-node probe signer. Fleet nodes also take caller-owned storage and log
transports. Startup recovers the predecessor log before installing a replacement
lease; a failed recovery preserves the predecessor generation. Each node retains
an exclusive data-directory lock through completion, including cancelled startup.
`Node::adopt` installs compiled generations explicitly and leaves the current
generation serving when a factory fails.

`ActorOptions`, `RuntimePolicy`, `LoaderPolicy`, startup and maintenance policy,
container infrastructure and metrics are captured per node. Library defaults do
not read environment variables. The CLI resolves its environment settings at
startup. Lower-level hosts can construct an `ExecutionDomain` directly.

Production dispatch and draining now share a driver that accepts a captured
`ShutdownPolicy` and a stop future. It returns `NodeOutcome` for requested stops,
fencing, core failure, or replication failure; the CLI alone maps these to process
exits. Both stop modes drain complete application event drivers and queued SDK
calls before changing cell lifecycle. This includes post-response `wait_until`
work, nested calls, alarms, and events from previous application generations.
`ShutdownReport` records ingress, application-event and cell drain completion,
pending events and dispatches, log sealing, local durability closure, and
clean-reload preparation. `HostServices::application_work` exposes per-host
pending and retired event counts; retirement includes cancellation and failure.
Stall timeouts and the absolute deadline remain distinct, so a bounded stop can
report incomplete work. HTTP tasks and facet dispatch retain their owning domain.
`Node::run_until` supervises the engine using the application's stop future;
`Node::shutdown` requests a drain and `Node::cancel` forces closure. The supervisor
closes and joins owned work before returning while leaving both executors running.
Keep the executors alive through completion. The semantic drain has a bounded
budget, but already-started blocking work must finish before the join completes.
Work left at a deadline remains visible in the report even after forced domain
closure retires it. Application tasks spawned outside Cellar's event lifecycle
remain the application's responsibility. Fleet-scale rollout and recovery
validation remain follow-up work.

`LoaderPolicy` fixes each host's loaded-worker capacity, per-script and generation
quotas, copied environment size, and entrypoint or class property size. Environment
budgets include supplied service-route metadata. Derived handles retain their
originating host's property budget; service capabilities reject calls from another
execution domain. Configure the policy before building a generation; reading it
pins the defaults. Encoded values remain subject to the structured-value decoder's
64 MiB ceiling. The CLI accepts `CELLAR_LOADER_MAX_WORKERS`,
`CELLAR_LOADER_MAX_WORKERS_PER_PRINCIPAL`, `CELLAR_LOADER_MAX_ENV_BYTES`, and
`CELLAR_LOADER_MAX_PROPS_BYTES`. The obsolete module-byte setting is rejected.

`Replication::start` takes `ReplicaStorage` and `ReplicationOptions`. Applications
supply the storage client, fleet prefix, upload and compaction policy, restore
and hydration limits, and durability timeout. Ordinary replication operations
share the supplied client's pool and credentials. Applications can supply a
`ReplicaRangeReaderFactory` to control paged reads; signed S3 fault reads are an
explicit option that retains the supplied credential provider.
`Bucket::from_clients` accepts caller-owned ordinary, CAS, and paginated clients.

`NodeLogManager::new` takes `LogOptions` and a caller-supplied `LogTransport`.
Each host controls its pipeline, follower window, ordered streams, hedging,
eviction, and lease publication timeout. `SignedPeerTransport::from_clients`
retains the application's request and stream HTTP pools; custom transports own
peer authentication. Recover the predecessor log before installing a node lease,
then retain one `DurabilityOwner` for the replication and log tasks. Cancelling
an execution domain is a forced stop and does not prove a graceful data drain.

Cellar leases require an explicit `generation` and logs require
`may_have_fleet_acks`. Recovery accepts complete ranged tails; original lease
aliases and entries-only tails are rejected. Expired node leases become CAS
tombstones after their logs are sealed, preserving recovery pointers and guarding
against deletion of a successor generation.

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
Local integration tests exercise follower disk proofs and recovery after leader
loss. Broader fleet rollout and recovery validation remain follow-up work.

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
