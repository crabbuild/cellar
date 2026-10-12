# Cellar Rust SDK

Use `cellar::sdk` for application code. `Cell` defines a durable entity,
`Registry` registers cell factories and stateless services, and `Host` runs the
compiled application's production node. `cellar-native` owns reusable Rust
values and codecs; `cellar-logic` owns lifecycle decisions; `cellar-ltx` owns
replication formats and storage transport.

```toml
[dependencies]
cellar = { path = "../cellar/crates/cellar" }
```

All builds are engine-free. No feature switch is necessary.

## Application primitives

| Primitive | Rust API | Runtime verification |
| --- | --- | --- |
| Cells, namespaces, identities | `Cell`, `Registry::cell`, `Context::namespace`, `cells` | `native_cells`, `native_call_order`, `native_concurrency` |
| SQLite, key/value storage, cursors, transactions, alarms, durability | `Context::storage`, `storage` | `native_storage`, `native_transactions`, `native_runtime` |
| Structured RPC and remote errors | `cells`, `service`, `clone`, `rpc` | `native_cells`, `native_egress`, primitive clone tests |
| SQL databases, prepared queries, batches, migrations | `Context::sql`, `sql` | `native_sql` |
| KV, metadata, expiration and lists | `Context::kv`, `kv` | `native_kv` |
| Queue producers, consumer ACK/retry, batching and broker policy | `Context::queue`, `Invocation::Queue`, `queue` | `native_queue` |
| Workflow steps, replay, sleep, events and instance controls | `Registry::workflow`, `Context::workflow`, `workflow` | `native_workflow`, `native_workflow_cell` |
| Cron occurrences | `Invocation::Scheduled`, `cron` | `native_cron` |
| Blob reads/writes, conditions, metadata, lists and multipart | `Context::blob`, `blob` | `native_blob` |
| HTTP requests/responses, streaming bodies, cancellation | `http`, `Context::fetch`, `service` | `native_body`, `native_egress`, `native_streams` |
| Assets, routing rules, headers and redirects | `Context::assets`, `assets` | `native_assets_cache`, `native_daemon` |
| Cache storage and matching | `Context::caches`, `cache` | `native_assets_cache` |
| Streaming HTML rewriting | `html_rewriter` | `native_html_rewriter` |
| Server, bound and outgoing WebSockets | `websocket`, `Context::accept_websocket`, WebSocket invocations | `native_websocket`, `native_bound_websocket`, `native_outbound_websocket` |
| TCP, TLS, STARTTLS, half-close and leased endpoints | `sockets`, `Context::connect` | `native_sockets` |
| Cell-owned containers, exec streams and ports | `Context::container`, `containers` | `native_containers`, `native_container_packages` |
| Facets and compiled application loading | `facets`, `loader` | `native_facets`, `native_loader`, `native_loopback` |
| Events, timers, abort and background work | `events`, `web`, `Context::wait_until` | `native_events` |
| Headers, URL/search params, URL patterns | `http`, `web` | `native_web`, primitive pattern tests |
| Blob/file/form/multipart values | `web` | primitive body tests, `native_body` |
| Byte pipes, BYOB, leases and cancellation | `streams`, `primitives::streams` | `native_streams`, primitive stream tests |
| Encoding, compression, crypto, random values | `encoding`, `compression`, `crypto` | primitive tests |
| Structured clone and JSON codecs | `clone`, `json` | primitive clone and JSON tests |

The test names identify Cargo integration targets under `crates/cellar/tests`
and `crates/cellar-native/tests`. A passing target proves its scenarios; it does
not establish complete distributed conformance for the primitive.

## Execution and delivery

Handlers return `HandlerFuture<Output>` and receive a fresh executing `Context`.
Binding handles may outlive a turn; call them with that turn's context. Storage,
streams, sockets, and cancellation retain their activation/event authority.
Use `wait_until` for event-owned background work. Do not keep and reuse a retired
context in a later invocation.

The [counter application](../examples/native_counter.rs) demonstrates
storage, named business cells, routing, assets, and factory registration.
The [WebSocket application](../examples/native_websocket.rs)
demonstrates server sockets, outgoing sockets, bound responses, and cell calls.

Build with `cellar build PROJECT.json --output DIRECTORY` and develop with
`cellar dev PROJECT.json`. A project selects a Cargo package, binary, profile,
primary application configuration, and optional cohosted applications. See the
[counter project](../examples/native-counter-project.json).

`cellar deploy DIRECTORY --bucket NAME` stages immutable resources and verified
executables before publishing deployment pointers. `cellar install DIRECTORY
--bucket NAME --output EXECUTABLE` verifies downloads and writes a new executable
path. Your process supervisor activates it. The host admits only compiled
registrations for the requested script/code versions. Configure each node by
running the application's executable with `--bucket`, `--listen`,
`--internal-listen`, and `--advertise` as appropriate.

## Embedding

The SDK is a Cargo library and applications link their factories into the host.
Currently `Host::run()` must be called once from `main`, outside Tokio. It owns
the runtime, process arguments, listeners, signals and process exit. Independent
instances, application-owned startup/shutdown and optional transport adapters
still require a library lifecycle refactor.

## System SQLite namespace

All framework-owned tables, indexes and triggers start with lowercase `__sys_`.
Application SQL cannot read, mutate, create or rename objects into this namespace.
SQLite-owned `sqlite_*` tables and application schemas keep their own names.

Cell state uses `__sys_cell_kv`, `__sys_cell_alarm`, `__sys_cell_metadata` and
`__sys_cell_wake`; replication uses `__sys_ltx_seq` and `__sys_ltx_lock`.
KV uses `__sys_kv` and `__sys_kv_meta`; queues use `__sys_queue_messages`,
`__sys_queue_meta`, `__sys_queue_stats` and `__sys_queue_transfer_receipts`.
The local dev store uses `__sys_local_objects` and `__sys_local_sequence`.
Workflows and cron persist through cell storage and add no separate tables.

SQL migration ledgers default to `__sys_sql_migrations`. Custom ledger names
must start with `__sys_sql_migrations_`. Use `Database::migrations(context, table)`
or `cellar sql migrations list` to read history. Migration files operate on
application tables and cannot alter reserved tables.

Cellar requires fresh configuration and state for the hard cutover. Original
Celld storage, environment names, protocol aliases and dev paths are unsupported.

## Boundaries requiring further work

Rust implementations exist for the primitives above. Some browser/Node facade
semantics, transferred capabilities, property/pipelined RPC, general streams,
and complete WebSocket lifecycle parity remain unfinished. Dynamic application
loading currently selects compiled registrations. It does not compile arbitrary
JavaScript or supply a native Python/Wasm execution host.

Container controls use `Context::container` and the current root cell context.
Handles cannot cross cell activations, including a same-epoch reactivation.
Starts, signals, destruction, exec and stdin effects wait for a durability gate.
Native projects stage application and fence image archives, verify checksums and
upload them before publishing selectors. See [containers](services/containers.md).
Container controls in facets and real-engine fleet fault conformance remain work.
Node.js libraries should use Rust crates; a complete Node facade is not supplied.

Native poll-time accounting is cooperative. It cannot preempt Rust code that
never yields, and per-application heap isolation is not implemented. Applications
sharing a native process are trusted Rust code. Fleet crash/recovery, ownership
transfer, hibernation, rollout and schema evolution require broader validation.
The [migration checklist](rust-native-migration.md) records that work.
