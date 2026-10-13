# SDK API map

This is a navigation map to implemented APIs, not a replacement for generated
Rust documentation. Public imports use `cellar::sdk`; physical paths below
identify their implementation at the documented source baseline.

![API responsibilities](diagrams/architecture.svg)

| Task | Public surface | Implementation |
| --- | --- | --- |
| Define handlers | `Application`, `Cell`, `Registry`, `Context`, `Invocation`, `Output`, `HandlerFuture` | [contracts](../crates/cellar/application/mod.rs) |
| Identify/call cells | `cells::Namespace`, `Id`, `Stub`; `Context::namespace`, `exported_namespace`; stub `fetch`, `rpc`, `call`, signal variants | [cells](../crates/cellar/application/cells.rs) |
| Current cell state | `Storage`, `ListOptions`; JSON/clone get/put/list, SQL, cursors, `sync`, alarms | [storage](../crates/cellar/application/storage.rs) |
| Atomic cell operations | `transaction`, `transaction_sync`, `rollback` | [transactions](../crates/cellar/application/storage/transaction.rs) |
| SQL binding | `sql::Database`, prepared statements, sessions, batch, migrate, migrations, dump | [SQL](../crates/cellar/application/sql.rs) |
| KV binding | `kv::Namespace`; reads/metadata, put text/bytes/stream, delete/list | [KV](../crates/cellar/application/kv.rs) |
| Queue | `queue::Producer`, `SendOptions`, `Body`, `Event`, `Message`; send/batch, ack/retry | [Queue](../crates/cellar/application/queue.rs) |
| Workflow | `workflow::Workflow`, `Instance`, `Event`, `Steps`, options/status types | [Workflow](../crates/cellar/application/workflow.rs) |
| Cron | `cron::Scheduled`, `no_retry` | [cron](../crates/cellar/application/cron.rs) |
| Services / egress | `service::Fetcher`, `Context::service`, outbound `fetch`, structured RPC and signals | [services](../crates/cellar/application/service.rs) |
| HTTP values | `http::Response`, URL and outbound request/body helpers | [HTTP](../crates/cellar/application/http.rs) |
| Blob objects | `blob::Bucket`, `ObjectBody`, ranges, conditionals, multipart, metadata/checksums | [Blob](../crates/cellar/application/blob.rs) |
| Assets / cache / rewrite | `assets`, `cache`, `html_rewriter` modules | [assets](../crates/cellar/application/assets.rs), [cache](../crates/cellar/application/cache.rs), [rewriter](../crates/cellar/application/html_rewriter.rs) |
| Streams | `streams` module and owned byte stream values | [streams](../crates/cellar/application/streams.rs) |
| WebSockets | `websocket::WebSocketPair`, `WebSocket`, upgrade, hibernation, tags/attachments | [WebSocket](../crates/cellar/application/websocket.rs) |
| TCP/TLS | `sockets::SocketAddress`, `SocketOptions`, `Socket`, reader/writer | [sockets](../crates/cellar/application/sockets.rs) |
| Containers | `containers::Container`, `Port`, `Exec`; start/monitor/forward | [containers](../crates/cellar/application/containers.rs) |
| Facets | `facets::Class`, `Facets`, `Facet`, `Startup`; create/get/abort/delete | [facets](../crates/cellar/application/facets.rs) |
| Compiled workers | `loader::WorkerCode`, `Loader`, `Worker`, `Entrypoint` | [loader](../crates/cellar/application/loader.rs) |
| Structured values | `encode_rpc`, `decode_rpc`, `decode_rpc_result`; `clone`, `rpc`, `primitives` | [contracts](../crates/cellar/application/mod.rs), [native primitives](../crates/cellar-native/lib.rs) |
| Start embedded host | `Node`, `NodeConfig`, `NodeServices`, `NodeFleet`, `DeploymentGraph`, `CompiledApplications` | [Node](../crates/cellar/node/embedded.rs), [SDK facade](../crates/cellar/sdk.rs) |
| Supervise / stop | `NodeOutcome`, `ShutdownMode`, `ShutdownPolicy`, `ShutdownReport`, `FenceReason` | [lifecycle](../crates/cellar/node/lifecycle.rs) |
| Runtime / loader limits | `RuntimePolicy`, `LoaderPolicy`, `ActorOptions`, `ActorPolicy` | [runtime](../crates/cellar/runtime/policy.rs), [loader](../crates/cellar/runtime/workers/loader_policy.rs), [actor](../crates/cellar/actor/mod.rs) |
| Replication / logs | `ReplicaStorage`, `Replication`, `ReplicationOptions`, `LogOptions`, `LogTransport`, `DurabilityOwner` | [replication](../crates/cellar/durability/replication/options.rs), [logs](../crates/cellar/durability/log/mod.rs) |
| Host observations | `HostMetricsSample`, `MetricsSampler`, `ApplicationWorkSnapshot` | [services](../crates/cellar/execution/services.rs) |
| Process runner / packaging | `Host`, `Project`, `ApplicationConfig`, `AssetProject`, `ContainerProject` | [native host](../crates/cellar/node/native.rs), [package](../crates/cellar/cli/native_deploy/mod.rs) |

For detailed signatures, build Rust docs with `cargo doc -p cellar --no-deps
--locked` using the private target directory required by repository guidance.
Consult source and existing integration tests before adopting an unfamiliar
binding. API availability does not imply a global transaction, exactly-once
external effect, tenant sandbox or automatic production resource envelope.

Next: [SDK lifetimes](05-sdk-and-lifetimes.md), [configuration](11-configuration.md),
[integrations](09-integrations.md).
