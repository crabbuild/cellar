# SDK API map

This is a navigation map to implemented APIs, not a replacement for generated
Rust documentation. Public imports use `cellar::sdk`; physical paths below
identify their implementation at the documented source baseline.

![API responsibilities](diagrams/architecture.svg)

| Task | Public surface | Implementation |
| --- | --- | --- |
| Define handlers | `Application`, `Cell`, `Registry`, `Context`, `Invocation`, `Output`, `HandlerFuture` | [contracts](../crates/cellar/application/mod.rs) |
| Identify/call cells | `cells::Namespace`, `Id`, `Stub`; `Context::namespace`, `exported_namespace`; stub `fetch`, `rpc`, `call`, signal variants | [cells](../crates/cellar/application/cells.rs) |
| Current cell state | `Storage`, `ListOptions`; JSON/clone get/put/list, SQL, cursors, `sync`, alarms | [storage](../crates/cellar/application/storage/mod.rs) |
| Atomic cell operations | `tx`, `transaction`, `transaction_sync`, `rollback`; activation/domain admission at access and commit | [transactions](../crates/cellar/application/storage/transaction.rs) |
| Outbox publication | `Context::stream::<E>`, `event_stream::Event`, `Stream<E>`, `tx.emit`; `Storage::outbox` | [event delivery guide](26-event-delivery.md), [handles](../crates/cellar/application/event_stream/mod.rs) |
| Durable history | `event_stream::Stream::read`, `ReadOptions`, `Cursor`, `Page`, `Record` | [stream client](../crates/cellar/application/event_stream/client.rs) |
| Resumable consumption | `Stream::subscription`, `Subscription::next`/`next_with_id`, `BatchOptions`, `Batch::ack`/`retry`/`extend_until`, `Receipt` | [subscriptions](../crates/cellar/application/event_stream/subscription.rs) |
| Local effect deduplication | `tx.inbox(...).accept(record)`, `Storage::sync` before remote acknowledgment | [storage guide](06-storage-transactions.md) |
| Event outcomes | `event_stream::DeliveryError`, `ErrorCode`, `ErrorCode::as_str` | [typed errors](../crates/cellar/application/event_stream/error.rs) |
| SQL binding | `sql::Database`, prepared statements, sessions, batch, migrate, migrations, dump | [SQL](../crates/cellar/application/sql/mod.rs) |
| KV binding | `kv::Namespace`; reads/metadata, put text/bytes/stream, delete/list | [KV](../crates/cellar/application/kv/mod.rs) |
| Queue | `queue::Producer`, `SendOptions`, `Body`, `Event`, `Message`; send/batch, ack/retry | [Queue](../crates/cellar/application/queue/mod.rs) |
| Workflow | `workflow::Workflow`, `Instance`, `Event`, `Step`; `run`/`run_with`, `WaitOptions`, options/status types | [Workflow](../crates/cellar/application/workflow/mod.rs) |
| Workflow batches and defaults | `BatchCreateOptions`, `BatchCreateResult`, `BatchCreateError`, `BindingSettings`, `CronSchedule` | [batches](../crates/cellar/application/workflow/batch.rs), [configuration](../crates/cellar/application/workflow/configuration.rs) |
| Workflow compensation | `run_with_rollback`, `RollbackOptions`, `RollbackContext`, `TerminateOptions` | [rollback](../crates/cellar/application/workflow/rollback.rs) |
| Workflow byte outputs | `run_stream`, `run_stream_with`, `run_stream_with_rollback`, `StreamOutput`, `StepSelector`, `Instance::step_stream` | [streams](../crates/cellar/application/workflow/stream.rs) |
| Workflow observations | `Instance::subscribe`, `SubscribeOptions`, `Subscription`, `InstanceEvent`, `InstanceEventType` | [history](../crates/cellar/application/workflow/history.rs) |
| Cron | `cron::Scheduled`, `no_retry` | [cron](../crates/cellar/application/cron/mod.rs) |
| Services / egress | `service::Fetcher`, `Context::service`, outbound `fetch`, structured RPC and signals | [services](../crates/cellar/application/service.rs) |
| HTTP values | `http::Response`, URL and outbound request/body helpers | [HTTP](../crates/cellar/application/http.rs) |
| Host HTTP configuration | `HttpPolicy`, `HttpClientFactory`; `NodeConfig::http`, `NodeServices::http_client`, `HostServices::configure_http` | [HTTP policy](../crates/cellar/transport/http/policy.rs), [host services](../crates/cellar/execution/services.rs) |
| Host wall clock | `WallClock`, `ExecutionDomain::with_wall_clock`; `NodeServices::wall_clock`, `ExternalNodeServices::wall_clock` | [Execution domain](../crates/cellar/execution/domain.rs), [Node](../crates/cellar/node/embedded.rs) |
| Blob objects | `blob::Bucket`, `ObjectBody`, ranges, conditionals, multipart, metadata/checksums | [Blob](../crates/cellar/application/blob/mod.rs) |
| Host asset storage | `AssetCache`, `AssetCacheOptions`, `AssetResolver`, `FileSystem`; explicit cache passed to `DeploymentGraph::load` | [asset cache](../crates/cellar/runtime/assets/cache.rs), [filesystem](../crates/cellar-ltx/src/host.rs) |
| Assets / cache / rewrite | `assets`, `cache`, `html_rewriter` modules | [assets](../crates/cellar/application/assets.rs), [cache](../crates/cellar/application/cache.rs), [rewriter](../crates/cellar/application/html_rewriter/mod.rs) |
| Byte streams | `streams` module and owned byte stream values | [byte streams](../crates/cellar/application/streams.rs) |
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
