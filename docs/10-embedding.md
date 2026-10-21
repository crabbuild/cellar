# Embed an application-owned node

Use `sdk::Node` when your application owns the process, executors, listeners
and supervision. Supply services and policies explicitly, then inspect the
returned lifecycle outcome. If the CLI can own those responsibilities, use
`sdk::Host` instead.

The library receives bound listeners and leaves signals and process exits to
you. Node configuration and runtime, loader and HTTP policy defaults do not
read environment variables.

![A Cellar public listener or an application-owned Axum server dispatches into the same durable cell runtime.](diagrams/hosting-modes.svg)

`Node::start` uses Cellar's public HTTP path. For an external Axum listener,
`Node::start_external` returns the ingress handle used by `CellarService`.
Startup must complete recovery in either mode; see the
[recovery sequence](diagrams/recovery-flow.svg).

## Construction contract

1. Own and keep alive the host and core executors. Bind distinct public and
   internal listeners, and select a unique data directory for each node.
2. Build an `ActorIdentity` and `ActorOptions`, then `NodeConfig`. Select actor,
   runtime, HTTP, cell API limits, loader, startup, maintenance and shutdown
   policies explicitly. `config.cell_limits = CellLimits::unrestricted()` lifts
   the inherited API ceilings for trusted native applications; see
   [configuration](11-configuration.md) for backend and protocol ceilings.
3. Supply `NodeServices`: executor handles, listeners, peer HTTP client,
   `PeerAuth`, `PeerProbeSigner`, optional `NodeFleet`, container engine and
   metrics sampler, optional UTC clock, and optional SDK HTTP client factory.
   Node and peer-auth identity must agree.
4. Provide a `DeploymentGraph` and `CompiledApplications` whose factories match
   the graph. Configure policy before generations are built.
5. Await `Node::start`, then supervise with `run_until` and inspect the outcome.

The following startup function uses the exact public signature. Infrastructure
construction is deliberately caller-specific; use the
[complete measuring host](../tools/perf/embedded.rs) for a runnable reference.

```rust
use cellar::sdk::{CompiledApplications, DeploymentGraph, Node, NodeConfig,
    NodeOutcome, NodeServices, ShutdownMode};

async fn serve(config: NodeConfig, services: NodeServices,
    graph: DeploymentGraph, applications: CompiledApplications,
    stop: impl std::future::Future<Output = ShutdownMode>)
    -> anyhow::Result<NodeOutcome>
{
    let node = Node::start(config, services, graph, applications).await?;
    node.run_until(stop).await
}
```

## Configure HTTP and clocks

`NodeConfig::http` sets the SDK request deadline, including response-body reads.
`NodeServices::http_client` optionally supplies an `HttpClientFactory` for TLS,
DNS, credentials, proxies and connection-pool settings. Set it to `None` for
library defaults: 120 seconds and no ambient proxy configuration. Cellar applies
each redirect mode's policy after the factory callback and retains invocation
subrequest accounting. Lower-level hosts call `HostServices::configure_http`
before their first fetch. HTTP configuration stays fixed across generations;
closing the domain releases cached pools and prevents reconstruction.

Set `NodeServices::wall_clock` and `ExternalNodeServices::wall_clock` to
`None` for system UTC. Supply `Some(Arc<WallClock>)` to capture a
caller-owned provider before bootstrap. It returns UTC milliseconds, may move
backward, and must sample quickly without blocking; negative samples clamp to
zero. Tasks, blocking work and cleanup retain their originating node's clock.
It affects runtime wall time, including ownership, alarms and stream expiry.
Monotonic deadlines/timers and peer signature timestamps retain their separate
host clocks. Keep fleet UTC sources consistent. Lower-level hosts use
`ExecutionDomain::with_wall_clock`; this never installs a process-wide override.

## Load deployment assets

Build a bucket-backed graph with an explicit `AssetCache`:

```rust
use cellar::sdk::{AssetCache, AssetCacheOptions, DeploymentGraph};

async fn deployment(bucket: &cellar::bucket::Bucket, node: String,
    directory: std::path::PathBuf) -> anyhow::Result<DeploymentGraph>
{
    let cache = AssetCache::new(AssetCacheOptions::new(directory))?;
    DeploymentGraph::load(bucket, node, &cache).await
}
```

`AssetCache::with_filesystem` accepts a caller-owned `FileSystem`. Its exclusive
creation operation must be atomic; cache recency updates are best effort. Each
resolver retains the cache for its generation and fallback indexes. Clone the
capability to share its download coordination and cleanup target deliberately.
Choose separate directories to isolate hosts. File reads are bounded to 64 KiB
chunks and retain the serving host when another runtime polls the body. Closing
that host rejects later reads and joins file operations already running.

`Node::start_with_filesystem` captures a caller-owned durability filesystem in
its execution domain before startup. Async tasks, blocking I/O and cancellation
cleanup retain that exact provider. `ExecutionDomain::with_filesystem` provides
the same contract for lower-level hosts and accepts an optional wall clock.
Providers must implement atomic exclusive creation, locking, rename and file/
directory sync against the same host paths used by SQLite and node directory
locks. Unsupported operations fail closed. Ordinary `Node::start` and external
Node startup continue to use `DirectFileSystem`.

## Fleet services and startup safety

`NodeFleet` takes ordinary and lease buckets, `ReplicaStorage`,
`ReplicationOptions`, an authenticated `LogTransport`, `LogOptions`, and
`fleet_durability`. Ownership and replication must identify the same fleet
prefix. Ordinary and lease clients must agree on bucket/backend. The supplied
CAS client should not automatically retry ambiguous writes. Fleet generation
must match the supplied probe signer's public key.

Startup recovers predecessor logs before replacing the node lease. Failed
recovery preserves the predecessor generation. An exclusive directory lock
remains held through completion, including failed/cancelled startup. Do not
reuse a directory or retire executors merely because the startup future was
cancelled; owned work must finish joining.

## Supervision, adoption and shutdown

`Node::adopt` explicitly installs compiled generations; a failing factory leaves
the current generation serving. It does not fetch or dynamically execute
arbitrary source code. Plan old/new state and workflow compatibility.

`NodeOutcome` distinguishes `Stopped(ShutdownReport)`, `Cancelled`,
`Fenced { code, reason }`, `CoreFailed` and `ReplicationFailed`. Only the CLI
maps these to process exits. A stopped report can contain incomplete phases.
`shutdown` requests drain; `cancel` forces closure. Dropping a node forces
closure and claims no graceful proof.

Keep both executors alive until returned completion. The semantic stop has an
absolute budget and a separate no-progress window; progress cannot extend the
absolute deadline. Already-started blocking work must finish before joining
completes, so the semantic budget is not a guaranteed wall-clock return bound.
Tasks spawned outside the Cellar event lifecycle need host supervision too.

Sources: [SDK](../crates/cellar/sdk.rs), [Node](../crates/cellar/node/embedded.rs),
[outcomes](../crates/cellar/node/lifecycle.rs). Next:
[configuration](11-configuration.md), [operations](17-operations.md).
