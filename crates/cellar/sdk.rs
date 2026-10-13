//! Cellar's Rust application SDK.
//!
//! Import cell and service contracts, binding APIs, and owned primitive values
//! from one namespace. [`Context`] supplies the executing event's authority;
//! retain binding handles across events and pass the current context per call.
//!
//! Start an embedded production engine with [`Node::start`], [`NodeConfig`],
//! [`NodeServices`], a [`DeploymentGraph`] and [`CompiledApplications`]. Supply
//! bound listeners, host and core executors, peer clients and authentication,
//! a per-node probe signer, and optional fleet, container and metrics services.
//! Startup recovers the predecessor log before installing a replacement lease.
//! Node configuration and runtime, loader and HTTP policy defaults do not
//! inspect process environment.
//!
//! [`Node::run_until`] accepts the application's stop future and returns
//! [`NodeOutcome`] after closing and joining its execution domain. Keep both
//! supplied executors alive through completion. [`ShutdownReport`] records
//! completed proof phases and distinguishes a stall timeout from the absolute
//! stop deadline; a requested stop can leave unfinished work for recovery.
//! Both stop modes wait for complete event drivers, including post-response
//! [`Context::wait_until`], nested SDK calls, alarms and previous generations,
//! before changing cell lifecycle. [`HostServices::application_work`] exposes
//! pending and retired event lifetimes. Retirement includes failure and
//! cancellation; it supplies progress, not a durability proof. A timed-out
//! report retains unfinished counts observed before forced domain closure.
//! Dropping or cancelling a node forces closure and claims no graceful proof.
//! Started blocking work must finish before its directory can be reused.
//! [`NodeOutcome::Fenced`] retains the core's [`FenceReason`]: an expired,
//! missing or unrecognized lease. Losing the reporting channel returns
//! [`NodeOutcome::CoreFailed`] without inventing an authority loss. Lower-level
//! [`Actor`] constructors report actual fences through [`FenceReport`].
//!
//! ```no_run
//! use cellar::sdk::{CompiledApplications, DeploymentGraph, Node, NodeConfig,
//!     NodeOutcome, NodeServices, ShutdownMode};
//!
//! async fn serve(config: NodeConfig, services: NodeServices,
//!     graph: DeploymentGraph, applications: CompiledApplications,
//!     stop: impl std::future::Future<Output = ShutdownMode>)
//!     -> anyhow::Result<NodeOutcome>
//! {
//!     let node = Node::start(config, services, graph, applications).await?;
//!     node.run_until(stop).await
//! }
//! ```
//!
//! [`Node::adopt`] installs compiled generations explicitly; a failed factory
//! leaves the current generation serving. [`Host`] is the process-owning CLI
//! runner. Lower-level hosts can configure an [`ExecutionDomain`] before building
//! generations and constructing [`Actor`] on their own core executor.
//! [`ActorPolicy::max_cached_routes`] bounds idle, unowned routing history;
//! zero disables caching. Ownership and unfinished work retain their records
//! through their normal lifecycle, independently of this cache limit.
//!
//! Configure an execution domain before loading application generations.
//!
//! ```no_run
//! use cellar::sdk::{ActorOptions, ExecutionDomain, HttpClientFactory, HttpPolicy,
//!     LoaderPolicy, RuntimePolicy};
//! use std::time::Duration;
//!
//! fn domain(handle: tokio::runtime::Handle, actor: ActorOptions)
//!     -> anyhow::Result<ExecutionDomain>
//! {
//!     let domain = ExecutionDomain::new(handle);
//!     domain.services().configure_runtime(RuntimePolicy {
//!         max_stateless_requests: Some(128),
//!         max_cell_requests: 32,
//!         handler_budget: Duration::from_secs(30),
//!         ..Default::default()
//!     })?;
//!     domain.services().configure_actor(&actor)?;
//!     domain.services().configure_http(HttpPolicy {
//!         request_timeout: Duration::from_secs(10),
//!     }, Some(HttpClientFactory::new(|| {
//!         reqwest::Client::builder().no_proxy().pool_max_idle_per_host(16)
//!     })))?;
//!     domain.services().configure_loader(LoaderPolicy {
//!         max_workers: 128,
//!         max_workers_per_principal: 64,
//!         ..Default::default()
//!     })?;
//!     Ok(domain)
//! }
//! ```
//!
//! Loader limits belong to the originating host and remain fixed across
//! generations and derived handles. Environment budgets include copied values
//! and supplied service-route metadata; property budgets apply to service
//! handles, loaded entrypoints, and cell classes. Both are bounded by the
//! structured-value decoder's 64 MiB ceiling. Library defaults do not inspect
//! process environment. [`LoaderPolicy::from_env`] is the explicit CLI adapter.
//! Service handles must be used with a context from their own execution domain.
//!
//! Set [`NodeConfig::http`] and [`NodeServices::http_client`] for SDK HTTP egress.
//! The factory supplies DNS, TLS, credentials, proxy and connection-pool settings;
//! Cellar installs redirect policy to preserve manual/error modes and invocation
//! subrequest budgets. Ordinary modes reuse separate pools per host. A worker
//! with a redirect budget retains its own client for the invocation's accounting.
//! [`HttpPolicy::request_timeout`] includes response-body consumption. Defaults
//! use a 120-second deadline and ignore ambient proxies. Client construction
//! failures return fetch errors. Policies remain fixed across generations, and
//! domain closure releases cached pools and prevents their reconstruction.
//! [`HttpPolicy::from_env`] explicitly reads `CELLAR_FETCH_TIMEOUT_S` for CLI
//! startup; the CLI also explicitly selects reqwest's system-proxy configuration.
//!
//! Asset deployment loading takes an explicit [`AssetCache`]. Select its
//! directory, cleanup target and serving file limit once; clones deliberately
//! share that cache across generations or nodes. There is no inferred directory
//! or environment lookup. [`AssetCache::with_filesystem`] also accepts a caller's
//! [`FileSystem`]; it must support atomic exclusive creation for publication.
//! File reads, refreshes and maintenance retain the serving execution domain.
//! Shutdown joins begun blocking reads even when another host consumes a body.
//!
//! ```no_run
//! use cellar::sdk::{AssetCache, AssetCacheOptions, DeploymentGraph};
//! async fn deployment(bucket: &cellar::bucket::Bucket, node: String,
//!     directory: std::path::PathBuf) -> anyhow::Result<DeploymentGraph> {
//!     let cache = AssetCache::new(AssetCacheOptions::new(directory))?;
//!     DeploymentGraph::load(bucket, node, &cache).await
//! }
//! ```
//!
//! Replication uses the application's storage client and explicit policy.
//! Start it inside the domain that will own its tasks and cell databases.
//!
//! ```no_run
//! use cellar::sdk::{ExecutionDomain, ReplicaStorage, ReplicaStorageOptions,
//!     Replication, ReplicationOptions};
//! use object_store::ObjectStore;
//! use std::{path::Path, sync::Arc, time::Duration};
//!
//! async fn replication(domain: &ExecutionDomain, watch: &Path,
//!     client: Arc<dyn ObjectStore>) -> anyhow::Result<Replication>
//! {
//!     let storage = ReplicaStorage::new(client, ReplicaStorageOptions {
//!         prefix: "fleets/accounts".into(), ..Default::default()
//!     })?;
//!     domain.run(async {
//!         Replication::start(watch, storage, ReplicationOptions {
//!             durability_timeout: Duration::from_secs(15),
//!             sync_concurrency: 32, restore_concurrency: 16,
//!             ..Default::default()
//!         })
//!     }).await?
//! }
//! ```
//!
//! [`ReplicaStorage::with_range_readers`] lets the application supply blocking
//! page-fault readers and their transport. The generic storage path uses the
//! supplied client on a helper executor; [`ReplicaStorage::s3`] explicitly
//! selects the signed S3 fault reader and the supplied client's provider.
//! [`crate::bucket::Bucket::from_clients`] accepts ordinary, CAS and paginated
//! clients. Configure the CAS client without retries so ambiguous writes remain
//! visible to the ownership protocol.
//!
//! Fleet logs take an immutable [`LogOptions`] and an authenticated
//! [`LogTransport`]. [`SignedPeerTransport::from_clients`] retains the host's
//! ordinary and ordered-stream HTTP pools. Custom implementations must verify
//! peer identity and protocol at both ends. Every replacement ensemble retains
//! the same host policy; [`NodeLogManager::from_environment`] is the CLI adapter.
//! Construct the manager in the replication's domain and call
//! [`NodeLogManager::recover_self`] before installing the node's lease. Retain
//! one [`DurabilityOwner`] to own the coupled log and replication tasks.
//! Its local shutdown joins those tasks after runtime admission has stopped;
//! a complete application shutdown must first drain accepted writes and preserve
//! ownership and recovery invariants. Closing an execution domain forcibly
//! cancels its work and provides no durability drain proof.
//!
//! ```no_run
//! use cellar::sdk::{Cell, Context, HandlerFuture, Invocation, Output, http::Response};
//! use std::sync::Arc;
//! struct Account;
//! impl Cell for Account {
//!     fn invoke(self: Arc<Self>, ctx: Context, event: Invocation) -> HandlerFuture<Output> {
//!         Box::pin(async move {
//!             let Invocation::Fetch(_) = event else { return Ok(Output::Done); };
//!             let storage = ctx.storage()?;
//!             let balance = storage.get_json("balance")?.unwrap_or(serde_json::json!(0));
//!             Ok(Output::Http(Response::from_json(&balance)?.into_http(&ctx)?))
//!         })
//!     }
//! }
//! ```
pub use crate::actor::{Actor, ActorIdentity, ActorOptions, ActorServices};
pub use crate::application::limits::{
    BlobLimits, CellLimits, KvLimits, SqlLimits, SqliteLimits, WorkflowLimits,
};
pub use crate::application::*;
pub use crate::assets::{AssetCache, AssetCacheOptions, AssetResolver};
#[cfg(not(cellar_internal_tests))]
pub use crate::asyncrt::{DomainClosed, ExecutionDomain};
pub use crate::container::EngineOptions as ContainerEngineOptions;
pub use crate::daemon::CompiledApplications;
/// The process-owning command-line runner.
pub use crate::daemon::NativeDaemon as Host;
#[cfg(not(cellar_internal_tests))]
pub use crate::daemon::{
    ExternalNodeServices, IngressBody, IngressHandle, IngressResponse, Node, NodeConfig, NodeFleet,
    NodeInfo, NodeMaintenancePolicy, NodeServices, NodeStartupPolicy,
};
pub use crate::generation::DeploymentGraph;
pub use crate::host_services::{
    ApplicationWorkSnapshot, HostMetricsSample, HostServices, MetricsSampler,
};
pub use crate::http_policy::{HttpClientFactory, HttpPolicy};
pub use crate::loader_policy::LoaderPolicy;
pub use crate::log_options::{
    EvictionPolicy as LogEvictionPolicy, HedgeMode, LogOptions, LogWindowOptions,
};
pub use crate::ltx_replication::Replication;
pub use crate::native_deploy::{ApplicationConfig, AssetProject, ContainerProject, Project};
pub use crate::node_lifecycle::{
    FenceReason, FenceReport, NodeOutcome, ShutdownMode, ShutdownPolicy, ShutdownReport,
};
pub use crate::node_log::{
    DurabilityOwner, FollowerStore, LogStreamIo, LogTransport, NodeLogManager, OwnLog,
    SignedPeerTransport,
};
pub use crate::peer_auth::PeerAuth;
pub use crate::peer_probe::PeerProbeSigner;
pub use crate::replica_storage::{
    ReplicaRangeReaderFactory, ReplicaStorage, ReplicaStorageOptions,
};
pub use crate::replication_options::{CompactionConfig, ReplicationOptions};
pub use crate::runtime_policy::RuntimePolicy;
pub use cellar_logic::event_stream::Limits as EventLimits;
pub use cellar_logic::queue::Limits as QueueLimits;
pub use cellar_ltx::TimestampMetadataKey as ReplicaTimestampMetadataKey;
pub use cellar_ltx::{
    DirectFileSystem, FileSystem, HostDirEntry, HostFile, HostFileIo, HostMetadata,
};
pub use cellar_native as primitives;
pub use cellar_native::{clone, compression, crypto, encoding, json, rpc, web};

pub use cellar_logic::pressure::PressureConfig;
/// Policy types consumed by the embedded actor.
pub use cellar_logic::{Config as ActorPolicy, OwnershipOnEvict};
