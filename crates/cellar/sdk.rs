//! Cellar's Rust application SDK.
//!
//! Import cell and service contracts, binding APIs, and owned primitive values
//! from one namespace. [`Context`] supplies the executing event's authority;
//! retain binding handles across events and pass the current context per call.
//!
//! Embedded engines enter an [`ExecutionDomain`] on the application's host
//! runtime. Configure [`ActorOptions`] and [`RuntimePolicy`] through
//! `domain.services()` before building application generations, then construct
//! [`Actor`] inside the domain and spawn it on the application's core executor.
//! The application owns fencing, task results and executor lifetimes. Full node
//! startup, recovery, and graceful durability drain still require library
//! lifecycle APIs.
//!
//! Configure an execution domain before loading application generations.
//!
//! ```no_run
//! use cellar::sdk::{ActorOptions, ExecutionDomain, RuntimePolicy};
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
//!     Ok(domain)
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
pub use crate::application::*;
#[cfg(not(cellar_internal_tests))]
pub use crate::asyncrt::{DomainClosed, ExecutionDomain};
pub use crate::container::EngineOptions as ContainerEngineOptions;
/// The process-owning command-line runner.
pub use crate::daemon::NativeDaemon as Host;
pub use crate::ltx_replication::Replication;
pub use crate::native_deploy::{ApplicationConfig, AssetProject, ContainerProject, Project};
pub use crate::replica_storage::{
    ReplicaRangeReaderFactory, ReplicaStorage, ReplicaStorageOptions,
};
pub use crate::replication_options::{CompactionConfig, ReplicationOptions};
pub use crate::runtime_policy::RuntimePolicy;
pub use cellar_ltx::TimestampMetadataKey as ReplicaTimestampMetadataKey;
pub use cellar_native as primitives;
pub use cellar_native::{clone, compression, crypto, encoding, json, rpc, web};

pub use cellar_logic::pressure::PressureConfig;
/// Policy types consumed by the embedded actor.
pub use cellar_logic::{Config as ActorPolicy, OwnershipOnEvict};
