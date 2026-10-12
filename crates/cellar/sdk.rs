//! Cellar's Rust application SDK.
//!
//! Import cell and service contracts, binding APIs, and owned primitive values
//! from one namespace. [`Context`] supplies the executing event's authority;
//! retain binding handles across events and pass the current context per call.
//!
//! Embedded engines enter an [`ExecutionDomain`] on the application's host
//! runtime. Configure [`ActorOptions`] through `domain.services()` before
//! building application generations, then construct [`Actor`] inside the
//! domain and spawn it on the application's core executor. The application
//! owns fencing, task results and executor lifetimes. Full node startup,
//! recovery and graceful durability drain still require library lifecycle APIs.
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
pub use crate::native_deploy::{ApplicationConfig, AssetProject, ContainerProject, Project};
pub use cellar_native as primitives;
pub use cellar_native::{clone, compression, crypto, encoding, json, rpc, web};

pub use cellar_logic::pressure::PressureConfig;
/// Policy types consumed by the embedded actor.
pub use cellar_logic::{Config as ActorPolicy, OwnershipOnEvict};
