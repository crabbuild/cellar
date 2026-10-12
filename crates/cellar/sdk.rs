// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Cellar's Rust application SDK.
//!
//! Import cell and service contracts, binding APIs, and owned primitive values
//! from one namespace. [`Context`] supplies the executing event's authority;
//! retain binding handles across events and pass the current context per call.
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
pub use crate::application::*;
pub use crate::daemon::NativeDaemon as Host;
pub use crate::native_deploy::{ApplicationConfig, AssetProject, ContainerProject, Project};
pub use cellar_native as primitives;
pub use cellar_native::{clone, compression, crypto, encoding, json, rpc, web};
