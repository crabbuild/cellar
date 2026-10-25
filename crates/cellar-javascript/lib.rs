// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "256"]
//! Optional V8 and Node.js adapters for native Cellar applications.
//!
//! Enable `embed` for a bundled JavaScript application in V8, or `node` for an
//! external Node module with access to the Node and npm ecosystem. TypeScript
//! is compiled before execution; neither backend changes the native Rust core.
//!
//! Both backends proxy SDK operations through the original native invocation.
//! JavaScript objects belong to an activation; durable state lives in Cellar's
//! storage APIs. Runtime bootstrap owns worker shutdown and joins its threads.
#[cfg(any(feature = "embed", feature = "node"))]
mod application;
#[cfg(any(feature = "embed", feature = "node"))]
mod runtime;
#[cfg(any(feature = "embed", feature = "node"))]
pub use application::{EventKind, JavaScriptApplication, Source};
#[cfg(feature = "embed")]
pub use runtime::JavaScriptRuntime;
#[cfg(any(feature = "embed", feature = "node"))]
pub use runtime::{BridgeUsage, Options};

#[cfg(feature = "node")]
pub use runtime::node::{NodeOptions, NodeRuntime};
