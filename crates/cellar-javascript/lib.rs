//! Optional V8 and Node.js adapters. TypeScript is compiled before execution.
#[cfg(any(feature = "embed", feature = "node"))]
mod application;
#[cfg(any(feature = "embed", feature = "node"))]
mod runtime;
#[cfg(any(feature = "embed", feature = "node"))]
pub use application::{EventKind, JavaScriptApplication, Source};
#[cfg(feature = "embed")]
pub use runtime::JavaScriptRuntime;
#[cfg(any(feature = "embed", feature = "node"))]
pub use runtime::Options;

#[cfg(feature = "node")]
pub use runtime::node::{NodeOptions, NodeRuntime};
