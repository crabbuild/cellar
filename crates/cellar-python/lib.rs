#![doc = include_str!("README.md")]
#![warn(clippy::disallowed_methods, clippy::disallowed_types)]

#[cfg(feature = "embed")]
mod application;
#[cfg(feature = "embed")]
mod failure;
#[cfg(feature = "embed")]
mod operations;
#[cfg(feature = "embed")]
mod runtime;
#[cfg(feature = "embed")]
mod storage;
#[cfg(feature = "embed")]
mod values;

#[cfg(feature = "embed")]
pub use application::{EventKind, PythonApplication, Source};
#[cfg(feature = "embed")]
pub use runtime::{
    is_verified_build, verified_inventory_sha256, BridgeReport, Environment, Incomplete,
    Interpreter, InterpreterReport, Options, PythonRuntime, Report, Stop,
};
