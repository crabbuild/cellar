// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "256"]
#![doc = include_str!("README.md")]
#![warn(clippy::disallowed_methods, clippy::disallowed_types)]

#[cfg(feature = "embed")]
mod application;
#[cfg(feature = "embed")]
mod failure;
#[cfg(feature = "embed")]
mod operations;
#[cfg(feature = "embed")]
mod read_batch;
#[cfg(feature = "embed")]
mod runtime;
#[cfg(feature = "embed")]
mod values;

#[cfg(feature = "embed")]
pub use application::{EventKind, PythonApplication, Source};
#[cfg(feature = "embed")]
pub use runtime::{
    is_verified_build, verified_inventory_sha256, BridgeReport, Environment, Incomplete,
    Interpreter, InterpreterReport, InterpreterSnapshot, InterpreterWork, LatencyReport,
    MemoryReport, MemorySample, OperationReport, Options, PythonRuntime, Report, Stop,
};
