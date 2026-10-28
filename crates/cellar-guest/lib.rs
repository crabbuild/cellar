// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native operation protocol shared by embedded language adapters.
//! These functions must be polled within the original SDK invocation, never
//! from a language runtime thread. Applications should use their typed facade.
mod chunk_stream;
pub mod containers;
pub mod failure;
pub mod operations;
mod request_body;
mod rpc_control;
pub mod rpc_failure;
mod storage;
pub mod transport;
pub mod values;
#[doc(hidden)]
pub mod workers;

/// Limits enforced before entering native effects.
#[derive(Clone, Debug)]
pub struct Limits {
    pub max_body_bytes: usize,
    pub max_batch_operations: usize,
}
