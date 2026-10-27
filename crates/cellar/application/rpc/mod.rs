// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Named, Protobuf-typed cell calls over the native routing and durability path.
//!
//! Generated clients use [`Client`] and generated server registrations use
//! [`Router`]. Every call retains the originating invocation's authority;
//! neither a service descriptor nor a method name grants a new capability.
use crate::engine_api::RpcData;
mod budget;
mod catalog;
mod client;
mod contract;
mod descriptor;
mod envelope;
mod failure;
mod graph;
mod lease;
mod provenance;
mod router;
mod schema;
mod shape;
mod wire;

pub use budget::{BudgetSource, CallBudget};
pub use catalog::Catalog;
pub use client::{CallOptions, Client};
pub use contract::{Contract, Limits, Method};
pub use envelope::response as decode_reply;
pub use envelope::{decode_envelope, encode_envelope};
pub use failure::{Code, EffectOutcome, Failure};
pub use graph::{
    discard_message, encode_message, validate_graph, GraphBudget, GraphDiscard, MessageGraph,
};
pub use lease::{Lease, LeaseClosed, LeaseControl, LeaseRevocation};
pub(crate) use provenance::Entered;
pub use router::{HandlerFuture, Prepared, Router};

/// Only the SDK submission boundary uses this: no request has left its host.
/// Returning a typed envelope binds the refusal to this exact call's digest;
/// arbitrary errors propagated by entered handlers remain conservative.
pub(crate) fn unsubmitted_refusal(args: &RpcData, code: Code) -> Option<RpcData> {
    let (digest, _) = envelope::view(args).ok()?;
    Prepared::refusal(&digest, Failure::refused(code), Limits::default()).ok()
}

// Preserve the existing sdk::rpc codec API while adding typed business calls.
pub use cellar_native::rpc::{decode_response, encode_error, RemoteError};

/// The Protobuf runtime used by generated Rust message and service code.
pub use prost;
/// Well-known message types used by the generated portable Protobuf profile.
pub use prost_types;

#[cfg(test)]
#[path = "rpc_tests.rs"]
mod tests;
