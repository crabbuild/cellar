// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exact-boot lifecycle guards and post-join closure. Neither changes votes.
mod closure;
mod records;
mod reporter;

pub(crate) use closure::Sink;
pub use closure::VerifiedFleetClosure;
pub(crate) use records::{BootBinding, Guard, LifecycleRecord, LifecycleUpdate, StopSelector};
pub use records::{FleetStopAcceptance, FleetStopAction, FleetStopRequest};
pub(crate) use reporter::{Reporter, Services};
