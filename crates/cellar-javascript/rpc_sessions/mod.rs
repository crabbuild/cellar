// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Invocation-owned callable results over the existing native CellRpc route.
mod registry;
mod session;
mod wire;

pub(crate) use registry::{Registry, METHOD};
pub(crate) use session::Session;
