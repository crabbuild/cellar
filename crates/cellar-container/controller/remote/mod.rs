// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Mutually authenticated, bounded, one-request TLS connections to an
//! independently durable controller. No early data, redirects or retries.
mod client;
mod config;
mod server;
mod wire;
pub use client::RemoteController;
pub use config::{ClientTls, Permission, RemoteOptions, ServerTls};
pub use server::ControllerServer;

#[cfg(test)]
#[path = "../remote_tests.rs"]
mod tests;
