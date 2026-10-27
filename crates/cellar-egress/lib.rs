// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Container network interception primitives. The engine owns run lifetime;
//! rule installation and request routing never depend on a retained JS closure.
//! This crate does not itself advertise a working container interception API.

#![warn(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    clippy::disallowed_macros
)]
#![doc = include_str!("README.md")]

pub mod control;
pub mod dns;
pub mod docker_io;
pub mod firewall;
#[cfg(all(feature = "helper", target_os = "linux"))]
pub mod linux;
mod rules;
mod target;
pub mod transport;
pub mod wire;
pub use rules::{Prepared, Rules, MAX_INTERCEPT_ENTRIES};
pub use target::{Destination, DnsFamily, Host, HttpTarget};
