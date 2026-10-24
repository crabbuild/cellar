// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Axum handlers and application-owned HTTP servers backed by Cellar's runtime.
//!
//! [`AxumApplication`] and [`AxumCell`] execute handlers inside existing SDK
//! invocations. [`CellarService`] mounts a node's external ingress into Axum.
//! Every handler receives fresh event authority through [`CellContext`].
#![doc = include_str!("README.md")]
#![warn(clippy::disallowed_methods, clippy::disallowed_types)]

mod application;
mod builder;
mod cell;
pub mod extract;
pub mod http;
#[cfg(feature = "openapi")]
mod openapi;
mod service;
mod websocket;

pub use application::AxumApplication;
pub use axum;
pub use builder::{ApplicationBuilder, ApplicationDefinition};
pub use cell::AxumCell;
pub use extract::{CellContext, CurrentCell};
pub use service::CellarService;
pub use websocket::CellWebSocketUpgrade;
#[cfg(feature = "openapi")]
pub use {utoipa, utoipa_axum};
