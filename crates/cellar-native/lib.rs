// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Rust application primitives shared by cellar's runtime adapters.
//!
//! This crate does not depend on a JavaScript engine. Runtime adapters own
//! dispatch, cell ownership and durability; these modules own operations on
//! Rust values. Resource state belongs to the caller and is released on drop.

pub mod call_order;
pub mod cell_id;
pub mod clone;
pub mod compression;
pub mod crypto;
pub mod encoding;
pub mod event_context;
pub mod json;
pub mod rpc;
pub mod sql;
pub mod streams;
pub mod web;
