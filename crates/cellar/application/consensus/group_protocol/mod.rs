// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cell-addressed entries for one physical group. Decoding verifies the wire
//! binding and borrows payload bytes; it does not grant quorum or image proofs.

mod codec;
mod types;

pub use types::*;
