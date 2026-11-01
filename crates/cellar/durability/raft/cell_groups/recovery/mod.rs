// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fresh-history restoration and activation. Archive integrity, infrastructure
//! fencing, physical genesis and a committed serving decision are distinct proofs.
mod admission;
mod import;
mod state;
pub use admission::{CellGroupRecoveryLimits, CellGroupRecoveryPool};
pub use import::PreparedCellGroupRecovery;
pub(super) use state::RecoveryState;

#[cfg(test)]
mod tests;
