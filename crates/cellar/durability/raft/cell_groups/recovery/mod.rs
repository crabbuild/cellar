// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fresh-history restoration and activation. Archive integrity, infrastructure
//! fencing, physical genesis and a committed serving decision are distinct proofs.
mod admission;
mod genesis;
mod import;
mod partition;
mod state;
pub use admission::{CellGroupRecoveryLimits, CellGroupRecoveryPool};
pub(crate) use genesis::RecoverySeed;
pub use genesis::SeededCellGroupRecovery;
pub use import::PreparedCellGroupRecovery;
pub use partition::{
    CellGroupTransferTarget, PreparedCellGroupTransfer, PreparedCellGroupTransferChild,
};
pub(super) use state::RecoveryState;

#[cfg(test)]
mod tests;
