// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fair admission to the existing single reserved SQL progress slot.

use super::{unavailable, ExecutionError};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// The caller charges its retained waiter before joining the FIFO: apply uses
/// its existing apply reservation; SQL completions borrow an ordinary slot.
/// An idle progress slot remains available when the ordinary queue is full.
pub(super) async fn reserve<T: Send>(
    reserved: &Arc<Semaphore>,
    admit_waiter: impl FnOnce() -> Result<T, ExecutionError> + Send,
) -> Result<OwnedSemaphorePermit, ExecutionError> {
    match reserved.clone().try_acquire_owned() {
        Ok(slot) => return Ok(slot),
        Err(TryAcquireError::Closed) => return Err(unavailable()),
        Err(TryAcquireError::NoPermits) => (),
    }
    let waiter = admit_waiter()?;
    let slot = reserved
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| unavailable())?;
    drop(waiter);
    Ok(slot)
}

#[cfg(test)]
#[path = "progress_tests.rs"]
mod tests;
