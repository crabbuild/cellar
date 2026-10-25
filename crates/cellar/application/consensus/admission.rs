// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public admission and work registration share a short barrier. Sealing first
//! prevents new callers, then waits for any prior registration to become visible
//! before the Node samples application work. No guard crosses an await or I/O.
use super::CommandFailure;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex, MutexGuard,
};

#[derive(Default)]
pub(crate) struct Admission {
    draining: AtomicBool,
    gate: Mutex<()>,
}

impl Admission {
    pub(crate) fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }

    pub(crate) fn registration(&self, cleanup: bool) -> Result<MutexGuard<'_, ()>, CommandFailure> {
        let guard = self.gate.lock().map_err(|_| CommandFailure::Unavailable)?;
        if self.is_draining() && !cleanup {
            return Err(CommandFailure::Unavailable);
        }
        Ok(guard)
    }

    pub(crate) fn seal(&self) -> Result<(), CommandFailure> {
        self.draining.store(true, Ordering::Release);
        self.barrier()
    }

    pub(crate) fn barrier(&self) -> Result<(), CommandFailure> {
        drop(self.gate.lock().map_err(|_| CommandFailure::Unavailable)?);
        Ok(())
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        self.gate.is_poisoned()
    }
}

#[cfg(all(test, not(cellar_internal_tests)))]
#[path = "admission_tests.rs"]
mod tests;
