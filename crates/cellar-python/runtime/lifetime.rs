// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Invocation authority and completion notification have different lifetimes.
use super::Runtime;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};

const ACTIVE: u8 = 1;
const FINISHED: u8 = 2;

pub(crate) struct InvocationState(AtomicU8);

impl InvocationState {
    pub(crate) fn new() -> Self {
        Self(AtomicU8::new(ACTIVE))
    }

    pub(crate) fn active(&self) -> bool {
        self.0.load(Ordering::Acquire) & ACTIVE != 0
    }

    pub(crate) fn expire(&self, finished: bool) {
        if finished {
            self.0.store(FINISHED, Ordering::Release);
        } else {
            self.0.fetch_and(!ACTIVE, Ordering::AcqRel);
        }
    }

    /// Revoke native authority and report whether Python still needs a wakeup.
    pub(crate) fn revoke(&self) -> bool {
        self.0.fetch_and(!ACTIVE, Ordering::AcqRel) & FINISHED == 0
    }
}

pub(crate) struct Lifetime(pub Arc<InvocationState>, pub Arc<Runtime>);

impl Drop for Lifetime {
    fn drop(&mut self) {
        // Expiration alone does not mean the Python task has finished. Native
        // cancellation must still wake it even after direct bridge expiration.
        if self.0.revoke() {
            self.1.invalidate();
        }
    }
}
