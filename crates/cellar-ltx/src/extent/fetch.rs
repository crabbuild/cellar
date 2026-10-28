// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One physical receive shared by concurrent readers of an exact extent.

use super::file::ExtentFile;
use crate::stream_snapshot::Error;
use std::sync::{Arc, Condvar, Mutex};

#[derive(Default)]
enum State {
    #[default]
    Idle,
    Receiving,
    Ready(ExtentFile),
}

#[derive(Default)]
pub(super) struct Fetch {
    state: Mutex<State>,
    changed: Condvar,
}

pub(super) enum Access {
    Ready(ExtentFile),
    Receive(Owner),
}

pub(super) struct Owner {
    fetch: Arc<Fetch>,
    active: bool,
}

impl Fetch {
    /// Wait only on the caller's blocking domain. No state mutex crosses
    /// source I/O, verification, file close or scratch reservation release.
    pub(super) fn acquire(self: &Arc<Self>) -> Result<Access, Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Invalid("poisoned extent fetch"))?;
        loop {
            match &*state {
                State::Ready(file) => return Ok(Access::Ready(file.clone())),
                State::Idle => {
                    *state = State::Receiving;
                    return Ok(Access::Receive(Owner {
                        fetch: self.clone(),
                        active: true,
                    }));
                }
                State::Receiving => {
                    state = self
                        .changed
                        .wait(state)
                        .map_err(|_| Error::Invalid("poisoned extent fetch"))?;
                }
            }
        }
    }
}

impl Owner {
    pub(super) fn complete(mut self, file: ExtentFile) -> Result<(), Error> {
        {
            let mut state = self
                .fetch
                .state
                .lock()
                .map_err(|_| Error::Invalid("poisoned extent fetch"))?;
            *state = State::Ready(file);
            self.active = false;
        }
        self.fetch.changed.notify_all();
        Ok(())
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        if self.active {
            // The receive's file and reservation unwind before this owner.
            // Preserve the producer's original error; another waiting read can
            // retry the idempotent fetch after its physical charge is released.
            if let Ok(mut state) = self.fetch.state.lock() {
                *state = State::Idle;
            }
            self.fetch.changed.notify_all();
        }
    }
}
