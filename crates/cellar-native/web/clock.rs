// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A scoped host clock for synchronous owned-value construction.
use std::{cell::Cell, marker::PhantomData, rc::Rc};
thread_local! {static CLOCK:Cell<Option<fn()->f64>>=const{Cell::new(None)};}

/// Hosts install their clock while polling a native handler or factory. The
/// guard is confined to this thread and must drop before the poll suspends.
pub struct WallClockGuard {
    previous: Option<fn() -> f64>,
    _thread: PhantomData<Rc<()>>,
}

impl WallClockGuard {
    pub fn enter(clock: fn() -> f64) -> Self {
        Self {
            previous: CLOCK.with(|current| current.replace(Some(clock))),
            _thread: PhantomData,
        }
    }
}

impl Drop for WallClockGuard {
    fn drop(&mut self) {
        CLOCK.with(|current| current.set(self.previous));
    }
}

pub(super) fn now_ms() -> f64 {
    match CLOCK.with(Cell::get) {
        Some(clock) => clock(),
        None => standalone_wall_ms(),
    }
}

// This fallback is the standalone library's host boundary. cellar installs
// its execution-domain clock for each native factory, handler and cleanup poll.
#[allow(clippy::disallowed_methods)]
fn standalone_wall_ms() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|time| time.as_millis() as f64)
        .unwrap_or(0.0)
}
