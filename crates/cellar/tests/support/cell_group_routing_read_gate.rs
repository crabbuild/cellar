// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A real application callback holds its immutable image after caller cancellation.
use super::*;
use std::{
    collections::BTreeSet,
    sync::{Condvar, Mutex},
};

#[derive(Default)]
struct State {
    held: bool,
    entered: BTreeSet<String>,
    timed_out: bool,
}

#[derive(Default)]
pub(super) struct Callbacks {
    state: Mutex<State>,
    release: Condvar,
    pub commands: AtomicUsize,
    pub queries: AtomicUsize,
}

pub(super) struct Release(Arc<Callbacks>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.resume();
    }
}

impl Callbacks {
    pub fn arm(self: &Arc<Self>) -> Release {
        let mut state = self.state.lock().unwrap();
        assert!(!state.held && !state.timed_out);
        state.held = true;
        state.entered.clear();
        Release(self.clone())
    }

    pub fn query(&self, cell: &CellHistory) {
        self.queries.fetch_add(1, Ordering::AcqRel);
        let mut state = self.state.lock().unwrap();
        if !state.held {
            return;
        }
        state.entered.insert(cell.cell.as_str().to_owned());
        let (mut state, waited) = self
            .release
            .wait_timeout_while(state, TIMEOUT, |s| s.held)
            .unwrap();
        state.timed_out |= waited.timed_out() && state.held;
    }

    pub fn entered(&self) -> Option<usize> {
        let state = self.state.lock().unwrap();
        if state.entered.contains("Counter:one") {
            Some(0)
        } else if state.entered.contains("Counter:two") {
            Some(1)
        } else {
            None
        }
    }

    pub fn clean(&self) -> Result<()> {
        ensure!(
            !self.state.lock().unwrap().timed_out,
            "query callback exceeded the original fixture bound"
        );
        Ok(())
    }

    pub fn resume(&self) {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).held = false;
        self.release.notify_all();
    }
}
