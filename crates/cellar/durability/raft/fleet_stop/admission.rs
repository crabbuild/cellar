// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Process-local reversible gates follow the synced lifecycle owner. They never
//! reset either of the Node's permanent shutdown flags.
use super::StopSelector;
use crate::raft::fleet_catalog::FleetCatalogError;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};
use tokio::sync::{mpsc, oneshot};

#[derive(Default)]
struct State {
    epoch: u64,
    selector: Option<StopSelector>,
}

#[derive(Default)]
pub(crate) struct Admission {
    state: Mutex<State>,
    public: AtomicBool,
    raft: AtomicBool,
}

impl Admission {
    pub fn public_closed(&self) -> bool {
        self.public.load(Ordering::SeqCst)
    }

    pub fn raft_closed(&self) -> bool {
        self.raft.load(Ordering::SeqCst)
    }

    pub fn matches(&self, selector: &StopSelector) -> bool {
        self.state
            .lock()
            .is_ok_and(|state| state.selector.as_ref() == Some(selector))
    }

    pub fn begin(&self, selector: &StopSelector) -> Result<(), FleetCatalogError> {
        let mut state = self.state.lock().map_err(|_| FleetCatalogError::Closed)?;
        if state.selector.as_ref() == Some(selector) {
            return Ok(());
        }
        if selector.epoch <= state.epoch {
            return Err(FleetCatalogError::Conflict);
        }
        state.epoch = selector.epoch;
        state.selector = Some(selector.clone());
        self.public.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub fn seal_raft(&self, selector: &StopSelector) -> Result<(), FleetCatalogError> {
        let state = self.state.lock().map_err(|_| FleetCatalogError::Closed)?;
        if state.selector.as_ref() != Some(selector) {
            return Err(FleetCatalogError::Conflict);
        }
        self.raft.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub fn fence(&self, selector: &StopSelector) -> Result<(), FleetCatalogError> {
        let mut state = self.state.lock().map_err(|_| FleetCatalogError::Closed)?;
        if selector.epoch < state.epoch {
            return Err(FleetCatalogError::Conflict);
        }
        state.epoch = selector.epoch;
        state.selector = None;
        self.raft.store(false, Ordering::SeqCst);
        self.public.store(false, Ordering::SeqCst);
        Ok(())
    }
}

pub(crate) struct Command {
    pub selector: StopSelector,
    pub deadline: u64,
    pub reply: oneshot::Sender<Result<(), FleetCatalogError>>,
}

pub(crate) type Sender = mpsc::Sender<Command>;
pub(crate) type Receiver = mpsc::Receiver<Command>;
