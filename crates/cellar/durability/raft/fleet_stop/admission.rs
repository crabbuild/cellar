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
    resumed: Option<StopSelector>,
}

#[derive(Default)]
pub(crate) struct Admission {
    state: Mutex<State>,
    public: AtomicBool,
    raft: AtomicBool,
}

impl Admission {
    /// Pending-return startup has no admitted application work. Keep admission
    /// closed even if boot confirmation fails before the new selector is synced.
    pub(crate) fn hold_startup(&self) {
        self.public.store(true, Ordering::SeqCst);
        self.raft.store(true, Ordering::SeqCst);
    }

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
        state.resumed = None;
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

    pub fn resume(
        &self,
        quiescence: &StopSelector,
        selector: &StopSelector,
    ) -> Result<(), FleetCatalogError> {
        let mut state = self.state.lock().map_err(|_| FleetCatalogError::Closed)?;
        if state.resumed.as_ref() == Some(selector) {
            return Ok(());
        }
        if state.selector.as_ref() != Some(quiescence) || state.epoch != selector.epoch {
            return Err(FleetCatalogError::Conflict);
        }
        state.selector = None;
        state.resumed = Some(selector.clone());
        self.raft.store(false, Ordering::SeqCst);
        self.public.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn resumed(&self, selector: &StopSelector) -> bool {
        self.state
            .lock()
            .is_ok_and(|state| state.resumed.as_ref() == Some(selector))
    }

    pub fn fence(&self, selector: &StopSelector) -> Result<(), FleetCatalogError> {
        let mut state = self.state.lock().map_err(|_| FleetCatalogError::Closed)?;
        if selector.epoch < state.epoch {
            return Err(FleetCatalogError::Conflict);
        }
        state.epoch = selector.epoch;
        state.selector = None;
        state.resumed = None;
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

/// Receiver-owned access to the actual synced guard and reversible barriers.
/// This capability is never serialized or supplied by a stop requester.
#[derive(Clone)]
pub(crate) struct LocalQuiescence {
    log: crate::raft::storage::ReplicaLog,
    admission: std::sync::Arc<Admission>,
}

impl LocalQuiescence {
    pub(crate) fn new(
        log: crate::raft::storage::ReplicaLog,
        admission: std::sync::Arc<Admission>,
    ) -> Self {
        Self { log, admission }
    }

    pub(crate) fn check(&self, selector: &StopSelector) -> Result<(), FleetCatalogError> {
        let guard = self
            .log
            .lifecycle_guard()
            .map_err(|_| FleetCatalogError::Closed)?
            .ok_or(FleetCatalogError::Conflict)?;
        if guard.boot != selector.boot
            || guard.quiescing() != Some(selector)
            || !self.admission.matches(selector)
            || !self.admission.public_closed()
            || !self.admission.raft_closed()
        {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResumeMode {
    Paused,
    Resumed,
}

#[derive(Clone)]
pub(crate) struct LocalResume {
    log: crate::raft::storage::ReplicaLog,
    admission: std::sync::Arc<Admission>,
}

impl LocalResume {
    pub fn new(
        log: crate::raft::storage::ReplicaLog,
        admission: std::sync::Arc<Admission>,
    ) -> Self {
        Self { log, admission }
    }

    pub fn mode(
        &self,
        quiescence: &StopSelector,
        selector: &StopSelector,
    ) -> Result<ResumeMode, FleetCatalogError> {
        let guard = self
            .log
            .lifecycle_guard()
            .map_err(|_| FleetCatalogError::Closed)?
            .ok_or(FleetCatalogError::Conflict)?;
        if guard.boot != selector.boot {
            return Err(FleetCatalogError::Conflict);
        }
        if guard.quiescing() == Some(quiescence)
            && self.admission.matches(quiescence)
            && self.admission.public_closed()
            && self.admission.raft_closed()
        {
            return Ok(ResumeMode::Paused);
        }
        if guard.resumed() == Some((selector, quiescence))
            && self.admission.resumed(selector)
            && !self.admission.public_closed()
            && !self.admission.raft_closed()
        {
            return Ok(ResumeMode::Resumed);
        }
        Err(FleetCatalogError::Unconfirmed)
    }

    pub fn check(
        &self,
        quiescence: &StopSelector,
        selector: &StopSelector,
        mode: ResumeMode,
    ) -> Result<(), FleetCatalogError> {
        if self.mode(quiescence, selector)? != mode {
            return Err(FleetCatalogError::Unconfirmed);
        }
        Ok(())
    }
}
