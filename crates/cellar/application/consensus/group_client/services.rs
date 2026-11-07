// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded per-node control services. Closing signals every admitted worker,
//! then joins its actual lifetime before consensus/storage shutdown proceeds.
use super::*;
use crate::raft::{
    budget::{Budget, Permit},
    fleet_catalog::FleetCatalogError,
};
use tokio::sync::{watch, Notify};

#[derive(Clone, Copy)]
pub(crate) enum ControlServiceKind {
    Movement,
    Restart,
    Directory,
    Recovery,
    Reclamation,
    Transfer,
    Transition,
}

impl ControlServiceKind {
    fn slot(self) -> usize {
        match self {
            Self::Movement => 0,
            Self::Restart => 1,
            Self::Directory => 2,
            Self::Recovery => 3,
            Self::Reclamation => 4,
            Self::Transfer => 5,
            Self::Transition => 6,
        }
    }
}

struct Entry {
    id: u64,
    stop: watch::Sender<bool>,
}

#[derive(Default)]
struct State {
    closing: bool,
    next: u64,
    slots: [Option<Entry>; 7],
}

pub(super) struct Services {
    state: Mutex<State>,
    finished: Notify,
    pub(super) memory: Arc<Budget>,
}

pub(crate) struct ControlServiceLease {
    owner: Arc<Services>,
    kind: ControlServiceKind,
    id: u64,
    stop: watch::Receiver<bool>,
    memory: Option<Permit>,
}

pub(crate) struct ControlServiceCompletion(watch::Sender<bool>);

impl ControlServiceCompletion {
    pub(crate) fn new(done: watch::Sender<bool>) -> Self {
        Self(done)
    }
}

impl Drop for ControlServiceCompletion {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}

impl ControlServiceLease {
    pub(crate) fn stop_receiver(&self) -> watch::Receiver<bool> {
        self.stop.clone()
    }
}

impl Drop for ControlServiceLease {
    fn drop(&mut self) {
        // Release the worker's memory before advertising completed physical
        // ownership. A cancelled close waiter does not remove this entry.
        self.memory.take();
        let mut state = self.owner.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.slots[self.kind.slot()]
            .as_ref()
            .is_some_and(|e| e.id == self.id)
        {
            state.slots[self.kind.slot()] = None;
        }
        drop(state);
        self.owner.finished.notify_waiters();
    }
}

impl Services {
    pub(super) fn new(bytes: usize, items: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            finished: Notify::new(),
            memory: Budget::new(bytes, items),
        })
    }

    fn register(
        self: &Arc<Self>,
        kind: ControlServiceKind,
        bytes: usize,
    ) -> Result<ControlServiceLease, FleetCatalogError> {
        let mut state = self.state.lock().map_err(|_| FleetCatalogError::Closed)?;
        if state.closing {
            return Err(FleetCatalogError::Closed);
        }
        if state.slots[kind.slot()].is_some() {
            return Err(FleetCatalogError::Overloaded);
        }
        let memory = self
            .memory
            .acquire(bytes)
            .ok_or(FleetCatalogError::Overloaded)?;
        state.next = state.next.checked_add(1).ok_or(FleetCatalogError::Limit)?;
        let id = state.next;
        let (stop, receiver) = watch::channel(false);
        state.slots[kind.slot()] = Some(Entry { id, stop });
        Ok(ControlServiceLease {
            owner: self.clone(),
            kind,
            id,
            stop: receiver,
            memory: Some(memory),
        })
    }

    pub(super) async fn close(&self) {
        loop {
            let notified = self.finished.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                state.closing = true;
                for entry in state.slots.iter().flatten() {
                    entry.stop.send_replace(true);
                }
                if state.slots.iter().all(Option::is_none) {
                    return;
                }
            }
            notified.await;
        }
    }
}

impl RaftCells {
    /// Node installs its existing stop owner from the actual bound boot, after
    /// startup has synced the lifecycle guard. No caller-provided preboot clone.
    pub(crate) fn fleet_stop_services(
        &self,
    ) -> Result<Option<crate::raft::fleet_stop::Services>, FleetCatalogError> {
        let core = &self.handle.core;
        let Some(authorization) = &core.managed else {
            return Ok(None);
        };
        let boot = authorization.boot_binding()?;
        let signer = crate::asyncrt::services()
            .peer_probe_signer()
            .map_err(|_| FleetCatalogError::Closed)?;
        if core
            .log
            .lifecycle_guard()
            .map_err(|_| FleetCatalogError::Closed)?
            .is_none_or(|guard| guard.boot != boot)
            || boot.node != core.node.node.bytes()
            || boot.disk != core.node.disk.bytes()
            || boot.process != core.node.process.bytes()
            || boot.engine != core.engine.bytes()
            || boot.join_key != signer.public_key_bytes()
        {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(Some(crate::raft::fleet_stop::Services {
            authorization: authorization.clone(),
            log: core.log.clone(),
            signer,
        }))
    }

    // Internal loops must not retain the last external SDK lifetime. An active
    // service is joined by the registry, independently of its public handle.
    pub(crate) fn internal_observer(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            lifetime: None,
        }
    }

    pub(crate) fn control_budget(&self) -> &Arc<Budget> {
        &self.handle.core.services.memory
    }

    pub(crate) fn register_control_service(
        &self,
        kind: ControlServiceKind,
        bytes: usize,
    ) -> Result<ControlServiceLease, FleetCatalogError> {
        if self.is_closed() || self.handle.admission.is_draining() {
            return Err(FleetCatalogError::Closed);
        }
        self.handle.core.services.register(kind, bytes)
    }

    pub(crate) async fn installed_host(
        &self,
        timeout: Duration,
    ) -> Result<crate::raft::fleet_catalog::FleetHostAuthorization, FleetCatalogError> {
        if self.is_closed() {
            return Err(FleetCatalogError::Closed);
        }
        let core = &self.handle.core;
        let host = core.managed.as_ref().ok_or(FleetCatalogError::Invalid(
            "control service requires managed host",
        ))?;
        let expected = host.boot_binding()?;
        let current = host.refresh(timeout).await?;
        let boot = current.boot_binding()?;
        if boot != expected
            || boot.node != core.node.node.bytes()
            || boot.disk != core.node.disk.bytes()
            || boot.process != core.node.process.bytes()
            || boot.engine != core.engine.bytes()
            || core
                .log
                .lifecycle_guard()
                .map_err(|_| FleetCatalogError::Closed)?
                .is_none_or(|guard| !guard.admits_group_start(&boot))
        {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(current)
    }

    pub(crate) async fn start_movement_worker(
        &self,
        mut work: crate::raft::fleet_catalog::MovementServiceWork,
        timeout: Duration,
    ) -> Result<(), FleetCatalogError> {
        let host = self.installed_host(timeout).await?;
        work.admit(&host)?;
        self.handle
            .domain
            .spawn_cleanup(async move { work.run().await })
            .map_err(|_| FleetCatalogError::Closed)?
            .detach();
        Ok(())
    }

    pub(crate) async fn start_restart_worker(
        &self,
        mut work: crate::raft::fleet_catalog::RestartServiceWork,
        timeout: Duration,
    ) -> Result<(), FleetCatalogError> {
        let host = self.installed_host(timeout).await?;
        work.admit(&host)?;
        self.handle
            .domain
            .spawn_cleanup(async move { work.run().await })
            .map_err(|_| FleetCatalogError::Closed)?
            .detach();
        Ok(())
    }
}

#[cfg(test)]
#[path = "services_tests.rs"]
mod tests;
