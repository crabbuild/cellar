// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The driver keeps polling accepted work while one reversible gate settles.
use crate::actor::AppHandle;
use crate::raft::fleet_catalog::FleetCatalogError;
use crate::raft::fleet_stop::MaintenanceCommand;
use std::sync::Arc;

pub(super) struct Driver {
    lifecycle: Arc<crate::raft::operations::NodeLifecycle>,
    pending: Option<MaintenanceCommand>,
    raft_sealed: bool,
}

impl Driver {
    pub fn new(lifecycle: Arc<crate::raft::operations::NodeLifecycle>) -> Self {
        Self {
            lifecycle,
            pending: None,
            raft_sealed: false,
        }
    }

    pub fn pending(&self) -> bool {
        self.pending.is_some()
    }

    pub fn accept(&mut self, command: MaintenanceCommand) {
        if self.pending.is_some() {
            let _ = command.reply.send(Err(FleetCatalogError::Overloaded));
            return;
        }
        self.raft_sealed = false;
        self.pending = Some(command);
    }

    pub fn pump(&mut self, app: &AppHandle, queued: usize, application_events: usize) {
        let Some(command) = &self.pending else {
            return;
        };
        let now = self.lifecycle.maintenance_now();
        let result = if !self.lifecycle.maintenance_matches(&command.selector) {
            Some(Err(FleetCatalogError::Conflict))
        } else if now.is_err() || now.is_ok_and(|now| now >= command.deadline) {
            Some(Err(FleetCatalogError::Unconfirmed))
        } else if app.public_in_flight() != 0 || queued != 0 {
            None
        } else if !self.raft_sealed {
            match self.lifecycle.seal_maintenance_raft(&command.selector) {
                Ok(()) => {
                    self.raft_sealed = true;
                    None
                }
                Err(error) => Some(Err(error)),
            }
        } else if application_events == 0 {
            Some(Ok(()))
        } else {
            None
        };
        if let Some(result) = result {
            if let Some(command) = self.pending.take() {
                let _ = command.reply.send(result);
            }
        }
    }
}
