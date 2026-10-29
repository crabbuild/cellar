// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Distinct receiving authority keeps resume work out of ordinary stop polling.
use super::*;
use crate::raft::fleet_catalog::{FleetCatalog, FleetCatalogError};
use crate::raft::storage::ReplicaLog;
use futures_util::future::BoxFuture;
use std::{sync::Arc, time::Duration};

type Result<T> = std::result::Result<T, FleetCatalogError>;

pub(super) struct Receiver<'a> {
    pub catalog: &'a FleetCatalog,
    pub log: &'a ReplicaLog,
    pub admission: Arc<MaintenanceAdmission>,
    pub gateway: &'a crate::application::consensus::RaftCells,
}

impl<'a> Receiver<'a> {
    fn remaining(&self, deadline: u64) -> Result<Duration> {
        deadline
            .checked_sub(
                self.log
                    .domain()
                    .mono_ms()
                    .map_err(|_| FleetCatalogError::Closed)?,
            )
            .filter(|ms| *ms > 0)
            .map(Duration::from_millis)
            .ok_or(FleetCatalogError::Unconfirmed)
    }

    pub fn apply(
        self,
        request: &'a FleetStopRequest,
        origin: Option<&'a BootBinding>,
        deadline: u64,
    ) -> BoxFuture<'a, Result<FleetStopAcceptance>> {
        Box::pin(async move {
            let guard = self
                .log
                .lifecycle_guard()
                .map_err(super::reporter::log_error)?
                .ok_or(FleetCatalogError::Conflict)?;
            if request.action == FleetStopAction::Resume {
                if let Some((selector, quiescence)) =
                    guard.resumed().filter(|(s, _)| **s == request.selector)
                {
                    // Finish the already-synced effect even after its waiter or
                    // deadline was lost. This grants no new scope or health.
                    self.admission.resume(quiescence, selector)?;
                    return Ok(request.acceptance());
                }
            }
            self.catalog
                .verify_local_stop(
                    &request.selector,
                    request.action,
                    origin,
                    self.remaining(deadline)?,
                )
                .await?;
            let local = LocalResume::new(self.log.clone(), self.admission.clone());
            match request.action {
                FleetStopAction::ObserveQuiescence => {
                    LocalQuiescence::new(self.log.clone(), self.admission.clone())
                        .check(&request.selector)?;
                }
                FleetStopAction::ObserveResume => {
                    let guard = self
                        .log
                        .lifecycle_guard()
                        .map_err(super::reporter::log_error)?
                        .ok_or(FleetCatalogError::Conflict)?;
                    let (selector, quiescence) = guard
                        .resumed()
                        .filter(|(s, _)| **s == request.selector)
                        .ok_or(FleetCatalogError::Unconfirmed)?;
                    local.check(quiescence, selector, ResumeMode::Resumed)?;
                }
                FleetStopAction::Resume => {
                    let checked = self
                        .catalog
                        .renew_restart_resume(
                            &request.selector,
                            self.gateway,
                            local,
                            self.remaining(deadline)?,
                        )
                        .await?;
                    self.log
                        .update_lifecycle(LifecycleUpdate::Resume {
                            selector: request.selector.clone(),
                            quiescence: Box::new(checked.quiescence.clone()),
                            deadline: deadline.min(checked.deadline),
                        })
                        .await
                        .map_err(super::reporter::log_error)?;
                    // The engine admitted this exact effect. Opening its local
                    // barriers is owned work and must finish after lost replies.
                    self.admission
                        .resume(&checked.quiescence, &request.selector)?;
                }
                _ => return Err(FleetCatalogError::PayloadConflict),
            }
            Ok(request.acceptance())
        })
    }
}
