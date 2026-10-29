// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Authenticated inventory and actual synced admission mode, including empty nodes.
use super::*;

pub(super) struct Context<'a> {
    pub catalog: &'a FleetCatalog,
    pub snapshot: &'a FleetSnapshot,
    pub request: &'a FleetRestartResumeRequest,
    pub observer: &'a RaftCells,
    pub cut: &'a ReturnedCut,
    pub basis: &'a basis::Basis,
    pub local: Option<&'a LocalResume>,
}

pub(super) struct Observed {
    pub inventory: Box<RaftPhysicalInventory>,
    pub mode: ResumeMode,
    pub sampled: u64,
}

impl Context<'_> {
    pub async fn observe(&self, deadline: &Deadline<'_>) -> Result<Observed> {
        self.catalog
            .run(
                deadline.remaining()?,
                self.basis.check_current(self.catalog),
            )
            .await?;
        let target = NodeId::new(self.cut.node.node)
            .map_err(|_| FleetCatalogError::Corrupt("resume physical node"))?;
        let started = proof::now(self.catalog)?;
        let observed = self
            .observer
            .inspect_peer_physical_inventory(target, self.basis.revision, deadline.remaining()?)
            .await
            .map_err(FleetCatalogError::Observation)?;
        let sampled = proof::now(self.catalog)?
            .checked_sub(observed.sample_age_ms)
            .ok_or(FleetCatalogError::Unconfirmed)?;
        let mode = if observed.view.node.profile.draining {
            ResumeMode::Paused
        } else {
            ResumeMode::Resumed
        };
        let selector = Intent::selector(self.request, self.cut)?;
        if mode == ResumeMode::Resumed {
            self.catalog
                .run(deadline.remaining()?, async {
                    let intent = self
                        .catalog
                        .current_resume_intent(self.snapshot, self.basis.operation)
                        .await?;
                    if !intent.request.same_scope(self.request)
                        || intent.effect_selector()? != selector
                    {
                        return Err(FleetCatalogError::PayloadConflict);
                    }
                    Ok(())
                })
                .await?;
        }
        if let Some(local) = self.local {
            local.check(&self.cut.quiescence, &selector, mode)?;
        } else {
            let action = if mode == ResumeMode::Paused {
                FleetStopAction::ObserveQuiescence
            } else {
                FleetStopAction::ObserveResume
            };
            let request = FleetStopRequest::new(
                if mode == ResumeMode::Paused {
                    self.cut.quiescence.clone()
                } else {
                    selector.clone()
                },
                action,
            )
            .map_err(|_| FleetCatalogError::Corrupt("resume mode request"))?;
            let ack = self
                .observer
                .request_peer_fleet_stop(
                    request.clone(),
                    self.basis.revision,
                    deadline.remaining()?,
                )
                .await?;
            if ack.action() != action
                || ack.operation() != selector.operation
                || ack.process() != selector.boot.process
                || ack.request() != request.request()
            {
                return Err(FleetCatalogError::Unconfirmed);
            }
        }
        let inventory = observed
            .inventory
            .ok_or(FleetCatalogError::InventoryIncomplete)?;
        self.catalog
            .run(deadline.remaining()?, async {
                self.basis.check_current(self.catalog).await?;
                let auth = self
                    .catalog
                    .host_record(self.snapshot, self.cut.node.clone())?;
                proof::node_with_admission(
                    self.catalog,
                    self.snapshot,
                    &observed.view,
                    &auth,
                    FleetNodeState::RestartPending,
                    mode == ResumeMode::Paused,
                )?;
                super::super::super::super::capacity::check_restart_capacity_view(
                    &auth,
                    &self.request.attestation,
                    &observed.view,
                    mode == ResumeMode::Paused,
                )?;
                if observed.view.seed_workers != 0
                    || observed.view.pending_history_registrations != 0
                    || inventory.initializing().is_some()
                    || inventory.histories().len() as u64
                        > u64::from(self.request.attestation.engine_slots)
                {
                    return Err(FleetCatalogError::InventoryIncomplete);
                }
                inventory
                    .validate(&observed.view)
                    .map_err(|_| FleetCatalogError::Conflict)?;
                self.basis.check_current(self.catalog).await
            })
            .await?;
        proof::fresh(self.catalog, started, sampled)?;
        if let Some(local) = self.local {
            local.check(&self.cut.quiescence, &selector, mode)?;
        }
        deadline.remaining()?;
        Ok(Observed {
            inventory,
            mode,
            sampled,
        })
    }
}
