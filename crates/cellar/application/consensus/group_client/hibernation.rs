// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{startup, *};
use crate::raft::{
    catalog::CatalogError,
    completion::RuntimeError,
    fleet_catalog::{FleetCatalogError, ManagedParkPermit},
    transport::ParkDeliveryReceipt,
};

impl RaftGroupHandle {
    pub(crate) async fn observe_park_admission(
        &self,
        timeout: Duration,
    ) -> Result<CellGroupQuorumMembership, CellGroupControllerError> {
        let (deadline, _memory) = self
            .owner
            .metadata_admission(timeout, 4096)
            .map_err(startup::admission_error)?;
        Ok(self
            .owner
            .handle
            .core
            .runtime
            .park_admission(self.history, deadline)
            .await?)
    }

    pub(crate) async fn execute_managed_park(
        &self,
        permit: &ManagedParkPermit,
        timeout: Duration,
    ) -> Result<ParkDeliveryReceipt, CellGroupControllerError> {
        let (deadline, _memory) = self
            .owner
            .metadata_admission(timeout, 64 * 1024)
            .map_err(startup::admission_error)?;
        let owner = self.owner.internal_observer();
        owner
            .handle
            .domain
            .run(Box::pin(crate::asyncrt::timeout_at(
                deadline,
                Box::pin(owner.deliver_managed_park(self.history, permit, deadline)),
            )))
            .await
            .map_err(|_| CellGroupControllerError::Closed)?
            .map_err(|_| CellGroupControllerError::Runtime(RuntimeError::DeadlineExceeded))?
    }

    pub(crate) async fn wake_managed_park(
        &self,
        permit: &ManagedParkPermit,
        unsettled: bool,
        timeout: Duration,
    ) -> Result<CellGroupQuorumMembership, CellGroupControllerError> {
        let (deadline, _memory) = self
            .owner
            .metadata_admission(timeout, 4096)
            .map_err(startup::admission_error)?;
        let core = &self.owner.handle.core;
        let host = core
            .managed
            .as_ref()
            .ok_or_else(|| startup::catalog_error(CatalogError::ManagedAdmissionRequired))?;
        self.owner.startup_live(host, deadline)?;
        let boot = host.boot_binding().map_err(park_catalog_error)?;
        if !permit.matches_fleet(self.history, &boot) {
            return Err(RuntimeError::StaleCompletion.into());
        }
        let generation = permit.generation().map_err(park_catalog_error)?;
        if unsettled {
            let term = permit.term();
            let domain = &self.owner.handle.domain;
            domain
                .run(Box::pin(crate::asyncrt::timeout_at(
                    deadline,
                    Box::pin(async {
                        loop {
                            self.owner.startup_live(host, deadline)?;
                            match core
                                .runtime
                                .fence_managed_park(self.history, generation, term, deadline)
                                .await
                            {
                                Ok(()) => return Ok(()),
                                Err(RuntimeError::NotReady | RuntimeError::Overloaded) => (),
                                Err(error) => return Err(CellGroupControllerError::from(error)),
                            }
                            let now = domain
                                .mono_ms()
                                .map_err(|_| CellGroupControllerError::Closed)?;
                            domain
                                .sleep_until(deadline.min(now.saturating_add(25)))
                                .map_err(|_| CellGroupControllerError::Closed)?
                                .await;
                        }
                    }),
                )))
                .await
                .map_err(|_| CellGroupControllerError::Closed)?
                .map_err(|_| CellGroupControllerError::Runtime(RuntimeError::DeadlineExceeded))??;
        }
        let observation = core
            .runtime
            .wake_park(self.history, generation, deadline)
            .await?;
        self.owner.startup_live(host, deadline)?;
        Ok(observation)
    }
}

impl RaftCells {
    async fn deliver_managed_park(
        &self,
        history: GroupHistory,
        permit: &ManagedParkPermit,
        deadline: u64,
    ) -> Result<ParkDeliveryReceipt, CellGroupControllerError> {
        let core = &self.handle.core;
        let host = core
            .managed
            .as_ref()
            .ok_or_else(|| startup::catalog_error(CatalogError::ManagedAdmissionRequired))?;
        self.startup_live(host, deadline)?;
        let boot = host.boot_binding().map_err(park_catalog_error)?;
        let group = core.log.cell_group(&history).map_err(RuntimeError::from)?;
        if !permit.matches_owner(history, &boot, group.identity().member) {
            return Err(RuntimeError::StaleCompletion.into());
        }
        let current = host
            .stop_catalog()
            .reopen_managed_park(
                permit.operation().map_err(park_catalog_error)?,
                self.startup_remaining(deadline)?,
            )
            .await
            .map_err(park_catalog_error)?;
        if !current.is_some_and(|current| permit.same_reservation(&current)) {
            return Err(RuntimeError::StaleCompletion.into());
        }
        self.startup_live(host, deadline)?;

        let domain = &self.handle.domain;
        let now = domain
            .mono_ms()
            .map_err(|_| CellGroupControllerError::Closed)?;
        let mut end = deadline.min(now.saturating_add(MAX_PARK_NEGOTIATION_MS));
        let request = permit.prepare_request(end).map_err(park_catalog_error)?;
        let generation = permit.generation().map_err(park_catalog_error)?;
        let cut = loop {
            self.startup_live(host, end)?;
            let (marker, parked) = {
                let cache = group.read().map_err(RuntimeError::from)?;
                (
                    cache.group_manifest.clone().ok_or(RuntimeError::NotReady)?,
                    cache
                        .local_park
                        .as_ref()
                        .is_some_and(LocalParkRecord::parked),
                )
            };
            let applied = marker.position();
            if !marker.is_active() || applied.index.get() > generation.get() {
                return Err(RuntimeError::StaleCompletion.into());
            }
            if parked {
                let cut = marker.park_cut().filter(|cut| {
                    cut.position() == applied
                        && cut.generation() == generation.get()
                        && cut.position().term == request.term
                        && cut.membership_revision() == request.membership.get()
                });
                break cut.cloned().ok_or(RuntimeError::StaleCompletion)?;
            }
            match core.runtime.prepare_park(history, request).await {
                Ok(prepared) => {
                    end = end.min(prepared.deadline_ms);
                    if let Some(cut) = marker.park_cut().filter(|cut| {
                        cut.position() == applied && cut.position() == prepared.proposal.position
                    }) {
                        break cut.clone();
                    }
                }
                Err(RuntimeError::NotReady | RuntimeError::Overloaded) => (),
                Err(error) => return Err(error.into()),
            }
            let now = domain
                .mono_ms()
                .map_err(|_| CellGroupControllerError::Closed)?;
            domain
                .sleep_until(end.min(now.saturating_add(25)))
                .map_err(|_| CellGroupControllerError::Closed)?
                .await;
        };
        let selected = core
            .transport
            .collect_park_certificate(history, cut, end)
            .await
            .map_err(CellGroupControllerError::Transport)?;
        self.startup_live(host, end)?;
        core.transport
            .deliver_park_certificate(history, &selected, end)
            .await
            .map_err(CellGroupControllerError::Transport)
    }
}

fn park_catalog_error(error: FleetCatalogError) -> CellGroupControllerError {
    startup::catalog_error(CatalogError::from(error))
}
