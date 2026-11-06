// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Reopen the published recovery voter's own synchronized page and image stores.
use super::*;
use crate::raft::fleet_catalog::{FleetHostAuthorization, FleetRecoveryAttachRequest};
use cellar_logic::cell_group::GroupHistory;

impl CellGroupController {
    pub(crate) async fn attach_managed_recovery_voter(
        &self,
        directory: &RangeDirectory,
        request: &FleetRecoveryAttachRequest,
        history: GroupHistory,
        host: &FleetHostAuthorization,
        transport: &RaftTransport,
        control: (u64, &tokio::sync::watch::Receiver<bool>),
    ) -> Result<(), CellGroupControllerError> {
        let (deadline, stop) = control;
        let shared = &self.handle.0;
        let check_live = || {
            if *stop.borrow() || shared.closing.load(Ordering::Acquire) {
                return Err(CellGroupControllerError::Closed);
            }
            check_delivery_deadline(&Some((shared.runtime.log().domain(), deadline)))
        };
        check_live()?;
        let config = shared
            .provisioning
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?
            .clone()
            .ok_or(CellGroupControllerError::Invalid(
                "recovery provisioning owner",
            ))?;
        let images = config.images(directory)?;
        let permit = directory
            .bootstrap_managed_pending_recovery_voter(
                request,
                history,
                shared.runtime.log(),
                host,
                deadline,
            )
            .await
            .map_err(catalog_error)?;
        check_live()?;
        if shared.groups.contains(&history)? {
            return Ok(());
        }
        let _preparation = config
            .preparations
            .acquire(config.bytes)
            .ok_or(RuntimeError::Overloaded)?;
        let _parallel = config
            .parallel
            .clone()
            .try_acquire_owned()
            .map_err(|_| RuntimeError::Overloaded)?;
        let physical = prepare::transport_route(
            &config,
            transport,
            permit.route(),
            permit.initial_voters().iter(),
            true,
        )?;
        let prepared = prepare::recover_pages(
            &config,
            &shared.runtime,
            permit.group().clone(),
            images,
            None,
        )
        .await?;
        check_live()?;
        self.register(CellGroupRegistration {
            permit,
            current: prepared.current,
            applier: prepared.applier,
            transport: physical,
        })
        .map_err(|error| error.error)?
        .response()
        .await
        .map_err(|error| error.as_ref().clone())?;
        Ok(())
    }
}
