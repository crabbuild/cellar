// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Prepare exactly one managed child in the destination's retained stores.
use super::*;
use crate::raft::{
    cell_groups::{
        CellGroupApplyImages, CellGroupArchiveCoverage, CellGroupArchiveStore,
        CellGroupRecoveryPool, CellGroupTransferTarget, InstalledTransferVoter, ManifestPageStore,
        PreparedCellGroupTransferChild, PreparedRecoveryData, TransitionPhase,
        VerifiedCellGroupTransitionSources,
    },
    fleet_catalog::{FleetHostAuthorization, ReopenedManagedCellGroupTransition},
};
use std::time::Duration;

impl Config {
    async fn open_import_target(
        self: &Arc<Self>,
        log: &crate::raft::storage::ReplicaLog,
        images: CellGroupApplyImages,
        route: &GroupRoute,
        spools: &'static str,
    ) -> Result<CellGroupTransferTarget, CellGroupControllerError> {
        let domain = log.domain();
        let target_route = route.clone();
        let scratch = log.snapshot_scratch();
        let staging = self.clone();
        domain
            .blocking(move || {
                let settings = &staging.settings;
                let directory = settings.directory.join(spools);
                cellar_ltx::host::create_dir_all_durable(settings.filesystem.as_ref(), &directory)
                    .map_err(crate::raft::execution::storage)?;
                let path =
                    ManifestPageStore::group_pages_path(&settings.directory, target_route.history);
                let pages = ManifestPageStore::open(
                    &path,
                    settings.filesystem.clone(),
                    settings.max_pages,
                    settings.max_page_bytes,
                    settings.max_retained_roots,
                )
                .map_err(crate::raft::execution::storage)?;
                let store = CellGroupArchiveStore::new(
                    target_route,
                    pages,
                    images,
                    settings.manifest,
                    settings.entries,
                    settings.max_effect_rows,
                )
                .map_err(crate::raft::execution::storage)?;
                Ok::<_, ExecutionError>(CellGroupTransferTarget {
                    store,
                    scratch,
                    directory,
                })
            })
            .map_err(|_| CellGroupControllerError::Closed)?
            .await
            .map_err(|_| CellGroupControllerError::Closed)?
            .map_err(apply_error)
    }

    fn retain_synced_transfer(
        &self,
        log: &crate::raft::storage::ReplicaLog,
        child: &PreparedCellGroupTransferChild,
    ) -> Result<(), CellGroupControllerError> {
        let history = child.route().history;
        let group = log.cell_group(&history).map_err(RuntimeError::from)?;
        let applier = child.installed_applier_on(&group)?;
        self.parked
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?
            .insert(history, PendingGroupOwner::Synced(Arc::new(applier)));
        Ok(())
    }
}

impl CellGroupController {
    pub(crate) async fn prepare_managed_recovery_seed(
        &self,
        directory: &RangeDirectory,
        route: &GroupRoute,
        source: CellGroupArchiveCoverage,
        identity: ([u8; 32], cellar_logic::consensus::ProposalId),
        recovery: &CellGroupRecoveryPool,
    ) -> Result<PreparedRecoveryData, CellGroupControllerError> {
        let shared = &self.handle.0;
        let config = shared
            .provisioning
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?
            .clone()
            .ok_or(CellGroupControllerError::Invalid(
                "recovery provisioning owner",
            ))?;
        let images = config.images(directory)?;
        let _preparation = config
            .preparations
            .acquire(config.bytes)
            .ok_or(RuntimeError::Overloaded)?;
        let _parallel = config
            .parallel
            .clone()
            .try_acquire_owned()
            .map_err(|_| RuntimeError::Overloaded)?;
        let log = shared.runtime.log();
        let target = config
            .open_import_target(log, images, route, "recovery-spools")
            .await?;
        Box::pin(recovery.prepare_cut(&log.domain(), source, target, identity.0, identity.1))
            .await
            .map_err(|error| CellGroupControllerError::Archive(Arc::new(error.into())))
    }

    pub(crate) async fn attach_managed_transfer_child(
        &self,
        directory: &RangeDirectory,
        transition: Arc<ReopenedManagedCellGroupTransition>,
        route: &GroupRoute,
        host: &FleetHostAuthorization,
        transport: &RaftTransport,
        control: (u64, &tokio::sync::watch::Receiver<bool>),
    ) -> Result<(), CellGroupControllerError> {
        let (deadline, stop) = control;
        let shared = &self.handle.0;
        let log = shared.runtime.log();
        let config = shared
            .provisioning
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?
            .clone()
            .ok_or(CellGroupControllerError::Invalid(
                "transfer provisioning owner",
            ))?;
        config.images(directory)?;
        let permit = directory
            .bootstrap_managed_pending_transfer_voter(transition, route, log, host, deadline)
            .await
            .map_err(catalog_error)?;
        if shared.groups.contains(&route.history)? {
            return Ok(());
        }
        let physical = prepare::transport_route(
            &config,
            transport,
            route,
            permit.initial_voters().iter(),
            true,
        )?;
        let owner = config
            .parked
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?
            .get(&route.history)
            .cloned()
            .ok_or_else(|| catalog_error(CatalogError::LostStableState))?;
        let prepared = match owner {
            PendingGroupOwner::Recovered(owner) => owner.as_ref().clone(),
            PendingGroupOwner::Synced(applier) => {
                recover_pending_owner(log, applier.as_ref().clone()).await?
            }
            PendingGroupOwner::Seeded(child) => {
                recover_pending_owner(log, child.installed_applier(&permit)?).await?
            }
        };
        if *stop.borrow() || shared.closing.load(Ordering::Acquire) {
            return Err(CellGroupControllerError::Closed);
        }
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
        // The roster owns this page store after registration joins. Release
        // the separate genesis reference so it cannot pin superseded images.
        config
            .parked
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?
            .remove(&route.history);
        Ok(())
    }

    pub(crate) async fn install_managed_transfer_child(
        &self,
        directory: &RangeDirectory,
        transfer: (
            &ReopenedManagedCellGroupTransition,
            &VerifiedCellGroupTransitionSources,
        ),
        route: &GroupRoute,
        host: &FleetHostAuthorization,
        recovery: &CellGroupRecoveryPool,
        control: (u64, &tokio::sync::watch::Receiver<bool>),
    ) -> Result<InstalledTransferVoter, CellGroupControllerError> {
        let (transition, sources) = transfer;
        let (deadline, stop) = control;
        let shared = &self.handle.0;
        let log = shared.runtime.log();
        let domain = log.domain();
        let remaining = || {
            if *stop.borrow() || shared.closing.load(Ordering::Acquire) {
                return Err(CellGroupControllerError::Closed);
            }
            let now = domain
                .mono_ms()
                .map_err(|_| CellGroupControllerError::Closed)?;
            deadline
                .checked_sub(now)
                .filter(|ms| *ms > 0)
                .map(Duration::from_millis)
                .ok_or_else(|| RuntimeError::DeadlineExceeded.into())
        };
        let config = shared
            .provisioning
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?
            .clone()
            .ok_or(CellGroupControllerError::Invalid(
                "transfer provisioning owner",
            ))?;
        let images = config.images(directory)?;
        let _preparation = config
            .preparations
            .acquire(config.bytes)
            .ok_or(RuntimeError::Overloaded)?;
        let _parallel = config
            .parallel
            .clone()
            .try_acquire_owned()
            .map_err(|_| RuntimeError::Overloaded)?;
        let authority = host
            .managed_transfer_install_authority(transition, directory, route, remaining()?)
            .await
            .map_err(CatalogError::from)
            .map_err(catalog_error)?;
        let selected = directory
            .managed_transition_status(&transition.plan, &authority.transition)
            .await
            .map_err(catalog_error)?
            .ok_or_else(|| catalog_error(CatalogError::Fenced))?;
        if selected.phase() == TransitionPhase::Retired
            || selected.sealed_sources() != sources.journal().sealed_sources()
        {
            return Err(catalog_error(CatalogError::Fenced));
        }
        let previous = config
            .parked
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?
            .get(&route.history)
            .cloned();
        let recovered = match &previous {
            Some(PendingGroupOwner::Recovered(owner)) => Some(&owner.current),
            _ => None,
        };
        if let Some(receipt) = directory
            .reconcile_managed_transfer_voter(transition, route, log, host, deadline, recovered)
            .await
            .map_err(catalog_error)?
        {
            if let Some(PendingGroupOwner::Seeded(prepared)) = &previous {
                config.retain_synced_transfer(log, prepared)?;
            }
            return Ok(receipt);
        }
        match previous {
            Some(PendingGroupOwner::Recovered(_) | PendingGroupOwner::Synced(_)) => {
                return Err(catalog_error(CatalogError::LostStableState))
            }
            Some(PendingGroupOwner::Seeded(prepared)) => {
                let result = directory
                    .install_managed_transfer_voter(transition, &prepared, log, host, remaining()?)
                    .await
                    .map_err(catalog_error);
                let owner = config.retain_synced_transfer(log, &prepared);
                let receipt = result?;
                owner?;
                return Ok(receipt);
            }
            None => (),
        }
        if config
            .parked
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?
            .len()
            >= shared.limits.max_groups
        {
            return Err(RuntimeError::Overloaded.into());
        }
        remaining()?;
        let target = config
            .open_import_target(log, images, route, "transfer-spools")
            .await?;
        remaining()?;
        let prepared = recovery
            .prepare_transfer_child(&domain, sources, target)
            .await
            .map_err(|error| CellGroupControllerError::Archive(Arc::new(error.into())))?;
        let remaining = remaining()?;
        let prepared = Arc::new(prepared);
        // Retain the exact page owner before the engine can enter sync. A lost
        // receipt or timeout can follow the durable mapping.
        config
            .parked
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?
            .insert(route.history, PendingGroupOwner::Seeded(prepared.clone()));
        let result = directory
            .install_managed_transfer_voter(transition, &prepared, log, host, remaining)
            .await
            .map_err(catalog_error);
        // A lost receipt can follow durable sync. Keep the applier but release
        // the sealed-source preparation's admission when a mapping exists.
        let owner = config.retain_synced_transfer(log, &prepared);
        let receipt = result?;
        owner?;
        Ok(receipt)
    }
}
