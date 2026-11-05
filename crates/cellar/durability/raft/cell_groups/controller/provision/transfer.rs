// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Prepare exactly one managed child in the destination's retained stores.
use super::*;
use crate::raft::{
    cell_groups::{
        CellGroupArchiveStore, CellGroupRecoveryPool, CellGroupTransferTarget,
        InstalledTransferVoter, ManifestPageStore, TransitionPhase,
        VerifiedCellGroupTransitionSources,
    },
    fleet_catalog::{FleetHostAuthorization, ReopenedManagedCellGroupTransition},
};
use std::time::Duration;

impl CellGroupController {
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
        match directory
            .reconcile_managed_transfer_voter(transition, route, log, host, deadline, None)
            .await
        {
            Ok(Some(receipt)) => return Ok(receipt),
            Ok(None) => (),
            Err(CatalogError::Runtime(RuntimeError::NotReady)) => {
                remaining()?;
                let group = log.cell_group(&route.history).map_err(RuntimeError::from)?;
                let recovered =
                    prepare::recover_pages(&config, &shared.runtime, group, images, None).await?;
                remaining()?;
                return directory
                    .reconcile_managed_transfer_voter(
                        transition,
                        route,
                        log,
                        host,
                        deadline,
                        Some(&recovered.current),
                    )
                    .await
                    .map_err(catalog_error)?
                    .ok_or_else(|| catalog_error(CatalogError::LostStableState));
            }
            Err(error) => return Err(catalog_error(error)),
        }
        remaining()?;
        let target_route = route.clone();
        let scratch = log.snapshot_scratch();
        let target = domain
            .blocking(move || {
                let settings = &config.settings;
                let directory = settings.directory.join("transfer-spools");
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
            .map_err(apply_error)?;
        remaining()?;
        let prepared = recovery
            .prepare_transfer_child(&domain, sources, target)
            .await
            .map_err(|error| CellGroupControllerError::Archive(Arc::new(error.into())))?;
        directory
            .install_managed_transfer_voter(transition, &prepared, log, host, remaining()?)
            .await
            .map_err(catalog_error)
    }
}
