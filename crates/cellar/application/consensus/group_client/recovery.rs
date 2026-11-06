// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Node-owned managed recovery seeding and attachment. Shutdown joins work even if
//! the operator stopped waiting for the exact durable receipt.
use super::*;
use crate::{
    management::RaftFleetPreparedRecoveryRange,
    raft::{
        catalog::{CatalogError, CellGroupRecoveryError, ManagedRecoveryVoterSeed},
        completion::RuntimeError,
        fleet_catalog::{
            FleetFencedTransitionRecovery, FleetRecoveryAttachRequest,
            FleetRecoveryRangeReservation, FleetTransitionRecoveryReplacement,
            ManagedRecoverySeedSelection,
        },
    },
};
use cellar_logic::consensus::LogPosition;

impl RaftGroupHandle {
    /// Submit or reconcile this range's exact managed recovery activation.
    /// Returns its applied position once the durable receipt is confirmed.
    /// A pending proposal returns None. Serving requires separate settlement.
    pub fn activate_managed_recovery(
        &self,
        request: FleetRecoveryAttachRequest,
        timeout: Duration,
    ) -> impl Future<Output = Result<Option<LogPosition>, CellGroupControllerError>> + Send + 'static
    {
        use super::startup::{admission_error, catalog_error};
        let admitted = self
            .owner
            .metadata_admission(timeout, 4096)
            .map_err(admission_error);
        let owner = self.owner.internal_observer();
        let history = self.history;
        async move {
            let (deadline, _memory) = admitted?;
            let host = owner
                .handle
                .core
                .managed
                .as_ref()
                .ok_or_else(|| catalog_error(CatalogError::ManagedAdmissionRequired))?;
            owner.startup_live(host, deadline)?;
            let lease = owner
                .register_control_service(ControlServiceKind::Recovery, 4096)
                .map_err(CatalogError::from)
                .map_err(catalog_error)?;
            let (reply, result) = tokio::sync::oneshot::channel();
            let domain = owner.handle.domain.clone();
            domain
                .spawn_cleanup(async move {
                    let stop = lease.stop_receiver();
                    let activated = Box::pin(
                        owner.run_managed_recovery_activation(&request, history, deadline, &stop),
                    )
                    .await;
                    drop(lease);
                    let _ = reply.send(activated);
                })
                .map_err(|_| CellGroupControllerError::Closed)?
                .detach();
            domain
                .run(crate::asyncrt::timeout_at(deadline, result))
                .await
                .map_err(|_| CellGroupControllerError::Closed)?
                .map_err(|_| CellGroupControllerError::Runtime(RuntimeError::DeadlineExceeded))?
                .map_err(|_| CellGroupControllerError::Closed)?
        }
    }
}

impl RaftCells {
    /// Open this boot's configured page and image stores and copy the exact
    /// retained archive into them. The returned preparation is engine-bound.
    #[doc(hidden)]
    pub async fn prepare_managed_recovery_seed(
        &self,
        fenced: &FleetFencedTransitionRecovery,
        plan: &FleetTransitionRecoveryReplacement,
        reservation: &FleetRecoveryRangeReservation,
        recovery: &CellGroupRecoveryPool,
        timeout: Duration,
    ) -> Result<RaftFleetPreparedRecoveryRange, CellGroupControllerError> {
        use super::startup::{admission_error, catalog_error};
        let (deadline, _memory) = self
            .metadata_admission(timeout, 4096)
            .map_err(admission_error)?;
        let core = &self.handle.core;
        let host = core
            .managed
            .as_ref()
            .ok_or_else(|| catalog_error(CatalogError::ManagedAdmissionRequired))?;
        let archives = core
            .archive_io
            .as_ref()
            .ok_or(CellGroupControllerError::Invalid("recovery archive owner"))?;
        self.startup_live(host, deadline)?;
        let lease = self
            .register_control_service(ControlServiceKind::Recovery, 4096)
            .map_err(CatalogError::from)
            .map_err(catalog_error)?;
        let stop = lease.stop_receiver();
        let route = reservation.route();
        let index = plan
            .births()
            .iter()
            .position(|birth| birth.route() == route)
            .ok_or_else(|| catalog_error(CatalogError::Fenced))?;
        let source = fenced
            .archives()
            .get(index)
            .ok_or_else(|| catalog_error(CatalogError::Fenced))?;
        let directory = core
            .host
            .namespace_directory(route.history.cluster, route.history.namespace)
            .map_err(|_| CellGroupControllerError::Invalid("recovery seed namespace"))?;
        let selected = ManagedRecoverySeedSelection {
            fenced,
            plan,
            reservation,
        };
        let (view, inventory) = self.capture_physical_node().map_err(admission_error)?;
        Box::pin(host.authorize_managed_recovery_seed(
            &directory,
            selected,
            (&view, inventory.group_history(&route.history).is_some()),
            archives,
            self.recovery_seed_remaining(host, deadline, &stop)?,
        ))
        .await
        .map_err(CatalogError::from)
        .map_err(catalog_error)?;
        let domain = &self.handle.domain;
        let work = Box::pin(core.controller.prepare_managed_recovery_seed(
            &directory,
            route,
            source.clone(),
            (plan.digest(), plan.mutation().operation()),
            recovery,
        ));
        let data = domain
            .run(Box::pin(crate::asyncrt::timeout_at(deadline, work)))
            .await
            .map_err(|_| CellGroupControllerError::Closed)?
            .map_err(|_| CellGroupControllerError::Runtime(RuntimeError::DeadlineExceeded))??;
        self.recovery_seed_remaining(host, deadline, &stop)?;
        Ok(RaftFleetPreparedRecoveryRange::for_node(
            plan.digest(),
            data,
            core.engine,
        ))
    }

    /// Sync this reserved boot's copy of the prepared recovery genesis. An
    /// exact retry can reopen the same durable receipt. Serving is gated by
    /// separate directory publication and quorum activation.
    #[doc(hidden)]
    pub fn seed_managed_recovery_range(
        &self,
        fenced: Arc<FleetFencedTransitionRecovery>,
        plan: Arc<FleetTransitionRecoveryReplacement>,
        reservation: Arc<FleetRecoveryRangeReservation>,
        prepared: RaftFleetPreparedRecoveryRange,
        timeout: Duration,
    ) -> impl Future<Output = Result<ManagedRecoveryVoterSeed, CellGroupControllerError>> + Send + 'static
    {
        let admitted = self
            .metadata_admission(timeout, 4096)
            .map_err(super::startup::admission_error);
        let owner = self.internal_observer();
        async move {
            let (deadline, _memory) = admitted?;
            let core = &owner.handle.core;
            let host = core.managed.as_ref().ok_or_else(|| {
                super::startup::catalog_error(CatalogError::ManagedAdmissionRequired)
            })?;
            owner.startup_live(host, deadline)?;
            let lease = owner
                .register_control_service(ControlServiceKind::Recovery, 4096)
                .map_err(CatalogError::from)
                .map_err(super::startup::catalog_error)?;
            let (reply, result) = tokio::sync::oneshot::channel();
            let domain = owner.handle.domain.clone();
            let worker_domain = domain.clone();
            domain
                .spawn_cleanup(async move {
                    let selected = ManagedRecoverySeedSelection {
                        fenced: &fenced,
                        plan: &plan,
                        reservation: &reservation,
                    };
                    let stop = lease.stop_receiver();
                    let seeded = Box::pin(worker_domain.run(Box::pin(
                        owner.run_managed_recovery_seed(selected, prepared, deadline, &stop),
                    )))
                    .await
                    .unwrap_or(Err(CellGroupControllerError::Closed));
                    drop(lease);
                    let _ = reply.send(seeded);
                })
                .map_err(|_| CellGroupControllerError::Closed)?
                .detach();
            domain
                .run(crate::asyncrt::timeout_at(deadline, result))
                .await
                .map_err(|_| CellGroupControllerError::Closed)?
                .map_err(|_| CellGroupControllerError::Runtime(RuntimeError::DeadlineExceeded))?
                .map_err(|_| CellGroupControllerError::Closed)?
        }
    }

    /// Attach this voter to a published recovery range using its retained seed.
    /// A successful attachment can participate in Raft. Application admission
    /// requires a separate committed activation.
    pub fn attach_managed_recovery_voter(
        &self,
        request: FleetRecoveryAttachRequest,
        history: GroupHistory,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), CellGroupControllerError>> + Send + 'static {
        use super::startup::{admission_error, catalog_error};
        let admitted = self
            .metadata_admission(timeout, 4096)
            .map_err(admission_error);
        let owner = self.internal_observer();
        async move {
            let (deadline, _memory) = admitted?;
            let core = &owner.handle.core;
            let host = core
                .managed
                .as_ref()
                .ok_or_else(|| catalog_error(CatalogError::ManagedAdmissionRequired))?;
            owner.startup_live(host, deadline)?;
            let directory = core
                .host
                .namespace_directory(history.cluster, history.namespace)
                .map_err(|_| CellGroupControllerError::Invalid("recovery seed namespace"))?;
            let lease = owner
                .register_control_service(ControlServiceKind::Recovery, 4096)
                .map_err(CatalogError::from)
                .map_err(catalog_error)?;
            let (reply, result) = tokio::sync::oneshot::channel();
            let controller = core.controller.clone();
            let transport = core.transport.clone();
            let host = host.clone();
            let domain = &owner.handle.domain;
            domain
                .spawn_cleanup(async move {
                    let stop = lease.stop_receiver();
                    let attached = Box::pin(controller.attach_managed_recovery_voter(
                        &directory,
                        &request,
                        history,
                        &host,
                        &transport,
                        (deadline, &stop),
                    ))
                    .await;
                    drop(lease);
                    let _ = reply.send(attached);
                })
                .map_err(|_| CellGroupControllerError::Closed)?
                .detach();
            domain
                .run(crate::asyncrt::timeout_at(deadline, result))
                .await
                .map_err(|_| CellGroupControllerError::Closed)?
                .map_err(|_| CellGroupControllerError::Runtime(RuntimeError::DeadlineExceeded))?
                .map_err(|_| CellGroupControllerError::Closed)?
        }
    }

    async fn run_managed_recovery_activation(
        &self,
        request: &FleetRecoveryAttachRequest,
        history: GroupHistory,
        deadline: u64,
        stop: &tokio::sync::watch::Receiver<bool>,
    ) -> Result<Option<LogPosition>, CellGroupControllerError> {
        use super::startup::catalog_error;
        let core = &self.handle.core;
        let host = core
            .managed
            .as_ref()
            .ok_or_else(|| catalog_error(CatalogError::ManagedAdmissionRequired))?;
        let directory = core
            .host
            .namespace_directory(history.cluster, history.namespace)
            .map_err(|_| CellGroupControllerError::Invalid("recovery activation namespace"))?;
        let remaining = || self.recovery_seed_remaining(host, deadline, stop);
        remaining()?;
        let prepare = || {
            directory
                .prepare_managed_recovery_activation(request, history, &core.log, host, deadline)
        };
        let permit = prepare().await.map_err(catalog_error)?;
        remaining()?;
        let applied = match core.runtime.activate_recovery(permit).await? {
            CellGroupRecoveryActivation::Accepted(_) => return Ok(None),
            CellGroupRecoveryActivation::Applied(applied) => applied,
        };
        remaining()?;
        let confirmed = prepare().await.map_err(catalog_error)?;
        remaining()?;
        directory
            .record_managed_recovery_activation(&confirmed, &applied, &core.log)
            .await
            .map(Some)
            .map_err(catalog_error)
    }

    async fn run_managed_recovery_seed(
        &self,
        selected: ManagedRecoverySeedSelection<'_>,
        prepared: RaftFleetPreparedRecoveryRange,
        deadline: u64,
        stop: &tokio::sync::watch::Receiver<bool>,
    ) -> Result<ManagedRecoveryVoterSeed, CellGroupControllerError> {
        use super::startup::{admission_error, catalog_error};
        let core = &self.handle.core;
        let host = core
            .managed
            .as_ref()
            .ok_or_else(|| catalog_error(CatalogError::ManagedAdmissionRequired))?;
        let archives = core
            .archive_io
            .as_ref()
            .ok_or(CellGroupControllerError::Invalid("recovery archive owner"))?;
        let route = selected.reservation.route();
        let target = selected
            .plan
            .births()
            .iter()
            .find(|birth| birth.route() == route)
            .ok_or_else(|| catalog_error(CatalogError::Fenced))?;
        if prepared.plan_digest() != selected.plan.digest()
            || prepared.engine() != Some(core.engine)
            || prepared.manifest().marker().route() != route
        {
            return Err(catalog_error(CatalogError::Fenced));
        }
        let directory = core
            .host
            .namespace_directory(route.history.cluster, route.history.namespace)
            .map_err(|_| CellGroupControllerError::Invalid("recovery seed namespace"))?;
        let remaining = || self.recovery_seed_remaining(host, deadline, stop);
        let (view, inventory) = self.capture_physical_node().map_err(admission_error)?;
        let already_seeded = inventory.group_history(&route.history).is_some();
        if prepared.data().image_bytes().map_err(archive_error)? > view.node.profile.bytes_per_group
        {
            return Err(RuntimeError::Overloaded.into());
        }
        let authority = Box::pin(host.authorize_managed_recovery_seed(
            &directory,
            selected,
            (&view, already_seeded),
            archives,
            remaining()?,
        ))
        .await
        .map_err(CatalogError::from)
        .map_err(catalog_error)?;
        remaining()?;
        let (pending, applier) = Box::pin(directory.seed_managed_recovery_voter(
            &self.handle.domain,
            target,
            prepared.data().clone(),
            authority,
            &core.log,
            deadline,
        ))
        .await
        .map_err(|error| match error {
            CellGroupRecoveryError::Catalog(error) => catalog_error(error),
            CellGroupRecoveryError::Archive(error) => archive_error(error),
        })?;
        core.controller
            .retain_managed_recovery_seed_owner(applier)?;
        let (view, inventory) = self.capture_physical_node().map_err(admission_error)?;
        if inventory.group_history(&route.history).is_none() {
            return Err(catalog_error(CatalogError::LostStableState));
        }
        let authority = Box::pin(host.authorize_managed_recovery_seed(
            &directory,
            selected,
            (&view, true),
            archives,
            remaining()?,
        ))
        .await
        .map_err(CatalogError::from)
        .map_err(catalog_error)?;
        remaining()?;
        directory
            .confirm_managed_recovery_voter(target, pending, &authority)
            .await
            .map_err(catalog_error)
    }

    fn recovery_seed_remaining(
        &self,
        host: &crate::raft::fleet_catalog::FleetHostAuthorization,
        deadline: u64,
        stop: &tokio::sync::watch::Receiver<bool>,
    ) -> Result<Duration, CellGroupControllerError> {
        if *stop.borrow() {
            return Err(CellGroupControllerError::Closed);
        }
        self.startup_live(host, deadline)?;
        let now = self
            .handle
            .domain
            .mono_ms()
            .map_err(|_| CellGroupControllerError::Closed)?;
        deadline
            .checked_sub(now)
            .filter(|left| *left > 0)
            .map(Duration::from_millis)
            .ok_or_else(|| RuntimeError::DeadlineExceeded.into())
    }
}

fn archive_error(error: CellGroupArchiveError) -> CellGroupControllerError {
    CellGroupControllerError::Archive(Arc::new(error.into()))
}
