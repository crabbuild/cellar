// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bind managed transition decisions to an existing local voter.
use super::*;
use crate::raft::{
    catalog::CatalogError, completion::RuntimeError,
    fleet_catalog::ReopenedManagedCellGroupTransition,
};
use cellar_logic::cell_group::GroupRoute;

const SEAL_BYTES: usize = 4 * 1024 * 1024;

impl RaftGroupHandle {
    /// Observe a durable fresh-term commit on a pending managed transfer child.
    /// The resulting proof supplies no application or routing authority.
    pub fn managed_pending_transfer_quorum(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<TransferChildQuorum, CellGroupControllerError>> + Send + 'static
    {
        let admitted = self
            .owner
            .metadata_admission(timeout, 4096)
            .map_err(super::startup::admission_error);
        let owner = self.owner.internal_observer();
        let history = self.history;
        async move {
            let (deadline, _memory) = admitted?;
            let core = &owner.handle.core;
            let host = core.managed.as_ref().ok_or_else(|| {
                super::startup::catalog_error(CatalogError::ManagedAdmissionRequired)
            })?;
            owner.startup_live(host, deadline)?;
            let proof = owner
                .handle
                .domain
                .run(crate::asyncrt::timeout_at(
                    deadline,
                    core.runtime.pending_transfer_quorum(history),
                ))
                .await
                .map_err(|_| CellGroupControllerError::Closed)?
                .map_err(|_| CellGroupControllerError::Runtime(RuntimeError::DeadlineExceeded))??;
            owner.startup_live(host, deadline)?;
            Ok(proof)
        }
    }

    /// Submit or observe the exact seal authorized for this managed parent.
    /// Accepted identifies a proposal. Applied returns the selected durable cut.
    /// A lost response can follow either outcome. Retry the same fleet plan.
    pub fn seal_managed_source<'a>(
        &self,
        transition: &'a ReopenedManagedCellGroupTransition,
        timeout: Duration,
    ) -> impl Future<Output = Result<CellGroupSealOutcome, CellGroupControllerError>> + Send + 'a
    {
        let admission = self
            .owner
            .metadata_admission(timeout, SEAL_BYTES)
            .map_err(super::startup::admission_error);
        let owner = self.owner.internal_observer();
        let source = self.history;
        async move {
            let (deadline, _memory) = admission?;
            let core = &owner.handle.core;
            let host = core.managed.as_ref().ok_or_else(|| {
                super::startup::catalog_error(CatalogError::ManagedAdmissionRequired)
            })?;
            let directory = core
                .host
                .namespace_directory(source.cluster, source.namespace)
                .map_err(|_| CellGroupControllerError::Invalid("managed source namespace"))?;
            owner
                .handle
                .domain
                .run(Box::pin(crate::asyncrt::timeout_at(
                    deadline,
                    Box::pin(async {
                        let remaining = owner.startup_remaining(deadline)?;
                        owner.startup_live(host, deadline)?;
                        let (permit, group) = host
                            .managed_source_seal_authority(
                                transition, &directory, source, remaining,
                            )
                            .await
                            .map_err(CatalogError::from)
                            .map_err(super::startup::catalog_error)?;
                        let seal = directory
                            .prepare_managed_source_seal(
                                &transition.plan,
                                source,
                                &core.log,
                                host,
                                (&permit, &group),
                                deadline,
                            )
                            .await
                            .map_err(super::startup::catalog_error)?;
                        owner.startup_live(host, deadline)?;
                        core.runtime.seal_source(seal).await.map_err(Into::into)
                    }),
                )))
                .await
                .map_err(|_| CellGroupControllerError::Closed)?
                .map_err(|_| CellGroupControllerError::Runtime(RuntimeError::DeadlineExceeded))?
        }
    }
}

impl RaftCells {
    /// Attach this boot's exact installed voter to the pending child. The
    /// majority of install receipts and current capacity are revalidated before
    /// attachment. The child can vote but cannot serve application requests.
    pub fn attach_managed_transfer_child(
        &self,
        transition: Arc<ReopenedManagedCellGroupTransition>,
        route: GroupRoute,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), CellGroupControllerError>> + Send + 'static {
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
            let directory = core
                .host
                .namespace_directory(route.history.cluster, route.history.namespace)
                .map_err(|_| CellGroupControllerError::Invalid("transfer child namespace"))?;
            let lease = owner
                .register_control_service(ControlServiceKind::Transfer, 4096)
                .map_err(CatalogError::from)
                .map_err(super::startup::catalog_error)?;
            let (reply, result) = tokio::sync::oneshot::channel();
            let controller = core.controller.clone();
            let transport = core.transport.clone();
            let host = host.clone();
            let domain = &owner.handle.domain;
            domain
                .spawn_cleanup(async move {
                    let stop = lease.stop_receiver();
                    let attached = Box::pin(controller.attach_managed_transfer_child(
                        &directory,
                        transition,
                        &route,
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

    /// Install one reserved transfer child into this node's retained stores.
    /// The receipt proves local sync. Attachment and activation are separate.
    /// After a timeout, retry the same transition, sources and child.
    pub fn install_managed_transfer_child(
        &self,
        transition: Arc<ReopenedManagedCellGroupTransition>,
        sources: VerifiedCellGroupTransitionSources,
        child: GroupHistory,
        recovery: CellGroupRecoveryPool,
        timeout: Duration,
    ) -> impl Future<Output = Result<InstalledTransferVoter, CellGroupControllerError>> + Send + 'static
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
            if sources.transition_plan() != (transition.plan.operation(), transition.plan.digest())
            {
                return Err(super::startup::catalog_error(CatalogError::Fenced));
            }
            let route = sources
                .children()
                .iter()
                .find(|route| route.history == child)
                .cloned()
                .ok_or_else(|| super::startup::catalog_error(CatalogError::Fenced))?;
            let directory = core
                .host
                .namespace_directory(child.cluster, child.namespace)
                .map_err(|_| CellGroupControllerError::Invalid("transfer child namespace"))?;
            let lease = owner
                .register_control_service(ControlServiceKind::Transfer, 4096)
                .map_err(CatalogError::from)
                .map_err(super::startup::catalog_error)?;
            let (reply, result) = tokio::sync::oneshot::channel();
            let controller = core.controller.clone();
            let host = host.clone();
            let domain = &owner.handle.domain;
            domain
                .spawn_cleanup(async move {
                    let stop = lease.stop_receiver();
                    let installed = Box::pin(controller.install_managed_transfer_child(
                        &directory,
                        (&transition, &sources),
                        &route,
                        &host,
                        &recovery,
                        (deadline, &stop),
                    ))
                    .await;
                    drop(lease);
                    let _ = reply.send(installed);
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
