// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bind managed transition decisions to an existing local voter.
use super::*;
use crate::raft::{
    catalog::CatalogError, completion::RuntimeError,
    fleet_catalog::ReopenedManagedCellGroupTransition,
};

const SEAL_BYTES: usize = 4 * 1024 * 1024;

impl RaftGroupHandle {
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
