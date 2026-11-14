// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Persistable archive acquisition inputs before selecting a disaster fence.
use super::*;
use crate::raft::{
    catalog::CellGroupArchiveBindingError,
    cell_groups::{CellGroupArchiveHold, CellGroupArchiveIntent, CellGroupArchiveRegistry},
};

impl RaftFleetControl {
    /// Prepare an acquisition for one exact recovery-basis source. This reads
    /// its namespace-owned registry and allocates no remote hold. Persist the
    /// complete intent before calling `acquire_transition_recovery_hold`.
    pub async fn prepare_transition_recovery_hold(
        &self,
        principal: &str,
        basis: &RaftFleetTransitionRecoveryBasis,
        source: &RaftFleetTransitionGroup,
        timeout: Duration,
    ) -> Result<CellGroupArchiveIntent> {
        let turn = Turn::new(&self.domain, timeout)?;
        let registry = self
            .recovery_source_registry(principal, basis, source, &turn)
            .await?;
        self.domain
            .run(crate::asyncrt::timeout(
                turn.remaining()?,
                registry.prepare_acquire(&self.domain),
            ))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map_err(FleetCatalogError::from)
            .map_err(Into::into)
    }

    /// Acquire or reconcile the caller's original persisted intent. A lost
    /// response is an unknown allocation outcome, not permission to mint another
    /// intent. The hold grants retention only; verify the complete selected
    /// closure before staging a recovery intent. Never release it on timeout.
    pub async fn acquire_transition_recovery_hold(
        &self,
        principal: &str,
        basis: &RaftFleetTransitionRecoveryBasis,
        source: &RaftFleetTransitionGroup,
        intent: &CellGroupArchiveIntent,
        timeout: Duration,
    ) -> Result<CellGroupArchiveHold> {
        let turn = Turn::new(&self.domain, timeout)?;
        let registry = self
            .recovery_source_registry(principal, basis, source, &turn)
            .await?;
        self.domain
            .run(crate::asyncrt::timeout(
                turn.remaining()?,
                registry.acquire(&self.domain, intent),
            ))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map_err(FleetCatalogError::from)
            .map_err(Into::into)
    }

    async fn recovery_source_registry(
        &self,
        principal: &str,
        basis: &RaftFleetTransitionRecoveryBasis,
        source: &RaftFleetTransitionGroup,
        turn: &Turn<'_>,
    ) -> Result<CellGroupArchiveRegistry> {
        self.check_catalog(&basis.catalog_id)?;
        let directory = self.managed_namespace(&basis.namespace_id)?;
        let operation = ProposalId::new(records::id(&basis.operation_id)?)
            .map_err(|_| RaftFleetFailure::invalid("recovery source operation ID"))?;
        self.authorize_transition_operator(
            directory,
            operation,
            principal,
            &basis.plan_digest,
            turn.remaining()?,
        )
        .await?;
        if !basis.archive_sources().contains(source)
            || self
                .transition_recovery_basis(
                    &basis.namespace_id,
                    &basis.operation_id,
                    turn.remaining()?,
                )
                .await?
                != *basis
        {
            return Err(FleetCatalogError::Conflict.into());
        }
        let birth = self
            .domain
            .run(crate::asyncrt::timeout(
                turn.remaining()?,
                directory.materialize_selected_history(source.history(directory)?),
            ))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map_err(FleetCatalogError::from)?;
        self.domain
            .run(crate::asyncrt::timeout(
                turn.remaining()?,
                directory.open_group_archive(&self.domain, &self.archive_pool, &birth),
            ))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map_err(|error| match error {
                CellGroupArchiveBindingError::Catalog(error) => FleetCatalogError::from(error),
                CellGroupArchiveBindingError::Archive(error) => FleetCatalogError::from(error),
            })
            .map_err(Into::into)
    }
}
