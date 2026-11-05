// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! SDK first-group provisioning over the existing fleet and directory journals.
use super::super::deadline::Deadline;
use super::*;
use crate::application::consensus::RaftNamespaceRegistration;
use crate::raft::{
    catalog::Catalog,
    cell_groups::{CellGroupBirthSpec, DirectoryLimits, DirectoryMutation, RangeDirectory},
};
use cellar_logic::{cell_group::GroupRoute, consensus::ProposalId};

/// Persist this exact setup before reserving the first group. Confirmation
/// has its own fleet request because an unrelated CAS may advance that root.
#[derive(Clone)]
pub struct RaftManagedFirstGroupRequest {
    pub reservation: FleetRequestIdentity,
    pub standby: [u8; 32],
    pub publication: ProposalId,
    pub group: CellGroupBirthSpec,
}

/// Exact admitted reservation and immutable range mutation. Reopen a lost
/// handle by replaying the original reservation and setup request.
pub struct RaftManagedFirstGroup {
    directory: RangeDirectory,
    authorization: FleetBirthAuthorization,
    mutation: DirectoryMutation,
    reservation: FleetRequestIdentity,
}

impl FleetCatalog {
    /// Read the selected empty standby for an exact managed declaration.
    pub async fn observe_managed_standby(
        &self,
        catalog: &Catalog,
        registration: &RaftNamespaceRegistration,
        limits: DirectoryLimits,
        timeout: Duration,
    ) -> Result<[u8; 32]> {
        let deadline = Deadline::new(self, timeout)?;
        let directory = self
            .open_managed_directory(catalog, registration, limits, &deadline)
            .await?;
        self.observe_first_group_standby(&directory, &deadline)
            .await
    }

    pub(crate) async fn observe_managed_standby_at(
        &self,
        directory: &RangeDirectory,
        timeout: Duration,
    ) -> Result<[u8; 32]> {
        let deadline = Deadline::new(self, timeout)?;
        self.first_group_class(directory, &deadline).await?;
        self.observe_first_group_standby(directory, &deadline).await
    }

    async fn observe_first_group_standby(
        &self,
        directory: &RangeDirectory,
        deadline: &Deadline<'_>,
    ) -> Result<[u8; 32]> {
        self.run(deadline.remaining()?, async {
            let view = self
                .inner
                .domain
                .run(directory.observe())
                .await
                .map_err(|_| FleetCatalogError::Closed)?
                .map_err(catalog_error)?;
            if view.revision() != 1 || view.groups() != 0 {
                return Err(FleetCatalogError::Conflict);
            }
            Ok(view.digest())
        })
        .await
    }

    /// Reserve the one full-range RF3 group against the original standby. The
    /// regular birth admission checks current boot capacity and separation.
    /// Replay accepts the exact already-published directory operation as well.
    pub async fn reserve_managed_first_group(
        &self,
        catalog: &Catalog,
        registration: &RaftNamespaceRegistration,
        request: RaftManagedFirstGroupRequest,
        limits: DirectoryLimits,
        timeout: Duration,
    ) -> Result<RaftManagedFirstGroup> {
        let deadline = Deadline::new(self, timeout)?;
        validate_request(&request.reservation)?;
        let directory = self
            .open_managed_directory(catalog, registration, limits, &deadline)
            .await?;
        self.reserve_first_group(&directory, request, &registration.class, &deadline)
            .await
    }

    pub(crate) async fn reserve_managed_first_group_at(
        &self,
        directory: &RangeDirectory,
        request: RaftManagedFirstGroupRequest,
        timeout: Duration,
    ) -> Result<RaftManagedFirstGroup> {
        let deadline = Deadline::new(self, timeout)?;
        validate_request(&request.reservation)?;
        let class = self.first_group_class(directory, &deadline).await?;
        self.reserve_first_group(directory, request, &class, &deadline)
            .await
    }

    async fn reserve_first_group(
        &self,
        directory: &RangeDirectory,
        request: RaftManagedFirstGroupRequest,
        class: &str,
        deadline: &Deadline<'_>,
    ) -> Result<RaftManagedFirstGroup> {
        let (mutation, _) = self
            .prepare_managed_first_group(directory, &request, deadline)
            .await?;
        let snapshot = self.read(deadline.remaining()?).await?;
        let reservation = self
            .reserve_birth_with_intent(
                &snapshot,
                &request.reservation,
                directory,
                (request.group, Some(mutation.digest())),
                class,
                deadline.remaining()?,
            )
            .await?;
        Ok(RaftManagedFirstGroup {
            directory: directory.clone(),
            authorization: reservation.authorization,
            mutation,
            reservation: request.reservation,
        })
    }

    /// Reopen an already admitted first-group plan from its original inputs.
    /// This checks the immutable reservation and range mutation without submitting
    /// another fleet or directory mutation.
    pub async fn reopen_managed_first_group(
        &self,
        catalog: &Catalog,
        registration: &RaftNamespaceRegistration,
        request: RaftManagedFirstGroupRequest,
        limits: DirectoryLimits,
        timeout: Duration,
    ) -> Result<RaftManagedFirstGroup> {
        let deadline = Deadline::new(self, timeout)?;
        validate_request(&request.reservation)?;
        let directory = self
            .open_managed_directory(catalog, registration, limits, &deadline)
            .await?;
        self.reopen_first_group(&directory, request, &deadline)
            .await
    }

    pub(crate) async fn reopen_managed_first_group_at(
        &self,
        directory: &RangeDirectory,
        request: RaftManagedFirstGroupRequest,
        timeout: Duration,
    ) -> Result<RaftManagedFirstGroup> {
        let deadline = Deadline::new(self, timeout)?;
        validate_request(&request.reservation)?;
        self.first_group_class(directory, &deadline).await?;
        self.reopen_first_group(directory, request, &deadline).await
    }

    async fn reopen_first_group(
        &self,
        directory: &RangeDirectory,
        request: RaftManagedFirstGroupRequest,
        deadline: &Deadline<'_>,
    ) -> Result<RaftManagedFirstGroup> {
        let (mutation, route) = self
            .prepare_managed_first_group(directory, &request, deadline)
            .await?;
        let snapshot = self.read(deadline.remaining()?).await?;
        let authorization = self
            .recover_birth_reservation_matching(
                &snapshot,
                directory,
                &request.group.history,
                Some((&request.reservation, mutation.digest())),
                deadline.remaining()?,
            )
            .await?;
        if authorization.route() != &route {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(RaftManagedFirstGroup {
            directory: directory.clone(),
            authorization,
            mutation,
            reservation: request.reservation,
        })
    }

    async fn first_group_class(
        &self,
        directory: &RangeDirectory,
        deadline: &Deadline<'_>,
    ) -> Result<String> {
        if directory.namespace_config().managed_fleet.as_ref() != Some(&self.birth_owner()?) {
            return Err(FleetCatalogError::Conflict);
        }
        self.run(deadline.remaining()?, async {
            let snapshot = self.inner.current().await?;
            let declaration = self
                .namespace_declaration(&snapshot, directory.namespace(), None)
                .await?;
            Ok(declaration.class)
        })
        .await
    }

    async fn prepare_managed_first_group(
        &self,
        directory: &RangeDirectory,
        request: &RaftManagedFirstGroupRequest,
        deadline: &Deadline<'_>,
    ) -> Result<(DirectoryMutation, GroupRoute)> {
        let preview = directory
            .prepare_fleet_group_birth(request.group.clone(), &self.birth_owner()?)
            .map_err(catalog_error)?;
        let mutation = directory
            .prepare_managed_first_group(request.standby, request.publication, preview.route())
            .map_err(catalog_error)?;
        self.run(deadline.remaining()?, async {
            self.inner
                .domain
                .run(directory.check_managed_first_group(&mutation))
                .await
                .map_err(|_| FleetCatalogError::Closed)?
                .map_err(catalog_error)
        })
        .await?;
        Ok((mutation, preview.route().clone()))
    }

    /// Publish the exact first range and confirm its birth. If the fleet CAS
    /// conflicts after range publication, use a new confirmation request at
    /// the observed revision with the same operation and prepared reservation.
    /// The returned group authorization still requires physical activation.
    pub async fn publish_managed_first_group(
        &self,
        prepared: &RaftManagedFirstGroup,
        confirmation: &FleetRequestIdentity,
        timeout: Duration,
    ) -> Result<FleetGroupAuthorization> {
        let deadline = Deadline::new(self, timeout)?;
        validate_request(confirmation)?;
        if confirmation.id == prepared.reservation.id
            || confirmation.operation != prepared.reservation.operation
            || confirmation.principal != prepared.reservation.principal
            || confirmation.expected_revision <= prepared.reservation.expected_revision
            || self.birth_owner()? != *prepared.authorization.owner()
        {
            return Err(FleetCatalogError::Invalid(
                "managed first group confirmation",
            ));
        }
        self.run(deadline.remaining()?, async {
            let current = self.inner.current().await?;
            let key = self
                .binding(prepared.authorization.scope())?
                .key()
                .map_err(|_| FleetCatalogError::Invalid("first group history binding"))?;
            let history = self.history_inner(&current, &key).await?;
            if !prepared.authorization.matches_active_reservation(&history) {
                return Err(FleetCatalogError::Conflict);
            }
            if self
                .first_group_intent(&current, &prepared.reservation.id)
                .await?
                != Some(prepared.mutation.digest())
            {
                return Err(FleetCatalogError::Corrupt(
                    "first group intent disagrees with reservation",
                ));
            }
            self.inner
                .domain
                .run(Box::pin(prepared.directory.publish_managed_first_group(
                    &prepared.mutation,
                    &prepared.authorization,
                )))
                .await
                .map_err(|_| FleetCatalogError::Closed)?
                .map_err(catalog_error)
        })
        .await?;
        let snapshot = self.read(deadline.remaining()?).await?;
        let committed = self
            .confirm_birth(
                &snapshot,
                confirmation,
                &prepared.directory,
                &prepared.authorization,
                deadline.remaining()?,
            )
            .await?;
        self.authorize_group(
            &committed.snapshot,
            prepared.authorization.scope(),
            deadline.remaining()?,
        )
        .await
    }
}
