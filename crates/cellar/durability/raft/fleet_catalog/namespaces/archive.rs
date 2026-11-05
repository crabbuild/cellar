// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Archive setup over the declared namespace and its selected live histories.
use super::super::deadline::Deadline;
use super::*;
use crate::application::consensus::RaftNamespaceRegistration;
use crate::raft::{
    catalog::{Catalog, CellGroupArchiveBindingError},
    cell_groups::{
        CellGroupArchiveLimits, CellGroupArchivePool, CellGroupBirth, DirectoryLimits,
        RangeDirectory,
    },
    group_host::RaftGroupArchiveInitialization,
};
use cellar_logic::cell_group::GroupHistory;

/// Operator archive setup for one exact managed namespace. Every operation
/// checks the current fleet history and selected birth. This handle grants
/// neither local replica activation nor archive coverage.
pub struct RaftManagedArchiveProvisioning {
    fleet: FleetCatalog,
    directory: RangeDirectory,
    archives: CellGroupArchivePool,
}

impl FleetCatalog {
    /// Open the existing, declared namespace for explicit archive setup.
    pub async fn open_managed_archive_provisioning(
        &self,
        catalog: &Catalog,
        registration: &RaftNamespaceRegistration,
        directory_limits: DirectoryLimits,
        archive_limits: CellGroupArchiveLimits,
        timeout: Duration,
    ) -> Result<RaftManagedArchiveProvisioning> {
        let archives = CellGroupArchivePool::new(archive_limits)?;
        let deadline = Deadline::new(self, timeout)?;
        let directory = self
            .open_managed_directory(catalog, registration, directory_limits, &deadline)
            .await?;
        Ok(RaftManagedArchiveProvisioning {
            fleet: self.clone(),
            directory,
            archives,
        })
    }
}

impl RaftManagedArchiveProvisioning {
    /// Prepare the one-use initializer for a confirmed managed group. Persist
    /// and sync the encoded intent before committing it.
    pub async fn prepare_archive(
        &self,
        history: GroupHistory,
        timeout: Duration,
    ) -> Result<RaftGroupArchiveInitialization> {
        self.fleet
            .run(timeout, async {
                let domain = &self.fleet.inner.domain;
                let birth = self.selected_birth(history).await?;
                let prepared = self
                    .directory
                    .prepare_group_archive_binding(domain, &self.archives, &birth)
                    .await
                    .map_err(binding_error)?;
                Ok(RaftGroupArchiveInitialization { prepared })
            })
            .await
    }

    /// Verify the exact persisted intent after a restart or uncertain commit.
    /// Missing binding, identity or counters refuse without creating metadata.
    pub async fn verify_archive(
        &self,
        history: GroupHistory,
        intent: &[u8],
        timeout: Duration,
    ) -> Result<()> {
        self.fleet
            .run(timeout, async {
                let domain = &self.fleet.inner.domain;
                let birth = self.selected_birth(history).await?;
                self.directory
                    .verify_group_archive_binding(domain, &self.archives, &birth, intent)
                    .await
                    .map_err(binding_error)
            })
            .await
    }

    async fn selected_birth(&self, history: GroupHistory) -> Result<CellGroupBirth> {
        let snapshot = self.fleet.inner.current().await?;
        let binding = self.fleet.binding(&history)?;
        let key = binding
            .key()
            .map_err(|_| FleetCatalogError::Invalid("archive history binding"))?;
        let authority = self
            .fleet
            .authorize_live_group(&snapshot, binding, &key)
            .await?;
        let owner = self.fleet.birth_owner()?;
        let birth = self
            .fleet
            .inner
            .domain
            .run(
                self.directory
                    .open_managed_selected_history(history, &owner),
            )
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(catalog_error)?;
        if !authority.check_birth(&self.directory, &birth) {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(birth)
    }
}

fn binding_error(error: CellGroupArchiveBindingError) -> FleetCatalogError {
    match error {
        CellGroupArchiveBindingError::Catalog(error) => catalog_error(error),
        CellGroupArchiveBindingError::Archive(error) => error.into(),
    }
}
