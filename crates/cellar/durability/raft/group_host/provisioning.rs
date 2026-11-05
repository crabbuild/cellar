// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit catalog provisioning, separate from node boot and serving authority.
use super::*;
use crate::asyncrt::ExecutionDomain;
use crate::raft::catalog::{Catalog, CatalogError};
use object_store::ObjectStore;

/// Operator-owned namespace setup using the same immutable range initialization
/// as node startup. It creates no local Raft member, image or serving capability.
/// Retained setup must pass `initialize: None` to open existing metadata only.
pub struct RaftNamespaceProvisioning {
    directory: RangeDirectory,
    archives: CellGroupArchivePool,
}

impl RaftNamespaceProvisioning {
    pub async fn open(
        domain: &ExecutionDomain,
        store: Arc<dyn ObjectStore>,
        catalog: crate::raft::catalog::CatalogOptions,
        namespace: RaftNamespaceConfig,
        directory_limits: DirectoryLimits,
        archive_limits: CellGroupArchiveLimits,
    ) -> anyhow::Result<Self> {
        domain
            .run(async {
                anyhow::ensure!(
                    namespace.namespace.managed_fleet.is_none(),
                    CatalogError::ManagedAdmissionRequired
                );
                anyhow::ensure!(
                    namespace.namespace.mode == cellar_logic::NamespaceMode::Raft,
                    "namespace is not Raft"
                );
                let catalog = Catalog::new(store, catalog)?;
                let selected = if namespace.initialize.is_some() {
                    catalog.open_namespace(namespace.namespace).await?
                } else {
                    catalog.existing_namespace(namespace.namespace).await?
                };
                let directory = RangeDirectory::new(selected, directory_limits)?;
                if let Some(initialization) = namespace.initialize {
                    initialize(&directory, initialization, directory_limits.max_groups).await?;
                }
                directory.observe().await?;
                Ok(Self {
                    directory,
                    archives: CellGroupArchivePool::new(archive_limits)?,
                })
            })
            .await?
    }

    /// Allocate a one-use archive initialization for an existing selected group.
    /// Persist and sync `encoded_intent()` before `commit`. On any uncertain
    /// outcome, retain that exact intent and use recovery; never recreate it.
    pub async fn prepare_archive(
        &self,
        domain: &ExecutionDomain,
        history: GroupHistory,
    ) -> anyhow::Result<RaftGroupArchiveInitialization> {
        domain
            .run(async {
                let birth = self.directory.materialize_selected_history(history).await?;
                let prepared = self
                    .directory
                    .prepare_group_archive_binding(domain, &self.archives, &birth)
                    .await?;
                Ok(RaftGroupArchiveInitialization { prepared })
            })
            .await?
    }

    /// Verify only the catalog-bound identity and existing registry counters.
    /// Missing or incomplete setup refuses; this never initializes replacements.
    pub async fn open_archive(
        &self,
        domain: &ExecutionDomain,
        history: GroupHistory,
    ) -> anyhow::Result<()> {
        domain
            .run(async {
                let birth = self.directory.materialize_selected_history(history).await?;
                self.directory
                    .open_group_archive(domain, &self.archives, &birth)
                    .await?;
                Ok(())
            })
            .await?
    }
}

/// One-use archive setup. An encoded intent is recovery evidence, not a seed,
/// active membership, local reclamation proof or disaster fencing authority.
pub struct RaftGroupArchiveInitialization {
    pub(crate) prepared: PreparedCellGroupArchiveBinding,
}

impl RaftGroupArchiveInitialization {
    pub fn encoded_intent(&self) -> &[u8] {
        self.prepared.encode()
    }

    /// The caller must have synced this exact encoded intent before entering.
    /// Failure can be uncertain; do not retry by allocating another initializer.
    pub async fn commit(self, domain: &ExecutionDomain) -> anyhow::Result<()> {
        self.prepared.commit(domain).await?;
        Ok(())
    }
}

pub(super) async fn initialize(
    directory: &RangeDirectory,
    initialization: RaftNamespaceInitialization,
    max_groups: u32,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        !initialization.groups.is_empty() && initialization.groups.len() <= max_groups as usize,
        "invalid initial range count"
    );
    let mut routes = Vec::new();
    for spec in initialization.groups {
        let request = directory.prepare_group_birth(spec)?;
        directory.reserve_group_birth(&request).await?;
        routes.push(request.route().clone());
    }
    let mutation = directory.prepare_initialization(initialization.operation, &routes)?;
    directory.publish(&mutation).await?;
    Ok(())
}
