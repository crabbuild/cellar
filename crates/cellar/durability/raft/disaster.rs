// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Operator API for restoring a cell after permanent quorum loss.
use super::archive::{ArchiveLimits, ArchiveReference};
use super::catalog::{
    BirthProposal, Catalog, CatalogError, CatalogOptions, DisasterFenceAuthority,
    DisasterFenceRequest, DisasterRecoveryRequest, NamespaceCatalog, NamespaceConfig,
};
use cellar_logic::consensus::GroupScope;
use std::sync::Arc;

/// An explicitly configured disaster operator. Keep the fencing authority and
/// its credentials outside voter processes. Its successful result selects a new
/// birth; start fresh hosts with that birth through the ordinary SDK startup.
/// Quorum acknowledgements newer than the selected archive cut can be lost.
/// This API does not assert a consistent cut across multiple cells.
pub struct S3DisasterRecovery {
    namespace: NamespaceCatalog,
    host: cellar_ltx::LtxHost,
    limits: ArchiveLimits,
}

impl S3DisasterRecovery {
    pub async fn open(
        store: Arc<dyn object_store::ObjectStore>,
        catalog: CatalogOptions,
        namespace: NamespaceConfig,
        host: cellar_ltx::LtxHost,
        limits: ArchiveLimits,
    ) -> Result<Self, CatalogError> {
        limits.validate().map_err(CatalogError::Archive)?;
        let namespace = Catalog::new(store, catalog)?
            .existing_namespace(namespace)
            .await?;
        namespace.require_mode(cellar_logic::NamespaceMode::Raft)?;
        Ok(Self {
            namespace,
            host,
            limits,
        })
    }

    /// Observe an archive address for review. Recovery must explicitly pin this
    /// reference in its request; a later head never substitutes for it.
    pub async fn archive_reference(
        &self,
        source: &GroupScope,
    ) -> Result<ArchiveReference, CatalogError> {
        let birth = self
            .namespace
            .birth(&source.cell)
            .await?
            .ok_or(CatalogError::Fenced)?;
        if birth.scope() != source {
            return Err(CatalogError::Fenced);
        }
        self.namespace
            .cell_archive(&birth, self.host.clone(), self.limits)?
            .recovery_metadata()
            .await
            .map_err(CatalogError::Archive)?
            .map(|metadata| metadata.reference())
            .ok_or(CatalogError::Conflict("archive missing"))
    }

    pub async fn fence_request(
        &self,
        request: DisasterRecoveryRequest,
    ) -> Result<DisasterFenceRequest, CatalogError> {
        self.namespace.disaster_fence_request(request).await
    }

    /// Fence then publish a replacement. Ambiguous failure requires retrying the
    /// identical request, never rolling back or choosing a different archive cut.
    pub async fn recover(
        &self,
        request: DisasterRecoveryRequest,
        authority: &dyn DisasterFenceAuthority,
    ) -> Result<BirthProposal, CatalogError> {
        let birth = self
            .namespace
            .recover_from_disaster(request, authority, self.host.clone(), self.limits)
            .await?;
        Ok(BirthProposal {
            scope: birth.scope().clone(),
            voters: birth.voters().to_vec(),
            learners: birth.learners().to_vec(),
        })
    }
}
