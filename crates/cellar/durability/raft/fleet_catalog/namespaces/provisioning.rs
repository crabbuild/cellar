// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Publish an exact managed standby before confirming its fleet declaration.
use super::*;
use crate::application::consensus::{RaftClass, RaftNamespaceRegistration};
use crate::raft::{
    catalog::{Catalog, CatalogError, NamespaceConfig},
    cell_groups::{DirectoryLimits, RangeDirectory},
};
use cellar_logic::consensus::ProposalId;

impl FleetCatalog {
    /// Provision a declared managed standby using the compiled class and the
    /// original fleet request. Persist that request and the catalog settings
    /// before calling. An exact retry resumes the same immutable setup. If
    /// the fleet root advances before declaration, a new request at the new
    /// revision can continue the same standby operation with the same config.
    /// This publishes an empty directory and does not allocate a group or voter.
    pub async fn provision_managed_namespace(
        &self,
        request: &FleetRequestIdentity,
        catalog: &Catalog,
        config: NamespaceConfig,
        class: &RaftClass,
        directory_limits: DirectoryLimits,
        timeout: Duration,
    ) -> Result<RaftNamespaceRegistration> {
        validate_request(request)?;
        let config = self.bind_namespace(config)?;
        if class.schema_digest() != config.schema_digest {
            return Err(FleetCatalogError::Invalid("compiled namespace schema"));
        }
        let namespace = catalog
            .describe_namespace(config.clone())
            .map_err(catalog_error)?;
        let declaration = Declaration::new(self, &namespace, class.name())?;
        let directory = RangeDirectory::new(namespace, directory_limits).map_err(catalog_error)?;
        let operation = ProposalId::new(request.operation)
            .map_err(|_| FleetCatalogError::Invalid("standby operation"))?;
        let standby = directory
            .prepare_standby_initialization(operation)
            .map_err(catalog_error)?;
        self.run(timeout, async {
            let snapshot = self.inner.current().await?;
            let mutation = self
                .prepare_namespace_declaration(&snapshot, request, &declaration)
                .await?;
            mutation.validate()?;
            let bytes = encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?;
            let key = hex::encode(request.id);
            let committed = self
                .inner
                .committed(&snapshot.head, &key, hash(&bytes))
                .await?
                .is_some();
            if !committed {
                if snapshot.revision() != request.expected_revision {
                    return Err(FleetCatalogError::Conflict);
                }
                // Pin the exact declaration before the first namespace write.
                // A losing root CAS retains the intent but grants no authority.
                self.inner
                    .create_exact(&format!("requests/{key}"), &bytes, REQUEST_LIMIT)
                    .await?;
            }
            self.inner
                .domain
                .run(Box::pin(async {
                    if committed {
                        catalog.existing_namespace(config.clone()).await?;
                        directory
                            .publication(&standby)
                            .await?
                            .ok_or(CatalogError::Conflict(
                                "missing managed standby publication",
                            ))?;
                    } else {
                        catalog.open_namespace(config.clone()).await?;
                        directory.publish(&standby).await?;
                    }
                    Ok::<_, CatalogError>(())
                }))
                .await
                .map_err(|_| FleetCatalogError::Closed)?
                .map_err(catalog_error)?;
            self.commit_inner(&snapshot, &mutation).await?;
            Ok(RaftNamespaceRegistration {
                namespace: config,
                class: declaration.class,
            })
        })
        .await
    }
}
