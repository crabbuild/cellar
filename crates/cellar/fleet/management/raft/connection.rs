// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit existing-root configuration for the standalone management server.
use super::*;
use crate::raft::catalog::{Catalog, CatalogOptions, NamespaceConfig};
use cellar_logic::consensus::{ClusterId, IncarnationId, NamespaceId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetConnection {
    pub cluster_id: String,
    pub incarnation_id: String,
    /// Relative to the selected bucket's scope, matching FleetCatalog::new.
    pub coordination_prefix: String,
    pub namespaces: Vec<RaftFleetNamespace>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetNamespace {
    pub namespace_id: String,
    pub schema_digest: String,
    /// The original CatalogOptions prefix, relative to the same bucket scope.
    pub catalog_prefix: String,
    /// Existing deployments may keep namespace metadata outside the selected
    /// coordination bucket prefix. This is trusted server configuration only.
    #[serde(default)]
    pub scope: RaftFleetNamespaceScope,
    #[serde(default = "managed")]
    pub managed: bool,
    /// Must match the existing namespace Root; opening never changes its mode.
    #[serde(default = "materialized")]
    pub image_storage: crate::raft::catalog::RaftImageStorage,
}

fn materialized() -> crate::raft::catalog::RaftImageStorage {
    crate::raft::catalog::RaftImageStorage::Materialized
}

fn managed() -> bool {
    true
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RaftFleetNamespaceScope {
    #[default]
    Bucket,
    StoreRoot,
}

impl RaftFleetControl {
    /// Connect only to exact existing coordination and namespace roots. This
    /// never initializes a fleet, namespace, node or controller. The complete
    /// namespace set is checked before exposing any mutation interface.
    pub async fn connect(
        domain: &ExecutionDomain,
        bucket: crate::bucket::Bucket,
        config: RaftFleetConnection,
        timeout: Duration,
    ) -> Result<Self> {
        if config.namespaces.len() > 64 {
            return Err(RaftFleetFailure::invalid("too many namespaces"));
        }
        let cluster = ClusterId::new(records::id(&config.cluster_id)?)
            .map_err(|_| RaftFleetFailure::invalid("cluster ID"))?;
        let incarnation = IncarnationId::new(records::id(&config.incarnation_id)?)
            .map_err(|_| RaftFleetFailure::invalid("fleet incarnation ID"))?;
        let catalog = FleetCatalog::new(
            domain,
            bucket.clone(),
            cluster,
            incarnation,
            &config.coordination_prefix,
        )?;
        let token = domain
            .services()
            .domain_token()
            .ok_or(FleetCatalogError::Closed)?;
        let turn = Turn::new(&token, timeout)?;
        let root = catalog.read(turn.remaining()?).await?;
        let mut namespaces = Vec::with_capacity(config.namespaces.len());
        for entry in config.namespaces {
            let namespace = NamespaceId::new(records::id(&entry.namespace_id)?)
                .map_err(|_| RaftFleetFailure::invalid("namespace ID"))?;
            let mut schema = [0; 32];
            if entry.schema_digest.len() != 64
                || hex::decode_to_slice(&entry.schema_digest, &mut schema).is_err()
                || hex::encode(schema) != entry.schema_digest
                || schema == [0; 32]
            {
                return Err(RaftFleetFailure::invalid("invalid schema digest"));
            }
            let mut namespace_config = NamespaceConfig::new(
                cluster,
                namespace,
                cellar_logic::NamespaceMode::Raft,
                schema,
            );
            namespace_config.image_storage = entry.image_storage;
            if entry.managed {
                namespace_config = catalog.bind_namespace(namespace_config)?;
            }
            let store = match entry.scope {
                RaftFleetNamespaceScope::Bucket => bucket.raft_store(),
                RaftFleetNamespaceScope::StoreRoot => bucket.cas_store.clone(),
            };
            let adapter = Catalog::new(
                store,
                CatalogOptions {
                    prefix: entry.catalog_prefix,
                    ..Default::default()
                },
            )
            .map_err(namespace_failure)?;
            let timer = token
                .sleep_until(turn.deadline)
                .map_err(|_| FleetCatalogError::Closed)?;
            let opened = crate::asyncrt::select! {
                _ = timer => return Err(FleetCatalogError::Unconfirmed.into()),
                result = adapter.existing_namespace(namespace_config) => result.map_err(namespace_failure)?,
            };
            namespaces.push(opened);
        }
        catalog
            .check_inventory(&root, &namespaces, turn.remaining()?)
            .await?;
        Self::new(domain, catalog, namespaces)
    }
}

fn namespace_failure(error: crate::raft::catalog::CatalogError) -> RaftFleetFailure {
    use crate::raft::catalog::CatalogError as E;
    let code = match error {
        E::Invalid(_) | E::WrongMode => RaftFleetFailureCode::InvalidInput,
        E::Conflict(_) => RaftFleetFailureCode::Conflict,
        E::Limit => RaftFleetFailureCode::Limit,
        _ => RaftFleetFailureCode::Unavailable,
    };
    RaftFleetFailure {
        code,
        outcome: RaftFleetOutcome::NotSubmitted,
        message:
            "The configured existing namespace could not be verified; no roots were initialized."
                .into(),
    }
}
