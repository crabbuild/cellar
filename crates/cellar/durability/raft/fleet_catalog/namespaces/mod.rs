//! Append-only compiled namespace registry. A descriptor grants no group
//! authority; complete inventory is checked against exact physical catalogs.
use super::histories::{catalog_error, history_mutation, validate_request};
use super::*;
use crate::raft::catalog::{NamespaceCatalog, VoterPolicy};
use cellar_logic::consensus::NamespaceId;
use serde::Deserialize;

mod inventory;
mod records;
use records::{Declaration, Registry};

pub(super) const KEY: &str = "compiled-namespaces";
const REGISTRY_KIND: u8 = 51;
const DECLARATION_KIND: u8 = 52;
const MAX_NAMESPACES: usize = 64;

pub(super) fn next_namespace_count(previous: u64, bytes: Option<&[u8]>) -> Result<u64> {
    let registry =
        Registry::decode(bytes.ok_or(FleetCatalogError::Invalid("namespace registry deletion"))?)?;
    let count = registry.count as u64;
    if previous.checked_add(1) != Some(count) {
        return Err(FleetCatalogError::Invalid(
            "namespace registry regression/replacement",
        ));
    }
    Ok(count)
}

/// A validated inventory at one fleet revision. This is not a capacity,
/// membership-health, archive or node-removal certificate. An admission must
/// still CAS that exact root and establish its operation-specific proofs.
#[derive(Clone, Debug)]
pub struct FleetInventory {
    revision: u64,
    namespaces: usize,
    histories: u64,
}

impl FleetInventory {
    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn namespaces(&self) -> usize {
        self.namespaces
    }

    pub fn histories(&self) -> u64 {
        self.histories
    }
}

impl FleetCatalog {
    /// Permanently declare one exact namespace/class before managed host or
    /// history admission. Adding a namespace never removes earlier coverage
    /// obligations. Changing a class, schema, owner or data prefix conflicts.
    pub async fn declare_namespace(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        namespace: &NamespaceCatalog,
        class: &str,
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.check_snapshot(snapshot)?;
        validate_request(request)?;
        let declaration = Declaration::new(self, namespace, class)?;
        self.run(timeout, async {
            // A reopened adapter may reconcile the original immutable candidate
            // after later root changes, without reconstructing its former index.
            let prior = self
                .inner
                .read::<FleetMutation>(
                    &format!("requests/{}", hex::encode(request.id)),
                    REQUEST_KIND,
                    REQUEST_LIMIT,
                )
                .await?;
            if let Some((candidate, _)) = prior {
                candidate.validate()?;
                if candidate.id != request.id
                    || candidate.operation != request.operation
                    || candidate.principal != request.principal
                    || candidate.expected_revision != request.expected_revision
                    || candidate.changes.len() != 1
                    || candidate.changes[0].table != FleetTable::Policy
                    || candidate.changes[0].key != KEY
                {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                let registry = Registry::decode(
                    candidate.changes[0]
                        .value
                        .as_deref()
                        .ok_or(FleetCatalogError::PayloadConflict)?,
                )?;
                let bytes = self
                    .inner
                    .lookup(registry.index, &declaration.key())
                    .await?
                    .ok_or(FleetCatalogError::PayloadConflict)?;
                if decode::<Declaration>(DECLARATION_KIND, &bytes, 4096)? != declaration {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                return self.commit_inner(snapshot, &candidate).await;
            }
            let mut registry = self.registry(snapshot).await?.unwrap_or(Registry {
                count: 0,
                index: None,
            });
            if self
                .inner
                .lookup(registry.index, &declaration.key())
                .await?
                .is_some()
            {
                return Err(FleetCatalogError::Conflict);
            }
            if registry.count >= MAX_NAMESPACES {
                return Err(FleetCatalogError::Limit);
            }
            registry.index = self
                .inner
                .update(
                    registry.index,
                    &declaration.key(),
                    Some(encode(DECLARATION_KIND, &declaration, 4096)?),
                    0,
                )
                .await?;
            registry.count += 1;
            let mutation = history_mutation(
                request,
                vec![FleetChange {
                    table: FleetTable::Policy,
                    key: KEY.into(),
                    value: Some(encode(REGISTRY_KIND, &registry, 4096)?),
                }],
            );
            self.commit_inner(snapshot, &mutation).await
        })
        .await
    }

    async fn registry(&self, snapshot: &FleetSnapshot) -> Result<Option<Registry>> {
        let registry = self
            .inner
            .lookup(snapshot.head.tables[FleetTable::Policy.slot()], KEY)
            .await?
            .map(|b| Registry::decode(&b))
            .transpose()?;
        if registry.as_ref().map_or(0, |r| r.count as u64) != snapshot.head.namespace_count {
            return Err(FleetCatalogError::Corrupt("lost namespace registry"));
        }
        Ok(registry)
    }

    async fn declarations(&self, snapshot: &FleetSnapshot) -> Result<Vec<Declaration>> {
        let registry = self
            .registry(snapshot)
            .await?
            .ok_or(FleetCatalogError::InventoryIncomplete)?;
        let page = self
            .inner
            .scan(registry.index, None, MAX_NAMESPACES + 1)
            .await?;
        if page.next.is_some() || page.entries.len() != registry.count {
            return Err(FleetCatalogError::Corrupt("namespace registry count"));
        }
        let mut entries = Vec::with_capacity(registry.count);
        for entry in page.entries {
            let declaration: Declaration = decode(DECLARATION_KIND, &entry.value, 4096)?;
            declaration.validate()?;
            if entry.key != declaration.key() {
                return Err(FleetCatalogError::Corrupt("namespace declaration key"));
            }
            entries.push(declaration);
        }
        Ok(entries)
    }

    pub(super) async fn require_namespace_declaration(
        &self,
        snapshot: &FleetSnapshot,
        namespace: &NamespaceCatalog,
        class: Option<&str>,
    ) -> Result<()> {
        let registry = self
            .registry(snapshot)
            .await?
            .ok_or(FleetCatalogError::InventoryIncomplete)?;
        let bytes = self
            .inner
            .lookup(
                registry.index,
                &hex::encode(namespace.config().namespace.bytes()),
            )
            .await?
            .ok_or(FleetCatalogError::InventoryIncomplete)?;
        let declaration: Declaration = decode(DECLARATION_KIND, &bytes, 4096)?;
        declaration.check(self, namespace, class)
    }
}

impl FleetCatalog {
    /// Verify the actual prepared catalogs, not only the caller's digest. This
    /// check is independent of inventory completion so import hosts can boot.
    pub(crate) async fn check_declared_namespaces(
        &self,
        entries: &[crate::raft::host::NamespaceRegistration],
        timeout: Duration,
    ) -> Result<()> {
        self.run(timeout, async {
            let root = self.inner.current().await?;
            for entry in entries {
                self.require_namespace_declaration(
                    &root,
                    &entry.namespace,
                    Some(entry.class.name()),
                )
                .await?;
            }
            Ok(())
        })
        .await
    }
}
