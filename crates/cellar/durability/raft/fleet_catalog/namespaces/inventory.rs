use super::super::histories::Origin;
use super::*;
use crate::raft::catalog::{BirthDisposition, ManagedBirthInventory};
use std::collections::BTreeMap;

impl FleetCatalog {
    /// Verify the entire rooted namespace set and all histories. Caller handles
    /// select adapters, never the coverage set. Missing/offline namespaces refuse
    /// success; pending births retain references and do not become absence proof.
    pub async fn check_inventory(
        &self,
        snapshot: &FleetSnapshot,
        namespaces: &[NamespaceCatalog],
        timeout: Duration,
    ) -> Result<FleetInventory> {
        self.check_snapshot(snapshot)?;
        if namespaces.len() > MAX_NAMESPACES {
            return Err(FleetCatalogError::Limit);
        }
        self.run(timeout, async {
            self.inner
                .domain
                .run(Box::pin(self.check_inventory_inner(snapshot, namespaces)))
                .await
                .map_err(|_| FleetCatalogError::Closed)?
        })
        .await
    }

    pub(in super::super) async fn check_inventory_inner(
        &self,
        snapshot: &FleetSnapshot,
        namespaces: &[NamespaceCatalog],
    ) -> Result<FleetInventory> {
        let declarations = self.declarations(snapshot).await?;
        let mut catalogs = BTreeMap::new();
        for namespace in namespaces {
            if catalogs
                .insert(namespace.config().namespace.bytes(), namespace)
                .is_some()
            {
                return Err(FleetCatalogError::Invalid("duplicate inventory namespace"));
            }
        }
        if catalogs.len() != declarations.len() {
            return Err(FleetCatalogError::InventoryIncomplete);
        }
        for declaration in &declarations {
            let namespace = catalogs
                .get(&declaration.namespace)
                .ok_or(FleetCatalogError::InventoryIncomplete)?;
            declaration.check(self, namespace, None)?;
            if namespace.config().managed_fleet.is_none() {
                self.verify_imported_inventory(snapshot, namespace).await?;
            }
            let cut: ManagedBirthInventory = namespace
                .managed_birth_inventory(&self.birth_owner()?)
                .await
                .map_err(catalog_error)?;
            if cut.count > HISTORY_LIMIT {
                return Err(FleetCatalogError::Limit);
            }
            let mut cursor = None;
            let mut count = 0u64;
            loop {
                let page = namespace
                    .managed_birth_page(&cut, cursor.as_ref())
                    .await
                    .map_err(catalog_error)?;
                for entry in page.entries {
                    count = count
                        .checked_add(1)
                        .filter(|n| *n <= cut.count)
                        .ok_or(FleetCatalogError::Corrupt("extra managed birth intent"))?;
                    let binding = self.binding(&entry.proposal().scope)?;
                    let history = self
                        .history_inner(
                            snapshot,
                            &binding
                                .key()
                                .map_err(|_| FleetCatalogError::Corrupt("inventory binding"))?,
                        )
                        .await
                        .map_err(|error| match error {
                            FleetCatalogError::Conflict => FleetCatalogError::InventoryIncomplete,
                            error => error,
                        })?;
                    let initial = history.initial_placement()?;
                    if history.birth_digest != entry.expected_digest()
                        || initial.scope != entry.proposal().scope
                        || initial.voters != entry.proposal().voters
                        || initial.learners != entry.proposal().learners
                    {
                        return Err(FleetCatalogError::Corrupt("uninventoried canonical birth"));
                    }
                }
                cursor = page.next;
                if cursor.is_none() {
                    break;
                }
            }
            if count != cut.count {
                return Err(FleetCatalogError::Corrupt("missing managed birth intent"));
            }
        }
        let nodes = self.node_records(snapshot).await?;
        let mut cursor = None;
        let mut histories = 0u64;
        loop {
            let page = self
                .inner
                .scan(
                    snapshot.head.tables[FleetTable::Histories.slot()],
                    cursor.as_ref(),
                    128,
                )
                .await?;
            for entry in page.entries {
                histories = histories
                    .checked_add(1)
                    .filter(|n| *n <= HISTORY_LIMIT)
                    .ok_or(FleetCatalogError::Limit)?;
                let history = FleetHistory::decode(&entry.value)?;
                let scope = history
                    .binding
                    .scope()
                    .map_err(|_| FleetCatalogError::Corrupt("inventory scope"))?;
                let declaration = declarations
                    .iter()
                    .find(|d| d.namespace == scope.namespace.bytes())
                    .ok_or(FleetCatalogError::InventoryIncomplete)?;
                let namespace = catalogs[&declaration.namespace];
                if entry.key
                    != history
                        .binding
                        .key()
                        .map_err(|_| FleetCatalogError::Corrupt("inventory key"))?
                    || history.binding != self.binding(&scope)?
                    || history.namespace_root != declaration.root
                    || history.schema_digest != declaration.schema
                    || scope.cell.split_once(':').map(|(class, _)| class)
                        != Some(declaration.class.as_str())
                {
                    return Err(FleetCatalogError::Corrupt("inventory history declaration"));
                }
                for key in [history.name_key()?, history.group_key()?] {
                    if self
                        .inner
                        .lookup(snapshot.head.tables[FleetTable::Policy.slot()], &key)
                        .await?
                        .as_deref()
                        != Some(entry.key.as_bytes())
                    {
                        return Err(FleetCatalogError::Corrupt("inventory history identity"));
                    }
                }
                for member in history.voters.iter().chain(&history.learners) {
                    if !nodes.iter().any(|n| n.node == member.node) {
                        return Err(FleetCatalogError::InventoryIncomplete);
                    }
                }
                namespace
                    .verify_group_identity(&scope)
                    .await
                    .map_err(catalog_error)?;
                let decision = namespace
                    .birth_disposition(&scope.cell)
                    .await
                    .map_err(catalog_error)?;
                match (&history.state, decision) {
                    (FleetHistoryState::Reserved, decision) => {
                        let (_, digest) = namespace
                            .prepare_birth_with_owner(
                                history.initial_placement()?,
                                Some(&self.birth_owner()?),
                            )
                            .map_err(catalog_error)?;
                        if history.origin != Origin::Managed
                            || digest != history.birth_digest
                            || decision.as_ref().is_some_and(|d| match d {
                                BirthDisposition::Live(b) => !history.check_birth(namespace, b),
                                BirthDisposition::Cancelled { scope: actual, .. } => {
                                    actual != &scope
                                }
                            })
                        {
                            return Err(FleetCatalogError::Corrupt("reserved inventory history"));
                        }
                    }
                    (FleetHistoryState::Live, Some(BirthDisposition::Live(birth)))
                        if history.check_birth(namespace, &birth) => {}
                    (
                        FleetHistoryState::Cancelled { operation },
                        Some(BirthDisposition::Cancelled {
                            scope: actual,
                            operation: actual_op,
                        }),
                    ) if actual == scope && actual_op.bytes() == *operation => {}
                    _ => return Err(FleetCatalogError::Corrupt("inventory birth disposition")),
                }
            }
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        if histories != snapshot.head.history_count {
            return Err(FleetCatalogError::Corrupt("fleet inventory count"));
        }
        Ok(FleetInventory {
            revision: snapshot.revision(),
            namespaces: declarations.len(),
            histories,
        })
    }
}
