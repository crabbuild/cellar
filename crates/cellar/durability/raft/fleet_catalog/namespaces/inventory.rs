// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bidirectional physical-group inventory. Selected ranges must be rooted in
//! the fleet, and all rooted reservations keep their node/debt references.
use super::*;
use crate::raft::cell_groups::{CellGroupBirthDisposition, CellGroupBirthRequest, RangeDirectory};
use std::collections::BTreeMap;

impl FleetCatalog {
    /// Verify every declared namespace and every retained physical history at
    /// this fleet cut. Caller adapters select neither the coverage set nor its
    /// count. An inventory is not a current eligibility or release certificate.
    pub async fn check_inventory(
        &self,
        snapshot: &FleetSnapshot,
        directories: &[RangeDirectory],
        timeout: Duration,
    ) -> Result<FleetInventory> {
        self.check_snapshot(snapshot)?;
        if directories.len() > MAX_NAMESPACES {
            return Err(FleetCatalogError::Limit);
        }
        self.run(timeout, async {
            self.inner
                .domain
                .run(Box::pin(self.check_inventory_inner(snapshot, directories)))
                .await
                .map_err(|_| FleetCatalogError::Closed)?
        })
        .await
    }

    pub(in super::super) async fn check_inventory_inner(
        &self,
        snapshot: &FleetSnapshot,
        directories: &[RangeDirectory],
    ) -> Result<FleetInventory> {
        let declarations = self.declarations(snapshot).await?;
        let mut catalogs = BTreeMap::new();
        for directory in directories {
            if catalogs
                .insert(directory.namespace_config().namespace.bytes(), directory)
                .is_some()
            {
                return Err(FleetCatalogError::Invalid("duplicate inventory namespace"));
            }
        }
        if catalogs.len() != declarations.len() {
            return Err(FleetCatalogError::InventoryIncomplete);
        }
        let owner = self.birth_owner()?;
        for declaration in &declarations {
            let directory = *catalogs
                .get(&declaration.namespace)
                .ok_or(FleetCatalogError::InventoryIncomplete)?;
            declaration.check(self, directory.namespace(), None)?;
            // Import must pin and certify the complete range directory before
            // it can acquire this owner. The predecessor cell-birth inventory
            // cannot certify a shared namespace.
            if self.managed_owner_for_birth(snapshot, directory).await? != owner {
                return Err(FleetCatalogError::InventoryIncomplete);
            }
            if directory.namespace_config().managed_fleet.is_none() {
                self.verify_imported_inventory(snapshot, directory).await?;
            }
            let view = directory.observe().await.map_err(catalog_error)?;
            if u64::from(view.groups()) > HISTORY_LIMIT {
                return Err(FleetCatalogError::Limit);
            }
            let mut cursor = None;
            let mut count = 0u64;
            loop {
                let page = directory
                    .group_page(&view, cursor.as_ref(), 16)
                    .await
                    .map_err(catalog_error)?;
                for route in page.routes {
                    count = count
                        .checked_add(1)
                        .filter(|n| *n <= u64::from(view.groups()))
                        .ok_or(FleetCatalogError::Corrupt("directory inventory count"))?;
                    let binding = self.binding(&route.history)?;
                    let key = binding
                        .key()
                        .map_err(|_| FleetCatalogError::Corrupt("inventory binding"))?;
                    let history =
                        self.history_inner(snapshot, &key)
                            .await
                            .map_err(|error| match error {
                                FleetCatalogError::Conflict => {
                                    FleetCatalogError::InventoryIncomplete
                                }
                                error => error,
                            })?;
                    let expected = self.inventory_birth(directory, &history)?;
                    if history.binding != binding
                        || expected.route() != &route
                        || !matches!(
                            history.state,
                            FleetHistoryState::Reserved | FleetHistoryState::Live
                        )
                    {
                        return Err(FleetCatalogError::Corrupt("uninventoried selected group"));
                    }
                }
                cursor = page.next;
                if cursor.is_none() {
                    break;
                }
            }
            if count != u64::from(view.groups()) {
                return Err(FleetCatalogError::Corrupt("missing selected group"));
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
                let directory = catalogs[&declaration.namespace];
                if entry.key
                    != history
                        .binding
                        .key()
                        .map_err(|_| FleetCatalogError::Corrupt("inventory key"))?
                    || history.binding != self.binding(&scope)?
                    || history.namespace_root != declaration.root
                    || history.schema_digest != declaration.schema
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
                    if !nodes.iter().any(|node| node.node == member.node) {
                        return Err(FleetCatalogError::InventoryIncomplete);
                    }
                }
                let request = self.inventory_birth(directory, &history)?;
                let birth = directory
                    .group_birth_disposition(&request)
                    .await
                    .map_err(catalog_error)?;
                match (&history.state, birth) {
                    (FleetHistoryState::Reserved, None) if history.allows_managed_genesis() => (),
                    (
                        FleetHistoryState::Reserved | FleetHistoryState::Live,
                        Some(CellGroupBirthDisposition::Live(birth)),
                    ) if history.check_birth(directory, &birth) => (),
                    (
                        FleetHistoryState::Retired {
                            operation,
                            retirement,
                        },
                        disposition,
                    ) => {
                        let materialized = match disposition {
                            Some(CellGroupBirthDisposition::Live(birth))
                                if history.check_birth(directory, &birth) =>
                            {
                                true
                            }
                            None => false,
                            _ => return Err(FleetCatalogError::InventoryIncomplete),
                        };
                        self.require_retired_history(
                            snapshot,
                            directory,
                            request.route(),
                            *operation,
                            *retirement,
                            (&nodes, materialized),
                        )
                        .await?;
                    }
                    (
                        FleetHistoryState::Cancelled { operation },
                        Some(CellGroupBirthDisposition::Cancelled {
                            history: actual,
                            operation: decision,
                        }),
                    ) if actual == scope && decision.bytes() == *operation => (),
                    // Cancellation is a terminal directory decision, not a
                    // caller-supplied enum value or an observation of absence.
                    _ => return Err(FleetCatalogError::InventoryIncomplete),
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

    fn inventory_birth(
        &self,
        directory: &RangeDirectory,
        history: &FleetHistory,
    ) -> Result<CellGroupBirthRequest> {
        let request = directory
            .prepare_fleet_group_birth(history.initial_placement()?, &self.birth_owner()?)
            .map_err(catalog_error)?;
        if request.route().birth_digest != history.birth_digest
            || directory.namespace().root_digest() != history.namespace_root
            || request.route().schema_digest != history.schema_digest
        {
            return Err(FleetCatalogError::Corrupt(
                "inventory immutable group birth",
            ));
        }
        Ok(request)
    }
}
