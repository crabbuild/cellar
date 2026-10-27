// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Current returned-boot inputs; unrelated Operations progress is not a lease.
use super::*;
use serde::Deserialize;

const TABLES: [FleetTable; 4] = [
    FleetTable::Policy,
    FleetTable::Nodes,
    FleetTable::Histories,
    FleetTable::Admissions,
];

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Basis {
    pub operation: [u8; 16],
    pub revision: u64,
    pub histories: u64,
    pub namespaces: u64,
    pub parent: [u8; 16],
    pub returned: [u8; 32],
    pub slot: [u8; 32],
    tables: [Option<[u8; 32]>; 4],
    separation: FleetReplicaSeparation,
}

impl Basis {
    pub fn capture(snapshot: &FleetSnapshot, cut: &ReturnedCut) -> Self {
        Self {
            operation: cut.boot_request.operation,
            revision: snapshot.revision(),
            histories: snapshot.head.history_count,
            namespaces: snapshot.head.namespace_count,
            parent: cut.boot_request.id,
            returned: cut.digest,
            slot: cut.slot,
            tables: TABLES.map(|t| snapshot.head.tables[t.slot()]),
            separation: snapshot.replica_separation(),
        }
    }

    pub fn digest(&self) -> Result<[u8; 32]> {
        Ok(hash(&encode(
            94,
            &(
                self.operation,
                self.histories,
                self.namespaces,
                self.parent,
                self.returned,
                self.slot,
                self.tables,
                self.separation,
            ),
            4096,
        )?))
    }

    pub fn check_current<'a>(&'a self, catalog: &'a FleetCatalog) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let now = catalog.inner.current().await?;
            if self.tables != TABLES.map(|t| now.head.tables[t.slot()])
                || now.head.disruption != Some(self.slot)
                || now.head.history_count != self.histories
                || now.head.namespace_count != self.namespaces
                || now.replica_separation() != self.separation
            {
                return Err(FleetCatalogError::Conflict);
            }
            let cut = catalog.checked_returned_cut(&now, self.operation).await?;
            if cut.boot_request.id != self.parent
                || cut.digest != self.returned
                || cut.slot != self.slot
            {
                return Err(FleetCatalogError::Conflict);
            }
            Ok(())
        })
    }
}
