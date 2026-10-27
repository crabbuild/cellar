// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Pin rooted inputs while allowing unrelated Operations journal progress.
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
    namespaces: u64,
    gate: [u8; 32],
    parent: [u8; 16],
    slot: [u8; 32],
    tables: [Option<[u8; 32]>; 4],
    separation: FleetReplicaSeparation,
}

impl Basis {
    // A current Root may have unrelated Operations progress. Pin every input
    // that this walk actually verified, without turning revision into a lease.
    pub fn input_digest(&self) -> Result<[u8; 32]> {
        Ok(hash(&encode(
            84,
            &(
                self.operation,
                self.histories,
                self.namespaces,
                self.gate,
                self.parent,
                self.slot,
                self.tables,
                self.separation,
            ),
            4096,
        )?))
    }

    pub fn capture(snapshot: &FleetSnapshot, gate: &Gate) -> Result<Self> {
        Ok(Self {
            operation: gate.request.operation,
            revision: snapshot.revision(),
            histories: snapshot.head.history_count,
            namespaces: snapshot.head.namespace_count,
            gate: gate.digest()?,
            parent: gate.request.id,
            slot: snapshot
                .head
                .disruption
                .ok_or(FleetCatalogError::Conflict)?,
            tables: TABLES.map(|table| snapshot.head.tables[table.slot()]),
            separation: snapshot.replica_separation(),
        })
    }

    pub async fn check_current(&self, catalog: &FleetCatalog) -> Result<()> {
        let current = catalog.inner.current().await?;
        if TABLES.map(|table| current.head.tables[table.slot()]) != self.tables
            || current.head.disruption != Some(self.slot)
            || current.head.history_count != self.histories
            || current.head.namespace_count != self.namespaces
            || current.replica_separation() != self.separation
        {
            return Err(FleetCatalogError::Conflict);
        }
        let gate = catalog
            .checked_restart_gate(&current, self.operation)
            .await?;
        if gate.request.id != self.parent || gate.digest()? != self.gate {
            return Err(FleetCatalogError::Conflict);
        }
        catalog
            .require_restart_settlement_idle(&current, self.operation)
            .await
    }
}
