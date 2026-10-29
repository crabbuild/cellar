// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Pin rooted inputs while allowing unrelated Operations journal progress.
use super::*;
use serde::Deserialize;
use std::sync::atomic::{AtomicU64, Ordering};

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
    // Only immutable catalog validation is memoized. Decoding a saved book
    // starts empty; every call still reads Root and checks its input pins.
    #[serde(skip)]
    checked_revision: Arc<AtomicU64>,
}

impl Basis {
    pub fn check_gate(&self, gate: &Gate) -> Result<()> {
        if self.operation != gate.request.operation
            || self.parent != gate.request.id
            || self.gate != gate.digest()?
            || self.slot != gate.disruption
            || self.revision <= gate.request.expected_revision
        {
            return Err(FleetCatalogError::Corrupt("restart candidate gate"));
        }
        Ok(())
    }

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
            checked_revision: Arc::default(),
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
        if self.checked_revision.load(Ordering::Acquire) == current.revision() {
            return Ok(());
        }
        let gate = catalog
            .checked_restart_gate(&current, self.operation)
            .await?;
        if gate.request.id != self.parent || gate.digest()? != self.gate {
            return Err(FleetCatalogError::Conflict);
        }
        catalog
            .require_restart_settlement_idle(&current, self.operation)
            .await?;
        self.checked_revision
            .store(current.revision(), Ordering::Release);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_catalog_memo_cannot_be_restored_or_injected() -> anyhow::Result<()> {
        let basis = Basis {
            operation: [1; 16],
            revision: 9,
            histories: 0,
            namespaces: 0,
            gate: [2; 32],
            parent: [3; 16],
            slot: [4; 32],
            tables: [None; 4],
            separation: FleetReplicaSeparation::PhysicalHosts,
            checked_revision: Arc::new(AtomicU64::new(9)),
        };
        let mut saved = serde_json::to_value(&basis)?;
        assert!(saved.get("checked_revision").is_none());
        let restored: Basis = serde_json::from_value(saved.clone())?;
        assert_eq!(restored.checked_revision.load(Ordering::Acquire), 0);
        assert_eq!(basis.input_digest()?, restored.input_digest()?);
        saved["checked_revision"] = serde_json::json!(9);
        assert!(serde_json::from_value::<Basis>(saved).is_err());
        Ok(())
    }
}
