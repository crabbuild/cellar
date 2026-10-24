use super::{FleetCatalogError, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum FleetTable {
    Policy,
    Nodes,
    Histories,
    Operations,
    Admissions,
}

impl FleetTable {
    pub(super) fn slot(self) -> usize {
        self as usize
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetChange {
    pub table: FleetTable,
    pub key: String,
    pub value: Option<Vec<u8>>,
}

/// One immutable coordination mutation, not a Raft membership permit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetMutation {
    pub id: [u8; 16],
    pub operation: [u8; 16],
    pub expected_revision: u64,
    pub principal: String,
    pub changes: Vec<FleetChange>,
}

impl FleetMutation {
    pub(super) fn validate(&self) -> Result<()> {
        if self.id == [0; 16]
            || self.operation == [0; 16]
            || self.expected_revision == 0
            || self.principal.is_empty()
            || self.principal.len() > 256
            || self.principal.chars().any(char::is_control)
        {
            return Err(FleetCatalogError::Invalid("mutation identity"));
        }
        if self.changes.is_empty() || self.changes.len() > 16 {
            return Err(FleetCatalogError::Limit);
        }
        let mut keys = BTreeSet::new();
        for change in &self.changes {
            validate_key(&change.key)?;
            if change
                .value
                .as_ref()
                .is_some_and(|value| value.len() > 4096)
            {
                return Err(FleetCatalogError::Limit);
            }
            if !keys.insert((change.table, &change.key)) {
                return Err(FleetCatalogError::Invalid("duplicate mutation key"));
            }
        }
        Ok(())
    }
}

pub(super) fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() || key.len() > 256 || key.chars().any(char::is_control) {
        return Err(FleetCatalogError::Invalid("index key"));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Head {
    pub format: u8,
    pub cluster: [u8; 16],
    pub incarnation: [u8; 16],
    pub revision: u64,
    pub tables: [Option<[u8; 32]>; 5],
    pub journal: Option<[u8; 32]>,
}

impl Head {
    pub fn new(cluster: [u8; 16], incarnation: [u8; 16]) -> Self {
        Self {
            format: 1,
            cluster,
            incarnation,
            revision: 1,
            tables: [None; 5],
            journal: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Pin {
    pub cluster: [u8; 16],
    pub incarnation: [u8; 16],
    pub genesis: [u8; 32],
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CommitRecord {
    pub request_digest: [u8; 32],
    pub revision: u64,
}
