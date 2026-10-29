// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Small canonical consent pins the joined predecessor and intended process.
use super::*;
use serde::Deserialize;

pub(super) const KIND: u8 = 88;
pub(super) const LIMIT: usize = 4096;
pub(super) const PREFIX: &str = "restart-return/";

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Return {
    pub format: u8,
    pub request: FleetRequestIdentity,
    pub offline: FleetRequestIdentity,
    pub input_root: [u8; 32],
    pub closure: [u8; 32],
    pub gate_parent: [u8; 16],
    pub gate_digest: [u8; 32],
    pub node: [u8; 16],
    pub old_process: [u8; 16],
    pub process: [u8; 16],
}

impl Return {
    pub fn key(operation: [u8; 16]) -> String {
        format!("{PREFIX}{}", hex::encode(operation))
    }

    pub fn validate(&self, catalog: &FleetCatalog, gate: &Gate) -> Result<()> {
        validate_request(&self.request)?;
        validate_request(&self.offline)?;
        gate.validate(catalog)?;
        ProcessGeneration::new(self.process)
            .map_err(|_| FleetCatalogError::Corrupt("restart return process"))?;
        if self.format != 1
            || self.input_root == [0; 32]
            || self.closure == [0; 32]
            || self.request.operation != gate.request.operation
            || self.offline.operation != self.request.operation
            || self.request.expected_revision <= self.offline.expected_revision
            || self.offline.expected_revision <= gate.request.expected_revision
            || self.gate_parent != gate.request.id
            || self.gate_digest != gate.digest()?
            || self.node != gate.node.node
            || self.old_process != gate.node.boot_binding(gate.preparation.catalog)?.process
            || self.process == self.old_process
        {
            return Err(FleetCatalogError::Corrupt("restart return consent"));
        }
        Ok(())
    }

    pub fn matches(&self, request: &FleetRestartReturnRequest) -> bool {
        self.request == request.identity
            && self.offline == request.offline
            && self.node == request.node.bytes()
            && self.old_process == request.expected_process.bytes()
            && self.process == request.next_process.bytes()
    }

    pub fn digest(&self) -> Result<[u8; 32]> {
        Ok(hash(&encode(KIND, self, LIMIT)?))
    }

    pub fn mutation(&self) -> Result<FleetMutation> {
        Ok(FleetMutation {
            id: self.request.id,
            operation: self.request.operation,
            principal: self.request.principal.clone(),
            expected_revision: self.request.expected_revision,
            changes: vec![FleetChange {
                table: FleetTable::Operations,
                key: Self::key(self.request.operation),
                value: Some(encode(KIND, self, LIMIT)?),
            }],
        })
    }
}
