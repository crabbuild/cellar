// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Human stop consent is rooted intent, not a live-health or shutdown proof.
use super::*;
use serde::Deserialize;

pub(super) const KIND: u8 = 86;
pub(super) const LIMIT: usize = 4096;
pub(super) const PREFIX: &str = "restart-stop/";

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::raft::fleet_catalog::maintenance) struct Consent {
    pub format: u8,
    pub request: FleetRequestIdentity,
    pub ready: FleetRequestIdentity,
    pub input_root: [u8; 32],
    pub gate_parent: [u8; 16],
    pub gate_digest: [u8; 32],
    pub stop: [u8; 16],
    /// First admitted consent fixes the physical selector for its whole life.
    pub anchor: [u8; 16],
    #[serde(deserialize_with = "required_previous")]
    pub previous: Option<[u8; 16]>,
}

impl Consent {
    pub fn key(operation: [u8; 16]) -> String {
        format!("{PREFIX}{}", hex::encode(operation))
    }

    pub fn validate(&self, gate: &Gate) -> Result<()> {
        validate_request(&self.request)?;
        validate_request(&self.ready)?;
        ProposalId::new(self.stop).map_err(|_| FleetCatalogError::Corrupt("restart stop child"))?;
        if self.format != 1
            || self.input_root == [0; 32]
            || self.request.operation != gate.request.operation
            || self.ready.operation != self.request.operation
            || self.request.expected_revision <= self.ready.expected_revision
            || self.ready.expected_revision <= gate.request.expected_revision
            || self.gate_parent != gate.request.id
            || self.gate_digest != gate.digest()?
            || self.anchor == [0; 16]
            || if self.anchor == self.request.id {
                self.previous.is_some()
            } else {
                self.previous
                    .is_none_or(|id| id == [0; 16] || id == self.request.id)
            }
        {
            return Err(FleetCatalogError::Corrupt("restart stop consent"));
        }
        Ok(())
    }

    pub fn matches(&self, request: &FleetRestartStopRequest, gate: &Gate) -> bool {
        self.request == request.identity
            && self.ready == request.ready
            && self.stop == request.stop.bytes()
            && gate.node.node == request.node.bytes()
            && gate.node.boot.as_ref().map(|boot| boot.process)
                == Some(request.expected_process.bytes())
    }

    pub fn selector(&self, gate: &Gate) -> Result<StopSelector> {
        let mut selector = gate.selector(self.stop)?;
        selector.finalization = self.anchor;
        selector
            .validate()
            .map_err(|_| FleetCatalogError::Corrupt("restart stop selector"))?;
        Ok(selector)
    }

    pub fn ready_request(&self, gate: &Gate) -> Result<FleetRestartReadyRequest> {
        Ok(FleetRestartReadyRequest {
            identity: self.ready.clone(),
            node: NodeId::new(gate.node.node)
                .map_err(|_| FleetCatalogError::Corrupt("restart stop node"))?,
            expected_process: ProcessGeneration::new(
                gate.node.boot_binding(gate.preparation.catalog)?.process,
            )
            .map_err(|_| FleetCatalogError::Corrupt("restart stop process"))?,
        })
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

fn required_previous<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<Option<[u8; 16]>, D::Error> {
    Option::<[u8; 16]>::deserialize(decoder)
}
