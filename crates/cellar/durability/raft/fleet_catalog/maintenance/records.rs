// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The immutable admission also owns the original shared disruption reference.
use super::super::nodes::{NodeRecord, NODE_KIND};
use super::*;
use serde::Deserialize;

pub(in crate::raft::fleet_catalog) const KIND: u8 = 76;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::raft::fleet_catalog) struct Record {
    pub format: u8,
    pub catalog: [u8; 32],
    pub request: FleetRequestIdentity,
    pub input_root: [u8; 32],
    pub namespaces: usize,
    pub histories: u64,
    pub grace_seconds: u16,
    pub node: NodeRecord,
}

impl Record {
    pub fn validate(&self, catalog: &FleetCatalog) -> Result<()> {
        validate_request(&self.request)?;
        self.node.validate(&catalog.inner)?;
        if self.format != 1
            || self.catalog != catalog.birth_owner()?.catalog
            || self.input_root == [0; 32]
            || !(1..=namespaces::MAX_NAMESPACES).contains(&self.namespaces)
            || self.histories > HISTORY_LIMIT
            || !(1..=300).contains(&self.grace_seconds)
            || self.node.state != FleetNodeState::RestartPending
            || self.node.operation() != Some(self.request.operation)
            || self.node.isolation.is_some()
            || self
                .node
                .boot
                .as_ref()
                .is_none_or(|boot| boot.epoch >= self.node.epoch)
            || self.node.storage_instance.is_none()
            || self.request.expected_revision.checked_add(1).is_none()
        {
            return Err(FleetCatalogError::Corrupt("restart admission"));
        }
        Ok(())
    }

    pub fn matches(&self, request: &FleetRestartRequest) -> bool {
        self.request == request.identity
            && self.node.node == request.node.bytes()
            && self.node.boot.as_ref().map(|b| b.process) == Some(request.expected_process.bytes())
            && self.grace_seconds == request.grace_seconds
    }

    pub fn mutation(&self) -> Result<FleetMutation> {
        let bytes = encode(KIND, self, 4096)?;
        Ok(FleetMutation {
            id: self.request.id,
            operation: self.request.operation,
            principal: self.request.principal.clone(),
            expected_revision: self.request.expected_revision,
            changes: vec![
                FleetChange {
                    table: FleetTable::Nodes,
                    key: hex::encode(self.node.node),
                    value: Some(encode(NODE_KIND, &self.node, 4096)?),
                },
                FleetChange {
                    table: FleetTable::Operations,
                    key: hex::encode(self.request.operation),
                    value: Some(bytes.clone()),
                },
                FleetChange {
                    table: FleetTable::Policy,
                    key: super::super::disruption::SLOT_KEY.into(),
                    value: Some(bytes),
                },
            ],
        })
    }

    pub fn check_basis(&self, snapshot: &FleetSnapshot) -> Result<()> {
        if self.request.expected_revision != snapshot.revision()
            || self.input_root != hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?)
        {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(())
    }
}
