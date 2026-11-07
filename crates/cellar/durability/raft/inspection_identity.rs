// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Inspection authenticates physical processes on both sides; a dormant
//! destination has no voting or learner member identity to put on this route.
use super::transport::TransportError;
use cellar_logic::consensus::{
    DiskLineage, EngineInstanceId, NodeId, NodeIdentity, ProcessGeneration,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PhysicalNode {
    pub identity: NodeIdentity,
    pub storage_instance: EngineInstanceId,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Endpoint {
    node: [u8; 16],
    disk: [u8; 16],
    process: [u8; 16],
    storage_instance: [u8; 16],
    label: String,
}

impl Endpoint {
    pub(crate) fn new(node: &PhysicalNode, label: &str) -> Self {
        Self {
            node: node.identity.node.bytes(),
            disk: node.identity.disk.bytes(),
            process: node.identity.process.bytes(),
            storage_instance: node.storage_instance.bytes(),
            label: label.into(),
        }
    }

    pub(crate) fn label(&self) -> &str {
        &self.label
    }

    pub(crate) fn decode(&self) -> Result<PhysicalNode, TransportError> {
        crate::machine::validate_node_name(&self.label).map_err(|_| TransportError::Identity)?;
        Ok(PhysicalNode {
            identity: NodeIdentity {
                node: NodeId::new(self.node).map_err(|_| TransportError::Identity)?,
                disk: DiskLineage::new(self.disk).map_err(|_| TransportError::Identity)?,
                process: ProcessGeneration::new(self.process)
                    .map_err(|_| TransportError::Identity)?,
            },
            storage_instance: EngineInstanceId::new(self.storage_instance)
                .map_err(|_| TransportError::Identity)?,
        })
    }
}
