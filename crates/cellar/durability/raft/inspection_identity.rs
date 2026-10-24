//! Inspection authenticates physical processes on both sides; a dormant
//! destination has no voting or learner member identity to put on this route.
use super::transport::TransportError;
use crate::application::consensus::protocol::WireScope;
use cellar_logic::consensus::{
    DiskLineage, EngineInstanceId, GroupScope, NodeId, NodeIdentity, ProcessGeneration,
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

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InspectionRoute {
    scope: WireScope,
    protocol: [u8; 32],
    schema: [u8; 32],
    birth: [u8; 32],
    source: Endpoint,
    target: Endpoint,
}

impl InspectionRoute {
    pub(crate) fn new(
        scope: &GroupScope,
        schema: [u8; 32],
        birth: [u8; 32],
        source: &PhysicalNode,
        source_label: &str,
        target: &PhysicalNode,
        target_label: &str,
    ) -> Result<Self, TransportError> {
        let route = Self {
            scope: WireScope::from(scope),
            protocol: crate::application::consensus::protocol::protocol_identity(),
            schema,
            birth,
            source: Endpoint::new(source, source_label),
            target: Endpoint::new(target, target_label),
        };
        route.decode()?;
        Ok(route)
    }

    pub(crate) fn decode(
        &self,
    ) -> Result<(GroupScope, PhysicalNode, PhysicalNode), TransportError> {
        let scope = self
            .scope
            .clone()
            .decode()
            .map_err(|_| TransportError::Identity)?;
        scope.validate().map_err(|_| TransportError::Identity)?;
        let source = self.source.decode()?;
        let target = self.target.decode()?;
        if self.protocol != crate::application::consensus::protocol::protocol_identity()
            || self.schema == [0; 32]
            || self.birth == [0; 32]
            || source.identity.node == target.identity.node
        {
            return Err(TransportError::Identity);
        }
        Ok((scope, source, target))
    }

    pub(crate) fn schema(&self) -> [u8; 32] {
        self.schema
    }

    pub(crate) fn birth(&self) -> [u8; 32] {
        self.birth
    }

    pub(crate) fn source_label(&self) -> &str {
        &self.source.label
    }

    pub(crate) fn target_label(&self) -> &str {
        &self.target.label
    }
}
