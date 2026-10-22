//! Query origins identify a node process, not a voting or learner member.
//! The complete target history remains authenticated in each request/reply.

use super::transport::TransportError;
use crate::application::consensus::protocol::WireScope;
use cellar_logic::consensus::{
    DiskLineage, MemberId, NodeId, NodeIdentity, ProcessGeneration, ReplicaIdentity,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Endpoint {
    node: [u8; 16],
    disk: [u8; 16],
    process: [u8; 16],
    label: String,
    member: Option<String>,
}

impl Endpoint {
    fn new(node: NodeIdentity, label: &str, member: Option<MemberId>) -> Self {
        Self {
            node: node.node.bytes(),
            disk: node.disk.bytes(),
            process: node.process.bytes(),
            label: label.into(),
            member: member.map(|id| id.get().to_string()),
        }
    }

    fn identity(&self) -> Result<NodeIdentity, TransportError> {
        crate::machine::validate_node_name(&self.label).map_err(|_| TransportError::Identity)?;
        Ok(NodeIdentity {
            node: NodeId::new(self.node).map_err(|_| TransportError::Identity)?,
            disk: DiskLineage::new(self.disk).map_err(|_| TransportError::Identity)?,
            process: ProcessGeneration::new(self.process).map_err(|_| TransportError::Identity)?,
        })
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct QueryRoute {
    scope: WireScope,
    schema: [u8; 32],
    source: Endpoint,
    target: Endpoint,
}

impl QueryRoute {
    pub fn new(
        source: NodeIdentity,
        source_label: &str,
        target: &ReplicaIdentity,
        target_label: &str,
        schema: [u8; 32],
    ) -> Result<Self, TransportError> {
        let route = Self {
            scope: WireScope::from(&target.scope),
            schema,
            source: Endpoint::new(source, source_label, None),
            target: Endpoint::new(target.node_identity(), target_label, Some(target.member)),
        };
        route.decode()?;
        Ok(route)
    }

    /// Request orientation only. A reversed response route is compared with
    /// the exact outstanding request; it cannot decode as another request.
    pub fn decode(&self) -> Result<(NodeIdentity, ReplicaIdentity), TransportError> {
        if self.source.member.is_some() {
            return Err(TransportError::Identity);
        }
        let scope = self
            .scope
            .clone()
            .decode()
            .map_err(|_| TransportError::Identity)?;
        scope.validate().map_err(|_| TransportError::Identity)?;
        let member = self
            .target
            .member
            .as_ref()
            .ok_or(TransportError::Identity)?;
        let id = member
            .parse::<u64>()
            .map_err(|_| TransportError::Identity)?;
        if id.to_string() != *member {
            return Err(TransportError::Identity);
        }
        let target = self.target.identity()?;
        Ok((
            self.source.identity()?,
            ReplicaIdentity {
                scope,
                node: target.node,
                disk: target.disk,
                process: target.process,
                member: MemberId::new(id).map_err(|_| TransportError::Identity)?,
            },
        ))
    }

    pub fn schema(&self) -> [u8; 32] {
        self.schema
    }

    pub fn source_label(&self) -> &str {
        &self.source.label
    }

    pub fn target_label(&self) -> &str {
        &self.target.label
    }

    pub fn reversed(&self) -> Self {
        Self {
            scope: self.scope.clone(),
            schema: self.schema,
            source: self.target.clone(),
            target: self.source.clone(),
        }
    }
}
