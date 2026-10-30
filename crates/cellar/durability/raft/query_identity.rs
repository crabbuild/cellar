// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Query origins identify a node process, not a voting or learner member.
//! The complete target history remains authenticated in each request/reply.

use super::transport::TransportError;
use crate::application::consensus::protocol::WireScope;
use cellar_logic::cell_group::{CellHistory, CellId, GroupHistory};
use cellar_logic::consensus::{
    ClusterId, DiskLineage, GroupId, IncarnationId, MemberId, NamespaceId, NodeId, NodeIdentity,
    ProcessGeneration, ReplicaIdentity,
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

// Marker-1 hard cutover: no reader for the former untagged query history.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "identity", deny_unknown_fields)]
enum History {
    Cell(WireScope),
    Shared {
        group: [[u8; 16]; 4],
        cell: String,
        incarnation: [u8; 16],
    },
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct QueryRoute {
    command: bool,
    scope: History,
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
            command: false,
            scope: History::Cell(WireScope::from(&target.scope)),
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
        if self.command {
            return Err(TransportError::Identity);
        }
        let History::Cell(scope) = &self.scope else {
            return Err(TransportError::Identity);
        };
        let scope = scope
            .clone()
            .decode()
            .map_err(|_| TransportError::Identity)?;
        scope.validate().map_err(|_| TransportError::Identity)?;
        self.endpoints(scope)
    }

    fn endpoints<S>(&self, scope: S) -> Result<(NodeIdentity, ReplicaIdentity<S>), TransportError> {
        if self.source.member.is_some() || self.schema == [0; 32] {
            return Err(TransportError::Identity);
        }
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

    pub fn new_shared(
        source: NodeIdentity,
        source_label: &str,
        target: &ReplicaIdentity<GroupHistory>,
        target_label: &str,
        cell: &CellHistory,
        schema: [u8; 32],
    ) -> Result<Self, TransportError> {
        if !target.scope.same_namespace(cell) {
            return Err(TransportError::Identity);
        }
        let g = target.scope;
        let route = Self {
            command: false,
            scope: History::Shared {
                group: [
                    g.cluster.bytes(),
                    g.namespace.bytes(),
                    g.group.bytes(),
                    g.incarnation.bytes(),
                ],
                cell: cell.cell.as_str().into(),
                incarnation: cell.incarnation.bytes(),
            },
            schema,
            source: Endpoint::new(source, source_label, None),
            target: Endpoint::new(target.node_identity(), target_label, Some(target.member)),
        };
        route.decode_shared()?;
        Ok(route)
    }

    pub fn decode_shared(
        &self,
    ) -> Result<(NodeIdentity, ReplicaIdentity<GroupHistory>, CellHistory), TransportError> {
        let History::Shared {
            group,
            cell,
            incarnation,
        } = &self.scope
        else {
            return Err(TransportError::Identity);
        };
        let scope = GroupHistory {
            cluster: ClusterId::new(group[0]).map_err(|_| TransportError::Identity)?,
            namespace: NamespaceId::new(group[1]).map_err(|_| TransportError::Identity)?,
            group: GroupId::new(group[2]).map_err(|_| TransportError::Identity)?,
            incarnation: IncarnationId::new(group[3]).map_err(|_| TransportError::Identity)?,
        };
        let cell = CellHistory {
            cluster: scope.cluster,
            namespace: scope.namespace,
            cell: CellId::new(cell).map_err(|_| TransportError::Identity)?,
            incarnation: IncarnationId::new(*incarnation).map_err(|_| TransportError::Identity)?,
        };
        let (source, target) = self.endpoints(scope)?;
        Ok((source, target, cell))
    }

    pub fn for_command(mut self) -> Result<Self, TransportError> {
        self.decode_shared()?;
        self.command = true;
        Ok(self)
    }

    pub fn is_command(&self) -> bool {
        self.command
    }

    pub fn is_shared(&self) -> bool {
        matches!(self.scope, History::Shared { .. })
    }

    pub fn validate(&self) -> Result<(), TransportError> {
        if self.is_shared() {
            self.decode_shared()?;
        } else {
            self.decode()?;
        }
        Ok(())
    }

    pub fn source(&self) -> Result<NodeIdentity, TransportError> {
        self.validate()?;
        self.source.identity()
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
            command: self.command,
            scope: self.scope.clone(),
            schema: self.schema,
            source: self.target.clone(),
            target: self.source.clone(),
        }
    }
}
