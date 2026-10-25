//! Fleet/process-scoped metadata. There is deliberately no business-cell scope.
use super::fleet_catalog::{FleetNodeState, FleetTrustSnapshot};
use super::host::{RaftHostEnrollmentObservation, RaftHostObservation, RaftHostStartupBounds};
use super::inspection::{NodeInspection, RaftNodeProfile};
use super::inspection_identity::{Endpoint, PhysicalNode};
use super::inspection_wire::Profile;
use super::transport::TransportError;
use super::transport_frame::decimal;
use cellar_logic::consensus::{
    ClusterId, DiskLineage, EngineInstanceId, IncarnationId, NodeId, NodeIdentity,
    ProcessGeneration,
};
use serde::{Deserialize, Serialize};

pub(crate) const MAX_BODY: usize = 16 * 1024;

#[cfg(test)]
#[path = "node_inspection_wire_tests.rs"]
mod tests;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NodeRoute {
    fleet: [u8; 16],
    catalog: [u8; 32],
    cluster: [u8; 16],
    protocol: [u8; 32],
    minimum_revision: String,
    source: Endpoint,
    target: Endpoint,
}

impl NodeRoute {
    pub(crate) fn new(
        trust: &FleetTrustSnapshot,
        minimum_revision: u64,
        source: &PhysicalNode,
        source_label: &str,
        target: &PhysicalNode,
        target_label: &str,
    ) -> Result<Self, TransportError> {
        let route = Self {
            fleet: trust.fleet(),
            catalog: trust.catalog(),
            cluster: trust.cluster().bytes(),
            protocol: crate::application::consensus::protocol::protocol_identity(),
            minimum_revision: minimum_revision.to_string(),
            source: Endpoint::new(source, source_label),
            target: Endpoint::new(target, target_label),
        };
        route.identities()?;
        Ok(route)
    }

    pub(crate) fn identities(&self) -> Result<(PhysicalNode, PhysicalNode), TransportError> {
        IncarnationId::new(self.fleet).map_err(|_| TransportError::Identity)?;
        ClusterId::new(self.cluster).map_err(|_| TransportError::Identity)?;
        let minimum = self.minimum_revision()?;
        let source = self.source.decode()?;
        let target = self.target.decode()?;
        if self.catalog == [0; 32]
            || minimum == 0
            || minimum >= u64::MAX - 2
            || source.identity.node == target.identity.node
            || self.protocol != crate::application::consensus::protocol::protocol_identity()
        {
            return Err(TransportError::Identity);
        }
        Ok((source, target))
    }

    pub(crate) fn minimum_revision(&self) -> Result<u64, TransportError> {
        decimal(&self.minimum_revision)
    }

    pub(crate) fn fleet(&self) -> [u8; 16] {
        self.fleet
    }

    pub(crate) fn catalog(&self) -> [u8; 32] {
        self.catalog
    }

    pub(crate) fn cluster(&self) -> [u8; 16] {
        self.cluster
    }

    pub(crate) fn source_label(&self) -> &str {
        self.source.label()
    }

    pub(crate) fn target_label(&self) -> &str {
        self.target.label()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Enrollment {
    fleet: [u8; 16],
    revision: String,
    epoch: String,
    physical_host: [u8; 16],
    state: FleetNodeState,
    capability_digest: [u8; 32],
}

impl From<&RaftHostEnrollmentObservation> for Enrollment {
    fn from(v: &RaftHostEnrollmentObservation) -> Self {
        Self {
            fleet: v.fleet,
            revision: v.revision.to_string(),
            epoch: v.epoch.to_string(),
            physical_host: v.physical_host.bytes(),
            state: v.state,
            capability_digest: v.capability_digest,
        }
    }
}

impl Enrollment {
    fn decode(self) -> Result<RaftHostEnrollmentObservation, TransportError> {
        IncarnationId::new(self.fleet).map_err(|_| TransportError::Identity)?;
        let revision = decimal(&self.revision)?;
        let epoch = decimal(&self.epoch)?;
        if revision == 0
            || revision >= u64::MAX - 2
            || epoch == 0
            || epoch >= u64::MAX - 2
            || self.capability_digest == [0; 32]
            || self.state == FleetNodeState::Removed
        {
            return Err(TransportError::Protocol);
        }
        Ok(RaftHostEnrollmentObservation {
            fleet: self.fleet,
            revision,
            epoch,
            physical_host: IncarnationId::new(self.physical_host)
                .map_err(|_| TransportError::Identity)?,
            state: self.state,
            capability_digest: self.capability_digest,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Body {
    format: u32,
    node: [u8; 16],
    disk: [u8; 16],
    process: [u8; 16],
    storage_instance: [u8; 16],
    boot: super::fleet_stop::BootBinding,
    bounds: Bounds,
    profile: Profile,
    groups: u32,
    reserved_bytes: String,
    startup: Enrollment,
    current: Enrollment,
    declared_namespaces: u32,
    registered_histories: u32,
    dormant_histories: u32,
    pending_history_registrations: u32,
    unattached_engine_histories: u32,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Bounds {
    host_groups: u32,
    consensus_groups: u32,
    engine_groups: u32,
    bounded_buffer_bytes: String,
}

pub(crate) fn validate(view: &RaftHostObservation) -> Result<(), TransportError> {
    let startup = view
        .startup_enrollment
        .as_ref()
        .ok_or(TransportError::Identity)?;
    let current = view
        .current_enrollment
        .as_ref()
        .ok_or(TransportError::Identity)?;
    let boot = view.fleet_boot.as_ref().ok_or(TransportError::Identity)?;
    boot.validate().map_err(|_| TransportError::Identity)?;
    let bounds = &view.startup_bounds;
    view.node
        .profile
        .validate(4096, 1)
        .map_err(|_| TransportError::Protocol)?;
    if startup.fleet != current.fleet
        || boot.fleet != current.fleet
        || boot.node != view.node.identity.node.bytes()
        || boot.disk != view.node.identity.disk.bytes()
        || boot.process != view.node.identity.process.bytes()
        || boot.engine != view.node.storage_instance.bytes()
        || boot.epoch > current.epoch
        || !(1..=4096).contains(&bounds.host_groups)
        || !(1..=32_768).contains(&bounds.consensus_groups)
        || !(1..=32_768).contains(&bounds.engine_groups)
        || !(1..=64 * 1024 * 1024 * 1024).contains(&bounds.bounded_buffer_bytes)
        || startup.physical_host != current.physical_host
        || startup.capability_digest != current.capability_digest
        || startup.revision > current.revision
        || startup.epoch > current.epoch
        || view.fleet_trust_revision != Some(current.revision)
        || !(1..=64).contains(&view.declared_namespaces)
        || view.registered_histories > 4096
        || view.dormant_histories > view.registered_histories
        || view.pending_history_registrations > 1
        || view.node.groups > 4097
        || view.unattached_engine_histories > view.node.groups
        || view
            .node
            .profile
            .bytes_per_group
            .checked_mul(u64::from(view.node.groups))
            != Some(view.node.reserved_bytes)
    {
        return Err(TransportError::Protocol);
    }
    Ok(())
}

pub(crate) fn encode(view: &RaftHostObservation) -> Result<Vec<u8>, TransportError> {
    validate(view)?;
    let body = Body {
        format: 1,
        node: view.node.identity.node.bytes(),
        disk: view.node.identity.disk.bytes(),
        process: view.node.identity.process.bytes(),
        storage_instance: view.node.storage_instance.bytes(),
        boot: view.fleet_boot.clone().ok_or(TransportError::Identity)?,
        bounds: Bounds {
            host_groups: view.startup_bounds.host_groups,
            consensus_groups: view.startup_bounds.consensus_groups,
            engine_groups: view.startup_bounds.engine_groups,
            bounded_buffer_bytes: view.startup_bounds.bounded_buffer_bytes.to_string(),
        },
        profile: Profile::from(&view.node.profile),
        groups: view.node.groups,
        reserved_bytes: view.node.reserved_bytes.to_string(),
        startup: Enrollment::from(
            view.startup_enrollment
                .as_ref()
                .ok_or(TransportError::Identity)?,
        ),
        current: Enrollment::from(
            view.current_enrollment
                .as_ref()
                .ok_or(TransportError::Identity)?,
        ),
        declared_namespaces: view.declared_namespaces,
        registered_histories: view.registered_histories,
        dormant_histories: view.dormant_histories,
        pending_history_registrations: view.pending_history_registrations,
        unattached_engine_histories: view.unattached_engine_histories,
    };
    let bytes = serde_json::to_vec(&body).map_err(|_| TransportError::Protocol)?;
    if bytes.len() > MAX_BODY {
        return Err(TransportError::Limit);
    }
    Ok(bytes)
}

pub(crate) fn decode(bytes: &[u8]) -> Result<Box<RaftHostObservation>, TransportError> {
    if bytes.is_empty() || bytes.len() > MAX_BODY {
        return Err(TransportError::Limit);
    }
    let body: Body = serde_json::from_slice(bytes).map_err(|_| TransportError::Protocol)?;
    if body.format != 1 {
        return Err(TransportError::Protocol);
    }
    let startup = body.startup.decode()?;
    let current = body.current.decode()?;
    let view = RaftHostObservation {
        fleet_boot: Some(body.boot),
        startup_bounds: RaftHostStartupBounds {
            host_groups: body.bounds.host_groups,
            consensus_groups: body.bounds.consensus_groups,
            engine_groups: body.bounds.engine_groups,
            bounded_buffer_bytes: decimal(&body.bounds.bounded_buffer_bytes)?,
        },
        node: NodeInspection {
            identity: NodeIdentity {
                node: NodeId::new(body.node).map_err(|_| TransportError::Identity)?,
                disk: DiskLineage::new(body.disk).map_err(|_| TransportError::Identity)?,
                process: ProcessGeneration::new(body.process)
                    .map_err(|_| TransportError::Identity)?,
            },
            storage_instance: EngineInstanceId::new(body.storage_instance)
                .map_err(|_| TransportError::Identity)?,
            profile: RaftNodeProfile::from(body.profile),
            groups: body.groups,
            reserved_bytes: decimal(&body.reserved_bytes)?,
        },
        fleet_trust_revision: Some(current.revision),
        startup_enrollment: Some(startup),
        current_enrollment: Some(current),
        declared_namespaces: body.declared_namespaces,
        registered_histories: body.registered_histories,
        dormant_histories: body.dormant_histories,
        pending_history_registrations: body.pending_history_registrations,
        unattached_engine_histories: body.unattached_engine_histories,
    };
    validate(&view)?;
    Ok(Box::new(view))
}
