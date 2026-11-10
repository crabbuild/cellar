//! Node identity is independent of cell inventory. Both control variants use
//! the same bounded administrative queue and physical waiter ownership.
use super::*;

/// Bounds installed in the running owners. Buffer bounds cover the listed
/// host/consensus/log pools, not process RSS or independently observed limits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftHostStartupBounds {
    pub host_groups: u32,
    pub consensus_groups: u32,
    pub engine_groups: u32,
    pub bounded_buffer_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftHostEnrollmentObservation {
    pub fleet: [u8; 16],
    pub revision: u64,
    pub epoch: u64,
    pub physical_host: IncarnationId,
    pub state: super::super::fleet_catalog::FleetNodeState,
    pub capability_digest: [u8; 32],
}

impl RaftHostEnrollmentObservation {
    pub(crate) fn capture(auth: &super::super::fleet_catalog::FleetHostAuthorization) -> Self {
        Self {
            fleet: auth.fleet(),
            revision: auth.revision(),
            epoch: auth.epoch(),
            physical_host: auth.physical_host(),
            state: auth.state(),
            capability_digest: auth.capability_digest(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftHostObservation {
    pub node: super::super::inspection::NodeInspection,
    /// Synced physical boot, independent of a caller-provided process hint.
    pub fleet_boot: Option<super::super::fleet_stop::BootBinding>,
    pub startup_bounds: RaftHostStartupBounds,
    /// Rooted observation used by this boot, never a fresh eligibility proof.
    pub startup_enrollment: Option<RaftHostEnrollmentObservation>,
    /// Enrollment in the installed current trust cut. It may differ from boot.
    pub current_enrollment: Option<RaftHostEnrollmentObservation>,
    /// Last complete rooted trust cut installed by the owned directory worker.
    /// This acknowledgement grants neither placement eligibility nor quorum.
    pub fleet_trust_revision: Option<u64>,
    pub declared_namespaces: u32,
    pub registered_histories: u32,
    /// Registered groups unavailable for application work, including quarantine.
    pub dormant_histories: u32,
    /// Pending physical attachment/provisioning operations. This is independent
    /// of durable engine occupancy and is not limited to one business cell.
    pub pending_history_registrations: u32,
    /// Physical engine slots not attached as running cells, including retained
    /// or retired histories. This count never claims inventory completeness.
    pub unattached_engine_histories: u32,
    /// Captures/transfers and metadata jobs retained until their owners exit.
    /// Zero alone is not terminal membership or a reservation release grant.
    pub seed_workers: u32,
}

/// Authenticated metadata from an exact remote process under the requested
/// minimum trust cut. This grants no capacity activation or quorum authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftPeerNodeObservation {
    pub view: RaftHostObservation,
    /// Complete immutable engine occupancy when explicitly requested. A normal
    /// node probe omits it; omission is never an empty physical inventory.
    pub inventory: Option<Box<super::super::physical_inventory::RaftPhysicalInventory>>,
    /// Complete origin duration through SDK delivery, including control queue
    /// residence and retention by an unpolled caller.
    pub sample_age_ms: u64,
}
