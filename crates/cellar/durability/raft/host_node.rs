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
    pub dormant_histories: u32,
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

impl Driver {
    pub(super) fn on_control(&mut self, request: ControlRequest) {
        let mut request = match request {
            ControlRequest::Cell(request) => {
                self.on_request(*request);
                return;
            }
            ControlRequest::Node(request) => *request,
        };
        if matches!(&request.kind, NodeKind::Drain) {
            self.start_node_drain(request);
            return;
        }
        if self.closing.load(Ordering::Acquire) || request.control.cancelled() {
            reply_node(&mut request, Err(CommandFailure::Unavailable));
            return;
        }
        match std::mem::replace(&mut request.kind, NodeKind::ControllerStatus) {
            NodeKind::PeerMovement(call) => self.start_peer_movement(request, *call),
            NodeKind::LocalMovement(call) => self.start_local_movement(request, *call),
            NodeKind::PeerFleetStop(call) => self.start_peer_fleet_stop(request, *call),
            NodeKind::InspectPeerFleet(call) => self.start_peer_fleet_inspection(request, *call),
            NodeKind::ObserveFleet {
                history,
                minimum_revision,
            } => self.start_local_fleet_inspection(request, *history, minimum_revision),
            NodeKind::InspectPeer {
                node,
                minimum_revision,
                physical,
            } => self.start_node_inspection(request, node, minimum_revision, physical),
            NodeKind::RegisterHistory(authorization) => {
                self.start_history_registration(request, *authorization)
            }
            NodeKind::Observe => {
                let result = self.node_observation().map(|o| Response::Node(Box::new(o)));
                reply_node(&mut request, result);
            }
            NodeKind::ObservePhysical => {
                let Some(bytes) = self
                    .control_budget
                    .acquire(super::super::physical_inventory::RESPONSE_BYTES)
                else {
                    reply_node(&mut request, Err(CommandFailure::Overloaded));
                    return;
                };
                let result = self
                    .physical_node_observation()
                    .map(|(node, inventory)| Response::PhysicalNode(Box::new((node, inventory))));
                reply_node_charged(&mut request, result, Some(bytes));
            }
            NodeKind::ControllerStatus => self.controller_control(request, None),
            NodeKind::SetControllerEnabled(enabled) => {
                self.controller_control(request, Some(enabled))
            }
            NodeKind::Drain => reply_node(&mut request, Err(CommandFailure::CorruptState)),
        }
    }

    pub(super) fn node_observation(&self) -> Result<RaftHostObservation, CommandFailure> {
        let node = self.capture_node_profile()?;
        let physical = self
            .log
            .group_count()
            .map_err(|_| CommandFailure::Unavailable)?;
        let unattached = physical
            .checked_sub(self.cells.len())
            .ok_or(CommandFailure::CorruptState)?;
        let bounded = |n| u32::try_from(n).map_err(|_| CommandFailure::Overloaded);
        let dormant = self
            .standbys
            .keys()
            .filter(|scope| !self.cells.contains_key(*scope))
            .count();
        let current_enrollment = self
            .directory
            .as_ref()
            .and_then(|d| d.authorization(node.identity.node))
            .map(RaftHostEnrollmentObservation::capture);
        Ok(RaftHostObservation {
            node,
            fleet_boot: self
                .log
                .lifecycle_guard()
                .map_err(|_| CommandFailure::Unavailable)?
                .map(|guard| guard.boot),
            startup_bounds: self.startup_bounds()?,
            startup_enrollment: self
                .options
                .managed
                .as_ref()
                .map(|managed| RaftHostEnrollmentObservation::capture(&managed.authorization)),
            current_enrollment,
            fleet_trust_revision: self
                .directory
                .as_ref()
                .and_then(directory::Directory::rooted_revision),
            declared_namespaces: bounded(self.declared_namespaces.len())?,
            registered_histories: bounded(
                self.cells
                    .len()
                    .checked_add(dormant)
                    .ok_or(CommandFailure::Overloaded)?,
            )?,
            dormant_histories: bounded(dormant)?,
            pending_history_registrations: u32::from(self.history_registration.is_some()),
            unattached_engine_histories: bounded(unattached)?,
            seed_workers: bounded(
                self.seed_archive_work
                    .usage()
                    .1
                    .checked_add(
                        self.transport
                            .resource_observer()
                            .capture()
                            .ok_or(CommandFailure::Unavailable)?
                            .snapshot_source
                            .items,
                    )
                    .ok_or(CommandFailure::Overloaded)?,
            )?,
        })
    }

    pub(super) fn physical_node_observation(
        &self,
    ) -> Result<super::super::physical_inventory::PhysicalNodeObservation, CommandFailure> {
        let inventory = self
            .log
            .physical_inventory(self.activating.as_ref())
            .map_err(|_| CommandFailure::Unavailable)?;
        let node = self.node_observation()?;
        // Engine sync can insert a group between the two lock acquisitions.
        // Refuse a mixed cut rather than silently omitting an occupied slot.
        inventory
            .validate(&node)
            .map_err(|_| CommandFailure::Overloaded)?;
        Ok((node, inventory))
    }

    fn startup_bounds(&self) -> Result<RaftHostStartupBounds, CommandFailure> {
        let runtime = self.runtime.limits();
        let log = self.log.limits();
        let buffers = [
            self.options.max_pending_bytes,
            self.options.max_control_bytes,
            runtime.max_request_bytes,
            runtime.max_event_bytes,
            runtime.max_ready_bytes,
            runtime.max_apply_bytes,
            log.max_pending_bytes,
            log.max_cache_bytes,
        ];
        let bounded_buffer_bytes = buffers
            .into_iter()
            .try_fold(0u64, |sum, bytes| {
                sum.checked_add(u64::try_from(bytes).ok()?)
            })
            .ok_or(CommandFailure::CorruptState)?;
        let count = |n| u32::try_from(n).map_err(|_| CommandFailure::CorruptState);
        Ok(RaftHostStartupBounds {
            host_groups: count(self.options.max_groups)?,
            consensus_groups: count(runtime.max_groups)?,
            engine_groups: count(log.max_groups)?,
            bounded_buffer_bytes,
        })
    }
}

pub(super) fn reply_node(request: &mut NodeRequest, result: Result<Response, CommandFailure>) {
    reply_node_charged(request, result, None);
}

pub(super) fn reply_node_charged(
    request: &mut NodeRequest,
    result: Result<Response, CommandFailure>,
    bytes: Option<Permit>,
) {
    if let Some(reply) = request.reply.take() {
        let _ = reply.send(result.map(|response| client::ChargedResponse {
            response,
            _bytes: bytes,
        }));
    }
}
