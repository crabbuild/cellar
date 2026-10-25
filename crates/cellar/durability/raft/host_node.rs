//! Node identity is independent of cell inventory. Both control variants use
//! the same bounded administrative queue and physical waiter ownership.
use super::*;

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
}

/// Authenticated metadata from an exact remote process under the requested
/// minimum trust cut. This grants no capacity activation or quorum authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftPeerNodeObservation {
    pub view: RaftHostObservation,
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
            NodeKind::InspectPeerFleet(call) => self.start_peer_fleet_inspection(request, *call),
            NodeKind::ObserveFleet {
                history,
                minimum_revision,
            } => self.start_local_fleet_inspection(request, *history, minimum_revision),
            NodeKind::InspectPeer {
                node,
                minimum_revision,
            } => self.start_node_inspection(request, node, minimum_revision),
            NodeKind::RegisterHistory(authorization) => {
                self.start_history_registration(request, *authorization)
            }
            NodeKind::Observe => {
                let result = self.node_observation().map(|o| Response::Node(Box::new(o)));
                reply_node(&mut request, result);
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
