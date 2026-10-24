//! Node identity is independent of cell inventory. Both control variants use
//! the same bounded administrative queue and physical waiter ownership.
use super::*;

#[derive(Clone, Debug)]
pub struct RaftHostEnrollmentObservation {
    pub fleet: [u8; 16],
    pub revision: u64,
    pub physical_host: IncarnationId,
    pub state: super::super::fleet_catalog::FleetNodeState,
    pub capability_digest: [u8; 32],
}

#[derive(Clone, Debug)]
pub struct RaftHostObservation {
    pub node: super::super::inspection::NodeInspection,
    /// Rooted observation used by this boot, never a fresh eligibility proof.
    pub startup_enrollment: Option<RaftHostEnrollmentObservation>,
    pub declared_namespaces: u32,
    pub registered_histories: u32,
    pub dormant_histories: u32,
    pub pending_history_registrations: u32,
    /// Physical engine slots not attached as running cells, including retained
    /// or retired histories. This count never claims inventory completeness.
    pub unattached_engine_histories: u32,
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

    fn node_observation(&self) -> Result<RaftHostObservation, CommandFailure> {
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
        Ok(RaftHostObservation {
            node,
            startup_enrollment: self.options.managed.as_ref().map(|managed| {
                let auth = &managed.authorization;
                RaftHostEnrollmentObservation {
                    fleet: auth.fleet(),
                    revision: auth.revision(),
                    physical_host: auth.physical_host(),
                    state: auth.state(),
                    capability_digest: auth.capability_digest(),
                }
            }),
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
    if let Some(reply) = request.reply.take() {
        let _ = reply.send(result.map(|response| client::ChargedResponse {
            response,
            _bytes: None,
        }));
    }
}
