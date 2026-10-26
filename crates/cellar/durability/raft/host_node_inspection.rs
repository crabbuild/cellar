//! Remote metadata retains control response ownership through caller delivery.
use super::*;
use crate::raft::inspection::{InspectionError, RESPONSE_RESERVATION};
use crate::raft::transport::node_inspections::Received;

pub(super) struct Completed {
    request: Box<NodeRequest>,
    started: u64,
    minimum_revision: u64,
    physical: bool,
    bytes: Permit,
    result: Result<Received, InspectionError>,
}

impl Driver {
    pub(super) fn start_node_inspection(
        &mut self,
        mut request: NodeRequest,
        node: NodeId,
        minimum_revision: u64,
        physical: bool,
    ) {
        let admitted = (|| {
            if self.options.managed.is_none() {
                return Err(InspectionError::Refused);
            }
            let bytes = self
                .control_budget
                .acquire(if physical {
                    crate::raft::physical_inventory::RESPONSE_BYTES
                } else {
                    RESPONSE_RESERVATION
                })
                .ok_or(InspectionError::Overloaded)?;
            let started = request.started;
            let delivery = self.transport.inspect_fleet_node(
                node,
                minimum_revision,
                request.control.deadline(),
                physical,
            )?;
            Ok((bytes, started, delivery))
        })();
        let (bytes, started, delivery) = match admitted {
            Ok(admitted) => admitted,
            Err(error) => {
                reply_node(&mut request, Ok(Response::PeerNodeInspection(Err(error))));
                return;
            }
        };
        self.jobs.push(Box::pin(async move {
            let result = crate::asyncrt::select_biased! { "cancel node inspection before retaining a reply";
                _ = request.control.cancellation() => Err(InspectionError::Unavailable),
                result = delivery.receive() => result,
            };
            Job::NodeInspection(Box::new(Completed { request:Box::new(request), started, minimum_revision, physical, bytes, result }))
        }));
    }

    pub(super) fn finish_node_inspection(&mut self, completed: Completed) {
        let Completed {
            mut request,
            started,
            minimum_revision,
            physical,
            bytes,
            result,
        } = completed;
        if request.control.cancelled() || self.closing.load(Ordering::Acquire) {
            return;
        }
        let result = result.and_then(|received| {
            if received.inventory.is_some() != physical {
                return Err(InspectionError::Refused);
            }
            self.transport
                .validate_fleet_node_observation(&received.view, minimum_revision)?;
            let sample_age_ms = self
                .log
                .domain()
                .mono_ms()
                .ok()
                .and_then(|now| now.checked_sub(started))
                .ok_or(InspectionError::Unavailable)?;
            Ok(Box::new(RaftPeerNodeObservation {
                view: *received.view,
                inventory: received.inventory,
                sample_age_ms,
            }))
        });
        node_control::reply_node_charged(
            &mut request,
            Ok(Response::PeerNodeInspection(result)),
            Some(bytes),
        );
    }
}
