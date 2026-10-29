// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded origin-side work; the receiver separately owns admitted mutations.
use super::*;
use crate::raft::fleet_catalog::FleetCatalogError;
use crate::raft::transport::fleet_stops::Received;

pub(super) struct Completed {
    request: Box<NodeRequest>,
    deadline: u64,
    result: Result<Received, FleetCatalogError>,
}

impl Driver {
    pub(super) fn start_peer_fleet_stop(
        &mut self,
        mut request: NodeRequest,
        call: client::PeerStopRequest,
    ) {
        let deadline = call.deadline.min(request.control.deadline());
        let delivery = if self.options.managed.is_none() {
            Err(FleetCatalogError::Invalid("no managed fleet origin"))
        } else {
            self.transport
                .request_fleet_stop(call.request, call.minimum_revision, deadline)
        };
        let delivery = match delivery {
            Ok(delivery) => delivery,
            Err(error) => {
                reply_node(&mut request, Ok(Response::PeerFleetStop(Err(error))));
                return;
            }
        };
        self.jobs.push(Box::pin(async move {
            let result=crate::asyncrt::select_biased! { "origin cancellation cannot roll back peer stop admission";
                _=request.control.cancellation()=>Err(FleetCatalogError::Unconfirmed),
                result=delivery.receive()=>result,
            };
            Job::PeerFleetStop(Box::new(Completed {request:Box::new(request),deadline,result}))
        }));
    }

    pub(super) fn finish_peer_fleet_stop(&mut self, completed: Completed) {
        let Completed {
            mut request,
            deadline,
            result,
        } = completed;
        if request.control.cancelled() || self.closing.load(Ordering::Acquire) {
            return;
        }
        let result = if request.control.before(deadline) {
            result.map(|received| received.acceptance)
        } else {
            Err(FleetCatalogError::Unconfirmed)
        };
        reply_node(&mut request, Ok(Response::PeerFleetStop(result)));
    }
}
