//! Remote quorum metadata owns nested SDK calls through cancellation and join.
use super::*;
use crate::application::consensus::client::PeerFleetRequest;
use crate::raft::fleet_inspection_wire::{History, LocalObservation, RESPONSE_BYTES};
use crate::raft::inspection::{InspectionError, InspectionSnapshot};
use crate::raft::transport::fleet_inspections::Received;
use std::time::Duration;

pub(super) struct PeerCompleted {
    request: Box<NodeRequest>,
    call: PeerFleetRequest,
    bytes: Permit,
    result: Result<Received, InspectionError>,
}

pub(super) struct LocalCompleted {
    request: Box<NodeRequest>,
    bytes: Permit,
    result: Result<(Box<InspectionSnapshot>, LogIndex), CommandFailure>,
}

impl Driver {
    pub(super) fn start_peer_fleet_inspection(
        &mut self,
        mut request: NodeRequest,
        call: PeerFleetRequest,
    ) {
        let admitted = (|| {
            if self.options.managed.is_none() {
                return Err(InspectionError::Refused);
            }
            let bytes = self
                .control_budget
                .acquire(RESPONSE_BYTES)
                .ok_or(InspectionError::Overloaded)?;
            let delivery = self.transport.inspect_fleet_membership(
                call.node,
                &call.authorization,
                call.minimum_revision,
                request.control.deadline(),
            )?;
            Ok((bytes, delivery))
        })();
        let (bytes, delivery) = match admitted {
            Ok(admitted) => admitted,
            Err(error) => {
                reply_node(&mut request, Ok(Response::PeerFleetObservation(Err(error))));
                return;
            }
        };
        self.jobs.push(Box::pin(async move {
            let result = crate::asyncrt::select_biased! { "cancel remote quorum observation before delivery";
                _ = request.control.cancellation() => Err(InspectionError::Unavailable),
                result = delivery.receive() => result,
            };
            Job::PeerFleetInspection(Box::new(PeerCompleted {
                request: Box::new(request), call, bytes, result,
            }))
        }));
    }

    pub(super) fn finish_peer_fleet_inspection(&mut self, completed: PeerCompleted) {
        let PeerCompleted {
            mut request,
            call,
            bytes,
            result,
        } = completed;
        if request.control.cancelled() || self.closing.load(Ordering::Acquire) {
            return;
        }
        let result = result.and_then(|received| {
            self.transport.validate_fleet_node_observation(
                &received.observation.node,
                call.minimum_revision,
            )?;
            let age = self
                .log
                .domain()
                .mono_ms()
                .ok()
                .and_then(|now| now.checked_sub(request.started))
                .ok_or(InspectionError::Unavailable)?;
            received
                .observation
                .into_peer(&call.authorization, age)
                .map(Box::new)
                .map_err(|_| InspectionError::Refused)
        });
        node_control::reply_node_charged(
            &mut request,
            Ok(Response::PeerFleetObservation(result)),
            Some(bytes),
        );
    }

    pub(super) fn start_local_fleet_inspection(
        &mut self,
        mut request: NodeRequest,
        history: History,
        minimum_revision: u64,
    ) {
        let admitted = (|| {
            let host = self
                .options
                .managed
                .as_ref()
                .ok_or(CommandFailure::UnsupportedCapability)?
                .authorization
                .clone();
            let scope = history
                .scope()
                .map_err(|_| CommandFailure::CommandConflict)?;
            let cell = self
                .cells
                .get(&scope)
                .ok_or(CommandFailure::UnsupportedCapability)?;
            if cell.failed || cell.retired {
                return Err(CommandFailure::Unavailable);
            }
            let gateway = RaftCells {
                handle: self.local.clone(),
            };
            let handle = gateway.cell(scope.namespace, &scope.cell)?;
            if handle.scope() != &scope {
                return Err(CommandFailure::CommandConflict);
            }
            let bytes = self
                .control_budget
                .acquire(RESPONSE_BYTES)
                .ok_or(CommandFailure::Overloaded)?;
            Ok((host, scope, handle, bytes))
        })();
        let (host, scope, handle, bytes) = match admitted {
            Ok(admitted) => admitted,
            Err(error) => {
                reply_node(&mut request, Err(error));
                return;
            }
        };
        let domain = self.log.domain();
        self.jobs.push(Box::pin(async move {
            let work = async {
                let remaining = || {
                    let now = domain.mono_ms().map_err(|_| CommandFailure::Unavailable)?;
                    request.control.deadline().checked_sub(now)
                        .filter(|ms| *ms > 0 && *ms <= 60_000)
                        .map(Duration::from_millis).ok_or(CommandFailure::Unavailable)
                };
                let authorization = host.group_authorization_at_least(&scope, minimum_revision, remaining()?).await
                    .map_err(|error| match error {
                        crate::raft::fleet_catalog::FleetCatalogError::Overloaded
                        | crate::raft::fleet_catalog::FleetCatalogError::Limit => CommandFailure::Overloaded,
                        _ => CommandFailure::Unavailable,
                    })?;
                if !history.matches(&authorization) { return Err(CommandFailure::CommandConflict); }
                let proof = handle.observe_fleet_membership(authorization, remaining()?).await?;
                let snapshot = handle.inspect_snapshot(remaining()?).await?;
                if !proof.matches_snapshot(&snapshot) { return Err(CommandFailure::Unavailable); }
                Ok((snapshot, proof.barrier()))
            };
            let result = crate::asyncrt::select_biased! { "cancel nested quorum wait before retaining metadata";
                _ = request.control.cancellation() => Err(CommandFailure::Unavailable),
                result = work => result,
            };
            Job::LocalFleetInspection(Box::new(LocalCompleted {
                request: Box::new(request), bytes, result,
            }))
        }));
    }

    pub(super) fn finish_local_fleet_inspection(&mut self, completed: LocalCompleted) {
        let LocalCompleted {
            mut request,
            bytes,
            result,
        } = completed;
        if request.control.cancelled() || self.closing.load(Ordering::Acquire) {
            return;
        }
        let result = result.and_then(|(snapshot, barrier)| {
            let node = self.node_observation()?;
            if snapshot.view.node.identity != node.node.identity
                || snapshot.view.node.storage_instance != node.node.storage_instance
            {
                return Err(CommandFailure::Unavailable);
            }
            Ok(Response::LocalFleetObservation(Box::new(
                LocalObservation {
                    node: Box::new(node),
                    snapshot,
                    barrier,
                },
            )))
        });
        node_control::reply_node_charged(&mut request, result, Some(bytes));
    }
}
