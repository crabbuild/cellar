// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! All nested work belongs to the host job set. Cancellation drops its waiter;
//! committed configuration and physical initialization retain their owners.
use super::*;
use crate::raft::fleet_catalog::FleetCatalogError;
use crate::raft::transport::fleet_moves::Delivery;
use std::time::Duration;

pub(super) struct Completed {
    request: Box<NodeRequest>,
    result: Result<(), FleetCatalogError>,
    _memory: Option<Permit>,
}

impl Driver {
    pub(super) fn start_movement_service(
        &mut self,
        mut request: NodeRequest,
        mut work: crate::raft::fleet_catalog::MovementServiceWork,
    ) {
        let result = self
            .options
            .managed
            .as_ref()
            .ok_or(FleetCatalogError::Invalid(
                "no managed movement service origin",
            ))
            .and_then(|managed| work.admit(&managed.authorization));
        if result.is_ok() {
            self.jobs.push(Box::pin(async move {
                work.run().await;
                Job::FleetMovementServiceStopped
            }));
        }
        reply_node(&mut request, Ok(Response::FleetMovementAction(result)));
    }

    pub(super) fn start_peer_movement(
        &mut self,
        mut request: NodeRequest,
        call: client::PeerMovementCall,
    ) {
        if call.command.target.node == self.log.limits().node.bytes() {
            let origin = self
                .log
                .lifecycle_guard()
                .ok()
                .flatten()
                .map(|guard| guard.boot);
            if let Some(origin) = origin {
                self.start_local_movement(
                    request,
                    client::LocalMovementCall {
                        command: call.command,
                        minimum_revision: call.minimum_revision,
                        origin,
                    },
                );
            } else {
                reply_node(
                    &mut request,
                    Ok(Response::FleetMovementAction(Err(
                        FleetCatalogError::Closed,
                    ))),
                );
            }
            return;
        }
        let delivery = if self.options.managed.is_some() {
            self.transport.request_movement(
                call.command,
                call.minimum_revision,
                request.control.deadline(),
            )
        } else {
            Err(FleetCatalogError::Invalid("no managed movement origin"))
        };
        let delivery: Delivery = match delivery {
            Ok(delivery) => delivery,
            Err(error) => {
                reply_node(&mut request, Ok(Response::FleetMovementAction(Err(error))));
                return;
            }
        };
        self.jobs.push(Box::pin(async move {
            let result = crate::asyncrt::select_biased! { "cancel movement origin without assuming rollback";
                _ = request.control.cancellation() => Err(FleetCatalogError::Unconfirmed),
                result = delivery.receive() => result,
            };
            Job::FleetMovement(Box::new(Completed { request: Box::new(request), result, _memory: None }))
        }));
    }

    pub(super) fn start_local_movement(
        &mut self,
        mut request: NodeRequest,
        call: client::LocalMovementCall,
    ) {
        let setup = (|| {
            call.command.validate()?;
            let host = self
                .options
                .managed
                .as_ref()
                .ok_or(FleetCatalogError::Invalid("no managed receiver"))?
                .authorization
                .clone();
            let boot = self
                .log
                .lifecycle_guard()
                .map_err(|_| FleetCatalogError::Closed)?
                .ok_or(FleetCatalogError::Closed)?
                .boot;
            if boot != call.command.target {
                return Err(FleetCatalogError::Conflict);
            }
            let memory = self
                .control_budget
                .acquire(128 * 1024)
                .ok_or(FleetCatalogError::Overloaded)?;
            Ok((host, memory))
        })();
        let (host, memory) = match setup {
            Ok(setup) => setup,
            Err(error) => {
                reply_node(&mut request, Ok(Response::FleetMovementAction(Err(error))));
                return;
            }
        };
        let gateway = RaftCells {
            handle: self.local.clone(),
        };
        let domain = self.log.domain();
        self.jobs.push(Box::pin(async move {
            let work = async {
                let remaining = request.control.deadline().checked_sub(domain.mono_ms().map_err(|_| FleetCatalogError::Closed)?)
                    .filter(|ms| *ms > 0).map(Duration::from_millis).ok_or(FleetCatalogError::Unconfirmed)?;
                host.execute_movement(call.command, call.origin, call.minimum_revision, &gateway, remaining).await
            };
            let result = crate::asyncrt::select_biased! { "cancel receiver waiter while physical owners retain admitted work";
                _ = request.control.cancellation() => Err(FleetCatalogError::Unconfirmed),
                result = work => result,
            };
            Job::FleetMovement(Box::new(Completed { request: Box::new(request), result, _memory: Some(memory) }))
        }));
    }

    pub(super) fn finish_fleet_movement(&mut self, completed: Completed) {
        let Completed {
            mut request,
            result,
            _memory,
        } = completed;
        if request.control.cancelled() || self.closing.load(Ordering::Acquire) {
            return;
        }
        let result = if request.control.before(request.control.deadline()) {
            result
        } else {
            Err(FleetCatalogError::Unconfirmed)
        };
        reply_node(&mut request, Ok(Response::FleetMovementAction(result)));
    }
}
