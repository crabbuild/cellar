//! Fresh managed moves wait for actual source/target trust acknowledgements.
//! Already committed plans continue under their guard without this admission.
use super::*;
use crate::raft::fleet_catalog::FleetPermitAuthorization;
use crate::raft::inspection::{InspectionError, RESPONSE_RESERVATION};
use crate::raft::member_state::RepairPlan;
use crate::raft::transport::node_inspections::Received;
use std::time::Duration;

pub(super) struct Completed {
    request: Box<Request>,
    plan: RepairPlan,
    authorization: FleetPermitAuthorization,
    result: Result<Received, InspectionError>,
    _bytes: Permit,
}

impl Driver {
    fn check_fleet_placement(
        &self,
        scope: &GroupScope,
        plan: &RepairPlan,
        minimum_revision: u64,
    ) -> Result<(), CommandFailure> {
        let cell = self.cells.get(scope).ok_or(CommandFailure::Unavailable)?;
        // A concurrent caller may have completed this exact admission while
        // its physical trust read was in flight. Continue committed work even
        // after placement exclusion; the ordinary guard is checked again.
        if cell
            .group
            .membership()
            .map_err(|_| CommandFailure::CorruptState)?
            .repair_status()
            .map_err(|_| CommandFailure::CorruptState)?
            .is_some_and(|status| status.plan == *plan)
        {
            return Ok(());
        }
        let route = super::membership::route(
            &cell.group,
            &cell.birth,
            &self.peers,
            cell.schema_digest,
            &self.label,
            &self.transport,
        )?;
        self.transport
            .check_fleet_placement(&route, &plan.replacement, minimum_revision)
            .map_err(|error| match error {
                super::super::transport::TransportError::Identity
                | super::super::transport::TransportError::Refused => {
                    CommandFailure::CommandConflict
                }
                _ => CommandFailure::Unavailable,
            })
    }

    pub(super) fn start_fleet_admission(
        &mut self,
        mut request: Request,
        plan: RepairPlan,
        authorization: FleetPermitAuthorization,
    ) {
        if self.options.managed.is_none() {
            self.begin_repair(request, plan, Some(authorization));
            return;
        }
        let admitted = (|| {
            let cell = self
                .cells
                .get(&request.scope)
                .ok_or(CommandFailure::Unavailable)?;
            if cell.configuration.is_some() {
                return Err(CommandFailure::Overloaded);
            }
            if cell.failed || cell.retired || cell.incoming.is_some() {
                return Err(CommandFailure::Unavailable);
            }
            let membership = cell
                .group
                .membership()
                .map_err(|_| CommandFailure::CorruptState)?;
            super::fleet_control::check_repair(
                &membership,
                &request.scope,
                &plan,
                Some(&authorization),
            )?;
            if membership
                .repair_status()
                .map_err(|_| CommandFailure::CorruptState)?
                .is_some_and(|status| status.plan == plan)
            {
                return Ok(None);
            }
            self.control_budget
                .acquire(RESPONSE_RESERVATION)
                .map(Some)
                .ok_or(CommandFailure::Overloaded)
        })();
        let bytes = match admitted {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                self.begin_repair(request, plan, Some(authorization));
                return;
            }
            Err(error) => {
                reply(&mut request, Err(error));
                return;
            }
        };
        let transport = self.transport.clone();
        let domain = self.log.domain();
        let control = request.control.clone();
        let closing = self.closing.clone();
        let admission = self.local.admission.clone();
        let retry_ms = self
            .options
            .node_directory
            .as_ref()
            .map_or(250, |options| options.poll_ms)
            .clamp(10, 250);
        let deadline = control.deadline();
        let minimum = authorization.minimum_trust_revision();
        self.jobs.push(Box::pin(async move {
            let observe = async {
                loop {
                    if closing.load(Ordering::Acquire) || admission.is_draining() { return Err(InspectionError::Unavailable); }
                    let now = domain.mono_ms().map_err(|_| InspectionError::Unavailable)?;
                    let remaining = deadline.checked_sub(now).filter(|ms| *ms > 0).ok_or(InspectionError::Unavailable)?;
                    // This operation requires both this exact source process
                    // and the target process to have installed the minimum cut.
                    let observed = match transport.inspect_fleet_node(plan.replacement.node, minimum, deadline) {
                        Ok(delivery) => delivery.receive().await,
                        Err(error) => Err(error),
                    };
                    match observed {
                        Ok(received) => return Ok(received),
                        Err(InspectionError::Overloaded) => return Err(InspectionError::Overloaded),
                        Err(InspectionError::Refused | InspectionError::Unavailable | InspectionError::Unreachable | InspectionError::StalePeer) => (),
                    }
                    // Pace retries with discovery rather than spinning TLS
                    // exchanges while the participant's root read is blocked.
                    crate::asyncrt::sleep(Duration::from_millis(remaining.min(retry_ms))).await;
                }
            };
            let result = crate::asyncrt::select_biased! { "cancel fresh fleet admission before a configuration can be submitted";
                _ = control.cancellation() => Err(InspectionError::Unavailable),
                result = observe => result,
            };
            Job::FleetAdmission(Box::new(Completed { request:Box::new(request), plan, authorization, result, _bytes:bytes }))
        }));
    }

    pub(super) fn finish_fleet_admission(&mut self, completed: Completed) {
        let Completed {
            mut request,
            plan,
            authorization,
            result,
            _bytes,
        } = completed;
        if request.control.cancelled() || self.closing.load(Ordering::Acquire) {
            return;
        }
        if self.local.admission.is_draining() {
            reply(&mut request, Err(CommandFailure::Unavailable));
            return;
        }
        let checked = result.and_then(|received| {
            self.transport.validate_fleet_node_observation(
                &received.view,
                authorization.minimum_trust_revision(),
            )?;
            if received.view.node.identity.node != plan.replacement.node
                || received.view.node.identity.disk != plan.replacement.disk
            {
                return Err(InspectionError::StalePeer);
            }
            Ok(())
        });
        match checked {
            Ok(()) => match self.check_fleet_placement(
                &request.scope,
                &plan,
                authorization.minimum_trust_revision(),
            ) {
                Ok(()) => self.begin_repair(*request, plan, Some(authorization)),
                Err(error) => reply(&mut request, Err(error)),
            },
            Err(error) => reply(
                &mut request,
                Err(if error == InspectionError::Overloaded {
                    CommandFailure::Overloaded
                } else {
                    CommandFailure::Unavailable
                }),
            ),
        }
    }
}
