//! Catalog capabilities select revision-bound changes; committed group state
//! owns replay protection and the response's durable completion boundary.
use super::*;
use crate::raft::fleet_catalog::FleetPermitAuthorization;
use crate::raft::member_state::{
    FleetMembershipStatus, FleetPermitPhase, GroupMembership, RepairPlan,
};

pub(super) fn check_repair(
    membership: &GroupMembership,
    scope: &GroupScope,
    plan: &RepairPlan,
    authorization: Option<&FleetPermitAuthorization>,
) -> Result<(), CommandFailure> {
    match (membership.fleet_status(), authorization) {
        (None, None) => Ok(()),
        (Some(status), Some(authorization)) => {
            let permit = authorization.permit();
            if !authorization.matches_scope(scope)
                || &status.binding != permit.binding()
                || permit.check_plan(plan).is_err()
            {
                return Err(CommandFailure::CommandConflict);
            }
            if status.permit.as_ref() != Some(permit)
                && (status.consumed_sequence.checked_add(1) != Some(permit.sequence())
                    || permit.expected_revision() != membership.revision())
            {
                return Err(CommandFailure::CommandConflict);
            }
            Ok(())
        }
        (Some(_), None) => Err(CommandFailure::UnsupportedCapability),
        _ => Err(CommandFailure::CommandConflict),
    }
}

enum Change {
    Observed(Box<FleetMembershipStatus>),
    Propose(::raft::prelude::ConfChangeV2, membership::Goal),
}

impl Driver {
    pub(super) fn fleet_configuration(
        &mut self,
        mut request: Request,
        action: client::FleetRequest,
    ) {
        let action = match action {
            client::FleetRequest::Begin {
                authorization,
                plan,
            } => {
                self.start_fleet_admission(request, plan, authorization);
                return;
            }
            client::FleetRequest::Advance {
                authorization,
                step,
                expected,
            } => {
                let id = match authorization.permit().repair_id() {
                    Ok(id) => id,
                    Err(_) => {
                        reply(&mut request, Err(CommandFailure::CommandConflict));
                        return;
                    }
                };
                self.advance_repair(request, id, step, expected, Some(authorization));
                return;
            }
            action => action,
        };
        let outcome = (|| -> Result<Change, CommandFailure> {
            let cell = self
                .cells
                .get(&request.scope)
                .ok_or(CommandFailure::Unavailable)?;
            if cell.failed || cell.retired || cell.incoming.is_some() {
                return Err(CommandFailure::Unavailable);
            }
            if cell.configuration.is_some() {
                return Err(CommandFailure::Overloaded);
            }
            let cache = cell
                .group
                .read()
                .map_err(|_| CommandFailure::CorruptState)?;
            let mut change = ::raft::prelude::ConfChangeV2::default();
            let goal = match action {
                client::FleetRequest::Enroll {
                    authorization,
                    expected,
                } => {
                    if !authorization.matches_scope(&request.scope)
                        || !authorization.matches_birth(&cell.birth, cell.schema_digest)
                    {
                        return Err(CommandFailure::CommandConflict);
                    }
                    let binding = authorization.binding();
                    if let Some(status) = cache.membership.fleet_status() {
                        return if status.binding == *binding {
                            Ok(Change::Observed(Box::new(status)))
                        } else {
                            Err(CommandFailure::CommandConflict)
                        };
                    }
                    if cache.membership.revision() != expected {
                        return Err(CommandFailure::CommandConflict);
                    }
                    cache
                        .membership
                        .bind_fleet_enrollment(&mut change, &cache.conf_state, binding)
                        .map_err(|_| CommandFailure::CommandConflict)?;
                    membership::Goal::Fleet {
                        binding: Box::new(binding.clone()),
                        fence: None,
                    }
                }
                client::FleetRequest::Fence { authorization } => {
                    if !authorization.matches_scope(&request.scope) {
                        return Err(CommandFailure::CommandConflict);
                    }
                    let permit = authorization.permit();
                    if let Some(status) = cache.membership.fleet_status() {
                        if status.permit.as_ref() == Some(permit)
                            && status.phase == Some(FleetPermitPhase::Fenced)
                        {
                            return Ok(Change::Observed(Box::new(status)));
                        }
                    }
                    cache
                        .membership
                        .bind_fleet_fence(&mut change, &cache.conf_state, permit)
                        .map_err(|_| CommandFailure::CommandConflict)?;
                    membership::Goal::Fleet {
                        binding: Box::new(permit.binding().clone()),
                        fence: Some(Box::new(permit.clone())),
                    }
                }
                _ => return Err(CommandFailure::CorruptState),
            };
            Ok(Change::Propose(change, goal))
        })();
        match outcome {
            Ok(Change::Observed(status)) => reply(&mut request, Ok(Response::Fleet(Some(status)))),
            Ok(Change::Propose(change, goal)) => self.propose_configuration(request, change, goal),
            Err(error) => reply(&mut request, Err(error)),
        }
    }
}
