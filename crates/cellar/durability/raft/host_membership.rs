//! Explicit native learner admission and routing from durable configuration.
//! A receipt is not a seed grant; seed authorization needs a fresh read barrier.

use super::*;
use crate::raft::member_state::{JoiningMember, RepairPhase, RepairPlan, RepairStatus, RepairStep};

pub(super) struct PendingConfiguration {
    request: Box<Request>,
    goal: Goal,
    accepted: Option<AcceptedProposal>,
}

pub(super) enum Goal {
    Fleet {
        binding: Box<crate::raft::member_state::FleetGroupBinding>,
        fence: Option<Box<crate::raft::member_state::FleetMembershipPermit>>,
    },
    Learner(JoiningMember),
    Repair {
        plan: RepairPlan,
        phase: RepairPhase,
    },
}

enum PlannedChange {
    Observed(RepairStatus),
    Propose {
        change: ::raft::prelude::ConfChangeV2,
        goal: Goal,
    },
}

fn reached(actual: RepairPhase, wanted: RepairPhase) -> bool {
    actual == wanted
        || wanted == RepairPhase::Seeding
        || (wanted == RepairPhase::Joint && actual == RepairPhase::Complete)
}

fn join_endpoint(
    driver: &Driver,
    cell: &Cell,
    member: &JoiningMember,
) -> Result<(), CommandFailure> {
    let endpoint = driver
        .peers
        .get(&member.node)
        .ok_or(CommandFailure::Unavailable)?;
    endpoint
        .validate_endpoint()
        .map_err(|_| CommandFailure::UnsupportedCapability)?;
    if endpoint.disk != member.disk
        || route(
            &cell.group,
            &cell.birth,
            &driver.peers,
            cell.schema_digest,
            &driver.label,
            &driver.transport,
        )?
        .peers
        .iter()
        .any(|p| p.replica.node == member.node)
    {
        return Err(CommandFailure::CommandConflict);
    }
    Ok(())
}

pub(super) fn route(
    group: &GroupLog,
    birth: &GroupBirth,
    peers: &BTreeMap<NodeId, RaftPeerNode>,
    schema_digest: [u8; 32],
    label: &str,
    transport: &RaftTransport,
) -> Result<GroupRoute, CommandFailure> {
    let cache = group.read().map_err(|_| CommandFailure::CorruptState)?;
    let members = super::super::member_state::members(&cache.conf_state);
    let membership = cache.membership.clone();
    drop(cache);
    let mut routes = Vec::new();
    for id in members {
        let member = MemberId::new(id).map_err(|_| CommandFailure::CorruptState)?;
        let joined = membership
            .joined(member)
            .map_err(|_| CommandFailure::CorruptState)?;
        let node = if let Some((joined, _)) = &joined {
            joined.node
        } else {
            birth
                .voters()
                .iter()
                .chain(birth.learners())
                .find(|m| m.member == member)
                .ok_or(CommandFailure::CorruptState)?
                .node
        };
        if member == group.identity().member {
            if node != group.identity().node || !membership.permits(group.identity()) {
                return Err(CommandFailure::CorruptState);
            }
            let endpoint = peers.get(&node);
            if let Some(peer) = endpoint {
                peer.validate_endpoint()
                    .map_err(|_| CommandFailure::UnsupportedCapability)?;
                if peer.disk != group.identity().disk
                    || peer.process != group.identity().process
                    || peer.label != label
                {
                    return Err(CommandFailure::CommandConflict);
                }
            }
            routes.push(RaftPeer {
                replica: group.identity().clone(),
                label: label.into(),
                server_name: endpoint
                    .map_or_else(|| "localhost".into(), |peer| peer.server_name.clone()),
                control_address: endpoint
                    .map_or_else(|| transport.control_address(), |peer| peer.control_address),
                data_address: endpoint
                    .map_or_else(|| transport.data_address(), |peer| peer.data_address),
                query_address: endpoint
                    .map_or_else(|| transport.query_address(), |peer| peer.query_address),
            });
        } else {
            let peer = peers.get(&node).ok_or(CommandFailure::Unavailable)?;
            if joined
                .as_ref()
                .is_some_and(|(binding, _)| binding.disk != peer.disk)
            {
                return Err(CommandFailure::CommandConflict);
            }
            routes.push(RaftPeer {
                replica: ReplicaIdentity {
                    scope: group.identity().scope.clone(),
                    member,
                    node,
                    disk: peer.disk,
                    process: peer.process,
                },
                label: peer.label.clone(),
                server_name: peer.server_name.clone(),
                control_address: peer.control_address,
                data_address: peer.data_address,
                query_address: peer.query_address,
            });
        }
    }
    Ok(GroupRoute {
        scope: group.identity().scope.clone(),
        schema_digest,
        revision: membership.revision().get(),
        peers: routes,
    })
}

impl Driver {
    pub(super) fn close_configurations(&mut self) {
        for cell in self.cells.values_mut() {
            if let Some(mut pending) = cell.configuration.take() {
                pending.request.control.cancel();
                reply(&mut pending.request, Err(CommandFailure::Unavailable));
            }
        }
    }

    pub(super) fn add_learner(
        &mut self,
        mut request: Request,
        member: JoiningMember,
        expected: LogIndex,
    ) {
        let scope = request.scope.clone();
        let Some(cell) = self.cells.get(&scope) else {
            reply(&mut request, Err(CommandFailure::Unavailable));
            return;
        };
        let validation = (|| {
            if cell.configuration.is_some() {
                return Err(CommandFailure::Overloaded);
            }
            if cell.incoming.is_some() || cell.retired {
                return Err(CommandFailure::Unavailable);
            }
            let membership = cell
                .group
                .membership()
                .map_err(|_| CommandFailure::CorruptState)?;
            if membership.fleet_status().is_some() {
                return Err(CommandFailure::UnsupportedCapability);
            }
            if let Some((existing, at)) = membership
                .joined(member.member)
                .map_err(|_| CommandFailure::CorruptState)?
            {
                if existing != member {
                    return Err(CommandFailure::CommandConflict);
                }
                return Ok((None, Some(at)));
            }
            if membership.revision() != expected || member.member.get() <= membership.high_water() {
                return Err(CommandFailure::CommandConflict);
            }
            join_endpoint(self, cell, &member)?;
            let mut change = ::raft::prelude::ConfChangeV2::default();
            change
                .mut_changes()
                .push(::raft::prelude::ConfChangeSingle {
                    node_id: member.member.get(),
                    change_type: ::raft::prelude::ConfChangeType::AddLearnerNode,
                    ..Default::default()
                });
            membership
                .bind_change(&mut change, std::slice::from_ref(&member))
                .map_err(|_| CommandFailure::CommandConflict)?;
            Ok((Some(change), None))
        })();
        let change = match validation {
            Ok((_, Some(at))) => {
                reply(
                    &mut request,
                    Ok(Response::Learner(client::LearnerReceipt {
                        scope,
                        member,
                        joined_at: at,
                    })),
                );
                return;
            }
            Ok((Some(change), None)) => change,
            Err(error) => {
                reply(&mut request, Err(error));
                return;
            }
            _ => {
                reply(&mut request, Err(CommandFailure::CorruptState));
                return;
            }
        };
        self.propose_configuration(request, change, Goal::Learner(member));
    }

    pub(super) fn begin_repair(
        &mut self,
        mut request: Request,
        plan: RepairPlan,
        authorization: Option<crate::raft::fleet_catalog::FleetPermitAuthorization>,
    ) {
        let result = (|| {
            let cell = self
                .cells
                .get(&request.scope)
                .ok_or(CommandFailure::Unavailable)?;
            if cell.configuration.is_some() {
                return Err(CommandFailure::Overloaded);
            }
            if cell.incoming.is_some() || cell.retired {
                return Err(CommandFailure::Unavailable);
            }
            let observed = {
                let cache = cell
                    .group
                    .read()
                    .map_err(|_| CommandFailure::CorruptState)?;
                super::fleet_control::check_repair(
                    &cache.membership,
                    &request.scope,
                    &plan,
                    authorization.as_ref(),
                )?;
                cache
                    .membership
                    .repair_status()
                    .map_err(|_| CommandFailure::CorruptState)?
            };
            if let Some(status) = observed {
                if status.plan.id == plan.id {
                    if status.plan != plan {
                        return Err(CommandFailure::CommandConflict);
                    }
                    return Ok(PlannedChange::Observed(status));
                }
            }
            join_endpoint(self, cell, &plan.replacement)?;
            // Endpoint inspection reads the group independently. Reacquire only
            // after it returns; recursive read locks can deadlock behind a writer.
            let cache = cell
                .group
                .read()
                .map_err(|_| CommandFailure::CorruptState)?;
            let mut change = ::raft::prelude::ConfChangeV2::default();
            match &authorization {
                Some(authorization) => cache.membership.bind_managed_repair_start(
                    &mut change,
                    &cache.conf_state,
                    &plan,
                    authorization.permit(),
                ),
                None => cache
                    .membership
                    .bind_repair_start(&mut change, &cache.conf_state, &plan),
            }
            .map_err(|_| CommandFailure::CommandConflict)?;
            Ok(PlannedChange::Propose {
                change,
                goal: Goal::Repair {
                    plan,
                    phase: RepairPhase::Seeding,
                },
            })
        })();
        match result {
            Ok(PlannedChange::Propose { change, goal }) => {
                self.propose_configuration(request, change, goal)
            }
            Ok(PlannedChange::Observed(status)) => {
                reply(&mut request, Ok(Response::Repair(Some(status))))
            }
            Err(error) => reply(&mut request, Err(error)),
        }
    }

    pub(super) fn advance_repair(
        &mut self,
        mut request: Request,
        id: cellar_logic::consensus::ProposalId,
        step: RepairStep,
        expected: LogIndex,
        authorization: Option<crate::raft::fleet_catalog::FleetPermitAuthorization>,
    ) {
        let phase = match step {
            RepairStep::PromoteAndRemove => RepairPhase::Joint,
            RepairStep::FinishJoint => RepairPhase::Complete,
            RepairStep::AbortLearner => RepairPhase::Aborted,
        };
        let result = (|| {
            let cell = self
                .cells
                .get(&request.scope)
                .ok_or(CommandFailure::Unavailable)?;
            if cell.configuration.is_some() {
                return Err(CommandFailure::Overloaded);
            }
            if cell.incoming.is_some() || cell.retired {
                return Err(CommandFailure::Unavailable);
            }
            let cache = cell
                .group
                .read()
                .map_err(|_| CommandFailure::CorruptState)?;
            let status = cache
                .membership
                .repair_status()
                .map_err(|_| CommandFailure::CorruptState)?
                .ok_or(CommandFailure::CommandConflict)?;
            super::fleet_control::check_repair(
                &cache.membership,
                &request.scope,
                &status.plan,
                authorization.as_ref(),
            )?;
            if status.plan.id != id {
                return Err(CommandFailure::CommandConflict);
            }
            if reached(status.phase, phase) {
                return Ok(PlannedChange::Observed(status));
            }
            if cache.membership.revision() != expected {
                return Err(CommandFailure::CommandConflict);
            }
            let mut change = ::raft::prelude::ConfChangeV2::default();
            match &authorization {
                Some(authorization) => cache.membership.bind_managed_repair_step(
                    &mut change,
                    &cache.conf_state,
                    id,
                    step,
                    authorization.permit(),
                ),
                None => cache
                    .membership
                    .bind_repair_step(&mut change, &cache.conf_state, id, step),
            }
            .map_err(|_| CommandFailure::CommandConflict)?;
            Ok(PlannedChange::Propose {
                change,
                goal: Goal::Repair {
                    plan: status.plan,
                    phase,
                },
            })
        })();
        match result {
            Ok(PlannedChange::Propose { change, goal }) => {
                self.propose_configuration(request, change, goal)
            }
            Ok(PlannedChange::Observed(status)) => {
                reply(&mut request, Ok(Response::Repair(Some(status))))
            }
            Err(error) => reply(&mut request, Err(error)),
        }
    }

    pub(super) fn propose_configuration(
        &mut self,
        mut request: Request,
        change: ::raft::prelude::ConfChangeV2,
        goal: Goal,
    ) {
        let scope = request.scope.clone();
        let Some(cell) = self.cells.get_mut(&scope) else {
            reply(&mut request, Err(CommandFailure::Unavailable));
            return;
        };
        if !request.control.begin_submission() {
            return;
        }
        let control = request.control.clone();
        cell.configuration = Some(PendingConfiguration {
            request: Box::new(request),
            goal,
            accepted: None,
        });
        let runtime = self.runtime.clone();
        self.jobs.push(Box::pin(async move {
            let result = if control.cancelled() {
                Err(RuntimeError::Closed)
            } else {
                runtime.configure(&scope, vec![], change).await
            };
            Job::Configured { scope, result }
        }));
    }

    pub(super) fn configured(
        &mut self,
        scope: GroupScope,
        result: Result<AcceptedProposal, RuntimeError>,
    ) {
        let Some(cell) = self.cells.get_mut(&scope) else {
            return;
        };
        let Some(mut pending) = cell.configuration.take() else {
            return;
        };
        match result {
            Ok(accepted) => {
                pending.accepted = Some(accepted);
                cell.configuration = Some(pending);
            }
            Err(error) => {
                let failure = match error {
                    RuntimeError::NotLeader => CommandFailure::NotLeader { leader: None },
                    RuntimeError::Overloaded => CommandFailure::Overloaded,
                    RuntimeError::Invalid(_) => CommandFailure::CommandConflict,
                    _ => CommandFailure::Unavailable,
                };
                reply(&mut pending.request, Err(failure));
            }
        }
    }

    pub(super) fn pump_membership(&mut self) {
        for scope in self.cells.keys().cloned().collect::<Vec<_>>() {
            self.reconcile_membership(&scope);
        }
    }

    // A request must observe completed configuration work before testing
    // admission, even between periodic inventory scans.
    pub(super) fn reconcile_membership(&mut self, scope: &GroupScope) {
        let Some(cell) = self.cells.get_mut(scope) else {
            return;
        };
        if cell.failed || cell.retired {
            return;
        }
        let observed = cell.group.read().ok().map(|cache| {
            (
                cache.membership.revision(),
                cache
                    .conf_state
                    .voters
                    .iter()
                    .chain(&cache.conf_state.voters_outgoing)
                    .chain(&cache.conf_state.learners)
                    .chain(&cache.conf_state.learners_next)
                    .any(|member| *member == cell.group.identity().member.get()),
            )
        });
        if observed.is_some_and(|(_, active)| !active) {
            cell.retired = true;
            cell.core.close();
            if let Some(mut pending) = cell.configuration.take() {
                pending.request.control.cancel();
                reply(&mut pending.request, Err(CommandFailure::Unavailable));
            }
            let _ = self.transport.remove_group(scope);
            self.refuse(scope, CommandFailure::Unavailable);
            return;
        }
        if observed.is_some_and(|(revision, _)| revision != cell.route_revision) {
            if let Ok(route) = route(
                &cell.group,
                &cell.birth,
                &self.peers,
                cell.schema_digest,
                &self.label,
                &self.transport,
            ) {
                let revision = LogIndex::new(route.revision);
                if self.transport.install_group(route).is_ok() {
                    cell.route_revision = revision;
                }
            }
        }
        let Some(pending) = cell.configuration.as_ref() else {
            return;
        };
        let Some(accepted) = pending.accepted.as_ref() else {
            return;
        };
        let outcome = cell
            .group
            .read()
            .ok()
            .and_then(|cache| match &pending.goal {
                Goal::Fleet { binding, fence } => match cache.membership.fleet_status() {
                    Some(status)
                        if &status.binding == binding.as_ref()
                            && fence.as_ref().is_none_or(|permit| {
                                status.permit.as_ref() == Some(permit.as_ref())
                                    && status.phase
                                        == Some(crate::raft::member_state::FleetPermitPhase::Fenced)
                            }) =>
                    {
                        (cell.route_revision >= status.changed_at)
                            .then_some(Ok(Response::Fleet(Some(Box::new(status)))))
                    }
                    _ if cache.applied.position.index >= accepted.position.index => {
                        Some(Err(CommandFailure::CommandConflict))
                    }
                    _ => None,
                },
                Goal::Learner(member) => match cache.membership.joined(member.member) {
                    Ok(Some((binding, at))) if binding == *member => (cell.route_revision >= at)
                        .then_some(Ok(Response::Learner(client::LearnerReceipt {
                            scope: scope.clone(),
                            member: member.clone(),
                            joined_at: at,
                        }))),
                    Ok(Some(_)) => Some(Err(CommandFailure::CommandConflict)),
                    _ if cache.applied.position.index >= accepted.position.index => {
                        Some(Err(CommandFailure::Unavailable))
                    }
                    _ => None,
                },
                Goal::Repair { plan, phase } => match cache.membership.repair_status() {
                    Ok(Some(status)) if status.plan == *plan && reached(status.phase, *phase) => {
                        (cell.route_revision >= status.changed_at)
                            .then_some(Ok(Response::Repair(Some(status))))
                    }
                    // A prior terminal repair can remain visible after this
                    // proposal is accepted and before it applies. It cannot
                    // establish a conflict with the submitted proposal.
                    Ok(Some(status))
                        if cache.applied.position.index >= accepted.position.index
                            && (status.plan != *plan || !status.phase.active()) =>
                    {
                        Some(Err(CommandFailure::CommandConflict))
                    }
                    Err(_) => Some(Err(CommandFailure::CorruptState)),
                    _ if cache.applied.position.index >= accepted.position.index => {
                        Some(Err(CommandFailure::Unavailable))
                    }
                    _ => None,
                },
            });
        if let Some(outcome) = outcome {
            if let Some(mut pending) = cell.configuration.take() {
                reply(&mut pending.request, outcome);
            }
        } else if pending.request.control.cancelled() {
            // The Raft runtime now owns the submitted proposal. Releasing
            // this waiter never discards it or proves it did not commit.
            if let Some(mut pending) = cell.configuration.take() {
                reply(&mut pending.request, Err(CommandFailure::Unavailable));
            }
        }
    }
}
