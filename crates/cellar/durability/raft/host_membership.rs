//! Explicit native learner admission and routing from durable configuration.
//! A receipt is not a seed grant; seed authorization needs a fresh read barrier.

use super::*;
use crate::raft::member_state::JoiningMember;

pub(super) struct PendingConfiguration {
    request: Box<Request>,
    member: JoiningMember,
    accepted: Option<AcceptedProposal>,
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
            routes.push(RaftPeer {
                replica: group.identity().clone(),
                label: label.into(),
                server_name: "localhost".into(),
                control_address: transport.control_address(),
                data_address: transport.data_address(),
                query_address: transport.query_address(),
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
            let endpoint = self
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
                    &self.peers,
                    cell.schema_digest,
                    &self.label,
                    &self.transport,
                )?
                .peers
                .iter()
                .any(|p| p.replica.node == member.node)
            {
                return Err(CommandFailure::CommandConflict);
            }
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
        if !request.control.begin_submission() {
            return;
        }
        let control = request.control.clone();
        if let Some(cell) = self.cells.get_mut(&scope) {
            cell.configuration = Some(PendingConfiguration {
                request: Box::new(request),
                member,
                accepted: None,
            });
        }
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
            let Some(cell) = self.cells.get_mut(&scope) else {
                continue;
            };
            if cell.failed || cell.retired {
                continue;
            }
            let observed = cell.group.read().ok().map(|cache| {
                (
                    cache.membership.revision(),
                    super::super::member_state::members(&cache.conf_state)
                        .contains(&cell.group.identity().member.get()),
                )
            });
            if observed.is_some_and(|(_, active)| !active) {
                cell.retired = true;
                cell.core.close();
                if let Some(mut pending) = cell.configuration.take() {
                    pending.request.control.cancel();
                    reply(&mut pending.request, Err(CommandFailure::Unavailable));
                }
                let _ = self.transport.remove_group(&scope);
                self.refuse(&scope, CommandFailure::Unavailable);
                continue;
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
                continue;
            };
            let Some(accepted) = pending.accepted.as_ref() else {
                continue;
            };
            let outcome = cell.group.read().ok().and_then(|cache| {
                match cache.membership.joined(pending.member.member) {
                    Ok(Some((binding, at))) if binding == pending.member => {
                        (cell.route_revision >= at).then_some(Ok(at))
                    }
                    Ok(Some(_)) => Some(Err(CommandFailure::CommandConflict)),
                    _ if cache.applied.position.index >= accepted.position.index => {
                        Some(Err(CommandFailure::Unavailable))
                    }
                    _ => None,
                }
            });
            if let Some(outcome) = outcome {
                if let Some(mut pending) = cell.configuration.take() {
                    let result = outcome.map(|joined_at| {
                        Response::Learner(client::LearnerReceipt {
                            scope: scope.clone(),
                            member: pending.member.clone(),
                            joined_at,
                        })
                    });
                    reply(&mut pending.request, result);
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
}
