// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Current returned cut and all-voter matched checks at the serialized leader.
use super::super::runtime::LeadershipAction;
use super::*;
use crate::raft::completion::RestartLeadershipCheck;
use crate::raft::fleet_catalog::FleetExecutionCommand;
use crate::raft::fleet_stop::BootBinding;
use crate::raft::member_state::FleetMembershipPermit;
use cellar_logic::consensus::{LogIndex, Term};
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResumeLeadershipCommand {
    pub command: RestartLeadershipCommand,
    pub paused: bool,
}

impl ResumeLeadershipCommand {
    pub(crate) fn validate(&self) -> Result<()> {
        self.command.validate()?;
        if !self.paused && matches!(self.command.action, LeadershipAction::Transfer { .. }) {
            return Err(FleetCatalogError::Invalid("open resume transfer"));
        }
        Ok(())
    }

    pub(crate) fn target(&self) -> &BootBinding {
        &self.command.context.target
    }
}

struct Input {
    cut: Box<ReturnedCut>,
    basis: basis::Basis,
    history: FleetHistory,
    voters: BTreeMap<NodeId, MemberId>,
    permit: Option<FleetMembershipPermit>,
}

impl FleetCatalog {
    fn resume_leadership_input<'a>(
        &'a self,
        snapshot: &'a FleetSnapshot,
        request: &'a ResumeLeadershipCommand,
    ) -> BoxFuture<'a, Result<Input>> {
        Box::pin(async move {
            request.validate()?;
            let command = &request.command;
            let cut = self
                .checked_returned_cut(snapshot, command.context.operation)
                .await?;
            let basis = basis::Basis::capture(snapshot, &cut);
            if command.context.parent != cut.boot_request.id
                || command.context.inputs != basis.digest()?
            {
                return Err(FleetCatalogError::Conflict);
            }
            let history = self.history_inner(snapshot, &command.context.group).await?;
            if *history.state() != FleetHistoryState::Live
                || hash(&history.encode()?) != command.context.history
            {
                return Err(FleetCatalogError::Conflict);
            }
            let (voters, permit) = self.settled_group_voters(snapshot, &history).await?;
            let source = NodeId::new(command.context.target.node)
                .map_err(|_| FleetCatalogError::Invalid("resume leader"))?;
            let main = NodeId::new(cut.node.node)
                .map_err(|_| FleetCatalogError::Corrupt("returned node"))?;
            let host = self.host_from_record(snapshot, source).await?;
            if !voters.contains_key(&source)
                || host.boot_binding()? != command.context.target
                || host.state()
                    != if source == main {
                        FleetNodeState::RestartPending
                    } else {
                        FleetNodeState::Active
                    }
            {
                return Err(FleetCatalogError::Conflict);
            }
            match &command.action {
                LeadershipAction::Observe if !request.paused || source != main => (),
                LeadershipAction::Transfer {
                    destination,
                    member,
                } if request.paused && source == main => {
                    let target = NodeId::new(destination.node)
                        .map_err(|_| FleetCatalogError::Invalid("resume destination"))?;
                    let host = self.host_from_record(snapshot, target).await?;
                    if target == main
                        || host.state() != FleetNodeState::Active
                        || host.boot_binding()? != *destination
                        || voters.get(&target).map(|m| m.get()) != Some(*member)
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                }
                _ => return Err(FleetCatalogError::Conflict),
            }
            basis.check_current(self).await?;
            Ok(Input {
                cut,
                basis,
                history,
                voters,
                permit,
            })
        })
    }
}

impl FleetHostAuthorization {
    pub(crate) fn execute_resume_leadership<'a>(
        &'a self,
        request: ResumeLeadershipCommand,
        origin: BootBinding,
        minimum_revision: u64,
        gateway: &'a RaftCells,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            request.validate()?;
            origin
                .validate()
                .map_err(|_| FleetCatalogError::Invalid("resume origin"))?;
            let catalog = &self.catalog;
            let deadline = Deadline::new(catalog, timeout)?;
            let started = proof::now(catalog)?;
            let owner_limit = gateway
                .handle
                .domain
                .mono_ms()
                .map_err(|_| FleetCatalogError::Closed)?
                .checked_add(proof::MAX_SAMPLE_AGE_MS)
                .ok_or(FleetCatalogError::Limit)?;
            let snapshot = catalog.read(deadline.remaining()?).await?;
            if snapshot.revision() < minimum_revision {
                return Err(FleetCatalogError::Conflict);
            }
            let input = catalog
                .run(deadline.remaining()?, async {
                    let current = catalog.host_from_record(&snapshot, self.node()).await?;
                    self.require_running_successor(&current)?;
                    if current.boot_binding()? != *request.target() {
                        return Err(FleetCatalogError::Conflict);
                    }
                    let sender = catalog
                        .host_from_record(
                            &snapshot,
                            NodeId::new(origin.node)
                                .map_err(|_| FleetCatalogError::Invalid("resume origin"))?,
                        )
                        .await?;
                    if sender.boot_binding()? != origin
                        || !sender.state().permits_admitted_movement()
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                    catalog.resume_leadership_input(&snapshot, &request).await
                })
                .await?;
            let command = &request.command;
            let scope = input
                .history
                .binding()
                .scope()
                .map_err(|_| FleetCatalogError::Corrupt("resume scope"))?;
            let cell = gateway
                .cell(scope.namespace, &scope.cell)
                .map_err(failure)?;
            if cell.scope() != &scope {
                return Err(FleetCatalogError::Conflict);
            }
            let progress = cell
                .replication_status_for(deadline.remaining()?)
                .await
                .map_err(failure)?;
            if progress.replica.scope != scope
                || progress.replica.node != self.node()
                || progress.replica.disk.bytes() != command.context.target.disk
                || progress.replica.process.bytes() != command.context.target.process
                || input.voters.get(&self.node()) != Some(&progress.replica.member)
                || progress.term.get() != command.term
                || progress.membership_revision.get() != command.revision
                || progress.committed.get() < command.through
            {
                return Err(FleetCatalogError::Unconfirmed);
            }
            let main = NodeId::new(input.cut.node.node)
                .map_err(|_| FleetCatalogError::Corrupt("resume main"))?;
            let observed = proof::observe_current_voting(
                proof::VotingCut {
                    catalog,
                    snapshot: &snapshot,
                    target: main,
                    observer: gateway,
                    paused: request.paused,
                },
                &input.history,
                &input.voters,
                input.permit.as_ref(),
                &deadline,
            )
            .await?;
            let mut sampled = observed.sampled;
            if observed.voting.leader != progress.replica.member.get()
                || observed.voting.term != command.term
                || observed.voting.fixed.configuration_revision()?.get() != command.revision
                || observed.voting.fixed.applied_position()?.index < progress.committed
            {
                return Err(FleetCatalogError::Unconfirmed);
            }
            let (survivors, transfer) = match &command.action {
                LeadershipAction::Observe => {
                    let other = *input
                        .voters
                        .values()
                        .find(|m| **m != progress.replica.member)
                        .ok_or(FleetCatalogError::Conflict)?;
                    ([progress.replica.member, other], None)
                }
                LeadershipAction::Transfer {
                    destination,
                    member,
                } => {
                    let auth = catalog
                        .authorize_group(&snapshot, &scope, deadline.remaining()?)
                        .await?;
                    let view = gateway
                        .observe_peer_fleet_membership(
                            NodeId::new(destination.node)
                                .map_err(|_| FleetCatalogError::Invalid("resume destination"))?,
                            auth,
                            snapshot.revision(),
                            deadline.remaining()?,
                        )
                        .await
                        .map_err(FleetCatalogError::Observation)?;
                    catalog
                        .run(deadline.remaining()?, async {
                            input.basis.check_current(catalog).await?;
                            let host = catalog
                                .host_from_record(
                                    &snapshot,
                                    NodeId::new(destination.node).map_err(|_| {
                                        FleetCatalogError::Invalid("resume destination")
                                    })?,
                                )
                                .await?;
                            proof::node_with_admission(
                                catalog,
                                &snapshot,
                                view.node(),
                                &host,
                                FleetNodeState::Active,
                                false,
                            )
                        })
                        .await?;
                    let load = view
                        .progress()
                        .leader_load
                        .as_ref()
                        .ok_or(FleetCatalogError::Unconfirmed)?;
                    if view.node().fleet_boot.as_ref() != Some(destination)
                        || view.progress().member.as_ref().is_none_or(|p| {
                            p.member.get() != *member
                                || p.term.get() != command.term
                                || p.membership_revision.get() != command.revision
                        })
                        || load.leaders >= load.capacity
                        || load.capacity != view.node().node.profile.leader_capacity
                        || load.sample_age_ms > proof::MAX_SAMPLE_AGE_MS
                        || view.sample_age_ms() > proof::MAX_SAMPLE_AGE_MS
                    {
                        return Err(FleetCatalogError::Unconfirmed);
                    }
                    sampled = sampled.min(
                        proof::now(catalog)?
                            .checked_sub(view.sample_age_ms().max(load.sample_age_ms))
                            .ok_or(FleetCatalogError::Unconfirmed)?,
                    );
                    let survivors = input
                        .voters
                        .iter()
                        .filter(|(n, _)| **n != main)
                        .map(|(_, m)| *m)
                        .collect::<Vec<_>>()
                        .try_into()
                        .map_err(|_| FleetCatalogError::Conflict)?;
                    (
                        survivors,
                        Some((
                            MemberId::new(*member)
                                .map_err(|_| FleetCatalogError::Invalid("resume destination"))?,
                            input.cut.quiescence.clone(),
                        )),
                    )
                }
            };
            let remaining = proof::budget(catalog, started, sampled)?;
            let expires = gateway
                .handle
                .domain
                .mono_ms()
                .map_err(|_| FleetCatalogError::Closed)?
                .checked_add(remaining)
                .ok_or(FleetCatalogError::Limit)?
                .min(owner_limit);
            cell.restart_leadership(
                RestartLeadershipCheck {
                    deadline: expires,
                    expected_term: Term::new(command.term),
                    expected_revision: LogIndex::new(command.revision),
                    through: progress.committed,
                    survivors,
                    all_voters: true,
                    transfer,
                },
                deadline.remaining()?,
            )
            .await
            .map_err(failure)?;
            catalog
                .run(deadline.remaining()?, input.basis.check_current(catalog))
                .await?;
            proof::fresh(catalog, started, sampled)?;
            deadline.remaining()?;
            Ok(())
        })
    }
}

pub(super) async fn reconcile(
    walk: &FleetRestartResumeVerification,
    history: &FleetHistory,
    voters: &BTreeMap<NodeId, MemberId>,
    voting: &runtime_records::Voting,
    deadline: &Deadline<'_>,
) -> Result<Option<FleetRestartResumeProgress>> {
    let catalog = &walk.catalog;
    let leader =
        MemberId::new(voting.leader).map_err(|_| FleetCatalogError::Corrupt("resume leader"))?;
    let source = *voters
        .iter()
        .find(|(_, m)| **m == leader)
        .map(|(n, _)| n)
        .ok_or(FleetCatalogError::Conflict)?;
    let main =
        NodeId::new(walk.cut.node.node).map_err(|_| FleetCatalogError::Corrupt("returned node"))?;
    let request = catalog
        .run(deadline.remaining()?, async {
            walk.book.basis.check_current(catalog).await?;
            let action = if source == main && walk.mode == ResumeMode::Paused {
                let (node, member) = voters
                    .iter()
                    .find(|(n, m)| {
                        **n != main
                            && voting.voters.iter().any(|v| {
                                v.node == n.bytes()
                                    && v.member == m.get()
                                    && v.leader_load.is_some_and(|[n, max]| max > 0 && n < max)
                            })
                    })
                    .ok_or(FleetCatalogError::Overloaded)?;
                LeadershipAction::Transfer {
                    destination: catalog
                        .host_from_record(&walk.snapshot, *node)
                        .await?
                        .boot_binding()?,
                    member: member.get(),
                }
            } else {
                LeadershipAction::Observe
            };
            let request = ResumeLeadershipCommand {
                paused: walk.mode == ResumeMode::Paused,
                command: RestartLeadershipCommand {
                    context: RestartActionContext {
                        parent: walk.cut.boot_request.id,
                        operation: walk.book.basis.operation,
                        inputs: walk.book.basis.digest()?,
                        group: history
                            .binding()
                            .key()
                            .map_err(|_| FleetCatalogError::Corrupt("resume key"))?,
                        history: hash(&history.encode()?),
                        target: catalog
                            .host_from_record(&walk.snapshot, source)
                            .await?
                            .boot_binding()?,
                    },
                    term: voting.term,
                    revision: voting.fixed.configuration_revision()?.get(),
                    through: voting.fixed.applied_position()?.index.get(),
                    action,
                },
            };
            catalog
                .resume_leadership_input(&walk.snapshot, &request)
                .await?;
            Ok(request)
        })
        .await?;
    let transfer = match &request.command.action {
        LeadershipAction::Transfer { member, .. } => Some(
            MemberId::new(*member).map_err(|_| FleetCatalogError::Corrupt("resume destination"))?,
        ),
        LeadershipAction::Observe => None,
    };
    walk.observer
        .request_peer_fleet_action(
            FleetExecutionCommand::ResumeLeadership(Box::new(request)),
            walk.book.basis.revision,
            deadline.remaining()?,
        )
        .await?;
    catalog
        .run(
            deadline.remaining()?,
            walk.book.basis.check_current(catalog),
        )
        .await?;
    deadline.remaining()?;
    let group = history
        .binding()
        .scope()
        .map_err(|_| FleetCatalogError::Corrupt("resume history scope"))?;
    Ok(
        transfer.map(|to| FleetRestartResumeProgress::HandoffRequested {
            group,
            from: leader,
            to,
        }),
    )
}

fn failure(error: crate::application::consensus::CommandFailure) -> FleetCatalogError {
    use crate::application::consensus::CommandFailure as C;
    match error {
        C::Overloaded => FleetCatalogError::Overloaded,
        C::CommandConflict => FleetCatalogError::Conflict,
        C::CorruptState => FleetCatalogError::Corrupt("resume leader state"),
        _ => FleetCatalogError::Unconfirmed,
    }
}
