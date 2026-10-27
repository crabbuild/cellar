// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The leader reacquires quorum/publication proof and checks its actual tracker.
use super::*;
use crate::raft::completion::RestartLeadershipCheck;
use crate::raft::fleet_catalog::maintenance::runtime::action::ActionHistory;
use cellar_logic::consensus::{LogIndex, Term};

impl FleetCatalog {
    pub(super) async fn restart_leadership_history(
        &self,
        snapshot: &FleetSnapshot,
        command: &RestartLeadershipCommand,
    ) -> Result<ActionHistory> {
        command.validate()?;
        let action = self
            .restart_action_history(snapshot, &command.context)
            .await?;
        let main = NodeId::new(action.gate.node.node)
            .map_err(|_| FleetCatalogError::Corrupt("restart main"))?;
        let source = NodeId::new(command.context.target.node)
            .map_err(|_| FleetCatalogError::Invalid("restart leader"))?;
        match &command.action {
            LeadershipAction::Observe if source != main => (),
            LeadershipAction::Transfer {
                destination,
                member,
            } if source == main => {
                let node = NodeId::new(destination.node)
                    .map_err(|_| FleetCatalogError::Invalid("restart transfer node"))?;
                let host = self.host_from_record(snapshot, node).await?;
                if node == main
                    || host.state() != FleetNodeState::Active
                    || host.boot_binding()? != *destination
                    || action.voters.get(&node).map(|m| m.get()) != Some(*member)
                {
                    return Err(FleetCatalogError::Conflict);
                }
            }
            _ => return Err(FleetCatalogError::Conflict),
        }
        Ok(action)
    }
}

impl FleetHostAuthorization {
    pub(crate) fn execute_restart_leadership<'a>(
        &'a self,
        command: RestartLeadershipCommand,
        origin: BootBinding,
        minimum_revision: u64,
        gateway: &'a RaftCells,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            command.validate()?;
            origin
                .validate()
                .map_err(|_| FleetCatalogError::Invalid("restart origin"))?;
            let catalog = &self.catalog;
            let deadline = Deadline::new(catalog, timeout)?;
            let started = proof::now(catalog)?;
            // Evidence and SDK requests can use different execution domains.
            // Pin expiry directly to the Raft owner's clock before gathering
            // proof, so queueing cannot extend the five-second evidence window.
            let evidence_deadline = gateway
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
            let action = catalog
                .run(deadline.remaining()?, async {
                    let current = catalog.host_from_record(&snapshot, self.node()).await?;
                    self.require_running_successor(&current)?;
                    if current.boot_binding()? != command.context.target {
                        return Err(FleetCatalogError::Conflict);
                    }
                    let sender = catalog
                        .host_from_record(
                            &snapshot,
                            NodeId::new(origin.node)
                                .map_err(|_| FleetCatalogError::Invalid("restart origin"))?,
                        )
                        .await?;
                    if sender.boot_binding()? != origin
                        || !sender.state().permits_admitted_movement()
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                    catalog
                        .restart_leadership_history(&snapshot, &command)
                        .await
                })
                .await?;
            let scope = action
                .history
                .binding()
                .scope()
                .map_err(|_| FleetCatalogError::Corrupt("restart scope"))?;
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
                || action.voters.get(&self.node()) != Some(&progress.replica.member)
                || progress.term.get() != command.term
                || progress.membership_revision.get() != command.revision
                || progress.committed.get() < command.through
            {
                return Err(FleetCatalogError::Unconfirmed);
            }
            let basis = Basis::capture(&snapshot, &action.gate)?;
            let observed = proof::observe_voting(
                proof::VotingContext {
                    catalog,
                    snapshot: &snapshot,
                    basis: &basis,
                    gate: &action.gate,
                    observer: gateway,
                },
                &action.history,
                &action.voters,
                action.permit.as_ref(),
                &deadline,
            )
            .await?;
            let mut sampled = observed.sampled;
            let voting = observed.voting;
            if voting.leader != progress.replica.member.get()
                || voting.term != command.term
                || voting.fixed.configuration_revision()?.get() != command.revision
                || voting.fixed.applied_position()?.index < progress.committed
            {
                return Err(FleetCatalogError::Unconfirmed);
            }
            let main = NodeId::new(action.gate.node.node)
                .map_err(|_| FleetCatalogError::Corrupt("restart main"))?;
            let survivors = action
                .voters
                .iter()
                .filter(|(node, _)| **node != main)
                .map(|(_, member)| *member)
                .collect::<Vec<_>>()
                .try_into()
                .map_err(|_| FleetCatalogError::Corrupt("restart survivors"))?;
            let transfer = match &command.action {
                LeadershipAction::Observe => None,
                LeadershipAction::Transfer {
                    destination,
                    member,
                } => {
                    // The full quorum proof above also supplies current load.
                    // Refresh destination load before the owner accepts a new leader.
                    let auth = catalog
                        .authorize_group(&snapshot, &scope, deadline.remaining()?)
                        .await?;
                    let observed = gateway
                        .observe_peer_fleet_membership(
                            NodeId::new(destination.node)
                                .map_err(|_| FleetCatalogError::Invalid("restart destination"))?,
                            auth,
                            snapshot.revision(),
                            deadline.remaining()?,
                        )
                        .await
                        .map_err(FleetCatalogError::Observation)?;
                    catalog
                        .run(deadline.remaining()?, async {
                            basis.check_current(catalog).await?;
                            let host = catalog
                                .host_from_record(
                                    &snapshot,
                                    NodeId::new(destination.node).map_err(|_| {
                                        FleetCatalogError::Invalid("restart destination")
                                    })?,
                                )
                                .await?;
                            proof::node(
                                catalog,
                                &snapshot,
                                observed.node(),
                                &host,
                                FleetNodeState::Active,
                            )
                        })
                        .await?;
                    let load = observed
                        .progress()
                        .leader_load
                        .as_ref()
                        .ok_or(FleetCatalogError::Unconfirmed)?;
                    if observed.node().fleet_boot.as_ref() != Some(destination)
                        || observed.progress().member.as_ref().is_none_or(|p| {
                            p.member.get() != *member
                                || p.term.get() != command.term
                                || p.membership_revision.get() != command.revision
                        })
                        || observed.node().node.profile.draining
                        || load.leaders >= load.capacity
                        || load.capacity != observed.node().node.profile.leader_capacity
                        || load.sample_age_ms > 5000
                        || observed.sample_age_ms() > 5000
                    {
                        return Err(FleetCatalogError::Unconfirmed);
                    }
                    sampled = sampled.min(
                        proof::now(catalog)?
                            .checked_sub(observed.sample_age_ms().max(load.sample_age_ms))
                            .ok_or(FleetCatalogError::Unconfirmed)?,
                    );
                    Some((
                        MemberId::new(*member)
                            .map_err(|_| FleetCatalogError::Invalid("restart destination"))?,
                        action.gate.quiesce_request()?.selector,
                    ))
                }
            };
            // Translate only the remaining budget between the two local
            // domain clocks. A cached peer sample cannot gain a new five
            // seconds while waiting for the Raft owner to execute the action.
            let budget = proof::budget(catalog, started, sampled)?;
            let evidence_deadline = evidence_deadline.min(
                gateway
                    .handle
                    .domain
                    .mono_ms()
                    .map_err(|_| FleetCatalogError::Closed)?
                    .checked_add(budget)
                    .ok_or(FleetCatalogError::Limit)?,
            );
            cell.restart_leadership(
                RestartLeadershipCheck {
                    deadline: evidence_deadline,
                    expected_term: Term::new(command.term),
                    expected_revision: LogIndex::new(command.revision),
                    through: progress.committed,
                    survivors,
                    all_voters: false,
                    transfer,
                },
                deadline.remaining()?,
            )
            .await
            .map_err(failure)?;
            catalog
                .run(deadline.remaining()?, basis.check_current(catalog))
                .await?;
            proof::fresh(catalog, started, sampled)?;
            deadline.remaining()?;
            Ok(())
        })
    }
}

fn failure(error: crate::application::consensus::CommandFailure) -> FleetCatalogError {
    use crate::application::consensus::CommandFailure as C;
    match error {
        C::Overloaded => FleetCatalogError::Overloaded,
        C::CommandConflict => FleetCatalogError::Conflict,
        C::CorruptState => FleetCatalogError::Corrupt("restart leadership host state"),
        _ => FleetCatalogError::Unconfirmed,
    }
}
