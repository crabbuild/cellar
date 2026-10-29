// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A request acknowledgement never consumes a leadership handoff obligation.
use super::*;
use crate::raft::fleet_catalog::FleetExecutionCommand;

pub(in crate::raft::fleet_catalog::maintenance::runtime) async fn reconcile(
    verification: &FleetRestartVerification,
    history: &FleetHistory,
    voters: &BTreeMap<NodeId, MemberId>,
    voting: &records::Voting,
    deadline: &Deadline<'_>,
) -> Result<Option<FleetRestartRuntimeProgress>> {
    let catalog = &verification.catalog;
    let leader =
        MemberId::new(voting.leader).map_err(|_| FleetCatalogError::Corrupt("restart leader"))?;
    let source = *voters
        .iter()
        .find(|(_, member)| **member == leader)
        .map(|(node, _)| node)
        .ok_or(FleetCatalogError::Conflict)?;
    let main = NodeId::new(verification.gate.node.node)
        .map_err(|_| FleetCatalogError::Corrupt("restart main"))?;
    let command = catalog
        .run(deadline.remaining()?, async {
            verification.book.basis.check_current(catalog).await?;
            let target = catalog
                .host_from_record(&verification.snapshot, source)
                .await?
                .boot_binding()?;
            let action = if source == main {
                let (node, member) = voters
                    .iter()
                    .find(|(node, member)| {
                        **node != main
                            && voting.voters.iter().any(|v| {
                                v.node == node.bytes()
                                    && v.member == member.get()
                                    && v.leader_load.is_some_and(|[leaders, capacity]| {
                                        capacity > 0 && leaders < capacity
                                    })
                            })
                    })
                    .ok_or(FleetCatalogError::Overloaded)?;
                LeadershipAction::Transfer {
                    destination: catalog
                        .host_from_record(&verification.snapshot, *node)
                        .await?
                        .boot_binding()?,
                    member: member.get(),
                }
            } else {
                LeadershipAction::Observe
            };
            let command = RestartLeadershipCommand {
                context: RestartActionContext {
                    parent: verification.gate.request.id,
                    operation: verification.gate.request.operation,
                    inputs: verification.book.basis.input_digest()?,
                    group: history
                        .binding()
                        .key()
                        .map_err(|_| FleetCatalogError::Corrupt("restart key"))?,
                    history: hash(&history.encode()?),
                    target,
                },
                term: voting.term,
                revision: voting.fixed.configuration_revision()?.get(),
                through: voting.fixed.applied_position()?.index.get(),
                action,
            };
            catalog
                .restart_leadership_history(&verification.snapshot, &command)
                .await?;
            Ok(command)
        })
        .await?;
    let transfer = match &command.action {
        LeadershipAction::Transfer { member, .. } => Some(
            MemberId::new(*member).map_err(|_| FleetCatalogError::Corrupt("restart transfer"))?,
        ),
        LeadershipAction::Observe => None,
    };
    verification
        .observer
        .request_peer_fleet_action(
            FleetExecutionCommand::RestartLeadership(Box::new(command)),
            verification.book.basis.revision,
            deadline.remaining()?,
        )
        .await?;
    catalog
        .run(
            deadline.remaining()?,
            verification.book.basis.check_current(catalog),
        )
        .await?;
    deadline.remaining()?;
    let group = history
        .binding()
        .scope()
        .map_err(|_| FleetCatalogError::Corrupt("restart scope"))?;
    Ok(
        transfer.map(|to| FleetRestartRuntimeProgress::HandoffRequested {
            group,
            from: leader,
            to,
            term: cellar_logic::consensus::Term::new(voting.term),
        }),
    )
}
