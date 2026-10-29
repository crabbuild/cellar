// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One physical action per turn; acknowledgement never substitutes for a probe.
use super::*;
use crate::raft::fleet_catalog::FleetExecutionCommand;
use crate::raft::host::RaftPeerNodeObservation;
use std::collections::BTreeMap;

pub(in crate::raft::fleet_catalog::maintenance::runtime) async fn next(
    verification: &mut FleetRestartVerification,
    history: &FleetHistory,
    deadline: &Deadline<'_>,
) -> Result<Option<FleetRestartRuntimeProgress>> {
    if *history.state() != FleetHistoryState::Live {
        return Ok(None);
    }
    let catalog = verification.catalog.clone();
    let main = NodeId::new(verification.gate.node.node)
        .map_err(|_| FleetCatalogError::Corrupt("genesis maintenance node"))?;
    let voters = catalog
        .run(deadline.remaining()?, async {
            verification.book.basis.check_current(&catalog).await?;
            Ok(catalog
                .restart_voters(&verification.snapshot, history, main)
                .await?
                .map(|(voters, _)| voters))
        })
        .await?;
    let Some(voters) = voters else {
        return Ok(None);
    };
    let scope = history
        .binding()
        .scope()
        .map_err(|_| FleetCatalogError::Corrupt("genesis scope"))?;
    let (physical, unattached) = proof::physical_with_unattached(
        &catalog,
        &verification.snapshot,
        &verification.book.basis,
        &verification.gate,
        &verification.observer,
        deadline,
    )
    .await?;
    refresh_target(verification, history, &voters, physical)?;
    let order = std::iter::once(main).chain(voters.keys().copied().filter(|node| *node != main));
    for node in order {
        let member = *voters.get(&node).ok_or(FleetCatalogError::Conflict)?;
        let physical = if node == main {
            None
        } else {
            Some(probe(verification, node, deadline).await?)
        };
        let inventory = match &physical {
            Some(p) => p
                .inventory
                .as_ref()
                .ok_or(FleetCatalogError::InventoryIncomplete)?
                .as_ref(),
            None => verification.inventory.as_ref(),
        };
        let slot = inventory
            .history(&scope)
            .map_err(|_| FleetCatalogError::Conflict)?;
        if let Some(slot) = slot {
            check_slot(history, member, *slot)?;
            let unregistered = physical
                .as_ref()
                .map_or(unattached, |p| p.view.unattached_engine_histories);
            let initial = history
                .voters
                .iter()
                .any(|v| v.node == node.bytes() && v.member == member.get());
            if unregistered == 0 || !initial || verification.genesis_checked.contains(&node) {
                continue;
            }
        }
        let previously_present = slot.is_some();
        drop(physical);
        let command = catalog
            .run(deadline.remaining()?, async {
                verification.book.basis.check_current(&catalog).await?;
                let command = RestartActionContext {
                    parent: verification.gate.request.id,
                    operation: verification.gate.request.operation,
                    inputs: verification.book.basis.input_digest()?,
                    group: history
                        .binding()
                        .key()
                        .map_err(|_| FleetCatalogError::Corrupt("genesis key"))?,
                    history: hash(&history.encode()?),
                    target: catalog
                        .host_from_record(&verification.snapshot, node)
                        .await?
                        .boot_binding()?,
                };
                catalog
                    .restart_genesis_history(&verification.snapshot, &command)
                    .await?;
                Ok(command)
            })
            .await?;
        verification
            .observer
            .request_peer_fleet_action(
                FleetExecutionCommand::RestartGenesis(command),
                verification.book.basis.revision,
                deadline.remaining()?,
            )
            .await?;
        // The receipt established local attachment. Check actual complete
        // occupancy as well; neither supplies a quorum/publication proof.
        if node == main {
            let actual = proof::physical(
                &catalog,
                &verification.snapshot,
                &verification.book.basis,
                &verification.gate,
                &verification.observer,
                deadline,
            )
            .await?;
            let slot = actual
                .history(&scope)
                .map_err(|_| FleetCatalogError::Conflict)?
                .ok_or(FleetCatalogError::Unconfirmed)?;
            check_slot(history, member, *slot)?;
            refresh_target(verification, history, &voters, actual)?;
        } else {
            let actual = probe(verification, node, deadline).await?;
            let slot = actual
                .inventory
                .as_ref()
                .ok_or(FleetCatalogError::InventoryIncomplete)?
                .history(&scope)
                .map_err(|_| FleetCatalogError::Conflict)?
                .copied()
                .ok_or(FleetCatalogError::Unconfirmed)?;
            check_slot(history, member, slot)?;
        }
        catalog
            .run(
                deadline.remaining()?,
                verification.book.basis.check_current(&catalog),
            )
            .await?;
        deadline.remaining()?;
        verification.genesis_checked.insert(node);
        return Ok(Some(FleetRestartRuntimeProgress::Materialized {
            group: scope,
            node,
            member,
            previously_present,
        }));
    }
    deadline.remaining()?;
    Ok(None)
}

fn check_slot(history: &FleetHistory, member: MemberId, slot: RaftPhysicalHistory) -> Result<()> {
    if slot.birth_digest() != history.birth_digest()
        || slot.member() != member
        || !slot.in_configuration()
    {
        return Err(FleetCatalogError::Conflict);
    }
    Ok(())
}

fn refresh_target(
    verification: &mut FleetRestartVerification,
    history: &FleetHistory,
    voters: &BTreeMap<NodeId, MemberId>,
    actual: Box<RaftPhysicalInventory>,
) -> Result<()> {
    if actual.digest() == verification.book.physical {
        return Ok(());
    }
    let scope = history
        .binding()
        .scope()
        .map_err(|_| FleetCatalogError::Corrupt("genesis scope"))?;
    let main = NodeId::new(verification.gate.node.node)
        .map_err(|_| FleetCatalogError::Corrupt("genesis maintenance node"))?;
    let member = *voters.get(&main).ok_or(FleetCatalogError::Conflict)?;
    let slot = actual
        .history(&scope)
        .map_err(|_| FleetCatalogError::Conflict)?
        .copied()
        .ok_or(FleetCatalogError::Conflict)?;
    check_slot(history, member, slot)?;
    if !history.allows_managed_genesis()
        || !history
            .voters
            .iter()
            .any(|v| v.node == main.bytes() && v.member == member.get())
    {
        return Err(FleetCatalogError::Conflict);
    }
    let omitted = slot.scope_digest();
    let omit = |entry: &&RaftPhysicalHistory| entry.scope_digest() != omitted;
    if !verification
        .inventory
        .histories()
        .iter()
        .filter(omit)
        .eq(actual.histories().iter().filter(omit))
    {
        return Err(FleetCatalogError::Conflict);
    }
    verification.book.physical = actual.digest();
    verification.inventory = actual;
    Ok(())
}

async fn probe(
    verification: &FleetRestartVerification,
    node: NodeId,
    deadline: &Deadline<'_>,
) -> Result<RaftPeerNodeObservation> {
    let catalog = &verification.catalog;
    let started = proof::now(catalog)?;
    let observed = verification
        .observer
        .inspect_peer_physical_inventory(
            node,
            verification.book.basis.revision,
            deadline.remaining()?,
        )
        .await
        .map_err(FleetCatalogError::Observation)?;
    let sampled = proof::now(catalog)?
        .checked_sub(observed.sample_age_ms)
        .ok_or(FleetCatalogError::Unconfirmed)?;
    catalog
        .run(deadline.remaining()?, async {
            verification.book.basis.check_current(catalog).await?;
            let auth = catalog
                .host_from_record(&verification.snapshot, node)
                .await?;
            proof::node(
                catalog,
                &verification.snapshot,
                &observed.view,
                &auth,
                FleetNodeState::Active,
            )?;
            let inventory = observed
                .inventory
                .as_ref()
                .ok_or(FleetCatalogError::InventoryIncomplete)?;
            inventory
                .validate(&observed.view)
                .map_err(|_| FleetCatalogError::Conflict)?;
            if inventory.initializing().is_some()
                || observed.view.seed_workers != 0
                || observed.view.pending_history_registrations != 0
            {
                return Err(FleetCatalogError::InventoryIncomplete);
            }
            verification.book.basis.check_current(catalog).await
        })
        .await?;
    proof::fresh(catalog, started, sampled)?;
    deadline.remaining()?;
    Ok(observed)
}
