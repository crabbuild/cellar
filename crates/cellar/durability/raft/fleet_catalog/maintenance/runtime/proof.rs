// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fresh authenticated runtime evidence bound to the exact rooted boot cut.
use super::*;
use crate::raft::fleet_stop::FleetStopAction;
use crate::raft::host::{FleetMembershipConfiguration, RaftHostObservation};
use crate::raft::member_state::{FleetMembershipPermit, FleetMembershipStatus};
use std::collections::{BTreeMap, BTreeSet};

// Preserve the existing fleet physical-evidence freshness envelope.
const MAX_SAMPLE_AGE_MS: u64 = 5000;

fn now(catalog: &FleetCatalog) -> Result<u64> {
    catalog
        .inner
        .domain
        .mono_ms()
        .map_err(|_| FleetCatalogError::Closed)
}

fn fresh(catalog: &FleetCatalog, started: u64, sampled: u64) -> Result<()> {
    if now(catalog)?
        .checked_sub(started.min(sampled))
        .is_none_or(|age| age > MAX_SAMPLE_AGE_MS)
    {
        return Err(FleetCatalogError::Unconfirmed);
    }
    Ok(())
}

fn node(
    catalog: &FleetCatalog,
    snapshot: &FleetSnapshot,
    view: &RaftHostObservation,
    auth: &FleetHostAuthorization,
    state: FleetNodeState,
) -> Result<()> {
    let enrolled = view
        .current_enrollment
        .as_ref()
        .ok_or(FleetCatalogError::Unconfirmed)?;
    if auth.state() != state
        || view.fleet_boot.as_ref()
            != Some(&auth.record.boot_binding(catalog.birth_owner()?.catalog)?)
        || view.node.identity.node != auth.node()
        || view.node.identity.disk != auth.disk()
        || view.fleet_boot.as_ref().map(|boot| boot.process)
            != Some(view.node.identity.process.bytes())
        || auth.storage_instance() != Some(view.node.storage_instance)
        || enrolled.fleet != auth.fleet()
        || enrolled.epoch != auth.epoch()
        || enrolled.state != state
        || enrolled.physical_host != auth.physical_host()
        || enrolled.capability_digest != auth.capability_digest()
        || enrolled.revision < snapshot.revision()
        || view
            .fleet_trust_revision
            .is_none_or(|revision| revision < snapshot.revision())
        // The captured profile includes reversible application admission. A
        // quiesced target must report paused, while both survivors must not.
        || view.node.profile.draining != (state == FleetNodeState::RestartPending)
    {
        return Err(FleetCatalogError::Unconfirmed);
    }
    Ok(())
}

pub(super) async fn physical(
    catalog: &FleetCatalog,
    snapshot: &FleetSnapshot,
    basis: &Basis,
    gate: &Gate,
    observer: &RaftCells,
    deadline: &Deadline<'_>,
) -> Result<Box<RaftPhysicalInventory>> {
    let target =
        NodeId::new(gate.node.node).map_err(|_| FleetCatalogError::Corrupt("restart target"))?;
    catalog
        .run(deadline.remaining()?, basis.check_current(catalog))
        .await?;
    let request = gate.quiesce_request()?;
    let accepted = observer
        .request_peer_fleet_stop(request.clone(), basis.revision, deadline.remaining()?)
        .await?;
    if accepted.action() != FleetStopAction::Quiesce
        || accepted.request() != request.request()
        || accepted.operation() != gate.request.operation
        || accepted.process() != gate.node.boot_binding(gate.preparation.catalog)?.process
    {
        return Err(FleetCatalogError::Unconfirmed);
    }
    let started = now(catalog)?;
    let observed = observer
        .inspect_peer_physical_inventory(target, basis.revision, deadline.remaining()?)
        .await
        .map_err(FleetCatalogError::Observation)?;
    let sampled = now(catalog)?
        .checked_sub(observed.sample_age_ms)
        .ok_or(FleetCatalogError::Unconfirmed)?;
    let inventory = observed
        .inventory
        .ok_or(FleetCatalogError::InventoryIncomplete)?;
    catalog
        .run(deadline.remaining()?, async {
            basis.check_current(catalog).await?;
            let auth = catalog.host_record(snapshot, gate.node.clone())?;
            node(
                catalog,
                snapshot,
                &observed.view,
                &auth,
                FleetNodeState::RestartPending,
            )?;
            if observed.view.seed_workers != 0
                || observed.view.pending_history_registrations != 0
                || inventory.initializing().is_some()
            {
                return Err(FleetCatalogError::InventoryIncomplete);
            }
            inventory
                .validate(&observed.view)
                .map_err(|_| FleetCatalogError::Conflict)?;
            basis.check_current(catalog).await
        })
        .await?;
    fresh(catalog, started, sampled)?;
    deadline.remaining()?;
    Ok(inventory)
}

fn stable(configuration: &FleetMembershipConfiguration, voters: &BTreeSet<MemberId>) -> bool {
    configuration.voters().len() == 3
        && configuration
            .voters()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            == *voters
        && configuration.outgoing_voters().is_empty()
        && configuration.learners().is_empty()
        && configuration.next_learners().is_empty()
        && !configuration.auto_leave()
}

fn settled(status: &FleetMembershipStatus, latest: Option<&FleetMembershipPermit>) -> bool {
    match latest {
        None => status.consumed_sequence == 0 && status.permit.is_none() && status.phase.is_none(),
        Some(permit) => {
            status.permit.as_ref() == Some(permit)
                && status.consumed_sequence == permit.sequence()
                && status.phase.is_some_and(|phase| !phase.active())
        }
    }
}

pub(super) async fn voting(
    verification: &FleetRestartVerification,
    history: &FleetHistory,
    expected: &BTreeMap<NodeId, MemberId>,
    latest: Option<&FleetMembershipPermit>,
    deadline: &Deadline<'_>,
) -> Result<records::Voting> {
    let catalog = &verification.catalog;
    let snapshot = &verification.snapshot;
    let target = NodeId::new(verification.gate.node.node)
        .map_err(|_| FleetCatalogError::Corrupt("restart target"))?;
    let scope = history
        .binding()
        .scope()
        .map_err(|_| FleetCatalogError::Corrupt("restart scope"))?;
    let authorization = catalog
        .authorize_group(snapshot, &scope, deadline.remaining()?)
        .await?;
    let started = now(catalog)?;
    let mut sampled = started;
    let mut observations = Vec::with_capacity(3);
    for peer in
        std::iter::once(target).chain(expected.keys().copied().filter(|peer| *peer != target))
    {
        let observation = verification
            .observer
            .observe_peer_fleet_membership(
                peer,
                authorization.clone(),
                verification.book.basis.revision,
                deadline.remaining()?,
            )
            .await
            .map_err(FleetCatalogError::Observation)?;
        sampled = sampled.min(
            now(catalog)?
                .checked_sub(observation.sample_age_ms())
                .ok_or(FleetCatalogError::Unconfirmed)?,
        );
        observations.push(observation);
    }
    if observations.len() != 3 {
        return Err(FleetCatalogError::InventoryIncomplete);
    }
    let fixed = observations[0]
        .published_cut()
        .ok_or(FleetCatalogError::Unconfirmed)?;
    let result = catalog
        .run(deadline.remaining()?, async {
            verification.book.basis.check_current(catalog).await?;
            let first = observations[0].membership();
            let first_member = observations[0]
                .progress()
                .member
                .as_ref()
                .ok_or(FleetCatalogError::Unconfirmed)?;
            let leader = first_member.leader.ok_or(FleetCatalogError::Unconfirmed)?;
            let voters = expected.values().copied().collect::<BTreeSet<_>>();
            if expected.len() != 3 || voters.len() != 3 || !voters.contains(&leader) {
                return Err(FleetCatalogError::Unconfirmed);
            }
            let mut hosts = BTreeSet::new();
            let mut results = Vec::with_capacity(3);
            for observed in &observations {
                let proof = observed.membership();
                let replica = proof.replica();
                let auth = catalog.host_from_record(snapshot, replica.node).await?;
                let state = if replica.node == target {
                    FleetNodeState::RestartPending
                } else {
                    FleetNodeState::Active
                };
                node(catalog, snapshot, observed.node(), &auth, state)?;
                let cut = observed
                    .published_cut()
                    .ok_or(FleetCatalogError::Unconfirmed)?;
                let member = observed
                    .progress()
                    .member
                    .as_ref()
                    .ok_or(FleetCatalogError::Unconfirmed)?;
                if expected.get(&replica.node) != Some(&replica.member)
                    || replica.node != observed.node().node.identity.node
                    || replica.disk != observed.node().node.identity.disk
                    || replica.process != observed.node().node.identity.process
                    || proof.status().binding != *history.binding()
                    || proof.birth_digest() != history.birth_digest()
                    || proof.schema_digest() != history.schema_digest()
                    || proof.membership_revision() != first.membership_revision()
                    || !stable(proof.configuration(), &voters)
                    || proof.status() != first.status()
                    || !settled(proof.status(), latest)
                    || proof
                        .repair_status()
                        .map_err(|_| FleetCatalogError::Corrupt("restart repair"))?
                        .is_some_and(|repair| repair.phase.active())
                    || proof.applied().index < proof.barrier()
                    || member.member != replica.member
                    || member.term != first_member.term
                    || member.leader != Some(leader)
                    || member.committed < proof.barrier()
                    || member.applied != proof.applied()
                    || member.membership_revision != proof.membership_revision()
                    || cut.binding() != history.binding()
                    || cut.replica() != replica
                    || cut.storage_instance() != observed.node().node.storage_instance
                    || cut.birth_digest() != history.birth_digest()
                    || cut.schema_digest() != history.schema_digest()
                    || cut.membership_revision() != proof.membership_revision()
                    || cut.applied() != proof.applied()
                    || !cut.covers(fixed).map_err(|_| FleetCatalogError::Conflict)?
                {
                    return Err(FleetCatalogError::Unconfirmed);
                }
                match proof
                    .joined_member(replica.member)
                    .map_err(|_| FleetCatalogError::Corrupt("restart joined member"))?
                {
                    Some((joined, _))
                        if joined.node == replica.node && joined.disk == replica.disk => {}
                    None if history.voters.iter().chain(&history.learners).any(|m| {
                        m.node == replica.node.bytes() && m.member == replica.member.get()
                    }) => {}
                    _ => return Err(FleetCatalogError::Conflict),
                }
                hosts.insert(auth.physical_host());
                results.push(records::Voter {
                    node: replica.node.bytes(),
                    boot: hash(
                        &serde_json::to_vec(&observed.node().fleet_boot)
                            .map_err(|_| FleetCatalogError::Limit)?,
                    ),
                    epoch: auth.epoch(),
                    member: replica.member.get(),
                    publication: hash(&encode(57, &Publication::from(cut), records::LIMIT)?),
                });
            }
            if snapshot.replica_separation() == FleetReplicaSeparation::PhysicalHosts
                && hosts.len() != 3
            {
                return Err(FleetCatalogError::Conflict);
            }
            verification.book.basis.check_current(catalog).await?;
            Ok(records::Voting {
                fixed: fixed.into(),
                voters: results
                    .try_into()
                    .map_err(|_| FleetCatalogError::Corrupt("restart voter count"))?,
                leader: leader.get(),
                term: first_member.term.get(),
            })
        })
        .await?;
    fresh(catalog, started, sampled)?;
    deadline.remaining()?;
    Ok(result)
}
