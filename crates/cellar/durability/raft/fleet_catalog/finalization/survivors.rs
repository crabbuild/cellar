// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bind actual quorum responses to rooted eligibility and settled admission.
use super::*;
use crate::raft::host::{
    FleetMembershipConfiguration, FleetPublishedCut, RaftPeerFleetMembershipObservation,
};
use crate::raft::member_state::{FleetMembershipPermit, FleetMembershipStatus};
use receipts::{Publication, Survivor};
use std::collections::BTreeSet;

fn stable_voters(
    configuration: &FleetMembershipConfiguration,
) -> Option<BTreeSet<cellar_logic::consensus::MemberId>> {
    if configuration.voters().len() != 3
        || !configuration.outgoing_voters().is_empty()
        || !configuration.learners().is_empty()
        || !configuration.next_learners().is_empty()
        || configuration.auto_leave()
    {
        return None;
    }
    // raft-rs does not sort ConfState vectors. Logical configuration equality
    // must survive different internal orderings after restore and membership work.
    let voters = configuration
        .voters()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    (voters.len() == 3).then_some(voters)
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

pub(super) async fn verify(
    catalog: &FleetCatalog,
    snapshot: &FleetSnapshot,
    history: &FleetHistory,
    observations: &[RaftPeerFleetMembershipObservation; 3],
    fixed: &FleetPublishedCut,
) -> Result<[Survivor; 3]> {
    let key = history
        .binding
        .key()
        .map_err(|_| FleetCatalogError::Corrupt("receipt binding"))?;
    let latest = catalog
        .inner
        .lookup(snapshot.head.tables[FleetTable::Admissions.slot()], &key)
        .await?
        .map(|bytes| {
            FleetMembershipPermit::decode(&bytes)
                .map_err(|_| FleetCatalogError::Corrupt("receipt permit"))
        })
        .transpose()?;
    if latest
        .as_ref()
        .is_some_and(|permit| permit.binding() != &history.binding)
    {
        return Err(FleetCatalogError::Corrupt("receipt permit binding"));
    }
    let records = catalog.node_records(snapshot).await?;
    let first = observations[0].membership();
    let voters = stable_voters(first.configuration()).ok_or(FleetCatalogError::Unconfirmed)?;
    let mut nodes = BTreeSet::new();
    let mut members = BTreeSet::new();
    let mut hosts = BTreeSet::new();
    let mut result = Vec::with_capacity(3);
    for observed in observations {
        let proof = observed.membership();
        let replica = proof.replica();
        let node = observed.node();
        let cut = observed
            .published_cut()
            .ok_or(FleetCatalogError::Unconfirmed)?;
        let record = records
            .iter()
            .find(|record| record.node == replica.node.bytes())
            .ok_or(FleetCatalogError::Conflict)?;
        let auth = catalog.host_record(snapshot, record.clone())?;
        let current = node
            .current_enrollment
            .as_ref()
            .ok_or(FleetCatalogError::Unconfirmed)?;
        if auth.state() != FleetNodeState::Active
            || node.fleet_boot.as_ref()
                != Some(&auth.record.boot_binding(catalog.birth_owner()?.catalog)?)
            || auth.node() != replica.node
            || auth.disk() != replica.disk
            || auth.storage_instance() != Some(node.node.storage_instance)
            || node.node.identity.node != replica.node
            || node.node.identity.disk != replica.disk
            || node.node.identity.process != replica.process
            || current.fleet != auth.fleet()
            || current.epoch != auth.epoch()
            || current.state != FleetNodeState::Active
            || current.physical_host != auth.physical_host()
            || current.capability_digest != auth.capability_digest()
            || current.revision < snapshot.revision()
            || node
                .fleet_trust_revision
                .is_none_or(|revision| revision < snapshot.revision())
            || proof.status().binding != history.binding
            || proof.birth_digest() != history.birth_digest
            || proof.schema_digest() != history.schema_digest
            || proof.membership_revision() != first.membership_revision()
            || stable_voters(proof.configuration()).as_ref() != Some(&voters)
            || proof.status() != first.status()
            || !settled(proof.status(), latest.as_ref())
            || proof
                .repair_status()
                .map_err(|_| FleetCatalogError::Corrupt("receipt repair"))?
                .is_some_and(|repair| repair.phase.active())
            || !voters.contains(&replica.member)
            || cut.binding() != &history.binding
            || cut.replica() != replica
            || cut.storage_instance() != node.node.storage_instance
            || cut.birth_digest() != history.birth_digest
            || cut.schema_digest() != history.schema_digest
            || cut.membership_revision() != proof.membership_revision()
            || cut.applied() != proof.applied()
            || !nodes.insert(replica.node)
            || !members.insert(replica.member)
        {
            return Err(FleetCatalogError::Unconfirmed);
        }
        // A dynamic member's node/disk is committed in the group ledger. An
        // original member is bound by immutable genesis and the root engine pin.
        match proof
            .joined_member(replica.member)
            .map_err(|_| FleetCatalogError::Corrupt("receipt joined member"))?
        {
            Some((joined, _)) if joined.node == replica.node && joined.disk == replica.disk => {}
            None if history
                .voters
                .iter()
                .chain(&history.learners)
                .any(|initial| {
                    initial.member == replica.member.get() && initial.node == replica.node.bytes()
                }) => {}
            _ => return Err(FleetCatalogError::Conflict),
        }
        if !cut.covers(fixed).map_err(|_| FleetCatalogError::Conflict)? {
            return Err(FleetCatalogError::Unconfirmed);
        }
        hosts.insert(auth.physical_host());
        result.push(Survivor {
            node: replica.node.bytes(),
            disk: replica.disk.bytes(),
            engine: node.node.storage_instance.bytes(),
            process: replica.process.bytes(),
            physical_host: auth.physical_host().bytes(),
            epoch: auth.epoch(),
            member: replica.member.get(),
            applied: proof.applied().into(),
            publication_digest: hash(&encode(57, &Publication::from(cut), 4096)?),
        });
    }
    if snapshot.replica_separation() == FleetReplicaSeparation::PhysicalHosts && hosts.len() != 3 {
        return Err(FleetCatalogError::Conflict);
    }
    result
        .try_into()
        .map_err(|_| FleetCatalogError::Corrupt("receipt survivor count"))
}
