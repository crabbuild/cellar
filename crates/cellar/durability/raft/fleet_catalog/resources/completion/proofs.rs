// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::host::{RaftPeerFleetMembershipObservation, RaftPeerNodeObservation};
use crate::raft::member_state::{FleetPermitPhase, RepairPhase};
use std::collections::BTreeSet;

impl FleetCatalog {
    pub(super) async fn check_movement_completion(
        &self,
        snapshot: &FleetSnapshot,
        row: &Record,
        quorum: &RaftPeerFleetMembershipObservation,
        target: &RaftPeerNodeObservation,
        sources: &[RaftPeerNodeObservation],
    ) -> Result<()> {
        let permit = row.permit.as_ref().ok_or_else(invalid)?;
        let scope = permit.binding().scope().map_err(|_| invalid())?;
        let plan = row.request.movement.as_ref().ok_or_else(invalid)?.plan()?;
        let proof = quorum.membership();
        let conf = proof.configuration();
        let status = proof.status();
        let history = self.history_inner(snapshot, &row.request.group).await?;
        let latest = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Admissions.slot()],
                &row.request.group,
            )
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        let replacement = permit.replacement().map_err(|_| invalid())?;
        let voters = conf.voters().iter().copied().collect::<BTreeSet<_>>();
        if latest != permit.encode().map_err(|_| invalid())?
            || proof.replica().node != row.node
            || proof.replica().member != replacement.member
            || proof.replica().disk.bytes() != row.request.attestation.boot.disk
            || proof.replica().process.bytes() != row.request.attestation.boot.process
            || proof.status().binding != *history.binding()
            || proof.birth_digest() != history.birth_digest()
            || proof.schema_digest() != history.schema_digest()
            || quorum.node().fleet_boot.as_ref() != Some(&row.request.attestation.boot)
            || status.permit.as_ref() != Some(permit)
            || status.consumed_sequence != permit.sequence()
            || status.phase != Some(FleetPermitPhase::Complete)
            || proof
                .repair_status()
                .map_err(|_| invalid())?
                .is_none_or(|s| s.phase != RepairPhase::Complete || s.plan != plan)
            || voters.len() != 3
            || conf.voters().len() != 3
            || !voters.contains(&replacement.member)
            || voters.contains(&plan.obsolete)
            || !conf.outgoing_voters().is_empty()
            || !conf.learners().is_empty()
            || !conf.next_learners().is_empty()
            || conf.auto_leave()
        {
            return Err(FleetCatalogError::Unconfirmed);
        }
        let mut nodes = BTreeSet::new();
        let mut hosts = BTreeSet::new();
        for member in voters {
            let node = match proof.joined_member(member).map_err(|_| invalid())? {
                Some((joined, _)) => joined.node,
                None => NodeId::new(
                    history
                        .voters
                        .iter()
                        .find(|v| v.member == member.get())
                        .ok_or(FleetCatalogError::Conflict)?
                        .node,
                )
                .map_err(|_| invalid())?,
            };
            let host = self.host_from(snapshot, node).await?;
            if !matches!(
                host.state(),
                FleetNodeState::Active | FleetNodeState::Draining
            ) || !nodes.insert(node)
                || snapshot.replica_separation() == FleetReplicaSeparation::PhysicalHosts
                    && !hosts.insert(host.physical_host())
            {
                return Err(FleetCatalogError::Conflict);
            }
        }
        let owners = row.seed_owners.as_ref().ok_or_else(invalid)?;
        if owners.len() != 3 || sources.len() != owners.len() {
            return Err(FleetCatalogError::Unconfirmed);
        }
        for (sample, boot, member) in std::iter::once((
            target,
            &row.request.attestation.boot,
            replacement.member.get(),
        ))
        .chain(
            sources
                .iter()
                .zip(owners)
                .map(|(sample, owner)| (sample, &owner.boot, owner.member)),
        ) {
            let host = self
                .host_from(snapshot, NodeId::new(boot.node).map_err(|_| invalid())?)
                .await?;
            let view = &sample.view;
            let current = view
                .current_enrollment
                .as_ref()
                .ok_or(FleetCatalogError::Unconfirmed)?;
            let inventory = sample
                .inventory
                .as_ref()
                .ok_or(FleetCatalogError::Unconfirmed)?;
            let physical = inventory
                .history(&scope)
                .map_err(|_| invalid())?
                .ok_or(FleetCatalogError::Unconfirmed)?;
            if host.boot_binding()? != *boot
                || view.fleet_boot.as_ref() != Some(boot)
                || !matches!(
                    host.state(),
                    FleetNodeState::Active | FleetNodeState::Draining
                )
                || view.node.identity.node.bytes() != boot.node
                || view.node.identity.disk.bytes() != boot.disk
                || view.node.identity.process.bytes() != boot.process
                || view.node.storage_instance.bytes() != boot.engine
                || current.fleet != host.fleet()
                || current.epoch != host.epoch()
                || current.state != host.state()
                || current.physical_host != host.physical_host()
                || current.capability_digest != host.capability_digest()
                || current.revision < snapshot.revision()
                || view
                    .fleet_trust_revision
                    .is_none_or(|r| r < snapshot.revision())
                || physical.birth_digest() != history.birth_digest()
                || physical.member().get() != member
                || physical.completed_sequence() != permit.sequence()
                || view.seed_workers != 0
                || inventory.initializing().is_some()
                || view.pending_history_registrations != 0
            {
                return Err(FleetCatalogError::Unconfirmed);
            }
        }
        Ok(())
    }
}
