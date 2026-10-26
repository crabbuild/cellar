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
        sources: &[OwnerClosure],
    ) -> Result<()> {
        let permit = row.permit.as_ref().ok_or_else(invalid)?;
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
        let mut bindings = Vec::with_capacity(3);
        for (closure, owner) in sources.iter().zip(owners) {
            let sample = match closure {
                OwnerClosure::Observed(sample) => sample.as_ref(),
                OwnerClosure::Isolated(proof) => {
                    // Only the obsolete owner may be absent. Its exact boot
                    // was pinned before this move and permanently quarantined.
                    if owner.member != plan.obsolete.get() || !owner.matches(&proof.boot)? {
                        return Err(FleetCatalogError::Unconfirmed);
                    }
                    let host = self
                        .host_from_record(snapshot, NodeId::new(owner.node).map_err(|_| invalid())?)
                        .await?;
                    let current = self.confirm_node_isolation(snapshot, &host.record).await?;
                    if current.boot != proof.boot || current.digest != proof.digest {
                        return Err(FleetCatalogError::Unconfirmed);
                    }
                    continue;
                }
            };
            let boot = sample
                .view
                .fleet_boot
                .as_ref()
                .ok_or(FleetCatalogError::Unconfirmed)?;
            if !owner.matches(boot)? {
                return Err(FleetCatalogError::Unconfirmed);
            }
            bindings.push((sample, boot, owner.member));
        }
        for (sample, boot, member) in std::iter::once((
            target,
            &row.request.attestation.boot,
            replacement.member.get(),
        ))
        .chain(bindings)
        {
            self.check_movement_physical(
                snapshot,
                row,
                sample,
                boot,
                PhysicalRole::Completed {
                    member,
                    sequence: permit.sequence(),
                },
            )
            .await?;
        }
        Ok(())
    }

    pub(super) async fn check_fenced_owner(
        &self,
        snapshot: &FleetSnapshot,
        row: &Record,
        owner: &super::super::movement::MovementOwner,
        sample: &RaftPeerFleetMembershipObservation,
    ) -> Result<()> {
        let permit = row.permit.as_ref().ok_or_else(invalid)?;
        let proof = sample.membership();
        let status = proof.status();
        let conf = proof.configuration();
        let history = self.history_inner(snapshot, &row.request.group).await?;
        let boot = sample
            .node()
            .fleet_boot
            .as_ref()
            .ok_or(FleetCatalogError::Unconfirmed)?;
        let voters = conf
            .voters()
            .iter()
            .map(|id| id.get())
            .collect::<BTreeSet<_>>();
        let expected = row
            .seed_owners
            .as_ref()
            .ok_or_else(invalid)?
            .iter()
            .map(|owner| owner.member)
            .collect::<BTreeSet<_>>();
        if !owner.matches(boot)?
            || proof.replica().node.bytes() != owner.node
            || proof.replica().member.get() != owner.member
            || proof.replica().disk.bytes() != boot.disk
            || proof.replica().process.bytes() != boot.process
            || proof.status().binding != *history.binding()
            || proof.birth_digest() != history.birth_digest()
            || proof.schema_digest() != history.schema_digest()
            || status.permit.as_ref() != Some(permit)
            || status.consumed_sequence != permit.sequence()
            || status.phase != Some(FleetPermitPhase::Fenced)
            || proof
                .repair_status()
                .map_err(|_| invalid())?
                .is_some_and(|s| s.phase.active())
            || expected.len() != 3
            || voters != expected
            || conf.voters().len() != 3
            || !conf.outgoing_voters().is_empty()
            || !conf.learners().is_empty()
            || !conf.next_learners().is_empty()
            || conf.auto_leave()
        {
            return Err(FleetCatalogError::Unconfirmed);
        }
        Ok(())
    }

    pub(super) async fn check_fenced_movement(
        &self,
        snapshot: &FleetSnapshot,
        row: &Record,
        target: &RaftPeerNodeObservation,
        sources: &[OwnerClosure],
        quorums: &[RaftPeerFleetMembershipObservation],
    ) -> Result<()> {
        let permit = row.permit.as_ref().ok_or_else(invalid)?;
        let latest = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Admissions.slot()],
                &row.request.group,
            )
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        let owners = row.seed_owners.as_ref().ok_or_else(invalid)?;
        if latest != permit.encode().map_err(|_| invalid())?
            || owners.len() != 3
            || sources.len() != 3
            || quorums.len() != 3
        {
            return Err(FleetCatalogError::Unconfirmed);
        }
        self.check_movement_physical(
            snapshot,
            row,
            target,
            &row.request.attestation.boot,
            PhysicalRole::EmptyTarget,
        )
        .await?;
        for ((owner, closure), quorum) in owners.iter().zip(sources).zip(quorums) {
            self.check_fenced_owner(snapshot, row, owner, quorum)
                .await?;
            let OwnerClosure::Observed(sample) = closure else {
                return Err(FleetCatalogError::Unconfirmed);
            };
            let boot = sample
                .view
                .fleet_boot
                .as_ref()
                .ok_or(FleetCatalogError::Unconfirmed)?;
            if !owner.matches(boot)? {
                return Err(FleetCatalogError::Unconfirmed);
            }
            self.check_movement_physical(
                snapshot,
                row,
                sample,
                boot,
                PhysicalRole::FencedOwner {
                    member: owner.member,
                },
            )
            .await?;
        }
        Ok(())
    }

    async fn check_movement_physical(
        &self,
        snapshot: &FleetSnapshot,
        row: &Record,
        sample: &RaftPeerNodeObservation,
        boot: &BootBinding,
        role: PhysicalRole,
    ) -> Result<()> {
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
        let history = self.history_inner(snapshot, &row.request.group).await?;
        let scope = history.binding().scope().map_err(|_| invalid())?;
        let physical = inventory.history(&scope).map_err(|_| invalid())?;
        let matches = match (role, physical) {
            (PhysicalRole::EmptyTarget, None) => true,
            (PhysicalRole::FencedOwner { member }, Some(h)) => {
                h.birth_digest() == history.birth_digest() && h.member().get() == member
            }
            (PhysicalRole::Completed { member, sequence }, Some(h)) => {
                h.birth_digest() == history.birth_digest()
                    && h.member().get() == member
                    && h.completed_sequence() == sequence
            }
            _ => false,
        };
        if !matches
            || host.boot_binding()? != *boot
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
            || view.seed_workers != 0
            || inventory.initializing().is_some()
            || view.pending_history_registrations != 0
        {
            return Err(FleetCatalogError::Unconfirmed);
        }
        Ok(())
    }
}
