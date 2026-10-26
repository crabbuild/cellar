// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Rooted terminal intent locates voters; actual quorum calls prove their state.
use super::*;
use crate::raft::member_state::FleetPermitPhase;

impl FleetTopologyVerification {
    pub(super) async fn verify_group(
        &self,
        history: FleetHistory,
        resources: FleetTopologyHistoryResources,
        deadline: &super::super::super::deadline::Deadline<'_>,
    ) -> Result<GroupInput> {
        let catalog = &self.catalog;
        let key = history
            .binding()
            .key()
            .map_err(|_| FleetCatalogError::Corrupt("topology group key"))?;
        let (voters, latest) = catalog
            .run(
                deadline.remaining()?,
                catalog.settled_group_voters(&self.snapshot, &history),
            )
            .await?;
        let scope = history
            .binding
            .scope()
            .map_err(|_| FleetCatalogError::Corrupt("topology scope"))?;
        if scope != resources.group {
            return Err(FleetCatalogError::Conflict);
        }
        // Unmaterialized genesis remains debt even if the namespace has already
        // committed its birth. Detect it before probing a not-yet-running group.
        for input in &self.nodes {
            let inventory = input
                .physical
                .inventory
                .as_ref()
                .ok_or(FleetCatalogError::InventoryIncomplete)?;
            if (history
                .voters
                .iter()
                .chain(&history.learners)
                .any(|m| m.node == input.host.node().bytes())
                || voters.contains_key(&input.host.node()))
                && inventory
                    .history(&scope)
                    .map_err(|_| FleetCatalogError::Corrupt("topology genesis"))?
                    .is_none()
            {
                return Err(FleetCatalogError::InventoryIncomplete);
            }
        }
        let authorization = catalog
            .authorize_group(&self.snapshot, &scope, deadline.remaining()?)
            .await?;
        let expected: BTreeSet<_> = voters.values().copied().collect();
        let mut previous = None;
        let mut high_water = 0;
        let mut revision = None;
        let mut retained = BTreeMap::new();
        for (node, member) in &voters {
            let input = self
                .nodes
                .iter()
                .find(|input| input.host.node() == *node)
                .ok_or(FleetCatalogError::InventoryIncomplete)?;
            let sample = self
                .observer
                .observe_peer_fleet_membership(
                    *node,
                    authorization.clone(),
                    self.snapshot.revision(),
                    deadline.remaining()?,
                )
                .await
                .map_err(FleetCatalogError::Observation)?;
            catalog.require_resource_age(
                catalog
                    .resource_now()?
                    .checked_sub(sample.sample_age_ms())
                    .ok_or(FleetCatalogError::Unconfirmed)?,
            )?;
            let proof = sample.membership();
            let actual = proof.configuration();
            let published = sample
                .published_cut()
                .ok_or(FleetCatalogError::Unconfirmed)?;
            let current = sample
                .node()
                .current_enrollment
                .as_ref()
                .ok_or(FleetCatalogError::Unconfirmed)?;
            let settled = match &latest {
                None => {
                    proof.status().consumed_sequence == 0
                        && proof.status().permit.is_none()
                        && proof.status().phase.is_none()
                }
                Some(permit) => {
                    proof.status().permit.as_ref() == Some(permit)
                        && proof.status().consumed_sequence == permit.sequence()
                        && matches!(
                            proof.status().phase,
                            Some(
                                FleetPermitPhase::Complete
                                    | FleetPermitPhase::Fenced
                                    | FleetPermitPhase::Aborted
                            )
                        )
                }
            };
            if sample.node().fleet_boot.as_ref() != Some(&input.capacity.boot)
                || sample.node().node.identity.node != *node
                || sample.node().node.identity.disk != input.host.disk()
                || sample.node().node.identity.process.bytes() != input.capacity.boot.process
                || Some(sample.node().node.storage_instance) != input.host.storage_instance()
                || current.epoch != input.host.epoch()
                || current.state != input.host.state()
                || current.fleet != input.host.fleet()
                || current.physical_host != input.host.physical_host()
                || current.capability_digest != input.host.capability_digest()
                || current.revision < self.snapshot.revision()
                || sample
                    .node()
                    .fleet_trust_revision
                    .is_none_or(|r| r < self.snapshot.revision())
                || proof.replica().node != *node
                || proof.replica().member != *member
                || proof.replica().disk != input.host.disk()
                || proof.replica().process.bytes() != input.capacity.boot.process
                || proof.status().binding != history.binding
                || proof.birth_digest() != history.birth_digest()
                || proof.schema_digest() != history.schema_digest()
                || !settled
                || previous
                    .as_ref()
                    .is_some_and(|status| status != proof.status())
                || revision.is_some_and(|r| r != proof.membership_revision())
                || actual.voters().len() != 3
                || actual.voters().iter().copied().collect::<BTreeSet<_>>() != expected
                || !actual.outgoing_voters().is_empty()
                || !actual.learners().is_empty()
                || !actual.next_learners().is_empty()
                || actual.auto_leave()
                || proof
                    .repair_status()
                    .map_err(|_| FleetCatalogError::Corrupt("topology repair"))?
                    .is_some_and(|s| s.phase.active())
                || published.binding() != history.binding()
                || published.replica() != proof.replica()
                || published.birth_digest() != history.birth_digest()
                || published.schema_digest() != history.schema_digest()
                || published.storage_instance() != sample.node().node.storage_instance
                || published.membership_revision() != proof.membership_revision()
                || published.applied() != proof.applied()
            {
                return Err(FleetCatalogError::Unconfirmed);
            }
            // Validate all occupied bindings, including original/retired copies,
            // while the one bounded membership ledger is still in memory.
            for input in &self.nodes {
                let inventory = input
                    .physical
                    .inventory
                    .as_ref()
                    .ok_or(FleetCatalogError::InventoryIncomplete)?;
                let physical = inventory
                    .history(&scope)
                    .map_err(|_| FleetCatalogError::Corrupt("topology physical binding"))?;
                if (history
                    .voters
                    .iter()
                    .chain(&history.learners)
                    .any(|m| m.node == input.host.node().bytes())
                    || voters.contains_key(&input.host.node()))
                    && physical.is_none()
                {
                    return Err(FleetCatalogError::InventoryIncomplete);
                }
                if let Some(physical) = physical {
                    let binding = match proof
                        .joined_member(physical.member())
                        .map_err(|_| FleetCatalogError::Corrupt("topology joined member"))?
                    {
                        Some((joined, _)) => {
                            joined.node == input.host.node() && joined.disk == input.host.disk()
                        }
                        None => {
                            if history.voters.iter().chain(&history.learners).any(|m| {
                                m.node == input.host.node().bytes()
                                    && m.member == physical.member().get()
                            }) {
                                true
                            } else if let Some(valid) =
                                retained.get(&(input.host.node(), physical.member()))
                            {
                                *valid
                            } else {
                                let valid = catalog
                                    .run(
                                        deadline.remaining()?,
                                        catalog.retained_resource_binding(
                                            &self.snapshot,
                                            &key,
                                            physical.member(),
                                            &input.capacity.boot,
                                        ),
                                    )
                                    .await?;
                                retained.insert((input.host.node(), physical.member()), valid);
                                valid
                            }
                        }
                    };
                    if physical.birth_digest() != history.birth_digest() || !binding {
                        return Err(FleetCatalogError::Conflict);
                    }
                }
                if resources.resident.durable_bytes
                    < input.physical.view.node.profile.bytes_per_group
                {
                    return Err(FleetCatalogError::Invalid(
                        "topology resident byte reservation",
                    ));
                }
            }
            previous = Some(proof.status().clone());
            revision = Some(proof.membership_revision());
            high_water = high_water.max(proof.member_high_water());
        }
        Ok(GroupInput {
            resources,
            voters,
            revision: revision.ok_or(FleetCatalogError::Unconfirmed)?,
            high_water,
        })
    }
}
