// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A rooted location hint never substitutes for a surviving quorum barrier.
use super::*;
use crate::raft::fleet_catalog::deadline::Deadline;
use crate::raft::host::RaftPeerFleetMembershipObservation;
use crate::raft::member_state::FleetPermitPhase;
use std::collections::BTreeSet;

impl FleetCatalog {
    pub(in super::super) async fn isolated_repair_source(
        &self,
        cut: &FleetSnapshot,
        group: &Group,
        failed: NodeId,
        observer: &crate::application::consensus::RaftCells,
        deadline: &Deadline<'_>,
    ) -> Result<RaftPeerFleetMembershipObservation> {
        let scope = group
            .history
            .binding()
            .scope()
            .map_err(|_| FleetCatalogError::Corrupt("isolated repair scope"))?;
        let authorization = self
            .authorize_group(cut, &scope, deadline.remaining()?)
            .await?;
        let mut last = FleetCatalogError::Unconfirmed;
        for (node, member) in &group.voters {
            if *node == failed {
                continue;
            }
            let host = self
                .run(deadline.remaining()?, async {
                    if self.inner.current().await?.token != cut.token {
                        return Err(FleetCatalogError::Conflict);
                    }
                    self.host_from(cut, *node).await
                })
                .await?;
            let sample = match observer
                .observe_peer_fleet_membership(
                    *node,
                    authorization.clone(),
                    cut.revision(),
                    deadline.remaining()?.min(Duration::from_millis(500)),
                )
                .await
            {
                Ok(sample) => sample,
                Err(crate::raft::inspection::InspectionError::Overloaded) => {
                    return Err(FleetCatalogError::Overloaded)
                }
                Err(error) => {
                    last = FleetCatalogError::Observation(error);
                    continue;
                }
            };
            let proof = sample.membership();
            let actual = proof.configuration();
            let current = sample
                .node()
                .current_enrollment
                .as_ref()
                .ok_or(FleetCatalogError::Unconfirmed)?;
            let published = sample
                .published_cut()
                .ok_or(FleetCatalogError::Unconfirmed)?;
            let expected = group.voters.values().copied().collect::<BTreeSet<_>>();
            let settled = match &group.latest {
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
            if sample.node().fleet_boot.as_ref() != Some(&host.boot_binding()?)
                || current.epoch != host.epoch()
                || current.state != host.state()
                || current.fleet != host.fleet()
                || current.physical_host != host.physical_host()
                || current.capability_digest != host.capability_digest()
                || current.revision < cut.revision()
                || sample
                    .node()
                    .fleet_trust_revision
                    .is_none_or(|r| r < cut.revision())
                || sample.node().node.identity.node != *node
                || sample.node().node.identity.disk != host.disk()
                || sample.node().node.identity.process.bytes() != host.boot_binding()?.process
                || Some(sample.node().node.storage_instance) != host.storage_instance()
                || proof.replica().node != *node
                || proof.replica().member != *member
                || proof.replica().disk != host.disk()
                || proof.replica().process != sample.node().node.identity.process
                || proof.status().binding != *group.history.binding()
                || proof.birth_digest() != group.history.birth_digest()
                || proof.schema_digest() != group.history.schema_digest()
                || !settled
                || actual.voters().len() != 3
                || actual.voters().iter().copied().collect::<BTreeSet<_>>() != expected
                || !actual.outgoing_voters().is_empty()
                || !actual.learners().is_empty()
                || !actual.next_learners().is_empty()
                || actual.auto_leave()
                || proof
                    .repair_status()
                    .map_err(|_| FleetCatalogError::Corrupt("isolated repair membership"))?
                    .is_some_and(|s| s.phase.active())
                || published.binding() != group.history.binding()
                || published.replica() != proof.replica()
                || published.birth_digest() != group.history.birth_digest()
                || published.schema_digest() != group.history.schema_digest()
                || published.storage_instance() != sample.node().node.storage_instance
                || published.membership_revision() != proof.membership_revision()
                || published.applied() != proof.applied()
            {
                return Err(FleetCatalogError::Unconfirmed);
            }
            self.require_resource_age(
                self.resource_now()?
                    .checked_sub(sample.sample_age_ms())
                    .ok_or(FleetCatalogError::Unconfirmed)?,
            )?;
            return Ok(sample);
        }
        Err(last)
    }
}
