// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Rooted settled intent locates voters; it does not replace fresh quorum proof.
use super::completion::Settlement;
use super::*;
use crate::raft::member_state::FleetMembershipPermit;
use cellar_logic::consensus::MemberId;
use std::collections::{BTreeMap, BTreeSet};

impl FleetCatalog {
    /// Candidate endpoints only. Settled managed placement narrows discovery;
    /// an incomplete/untracked admission keeps the existing bounded fleet scan.
    /// Neither a target list nor a settled journal replaces fresh quorum proof.
    pub(in crate::raft::fleet_catalog) async fn router_voter_hints(
        &self,
        snapshot: &FleetSnapshot,
        history: &super::super::histories::FleetHistory,
    ) -> Result<Option<Vec<NodeId>>> {
        let key = history
            .binding()
            .key()
            .map_err(|_| FleetCatalogError::Corrupt("router group key"))?;
        if let Some(bytes) = self
            .inner
            .lookup(snapshot.head.tables[FleetTable::Admissions.slot()], &key)
            .await?
        {
            let permit = FleetMembershipPermit::decode(&bytes)
                .map_err(|_| FleetCatalogError::Corrupt("router membership permit"))?;
            if self
                .inner
                .lookup(
                    snapshot.head.tables[FleetTable::Operations.slot()],
                    &row_key(permit.id()),
                )
                .await?
                .is_none()
            {
                return Ok(None);
            }
        }
        match self.settled_group_voters(snapshot, history).await {
            Ok((voters, _)) => Ok(Some(voters.into_keys().collect())),
            Err(FleetCatalogError::InventoryIncomplete) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Locate the exact settled voters for bounded partition observations.
    /// Addresses remain advisory; the peer must supply its own quorum proof.
    pub(crate) async fn partition_voters(
        &self,
        snapshot: &FleetSnapshot,
        history: &super::super::histories::FleetHistory,
        timeout: Duration,
    ) -> Result<BTreeMap<NodeId, MemberId>> {
        self.run(timeout, async {
            if self.inner.current().await?.token != snapshot.token {
                return Err(FleetCatalogError::Conflict);
            }
            self.require_transition_membership_idle(snapshot).await?;
            Ok(self.settled_group_voters(snapshot, history).await?.0)
        })
        .await
    }

    /// Revalidate a scheduler's source against the exact fleet root and a
    /// purpose-specific quorum observation. The subsequent admission still
    /// owns the topology slot, capacity checks and directory CAS.
    pub(crate) async fn require_partition_source(
        &self,
        snapshot: &FleetSnapshot,
        directory: &crate::raft::catalog::RangeDirectory,
        observer: &crate::application::consensus::RaftCells,
        basis: &cellar_logic::cell_group_placement::PartitionBasis,
        timeout: Duration,
    ) -> Result<()> {
        use crate::raft::inspection::InspectionError;
        let deadline = super::super::deadline::Deadline::new(self, timeout)?;
        let scope = &basis.route.history;
        let history = self.history(snapshot, scope, deadline.remaining()?).await?;
        if !history.check_route(directory, &basis.route)
            || directory.namespace_config().minimum_durability != basis.durability
        {
            return Err(FleetCatalogError::Conflict);
        }
        let voters = self
            .partition_voters(snapshot, &history, deadline.remaining()?)
            .await?;
        let authorization = self
            .authorize_group(snapshot, scope, deadline.remaining()?)
            .await?;
        let expected: BTreeSet<_> = voters.values().copied().collect();
        let mut last = FleetCatalogError::Unconfirmed;
        for (node, member) in voters {
            let sample = match observer
                .observe_peer_fleet_membership(
                    node,
                    authorization.clone(),
                    snapshot.revision(),
                    deadline.remaining()?.min(Duration::from_millis(500)),
                )
                .await
            {
                Ok(sample) => sample,
                Err(InspectionError::Overloaded) => return Err(FleetCatalogError::Overloaded),
                Err(error) => {
                    last = FleetCatalogError::Observation(error);
                    continue;
                }
            };
            let started = self
                .resource_now()?
                .checked_sub(sample.sample_age_ms())
                .ok_or(FleetCatalogError::Unconfirmed)?;
            self.require_resource_age(started)?;
            let proof = sample.membership();
            let configuration = proof.configuration();
            if proof.replica().node != node
                || proof.replica().member != member
                || proof.root().route() != &basis.route
                || proof.applied().term != basis.term
                || proof.membership_revision().get() != basis.membership.get()
                || configuration
                    .voters()
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>()
                    != expected
                || !configuration.outgoing_voters().is_empty()
                || !configuration.learners().is_empty()
                || !configuration.next_learners().is_empty()
                || configuration.auto_leave()
            {
                return Err(FleetCatalogError::Conflict);
            }
            return Ok(());
        }
        Err(last)
    }

    pub(in crate::raft::fleet_catalog) async fn resource_permit_authorization(
        &self,
        snapshot: &FleetSnapshot,
        permit: FleetMembershipPermit,
    ) -> Result<FleetPermitAuthorization> {
        if self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &row_key(permit.id()),
            )
            .await?
            .is_none()
        {
            // Preserve the standalone low-level permit API. Managed physical
            // admission independently requires Some(original destination boot).
            return Ok(FleetPermitAuthorization::admitted(
                permit,
                snapshot.revision(),
                None,
            ));
        }
        let row = self.admitted_resource(snapshot, permit.id()).await?;
        if row.permit.as_ref() != Some(&permit) {
            return Err(FleetCatalogError::Corrupt("resource permit projection"));
        }
        Ok(FleetPermitAuthorization::admitted(
            permit,
            snapshot.revision(),
            Some(row.request.attestation.boot),
        ))
    }

    // Called within an owned catalog turn. Return only the configuration pinned
    // by immutable birth/permit/settlement journals, never a current-health lease.
    pub(crate) async fn settled_group_voters(
        &self,
        snapshot: &FleetSnapshot,
        history: &super::super::histories::FleetHistory,
    ) -> Result<(BTreeMap<NodeId, MemberId>, Option<FleetMembershipPermit>)> {
        let key = history
            .binding
            .key()
            .map_err(|_| FleetCatalogError::Corrupt("topology group key"))?;
        let latest = self
            .inner
            .lookup(snapshot.head.tables[FleetTable::Admissions.slot()], &key)
            .await?
            .map(|bytes| {
                FleetMembershipPermit::decode(&bytes)
                    .map_err(|_| FleetCatalogError::Corrupt("topology permit"))
            })
            .transpose()?;
        let mut voters = BTreeMap::new();
        if let Some(permit) = &latest {
            let row = self.admitted_resource(snapshot, permit.id()).await?;
            let (kind, _) = self
                .resource_settlement(snapshot, &row)
                .await?
                .ok_or(FleetCatalogError::InventoryIncomplete)?;
            if row.permit.as_ref() != Some(permit) || permit.binding() != history.binding() {
                return Err(FleetCatalogError::Conflict);
            }
            for owner in row
                .seed_owners
                .as_ref()
                .ok_or(FleetCatalogError::InventoryIncomplete)?
            {
                voters.insert(
                    NodeId::new(owner.node)
                        .map_err(|_| FleetCatalogError::Corrupt("topology seed owner"))?,
                    MemberId::new(owner.member)
                        .map_err(|_| FleetCatalogError::Corrupt("topology seed member"))?,
                );
            }
            if kind == Settlement::Complete {
                let plan = row
                    .request
                    .movement
                    .as_ref()
                    .ok_or(FleetCatalogError::InventoryIncomplete)?
                    .plan()?;
                voters.retain(|_, member| *member != plan.obsolete);
                voters.insert(plan.replacement.node, plan.replacement.member);
            }
        } else {
            if history.voters.len() != 3 || !history.learners.is_empty() {
                return Err(FleetCatalogError::InventoryIncomplete);
            }
            for member in &history.voters {
                voters.insert(
                    NodeId::new(member.node)
                        .map_err(|_| FleetCatalogError::Corrupt("topology voter"))?,
                    MemberId::new(member.member)
                        .map_err(|_| FleetCatalogError::Corrupt("topology member"))?,
                );
            }
        }
        if voters.len() != 3 || voters.values().copied().collect::<BTreeSet<_>>().len() != 3 {
            return Err(FleetCatalogError::InventoryIncomplete);
        }
        Ok((voters, latest))
    }
}
