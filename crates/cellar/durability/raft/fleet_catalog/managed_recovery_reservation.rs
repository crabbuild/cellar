// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Root replacement history and its original voter boots while the recovery owns topology.
use super::{
    histories::{birth_mutation, catalog_error, validate_request},
    managed_transition_admission::TransitionOperatorStep,
    *,
};
use crate::raft::{
    catalog::{CellGroupBirthRequest, RangeDirectory},
    cell_groups::CellGroupArchivePool,
    fleet_stop::BootBinding,
};
use cellar_logic::{
    cell_group::{GroupHistory, GroupRoute},
    consensus::{NodeId, ProposalId},
};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

pub(super) const PREFIX: &str = "managed-recovery-birth/";
const KIND: u8 = 225;
const LIMIT: usize = 4096;

#[derive(PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    plan: [u8; 32],
    request: FleetRequestIdentity,
    voters: Vec<BootBinding>,
}

/// A rooted placement reservation for one recovery range. Its voter boots
/// are historical evidence. The seeding owner must recheck current boot and
/// engine admission before performing physical work.
pub struct FleetRecoveryRangeReservation {
    record: Record,
    route: GroupRoute,
    applied_revision: u64,
}

impl FleetRecoveryRangeReservation {
    pub fn plan_digest(&self) -> [u8; 32] {
        self.record.plan
    }

    pub fn route(&self) -> &GroupRoute {
        &self.route
    }

    pub fn voter_boots(&self) -> &[BootBinding] {
        &self.record.voters
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied_revision
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ManagedRecoverySeedSelection<'a> {
    pub(crate) fenced: &'a FleetFencedTransitionRecovery,
    pub(crate) plan: &'a FleetTransitionRecoveryReplacement,
    pub(crate) reservation: &'a FleetRecoveryRangeReservation,
}

pub(crate) struct ManagedRecoverySeedAuthority {
    pub(crate) group: crate::raft::member_state::FleetGroupBinding,
    pub(crate) boot: BootBinding,
}

impl FleetHostAuthorization {
    pub(crate) async fn authorize_managed_recovery_seed(
        &self,
        directory: &RangeDirectory,
        selected: ManagedRecoverySeedSelection<'_>,
        observed: (&crate::raft::host::RaftHostObservation, bool),
        archives: &CellGroupArchivePool,
        timeout: Duration,
    ) -> Result<ManagedRecoverySeedAuthority> {
        let catalog = &self.catalog;
        let ManagedRecoverySeedSelection {
            fenced,
            plan,
            reservation,
        } = selected;
        let (view, already_seeded) = observed;
        catalog
            .run(timeout, async {
                catalog.require_managed_replacement(fenced, plan).await?;
                catalog
                    .require_confirmed_managed_fence(directory, fenced, archives)
                    .await?;
                let birth = plan
                    .births()
                    .iter()
                    .find(|birth| birth.route() == reservation.route())
                    .ok_or(FleetCatalogError::Conflict)?;
                if reservation.plan_digest() != plan.digest() {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                let intent = fenced.request().intent();
                let snapshot = catalog.inner.current().await?;
                catalog
                    .managed_recovery_permit_at(&snapshot, directory, intent.basis())
                    .await?;
                let rooted = catalog
                    .managed_recovery_reservation_at(&snapshot, directory, intent, plan, birth)
                    .await?
                    .ok_or(FleetCatalogError::Conflict)?;
                if rooted.record != reservation.record
                    || rooted.applied_revision != reservation.applied_revision
                {
                    return Err(FleetCatalogError::Conflict);
                }
                let group = catalog.binding(&birth.route().history)?;
                let boot = self.boot_binding()?;
                if !rooted.voter_boots().contains(&boot) {
                    return Err(FleetCatalogError::Conflict);
                }
                let current = catalog.host_from(&snapshot, self.node()).await?;
                let capacity = catalog.capacity_from(&snapshot, self.node()).await?;
                let attestation = capacity.attestation();
                if !capacity.receive_replicas()
                    || boot != attestation.boot
                    || current.boot_binding()? != boot
                {
                    return Err(FleetCatalogError::Conflict);
                }
                super::capacity::check_restart_capacity_view(&current, attestation, view, false)?;
                if !already_seeded {
                    let profile = &view.node.profile;
                    let bytes = view
                        .node
                        .reserved_bytes
                        .max(attestation.used_durable_bytes)
                        .checked_add(profile.bytes_per_group)
                        .ok_or(FleetCatalogError::Limit)?;
                    if view.node.groups >= profile.group_capacity
                        || bytes > profile.byte_capacity.min(attestation.durable_bytes)
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                }
                let old = fenced.request().intent().required_voter_lineages();
                let old_nodes: BTreeSet<_> = old.iter().map(|member| member.node.bytes()).collect();
                for node in catalog.node_records(&snapshot).await? {
                    if old_nodes.contains(&node.node) && node.storage_instance == Some(boot.engine)
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                }
                catalog
                    .inner
                    .domain
                    .run(directory.require_managed_recovery_identity(birth, plan.digest()))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(catalog_error)?;
                if catalog.inner.current().await?.token != snapshot.token {
                    return Err(FleetCatalogError::Conflict);
                }
                Ok(ManagedRecoverySeedAuthority { group, boot })
            })
            .await
    }
}

impl FleetCatalog {
    pub(crate) async fn reserve_managed_recovery_range(
        &self,
        directory: &RangeDirectory,
        fenced: &FleetFencedTransitionRecovery,
        plan: &FleetTransitionRecoveryReplacement,
        scope: GroupHistory,
        archives: &CellGroupArchivePool,
        timeout: Duration,
    ) -> Result<FleetRecoveryRangeReservation> {
        self.run(timeout, async {
            self.require_managed_replacement(fenced, plan).await?;
            self.require_confirmed_managed_fence(directory, fenced, archives)
                .await?;
            let birth = plan
                .births()
                .iter()
                .find(|birth| birth.route().history == scope)
                .ok_or(FleetCatalogError::Conflict)?;
            let intent = fenced.request().intent();
            let step = recovery_reservation_step(intent, plan, birth);
            let binding = self.binding(&scope)?;
            let history_key = binding
                .key()
                .map_err(|_| FleetCatalogError::Invalid("recovery history key"))?;
            let key = format!("{PREFIX}{history_key}");
            let snapshot = self.inner.current().await?;
            self.managed_recovery_permit_at(&snapshot, directory, intent.basis())
                .await?;
            let domain = &self.inner.domain;
            if let Some(reserved) = self
                .managed_recovery_reservation_at(&snapshot, directory, intent, plan, birth)
                .await?
            {
                domain
                    .run(directory.require_managed_recovery_identity(birth, plan.digest()))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(catalog_error)?;
                return Ok(reserved);
            }
            self.require_namespace_declaration(&snapshot, directory.namespace(), None)
                .await?;
            let owner = self.managed_owner_for_birth(&snapshot, directory).await?;
            let request = step.identity(snapshot.revision());
            validate_request(&request)?;
            let history = FleetHistory::reserved_group(binding, directory, birth, &request)?;
            self.require_unoccupied_birth(&snapshot, &history, &history_key)
                .await?;
            let voters = self.recovery_voter_boots(&snapshot, fenced, birth).await?;
            let record = Record {
                plan: plan.digest(),
                request,
                voters,
            };
            check_boots(birth, &record, self.catalog_id()?)?;
            let mutation = reservation_mutation(&record, &history, &history_key, key)?;
            mutation.validate()?;

            let transition = ProposalId::new(intent.transition())
                .map_err(|_| FleetCatalogError::Corrupt("recovery transition operation"))?;
            domain
                .run(Box::pin(directory.reserve_managed_recovery_identity(
                    birth,
                    &owner,
                    transition,
                    fenced.request().barrier().directory_digest(),
                    plan.digest(),
                )))
                .await
                .map_err(|_| FleetCatalogError::Closed)?
                .map_err(catalog_error)?;
            let committed = Box::pin(self.commit_with_disruption(
                &snapshot,
                &mutation,
                None,
                resources::Authority::ManagedRecoveryReservation,
                || Ok(()),
            ))
            .await?;
            let applied_revision = self
                .recovery_birth_commit(&committed.snapshot, &mutation)
                .await?;
            Ok(FleetRecoveryRangeReservation {
                record,
                route: birth.route().clone(),
                applied_revision,
            })
        })
        .await
    }

    pub(super) async fn managed_recovery_reservation_at(
        &self,
        snapshot: &FleetSnapshot,
        directory: &RangeDirectory,
        intent: &FleetTransitionRecoveryIntent,
        plan: &FleetTransitionRecoveryReplacement,
        birth: &CellGroupBirthRequest,
    ) -> Result<Option<FleetRecoveryRangeReservation>> {
        let binding = self.binding(&birth.route().history)?;
        let history_key = binding
            .key()
            .map_err(|_| FleetCatalogError::Invalid("recovery history key"))?;
        let key = format!("{PREFIX}{history_key}");
        let Some(bytes) = self
            .inner
            .lookup(snapshot.head.tables[FleetTable::Policy.slot()], &key)
            .await?
        else {
            return Ok(None);
        };
        let record: Record = decode(KIND, &bytes, LIMIT)?;
        let step = recovery_reservation_step(intent, plan, birth);
        if record.plan != plan.digest()
            || record.request != step.identity(record.request.expected_revision)
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        check_boots(birth, &record, self.catalog_id()?)?;
        let history = self.history_inner(snapshot, &history_key).await?;
        if history != FleetHistory::reserved_group(binding, directory, birth, &record.request)? {
            return Err(FleetCatalogError::Conflict);
        }
        let mutation = reservation_mutation(&record, &history, &history_key, key)?;
        let applied_revision = self.recovery_birth_commit(snapshot, &mutation).await?;
        Ok(Some(FleetRecoveryRangeReservation {
            record,
            route: birth.route().clone(),
            applied_revision,
        }))
    }

    async fn recovery_birth_commit(
        &self,
        snapshot: &FleetSnapshot,
        mutation: &FleetMutation,
    ) -> Result<u64> {
        let digest = hash(&encode(REQUEST_KIND, mutation, REQUEST_LIMIT)?);
        self.inner
            .committed(&snapshot.head, &hex::encode(mutation.id), digest)
            .await?
            .filter(|applied| Some(*applied) == mutation.expected_revision.checked_add(1))
            .ok_or(FleetCatalogError::Corrupt("recovery reservation commit"))
    }

    async fn recovery_voter_boots(
        &self,
        snapshot: &FleetSnapshot,
        fenced: &FleetFencedTransitionRecovery,
        birth: &CellGroupBirthRequest,
    ) -> Result<Vec<BootBinding>> {
        let old = fenced.request().intent().required_voter_lineages();
        let old_hosts: BTreeSet<_> = old.iter().map(|member| member.physical_host).collect();
        let old_disks: BTreeSet<_> = old.iter().map(|member| member.disk).collect();
        let old_nodes: BTreeSet<_> = old.iter().map(|member| member.node).collect();
        let mut separation = BTreeSet::new();
        let mut voters = Vec::with_capacity(birth.voters().len());
        let mut slots = self.recovery_history_slots(snapshot, birth).await?;
        self.charge_recovery_movement_slots(snapshot, &mut slots)
            .await?;
        for voter in birth.voters() {
            let host = self.host_from(snapshot, voter.node).await?;
            let capacity = self.capacity_from(snapshot, voter.node).await?;
            let partition = match snapshot.replica_separation() {
                FleetReplicaSeparation::Nodes => host.node().bytes(),
                FleetReplicaSeparation::PhysicalHosts => host.physical_host().bytes(),
            };
            let occupied = slots
                .get(&voter.node)
                .ok_or(FleetCatalogError::Corrupt("recovery voter slots"))?;
            if old_hosts.contains(&host.physical_host())
                || old_disks.contains(&host.disk())
                || old_nodes.contains(&host.node())
                || !separation.insert(partition)
                || !capacity.receive_replicas()
                || *occupied >= capacity.attestation().engine_slots
            {
                return Err(FleetCatalogError::Conflict);
            }
            voters.push(capacity.attestation().boot.clone());
        }
        Ok(voters)
    }

    async fn recovery_history_slots(
        &self,
        snapshot: &FleetSnapshot,
        birth: &CellGroupBirthRequest,
    ) -> Result<BTreeMap<NodeId, u32>> {
        let mut slots: BTreeMap<_, _> = birth
            .voters()
            .iter()
            .map(|voter| (voter.node, 0u32))
            .collect();
        let mut cursor = None;
        let mut seen = 0u64;
        loop {
            let page = self
                .inner
                .scan(
                    snapshot.head.tables[FleetTable::Histories.slot()],
                    cursor.as_ref(),
                    128,
                )
                .await?;
            for entry in page.entries {
                seen = seen
                    .checked_add(1)
                    .filter(|seen| *seen <= HISTORY_LIMIT)
                    .ok_or(FleetCatalogError::Limit)?;
                let history = FleetHistory::decode(&entry.value)?;
                if matches!(history.state(), FleetHistoryState::Cancelled { .. }) {
                    continue;
                }
                // Include reserved and retired occupancy until its owner
                // settles it. Historical initial placements are conservative.
                for member in history.voters.iter().chain(&history.learners) {
                    let node = NodeId::new(member.node)
                        .map_err(|_| FleetCatalogError::Corrupt("recovery placement node"))?;
                    if let Some(count) = slots.get_mut(&node) {
                        *count = count.checked_add(1).ok_or(FleetCatalogError::Limit)?;
                    }
                }
            }
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        if seen != snapshot.head.history_count {
            return Err(FleetCatalogError::Corrupt("recovery placement inventory"));
        }
        Ok(slots)
    }
}

fn recovery_reservation_step(
    intent: &FleetTransitionRecoveryIntent,
    plan: &FleetTransitionRecoveryReplacement,
    birth: &CellGroupBirthRequest,
) -> TransitionOperatorStep {
    TransitionOperatorStep {
        operation: plan.mutation().operation(),
        principal: intent.principal().into(),
        request_digest: plan.digest(),
        step: birth.creation().bytes(),
    }
}

fn check_boots(birth: &CellGroupBirthRequest, record: &Record, catalog: [u8; 32]) -> Result<()> {
    if birth.voters().len() != record.voters.len() {
        return Err(FleetCatalogError::Corrupt("recovery voter count"));
    }
    for (voter, boot) in birth.voters().iter().zip(&record.voters) {
        boot.validate()
            .map_err(|_| FleetCatalogError::Corrupt("recovery voter boot"))?;
        if boot.catalog != catalog || boot.node != voter.node.bytes() {
            return Err(FleetCatalogError::Corrupt("recovery voter binding"));
        }
    }
    Ok(())
}

fn reservation_mutation(
    record: &Record,
    history: &FleetHistory,
    history_key: &str,
    key: String,
) -> Result<FleetMutation> {
    let mut mutation = birth_mutation(history, &record.request, history_key, None)?;
    mutation.changes.push(FleetChange {
        table: FleetTable::Policy,
        key,
        value: Some(encode(KIND, record, LIMIT)?),
    });
    Ok(mutation)
}
