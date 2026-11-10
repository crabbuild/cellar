// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Rooted source cleanup owns the transition until every local slot settles.
use super::{histories::catalog_error, *};
use crate::application::consensus::RaftCells;
use crate::raft::cell_groups::{RangeDirectory, TransitionPhase};
use cellar_logic::consensus::NodeId;
use serde::Deserialize;
use std::collections::BTreeMap;

pub(super) const PREFIX: &str = "source-cleanup/";
const KIND: u8 = 236;
const LIMIT: usize = 4096;
const MAX_NODES: usize = 6;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Node {
    node: [u8; 16],
    disk: [u8; 16],
    engine: [u8; 16],
    before: u32,
    floor: u32,
    removed: u32,
    settled: bool,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cleanup {
    format: u8,
    namespace: [u8; 32],
    operation: [u8; 16],
    plan: [u8; 32],
    retirement: [u8; 32],
    nodes: Vec<Node>,
}

pub(super) struct SlotRelease {
    pub(super) before: u32,
    pub(super) floor: u32,
    pub(super) removed: u32,
    pub(super) physical: FleetResourceBounds,
}

impl Cleanup {
    fn check(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
    ) -> Result<()> {
        if self.format != 1
            || self.namespace != directory.namespace().root_digest()
            || self.operation != transition.request.operation
            || self.plan != transition.plan.digest()
            || self.retirement == [0; 32]
            || self.nodes.is_empty()
            || self.nodes.len() > MAX_NODES
            || self.nodes.windows(2).any(|p| p[0].node >= p[1].node)
            || self.nodes.iter().any(|n| {
                [n.node, n.disk, n.engine].contains(&[0; 16])
                    || !(1..=2).contains(&n.removed)
                    || n.before < n.removed
            })
        {
            return Err(FleetCatalogError::Corrupt("source cleanup binding"));
        }
        Ok(())
    }

    fn encode(&self) -> Result<Vec<u8>> {
        encode(KIND, self, LIMIT)
    }

    fn complete(&self) -> bool {
        self.nodes.iter().all(|n| n.settled)
    }
}

fn key(operation: [u8; 16]) -> String {
    format!("{PREFIX}{}", hex::encode(operation))
}

fn attestation(capacity: &FleetCapacityAttestation) -> FleetResourceAttestation {
    FleetResourceAttestation {
        boot: capacity.boot.clone(),
        profile_revision: capacity.profile_revision,
        used: FleetResourceBounds {
            engine_slots: 0,
            durable_bytes: capacity.used_durable_bytes,
            memory_bytes: capacity.used_memory_bytes,
        },
        evidence_digest: capacity.evidence_digest,
        evidence_reference: capacity.evidence_reference.clone(),
    }
}

fn mutation(
    snapshot: &FleetSnapshot,
    transition: &ReopenedManagedCellGroupTransition,
    changes: Vec<FleetChange>,
) -> Result<FleetMutation> {
    let seed = encode(
        KIND,
        &(
            snapshot.revision(),
            transition.request.operation,
            transition.plan.digest(),
            &changes,
        ),
        REQUEST_LIMIT,
    )?;
    let digest = hash(&seed);
    let mut id = [0; 16];
    id.copy_from_slice(&digest[..16]);
    id[0] |= 1;
    Ok(FleetMutation {
        id,
        operation: transition.request.operation,
        expected_revision: snapshot.revision(),
        principal: transition.request.principal.clone(),
        changes,
    })
}

impl FleetCatalog {
    async fn source_cleanup(
        &self,
        snapshot: &FleetSnapshot,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
    ) -> Result<Option<Cleanup>> {
        let Some(bytes) = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &key(transition.request.operation),
            )
            .await?
        else {
            return Ok(None);
        };
        let cleanup: Cleanup = decode(KIND, &bytes, LIMIT)?;
        cleanup.check(transition, directory)?;
        Ok(Some(cleanup))
    }

    async fn require_cleanup_owner(
        &self,
        snapshot: &FleetSnapshot,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
    ) -> Result<()> {
        self.managed_transition_permit_at(
            snapshot,
            &transition.request,
            directory,
            &transition.plan,
        )
        .await?;
        match self.active_disruption(snapshot).await? {
            Some(disruption::Disruption::Transition(active))
                if active.operation() == transition.request.operation => {}
            _ => return Err(FleetCatalogError::Conflict),
        }
        self.require_transition_membership_idle(snapshot).await
    }

    pub(crate) async fn source_cleanup_status(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        timeout: Duration,
    ) -> Result<Option<bool>> {
        self.run(
            timeout,
            Box::pin(async {
                let snapshot = self.inner.current().await?;
                Ok(self
                    .source_cleanup(&snapshot, transition, directory)
                    .await?
                    .map(|c| c.complete()))
            }),
        )
        .await
    }

    pub(super) async fn require_source_cleanup_complete(
        &self,
        snapshot: &FleetSnapshot,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
    ) -> Result<()> {
        if self
            .source_cleanup(snapshot, transition, directory)
            .await?
            .is_some_and(|c| !c.complete())
        {
            return Err(FleetCatalogError::Unconfirmed);
        }
        Ok(())
    }

    /// An active transition must select cleanup ownership before a manual call
    /// can remove its inventory. Completed older operations keep their existing
    /// physical-only API; their floors remain conservative.
    pub(crate) async fn require_source_cleanup_admission(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        timeout: Duration,
    ) -> Result<()> {
        self.run(timeout, Box::pin(async {
            let snapshot = self.inner.current().await?;
            if self.source_cleanup(&snapshot, transition, directory).await?.is_some() { return Ok(()); }
            for route in directory.transition_routes(&transition.plan).map_err(catalog_error)?.0 {
                let history = self.history_inner(&snapshot, &self.binding(&route.history)?.key().map_err(|_| FleetCatalogError::Conflict)?).await?;
                if !matches!(history.state, FleetHistoryState::Retired { operation, .. } if operation == transition.request.operation) { return Err(FleetCatalogError::Conflict); }
            }
            Ok(())
        })).await
    }

    /// Freeze exact pre-cleanup occupancy while the original disruption owns all
    /// affected nodes. Replays precede probes because selected cleanup may have
    /// already erased a source. No expiry or lower sample authorizes a refund.
    pub(crate) async fn begin_source_cleanup(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        observer: &RaftCells,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = deadline::Deadline::new(self, timeout)?;
        let prepared = self
            .run(
                deadline.remaining()?,
                Box::pin(async {
                    let snapshot = self.inner.current().await?;
                    if self
                        .source_cleanup(&snapshot, transition, directory)
                        .await?
                        .is_some()
                    {
                        return Ok(None);
                    }
                    self.require_cleanup_owner(&snapshot, transition, directory)
                        .await?;
                    let permit = self
                        .managed_transition_permit_at(
                            &snapshot,
                            &transition.request,
                            directory,
                            &transition.plan,
                        )
                        .await?;
                    let (journal, sources) = self
                        .inner
                        .domain
                        .run(Box::pin(
                            directory.managed_retired_sources(&transition.plan, &permit),
                        ))
                        .await
                        .map_err(|_| FleetCatalogError::Closed)?
                        .map_err(catalog_error)?;
                    if journal.phase() != TransitionPhase::Retired
                        || !journal.parent_holds_released()
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                    for child in directory
                        .transition_routes(&transition.plan)
                        .map_err(catalog_error)?
                        .1
                    {
                        let (_, history) = self
                            .transfer_child_history(&snapshot, transition, directory, &child)
                            .await?;
                        if history.state != FleetHistoryState::Live {
                            return Err(FleetCatalogError::Conflict);
                        }
                    }
                    Ok(Some((
                        snapshot,
                        sources,
                        journal
                            .retired_parents()
                            .ok_or(FleetCatalogError::Conflict)?,
                    )))
                }),
            )
            .await?;
        let Some((snapshot, sources, retirement)) = prepared else {
            return Ok(());
        };
        let _memory = observer
            .control_budget()
            .acquire((MAX_NODES + 1) * crate::raft::physical_inventory::RESPONSE_BYTES)
            .ok_or(FleetCatalogError::Overloaded)?;
        let mut targets = BTreeMap::new();
        for source in &sources {
            for member in &source.members {
                targets.insert(member.node, ());
            }
        }
        if targets.len() > MAX_NODES {
            return Err(FleetCatalogError::Limit);
        }
        let mut nodes = Vec::new();
        let mut floors = Vec::new();
        let mut ages = Vec::new();
        for raw in targets.keys() {
            let node = NodeId::new(*raw).map_err(|_| FleetCatalogError::Conflict)?;
            let (host, capacity) = self
                .run(
                    deadline.remaining()?,
                    Box::pin(async {
                        Ok((
                            self.host_from(&snapshot, node).await?,
                            self.capacity_from(&snapshot, node).await?,
                        ))
                    }),
                )
                .await?;
            let boot = host.boot_binding()?;
            let sample = observer
                .inspect_peer_physical_inventory(node, snapshot.revision(), deadline.remaining()?)
                .await
                .map_err(FleetCatalogError::Observation)?;
            capacity::check_observation(&host, capacity.attestation(), &sample)?;
            let inventory = sample
                .inventory
                .as_ref()
                .ok_or(FleetCatalogError::InventoryIncomplete)?;
            if inventory.initializing().is_some()
                || inventory.histories().len() != sample.view.node.groups as usize
            {
                return Err(FleetCatalogError::InventoryIncomplete);
            }
            let started = self
                .resource_now()?
                .checked_sub(sample.sample_age_ms)
                .ok_or(FleetCatalogError::Unconfirmed)?;
            self.require_resource_age(started)?;
            ages.push(started);
            let mut removed = 0;
            for source in &sources {
                let Some(member) = source.members.iter().find(|m| m.node == *raw) else {
                    continue;
                };
                let row = inventory
                    .group_history(&source.route.history)
                    .ok_or(FleetCatalogError::InventoryIncomplete)?;
                if member.disk.is_some_and(|disk| disk != boot.disk)
                    || row.member().get() != member.member
                    || row.birth_digest() != source.route.birth_digest
                    || !row.source_retirement_applied()
                    || !row.parked_retired_source()
                {
                    return Err(FleetCatalogError::Unconfirmed);
                }
                removed += 1;
            }
            let floor = self
                .run(
                    deadline.remaining()?,
                    self.resource_floor(&snapshot, &attestation(capacity.attestation())),
                )
                .await?;
            floors.push(
                self.run(
                    deadline.remaining()?,
                    self.source_floor_capture_change(
                        &snapshot,
                        &attestation(capacity.attestation()),
                        FleetResourceBounds {
                            engine_slots: sample.view.node.groups,
                            durable_bytes: sample.view.node.reserved_bytes,
                            memory_bytes: sample.view.startup_bounds.bounded_buffer_bytes,
                        },
                    ),
                )
                .await?,
            );
            nodes.push(Node {
                node: *raw,
                disk: boot.disk,
                engine: boot.engine,
                before: sample.view.node.groups,
                floor: floor.engine_slots.max(sample.view.node.groups),
                removed,
                settled: false,
            });
        }
        let cleanup = Cleanup {
            format: 1,
            namespace: directory.namespace().root_digest(),
            operation: transition.request.operation,
            plan: transition.plan.digest(),
            retirement,
            nodes,
        };
        cleanup.check(transition, directory)?;
        let change = FleetChange {
            table: FleetTable::Operations,
            key: key(cleanup.operation),
            value: Some(cleanup.encode()?),
        };
        let mut changes = vec![change];
        changes.extend(floors);
        let mutation = mutation(&snapshot, transition, changes)?;
        self.run(
            deadline.remaining()?,
            Box::pin(async {
                self.require_cleanup_owner(&snapshot, transition, directory)
                    .await?;
                self.commit_with_disruption(
                    &snapshot,
                    &mutation,
                    None,
                    resources::Authority::SourceSettlement,
                    || {
                        for age in ages {
                            self.require_resource_age(age)?;
                        }
                        Ok(())
                    },
                )
                .await
                .map(|_| ())
            }),
        )
        .await
    }

    /// Every participant presents process-local engine receipts to its own root
    /// settlement. Serialized peer inventory alone cannot spend this authority.
    pub(crate) async fn settle_local_source_cleanup(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        observer: &RaftCells,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = deadline::Deadline::new(self, timeout)?;
        let (snapshot, mut cleanup, sources, capacity) = self
            .run(
                deadline.remaining()?,
                Box::pin(async {
                    let snapshot = self.inner.current().await?;
                    let cleanup = self
                        .source_cleanup(&snapshot, transition, directory)
                        .await?
                        .ok_or(FleetCatalogError::Conflict)?;
                    let node = observer.identity().node;
                    if cleanup
                        .nodes
                        .iter()
                        .find(|n| n.node == node.bytes())
                        .is_none_or(|n| n.settled)
                    {
                        return Ok((snapshot, cleanup, None, None));
                    }
                    self.require_cleanup_owner(&snapshot, transition, directory)
                        .await?;
                    let permit = self
                        .managed_transition_permit_at(
                            &snapshot,
                            &transition.request,
                            directory,
                            &transition.plan,
                        )
                        .await?;
                    let (journal, sources) = self
                        .inner
                        .domain
                        .run(Box::pin(
                            directory.managed_retired_sources(&transition.plan, &permit),
                        ))
                        .await
                        .map_err(|_| FleetCatalogError::Closed)?
                        .map_err(catalog_error)?;
                    if !journal.parent_holds_released()
                        || journal.retired_parents() != Some(cleanup.retirement)
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                    let capacity = self.capacity_from(&snapshot, node).await?;
                    let boot = self.host_from(&snapshot, node).await?.boot_binding()?;
                    if boot != capacity.attestation().boot {
                        return Err(FleetCatalogError::Conflict);
                    }
                    Ok((snapshot, cleanup, Some(sources), Some(capacity)))
                }),
            )
            .await?;
        let (Some(sources), Some(capacity)) = (sources, capacity) else {
            return Ok(());
        };
        let local = cleanup
            .nodes
            .iter_mut()
            .find(|n| n.node == observer.identity().node.bytes())
            .ok_or(FleetCatalogError::Conflict)?;
        let boot = &capacity.attestation().boot;
        if local.disk != boot.disk || local.engine != boot.engine {
            return Err(FleetCatalogError::Conflict);
        }
        let _memory = observer
            .control_budget()
            .acquire(2 * crate::raft::physical_inventory::RESPONSE_BYTES)
            .ok_or(FleetCatalogError::Overloaded)?;
        let mut count = 0;
        for source in &sources {
            let Some(member) = source.members.iter().find(|m| m.node == local.node) else {
                continue;
            };
            if !observer
                .reclaim_managed_source(transition, source.route.history, deadline.remaining()?)
                .await
                .map_err(|_| FleetCatalogError::Unconfirmed)?
            {
                return Err(FleetCatalogError::Unconfirmed);
            }
            let proof = observer
                .managed_source_reclamation_proof(source.route.history, deadline.remaining()?)
                .await
                .map_err(|_| FleetCatalogError::Unconfirmed)?;
            let record = proof.record();
            let exact = &record.source;
            if !record.complete
                || record.node != local.node
                || record.disk != local.disk
                || record.engine != local.engine
                || exact.scope().map_err(|_| FleetCatalogError::Conflict)? != source.route.history
                || exact.birth != source.route.birth_digest
                || exact.member != member.member
                || exact.operation != cleanup.operation
                || exact.plan != cleanup.plan
                || exact.retirement != cleanup.retirement
            {
                return Err(FleetCatalogError::Conflict);
            }
            count += 1;
        }
        if count != local.removed {
            return Err(FleetCatalogError::Conflict);
        }
        let sample = observer
            .inspect_peer_physical_inventory(
                observer.identity().node,
                snapshot.revision(),
                deadline.remaining()?,
            )
            .await
            .map_err(FleetCatalogError::Observation)?;
        let host = self
            .run(
                deadline.remaining()?,
                self.host_from(&snapshot, observer.identity().node),
            )
            .await?;
        capacity::check_observation(&host, capacity.attestation(), &sample)?;
        let inventory = sample
            .inventory
            .as_ref()
            .ok_or(FleetCatalogError::InventoryIncomplete)?;
        if inventory.initializing().is_some()
            || inventory.histories().len() != sample.view.node.groups as usize
            || sources
                .iter()
                .any(|s| inventory.group_history(&s.route.history).is_some())
        {
            return Err(FleetCatalogError::InventoryIncomplete);
        }
        let started = self
            .resource_now()?
            .checked_sub(sample.sample_age_ms)
            .ok_or(FleetCatalogError::Unconfirmed)?;
        let release = SlotRelease {
            before: local.before,
            floor: local.floor,
            removed: count,
            physical: FleetResourceBounds {
                engine_slots: sample.view.node.groups,
                durable_bytes: sample.view.node.reserved_bytes,
                memory_bytes: sample.view.startup_bounds.bounded_buffer_bytes,
            },
        };
        let floor = self
            .run(
                deadline.remaining()?,
                self.source_slot_floor_change(
                    &snapshot,
                    &attestation(capacity.attestation()),
                    &release,
                ),
            )
            .await?;
        local.settled = true;
        let mutation = mutation(
            &snapshot,
            transition,
            vec![
                FleetChange {
                    table: FleetTable::Operations,
                    key: key(cleanup.operation),
                    value: Some(cleanup.encode()?),
                },
                floor,
            ],
        )?;
        self.run(
            deadline.remaining()?,
            Box::pin(async {
                self.require_cleanup_owner(&snapshot, transition, directory)
                    .await?;
                self.commit_with_disruption(
                    &snapshot,
                    &mutation,
                    None,
                    resources::Authority::SourceSettlement,
                    || self.require_resource_age(started),
                )
                .await
                .map(|_| ())
            }),
        )
        .await
    }

    pub(super) async fn check_source_settlement_mutation(
        &self,
        snapshot: &FleetSnapshot,
        mutation: &FleetMutation,
    ) -> Result<()> {
        let change = mutation
            .changes
            .first()
            .ok_or(FleetCatalogError::Invalid("source settlement shape"))?;
        if change.table != FleetTable::Operations || !change.key.starts_with(PREFIX) {
            return Err(FleetCatalogError::Invalid("source settlement key"));
        }
        let after: Cleanup = decode(
            KIND,
            change.value.as_deref().ok_or(FleetCatalogError::Conflict)?,
            LIMIT,
        )?;
        let before = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &change.key,
            )
            .await?;
        let Some(before) = before else {
            if after.format != 1
                || after.operation != mutation.operation
                || change.key != key(after.operation)
                || after.nodes.is_empty()
                || after.nodes.len() > MAX_NODES
                || mutation.changes.len() != after.nodes.len() + 1
                || after.nodes.iter().any(|n| n.settled || n.floor < n.before)
                || after
                    .nodes
                    .iter()
                    .zip(&mutation.changes[1..])
                    .any(|(n, c)| {
                        c.table != FleetTable::Operations
                            || c.key != format!("resource-floor/{}", hex::encode(n.node))
                            || c.value.is_none()
                    })
            {
                return Err(FleetCatalogError::Invalid("source floor capture binding"));
            }
            return Ok(());
        };
        if mutation.changes.len() != 2
            || mutation.changes[1].table != FleetTable::Operations
            || !mutation.changes[1].key.starts_with("resource-floor/")
        {
            return Err(FleetCatalogError::Invalid("source floor settlement shape"));
        }
        let before: Cleanup = decode(KIND, &before, LIMIT)?;
        let mut expected = before.clone();
        let mut changed = None;
        for (old, new) in expected.nodes.iter_mut().zip(&after.nodes) {
            if !old.settled && new.settled {
                if changed.replace(old.node).is_some() {
                    return Err(FleetCatalogError::Conflict);
                }
                old.settled = true;
            }
        }
        if expected != after
            || before.operation != mutation.operation
            || change.key != key(before.operation)
            || changed.is_none_or(|node| {
                mutation.changes[1].key != format!("resource-floor/{}", hex::encode(node))
            })
        {
            return Err(FleetCatalogError::Invalid(
                "source floor settlement binding",
            ));
        }
        Ok(())
    }
}
