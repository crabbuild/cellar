// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Peak configured destination capacity is checked before irreversible sealing.
use super::{histories::validate_request, managed_transitions::ManagedTransitionPermit, *};
use crate::application::consensus::RaftCells;
use crate::raft::{
    cell_groups::{DirectoryMutation, RangeDirectory, TransitionJournal, TransitionPhase},
    host::RaftPeerNodeObservation,
};
use cellar_logic::{
    consensus::NodeId,
    raft_fleet::{LedgerError, ResourceKind},
};
use std::collections::BTreeMap;

// A split has two RF3 children; a merge has one. This is independent of the
// fleet size and business-cell count. Each owned response has its wire bound.
const MAX_TARGETS: usize = 6;

enum Start {
    Replayed(Box<TransitionJournal>),
    Prepared(Box<Preparation>),
}

struct Preparation {
    snapshot: FleetSnapshot,
    permit: ManagedTransitionPermit,
    targets: BTreeMap<NodeId, (FleetHostAuthorization, FleetCapacityAttestation)>,
}

pub(super) struct Node {
    pub(super) capacity: FleetCapacityAttestation,
    pub(super) physical: RaftPeerNodeObservation,
    pub(super) started: u64,
    pub(super) known: usize,
    pub(super) missing: u32,
}

impl FleetCatalog {
    /// Select the irreversible seal decision only with fresh complete physical
    /// inventories and room for every reserved child alongside retained parents.
    /// The rooted disruption slot prevents new births or membership admissions;
    /// existing reservations remain charged even before their engines initialize.
    /// Replaying a selected authorization requires no new health observation.
    pub fn authorize_managed_transition_seal<'a>(
        &'a self,
        request: &'a FleetRequestIdentity,
        directory: &'a RangeDirectory,
        plan: &'a DirectoryMutation,
        observer: &'a RaftCells,
        timeout: Duration,
    ) -> impl std::future::Future<Output = Result<TransitionJournal>> + 'a {
        // Allocate the bounded inventory/catalog operation before a caller's
        // composite transition future can embed its complete polling frame.
        Box::pin(
            self.authorize_managed_transition_seal_inner(
                request, directory, plan, observer, timeout,
            ),
        )
    }

    async fn authorize_managed_transition_seal_inner(
        &self,
        request: &FleetRequestIdentity,
        directory: &RangeDirectory,
        plan: &DirectoryMutation,
        observer: &RaftCells,
        timeout: Duration,
    ) -> Result<TransitionJournal> {
        validate_request(request)?;
        let deadline = deadline::Deadline::new(self, timeout)?;
        let start = self
            .run(
                deadline.remaining()?,
                Box::pin(async {
                    let snapshot = self.inner.current().await?;
                    let permit = self
                        .managed_transition_permit_at(&snapshot, request, directory, plan)
                        .await?;
                    let journal = self
                        .inner
                        .domain
                        .run(Box::pin(directory.managed_transition_status(plan, &permit)))
                        .await
                        .map_err(|_| FleetCatalogError::Closed)?
                        .map_err(histories::catalog_error)?
                        .ok_or(FleetCatalogError::Conflict)?;
                    if journal.phase() != TransitionPhase::Prepared {
                        return self
                            .inner
                            .domain
                            .run(Box::pin(
                                directory.authorize_managed_transition_seal(plan, &permit),
                            ))
                            .await
                            .map_err(|_| FleetCatalogError::Closed)?
                            .map_err(histories::catalog_error)
                            .map(|journal| Start::Replayed(Box::new(journal)));
                    }
                    self.require_transition_membership_idle(&snapshot).await?;
                    let (_, children) = directory
                        .transition_routes(plan)
                        .map_err(histories::catalog_error)?;
                    let mut targets = BTreeMap::new();
                    for route in children {
                        let key = self
                            .binding(&route.history)?
                            .key()
                            .map_err(|_| FleetCatalogError::Invalid("transition child binding"))?;
                        let history = self.history_inner(&snapshot, &key).await?;
                        if history.origin != histories::Origin::Managed
                            || history.state != FleetHistoryState::Reserved
                            || history.operation != request.operation
                            || !history.check_route(directory, &route)
                        {
                            return Err(FleetCatalogError::Conflict);
                        }
                        for voter in history.initial_placement()?.voters {
                            if targets.contains_key(&voter.node) {
                                continue;
                            }
                            if targets.len() >= MAX_TARGETS {
                                return Err(FleetCatalogError::Limit);
                            }
                            let host = self.host_from(&snapshot, voter.node).await?;
                            let capacity = self.capacity_from(&snapshot, voter.node).await?;
                            if !capacity.receive_replicas()
                                || host.boot_binding()? != capacity.attestation().boot
                            {
                                return Err(FleetCatalogError::Conflict);
                            }
                            targets.insert(voter.node, (host, capacity.attestation().clone()));
                        }
                    }
                    Ok(Start::Prepared(Box::new(Preparation {
                        snapshot,
                        permit,
                        targets,
                    })))
                }),
            )
            .await?;
        let Preparation {
            snapshot,
            permit,
            targets,
        } = match start {
            Start::Replayed(journal) => return Ok(*journal),
            Start::Prepared(prepared) => *prepared,
        };
        // The caller owns all retained inventories and one decoded catalog page
        // until authorization exits, including after a dropped reply waiter.
        let _memory = observer
            .control_budget()
            .acquire((MAX_TARGETS + 1) * crate::raft::physical_inventory::RESPONSE_BYTES)
            .ok_or(FleetCatalogError::Overloaded)?;
        // Peer RPCs run outside the catalog turn. Retain at most six complete
        // inventories; never hold a synchronous lock across their awaits.
        let mut nodes = BTreeMap::new();
        for (node, (host, capacity)) in targets {
            let physical = observer
                .inspect_peer_physical_inventory(node, snapshot.revision(), deadline.remaining()?)
                .await
                .map_err(FleetCatalogError::Observation)?;
            capacity::check_observation(&host, &capacity, &physical)?;
            let inventory = physical
                .inventory
                .as_ref()
                .ok_or(FleetCatalogError::InventoryIncomplete)?;
            if inventory.initializing().is_some() {
                return Err(FleetCatalogError::InventoryIncomplete);
            }
            let started = self
                .resource_now()?
                .checked_sub(physical.sample_age_ms)
                .ok_or(FleetCatalogError::Unconfirmed)?;
            self.require_resource_age(started)?;
            nodes.insert(
                node,
                Node {
                    capacity,
                    physical,
                    started,
                    known: 0,
                    missing: 0,
                },
            );
        }
        self.run(
            deadline.remaining()?,
            Box::pin(async {
                if self.inner.current().await?.token != snapshot.token {
                    return Err(FleetCatalogError::Conflict);
                }
                self.require_transition_membership_idle(&snapshot).await?;
                Box::pin(self.charge_transition_births(&snapshot, &mut nodes)).await?;
                for input in nodes.values() {
                    let inventory = input
                        .physical
                        .inventory
                        .as_ref()
                        .ok_or(FleetCatalogError::InventoryIncomplete)?;
                    if input.known != inventory.histories().len() {
                        return Err(FleetCatalogError::InventoryIncomplete);
                    }
                    let attestation = resource_attestation(&input.capacity);
                    let floor = self.resource_floor(&snapshot, &attestation).await?;
                    require_room(input, floor)?;
                }
                // No sample-age reset after catalog I/O, retry or root refresh. The
                // directory's own CAS still arbitrates cancellation versus sealing.
                for input in nodes.values() {
                    self.require_resource_age(input.started)?;
                }
                if self.inner.current().await?.token != snapshot.token {
                    return Err(FleetCatalogError::Conflict);
                }
                self.inner
                    .domain
                    .run(Box::pin(
                        directory.authorize_managed_transition_seal(plan, &permit),
                    ))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(histories::catalog_error)
            }),
        )
        .await
    }

    pub(super) async fn charge_transition_births(
        &self,
        snapshot: &FleetSnapshot,
        nodes: &mut BTreeMap<NodeId, Node>,
    ) -> Result<()> {
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
                if history
                    .binding()
                    .key()
                    .map_err(|_| FleetCatalogError::Corrupt("transition history key"))?
                    != entry.key
                {
                    return Err(FleetCatalogError::Corrupt("transition history index"));
                }
                let scope = history
                    .binding()
                    .scope()
                    .map_err(|_| FleetCatalogError::Corrupt("transition history scope"))?;
                let future = match history.state() {
                    FleetHistoryState::Reserved | FleetHistoryState::Live => {
                        self.settled_group_voters(snapshot, &history).await?.0
                    }
                    FleetHistoryState::Retired { .. } | FleetHistoryState::Cancelled { .. } => {
                        BTreeMap::new()
                    }
                };
                for (node, input) in nodes.iter_mut() {
                    let inventory = input
                        .physical
                        .inventory
                        .as_ref()
                        .ok_or(FleetCatalogError::InventoryIncomplete)?;
                    if let Some(row) = inventory.group_history(&scope) {
                        // A retained old member still consumes storage. This
                        // identity match grants no serving or release authority.
                        if row.birth_digest() != history.birth_digest()
                            || future
                                .get(node)
                                .is_some_and(|member| *member != row.member())
                        {
                            return Err(FleetCatalogError::Conflict);
                        }
                        input.known = input.known.checked_add(1).ok_or(FleetCatalogError::Limit)?;
                    } else if future.contains_key(node) {
                        input.missing = input
                            .missing
                            .checked_add(1)
                            .ok_or(FleetCatalogError::Limit)?;
                    }
                }
            }
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        if seen != snapshot.head.history_count {
            return Err(FleetCatalogError::Corrupt(
                "transition capacity history count",
            ));
        }
        Ok(())
    }
}

pub(super) fn resource_attestation(
    capacity: &FleetCapacityAttestation,
) -> FleetResourceAttestation {
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

fn require_room(input: &Node, floor: FleetResourceBounds) -> Result<()> {
    require_projected_room(input, floor, 0)
}

pub(super) fn require_projected_room(
    input: &Node,
    floor: FleetResourceBounds,
    additional: u32,
) -> Result<()> {
    let view = &input.physical.view;
    let qualified = &input.capacity;
    let overflow = || FleetCatalogError::Resource(LedgerError::ArithmeticOverflow);
    let full = |kind| FleetCatalogError::Resource(LedgerError::CapacityExceeded(kind));
    let missing = input.missing.checked_add(additional).ok_or_else(overflow)?;
    let slots = view
        .node
        .groups
        .max(floor.engine_slots)
        .checked_add(missing)
        .ok_or_else(overflow)?;
    let registrations = view
        .registered_histories
        .checked_add(view.unattached_engine_histories)
        .map(|count| count.max(view.node.groups).max(floor.engine_slots))
        .and_then(|count| count.checked_add(missing))
        .ok_or_else(overflow)?;
    let bound = &view.startup_bounds;
    if slots
        > qualified
            .engine_slots
            .min(view.node.profile.group_capacity)
            .min(bound.engine_groups)
            .min(bound.consensus_groups)
        || registrations > bound.host_groups
    {
        return Err(full(ResourceKind::EngineSlots));
    }
    // Placement bytes are a per-group reservation quota. Deployment-used bytes
    // include node-wide storage too; retain them against the platform capacity.
    let reservation_bytes = u64::from(slots)
        .checked_mul(view.node.profile.bytes_per_group)
        .ok_or_else(overflow)?;
    if reservation_bytes > view.node.profile.byte_capacity {
        return Err(full(ResourceKind::DurableBytes));
    }
    let future_bytes = view
        .node
        .profile
        .bytes_per_group
        .checked_mul(u64::from(missing))
        .ok_or_else(overflow)?;
    let bytes = view
        .node
        .reserved_bytes
        .max(qualified.used_durable_bytes)
        .max(floor.durable_bytes)
        .checked_add(future_bytes)
        .ok_or_else(overflow)?;
    if bytes > qualified.durable_bytes {
        return Err(full(ResourceKind::DurableBytes));
    }
    // Installed node-wide pools are bounded already, including unused slots.
    // This does not invent a per-group RSS estimate or a platform measurement.
    if bound
        .bounded_buffer_bytes
        .max(qualified.used_memory_bytes)
        .max(floor.memory_bytes)
        > qualified.memory_bytes
    {
        return Err(full(ResourceKind::MemoryBytes));
    }
    Ok(())
}
