// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit resource inputs stay separate from authenticated runtime evidence.
use super::*;
use cellar_logic::raft_fleet::{LedgerPolicy, LedgerSnapshot, PhysicalSample, TopologyNode};

type Validated = (
    TopologyPolicy,
    BTreeSet<NodeId>,
    BTreeMap<String, FleetTopologyHistoryResources>,
);

pub(super) fn validate(
    catalog: &FleetCatalog,
    snapshot: &FleetSnapshot,
    request: &FleetTopologyRequest,
) -> Result<Validated> {
    if request.nodes.is_empty()
        || request.nodes.len() > 64
        || request.keep.is_empty()
        || request.keep.len() > 64
        || request.histories.len() > HISTORY_LIMIT as usize
    {
        return Err(FleetCatalogError::Limit);
    }
    if request.policy.revision == 0 || !(1..=50_000_000).contains(&request.policy.max_work) {
        return Err(FleetCatalogError::Topology(
            cellar_logic::raft_fleet::TopologyError::InvalidPolicy,
        ));
    }
    let keep = request.keep.iter().copied().collect::<BTreeSet<_>>();
    if keep.len() != request.keep.len() {
        return Err(FleetCatalogError::Invalid("duplicate topology keep node"));
    }
    let mut nodes = BTreeSet::new();
    for input in &request.nodes {
        input.attestation.validate()?;
        if input.attestation.boot.catalog != catalog.birth_owner()?.catalog
            || input.attestation.boot.fleet != catalog.inner.incarnation
            || !nodes.insert(input.attestation.boot.node)
        {
            return Err(FleetCatalogError::Invalid("topology node input"));
        }
    }
    let mut bounds = BTreeMap::new();
    for input in &request.histories {
        let key = catalog
            .binding(&input.group)?
            .key()
            .map_err(|_| FleetCatalogError::Invalid("topology history input"))?;
        if input.resident.engine_slots != 1
            || input.peak.engine_slots != 1
            || input.resident.durable_bytes == 0
            || input.resident.memory_bytes == 0
            || input.peak.high_water(input.resident) != input.peak
            || bounds.insert(key, input.clone()).is_some()
        {
            return Err(FleetCatalogError::Invalid("topology history bounds"));
        }
    }
    Ok((
        TopologyPolicy {
            revision: request.policy.revision,
            distinct_hosts: snapshot.replica_separation() == FleetReplicaSeparation::PhysicalHosts,
            distinct_zones: request.policy.distinct_zones,
            distinct_regions: request.policy.distinct_regions,
            max_work: request.policy.max_work,
        },
        keep,
        bounds,
    ))
}

pub(super) fn check_node(
    host: &FleetHostAuthorization,
    capacity: &FleetCapacityAttestation,
    physical: &RaftPeerNodeObservation,
) -> Result<()> {
    super::super::super::capacity::check_observation(host, capacity, physical)?;
    let inventory = physical
        .inventory
        .as_ref()
        .ok_or(FleetCatalogError::InventoryIncomplete)?;
    if inventory.initializing().is_some() {
        return Err(FleetCatalogError::InventoryIncomplete);
    }
    Ok(())
}

pub(super) fn identity_digest(physical: &RaftPeerNodeObservation) -> Result<[u8; 32]> {
    let inventory = physical
        .inventory
        .as_ref()
        .ok_or(FleetCatalogError::InventoryIncomplete)?;
    let mut digest = Sha256::new();
    digest.update(b"cellar-topology-physical-bindings-1");
    digest.update((inventory.histories().len() as u64).to_be_bytes());
    for history in inventory.histories() {
        digest.update(history.scope_digest());
        digest.update(history.birth_digest());
        digest.update(history.member().get().to_be_bytes());
    }
    Ok(digest.finalize().into())
}

impl FleetTopologyVerification {
    pub(super) async fn refresh_nodes(
        &self,
        deadline: &super::super::super::deadline::Deadline<'_>,
    ) -> Result<(Vec<TopologyNode>, u64, [u8; 32])> {
        let catalog = &self.catalog;
        let mut result = Vec::with_capacity(self.nodes.len());
        let mut oldest = catalog.resource_now()?;
        let mut transcript = self.transcript.clone();
        let live = self
            .groups
            .iter()
            .map(|group| {
                crate::raft::physical_inventory::scope_digest(&group.resources.group)
                    .map_err(|_| FleetCatalogError::Corrupt("topology scope digest"))
            })
            .collect::<Result<BTreeSet<_>>>()?;
        for node in &self.nodes {
            let original = node
                .physical
                .inventory
                .as_ref()
                .ok_or(FleetCatalogError::InventoryIncomplete)?;
            if original
                .histories()
                .iter()
                .any(|h| !live.contains(&h.scope_digest()))
            {
                return Err(FleetCatalogError::InventoryIncomplete);
            }
            let observed = self
                .observer
                .inspect_peer_physical_inventory(
                    node.host.node(),
                    self.snapshot.revision(),
                    deadline.remaining()?,
                )
                .await
                .map_err(FleetCatalogError::Observation)?;
            check_node(&node.host, &node.capacity, &observed)?;
            if identity_digest(&observed)? != identity_digest(&node.physical)? {
                return Err(FleetCatalogError::Conflict);
            }
            let now = catalog.resource_now()?;
            let started = now
                .checked_sub(observed.sample_age_ms)
                .ok_or(FleetCatalogError::Unconfirmed)?;
            catalog.require_resource_age(started)?;
            oldest = oldest.min(started);
            let physical = catalog
                .run(deadline.remaining()?, async {
                    if catalog.inner.current().await?.token != self.snapshot.token {
                        return Err(FleetCatalogError::Conflict);
                    }
                    let qualified = &node.capacity;
                    let view = &observed.view;
                    Ok(node
                        .resources
                        .attestation
                        .used
                        .high_water(FleetResourceBounds {
                            engine_slots: view.node.groups,
                            durable_bytes: view
                                .node
                                .reserved_bytes
                                .max(qualified.used_durable_bytes),
                            memory_bytes: view
                                .startup_bounds
                                .bounded_buffer_bytes
                                .max(qualified.used_memory_bytes),
                        })
                        .high_water(
                            catalog
                                .resource_floor(&self.snapshot, &node.resources.attestation)
                                .await?,
                        ))
                })
                .await?;
            let view = &observed.view;
            let limits = &view.startup_bounds;
            let qualified = &node.capacity;
            transcript.update(
                serde_json::to_vec(&node.resources.attestation)
                    .map_err(|_| FleetCatalogError::Limit)?,
            );
            transcript.update(
                serde_json::to_vec(&node.resources.reserve)
                    .map_err(|_| FleetCatalogError::Limit)?,
            );
            transcript.update(serde_json::to_vec(&physical).map_err(|_| FleetCatalogError::Limit)?);
            result.push(TopologyNode {
                ledger: LedgerSnapshot {
                    scope: super::super::admission::capacity_scope(
                        catalog,
                        &node.resources.attestation,
                    )?,
                    policy: LedgerPolicy {
                        capacity: Resources {
                            engine_slots: u64::from(
                                qualified
                                    .engine_slots
                                    .min(view.node.profile.group_capacity)
                                    .min(limits.host_groups)
                                    .min(limits.consensus_groups)
                                    .min(limits.engine_groups),
                            ),
                            durable_bytes: qualified
                                .durable_bytes
                                .min(view.node.profile.byte_capacity),
                            memory_bytes: qualified.memory_bytes,
                        },
                        reserve: node.resources.reserve.resources(),
                        max_active_moves: MAX_MOVES as u32,
                        max_sample_age_ms: MAX_SAMPLE_AGE_MS,
                    },
                    sample: PhysicalSample {
                        scope: super::super::admission::capacity_scope(
                            catalog,
                            &node.resources.attestation,
                        )?,
                        sequence: self.snapshot.revision(),
                        observed_ms: started,
                        physical: physical.resources(),
                    },
                    reservations: Vec::new(),
                    complete: true,
                },
                host: hex::encode(node.host.physical_host().bytes()),
                region: view.node.profile.region.clone(),
                zone: view.node.profile.zone.clone(),
                keep: self.keep.contains(&node.host.node()),
                receives_replicas: node.host.eligibility().receive_replicas,
            });
        }
        transcript.update(b"cellar-topology-final-history-bounds-1");
        for group in &self.groups {
            transcript.update(
                serde_json::to_vec(&group.resources.resident)
                    .map_err(|_| FleetCatalogError::Limit)?,
            );
            transcript.update(
                serde_json::to_vec(&group.resources.peak).map_err(|_| FleetCatalogError::Limit)?,
            );
        }
        Ok((result, oldest, transcript.finalize().into()))
    }
}
