// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Advisory deployment placement over rooted capacity and bounded live probes.
use super::{
    managed_transition_capacity::{self as room, Node},
    *,
};
use crate::application::consensus::RaftCells;
use crate::raft::{catalog::RangeDirectory, inspection::RaftNodeProfile};
use cellar_logic::consensus::NodeId;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const PARTITION_TARGETS: usize =
    cellar_logic::cell_group_placement::MAX_PARTITION_DESTINATIONS;

pub(crate) struct PartitionPlacement {
    pub class: String,
    pub voters: Option<Vec<[NodeId; 3]>>,
    /// Advisory retry backoff only, never failure or resource-release evidence.
    pub deferred: Vec<NodeId>,
}

pub(crate) struct PartitionPlacementRequest<'a> {
    pub directory: &'a RangeDirectory,
    pub revision: u64,
    pub policy: &'a cellar_logic::cell_group_placement::PartitionPlacementPolicy,
    pub children: usize,
    pub parents: &'a [cellar_logic::cell_group::GroupHistory],
    pub excluded: &'a BTreeSet<NodeId>,
}

struct Candidate {
    host: FleetHostAuthorization,
    capacity: FleetCapacityAttestation,
    profile: RaftNodeProfile,
    score: u128,
}

impl FleetCatalog {
    pub(crate) async fn partition_placement(
        &self,
        observer: &RaftCells,
        request: PartitionPlacementRequest<'_>,
        timeout: Duration,
    ) -> Result<PartitionPlacement> {
        let PartitionPlacementRequest {
            directory,
            revision,
            policy,
            children,
            parents,
            excluded,
        } = request;
        if !matches!((parents.len(), children), (1, 2) | (2, 1))
            || parents.iter().any(|parent| {
                parent.cluster != directory.namespace_config().cluster
                    || parent.namespace != directory.namespace_config().namespace
            })
        {
            return Err(FleetCatalogError::Invalid("partition child count"));
        }
        let deadline = deadline::Deadline::new(self, timeout)?;
        let (snapshot, class, candidates) = self
            .run(
                deadline.remaining()?,
                Box::pin(async {
                    let snapshot = self.inner.current().await?;
                    if snapshot.revision() != revision {
                        return Err(FleetCatalogError::Conflict);
                    }
                    self.require_transition_membership_idle(&snapshot).await?;
                    let class = self
                        .declared_class(&snapshot, directory.namespace())
                        .await?;
                    // Capacity was admitted before these serving parents were
                    // born. Their settled placements supply a rooted occupancy
                    // lower bound for ranking the bounded candidate window.
                    // Physical probing and full resource accounting still own
                    // feasibility; this ranking grants no capacity authority.
                    let mut source_slots = BTreeMap::<NodeId, u32>::new();
                    for parent in parents {
                        let key = self
                            .binding(parent)?
                            .key()
                            .map_err(|_| FleetCatalogError::Invalid("partition parent history"))?;
                        let history = self.history_inner(&snapshot, &key).await?;
                        if *history.state() != FleetHistoryState::Live {
                            return Err(FleetCatalogError::Conflict);
                        }
                        let (voters, _) = self.settled_group_voters(&snapshot, &history).await?;
                        for node in voters.keys() {
                            // At most two parents, each with distinct voters.
                            *source_slots.entry(*node).or_default() += 1;
                        }
                    }
                    let mut candidates = Vec::new();
                    // The rooted fleet has a 64-node ceiling. Only metadata is read here;
                    // At most nine selected nodes receive physical-inventory
                    // requests, covering two RF3 children plus recovery room.
                    for node in self.node_records(&snapshot).await? {
                        let id = NodeId::new(node.node)
                            .map_err(|_| FleetCatalogError::Corrupt("placement node"))?;
                        if node.state != FleetNodeState::Active || excluded.contains(&id) {
                            continue;
                        }
                        let capacity = match self.capacity_from(&snapshot, id).await {
                            Ok(value) if value.receive_replicas() => value,
                            Ok(_) | Err(FleetCatalogError::Conflict) => continue,
                            Err(error) => return Err(error),
                        };
                        let profile = self.capacity_profile(capacity.attestation()).await?;
                        if profile.pool != policy.pool || profile.draining {
                            continue;
                        }
                        let capacity = capacity.attestation().clone();
                        let floor = self
                            .resource_floor(&snapshot, &room::resource_attestation(&capacity))
                            .await?;
                        let slots = floor
                            .engine_slots
                            .max(source_slots.get(&id).copied().unwrap_or(0));
                        let limit = capacity.engine_slots.min(profile.group_capacity);
                        if slots >= limit {
                            continue;
                        }
                        let score = (u128::from(slots) * 10_000 / u128::from(limit)).max(
                            u128::from(floor.durable_bytes.max(capacity.used_durable_bytes))
                                * 10_000
                                / u128::from(capacity.durable_bytes),
                        );
                        candidates.push(Candidate {
                            host: self.host_from(&snapshot, id).await?,
                            capacity,
                            profile,
                            score,
                        });
                    }
                    candidates.sort_by_key(|c| (c.score, c.host.node()));
                    // Seed the window with three separated destinations, then retain
                    // the best remaining candidates for alternate children and reserves.
                    let mut selected = Vec::new();
                    if let Some(triple) = triples(candidates.len()).find(|indices| {
                        separated(
                            indices.iter().map(|i| &candidates[*i]),
                            snapshot.replica_separation(),
                            policy,
                        )
                    }) {
                        for index in triple {
                            selected.push(index);
                        }
                        for index in 0..candidates.len() {
                            if selected.len() == PARTITION_TARGETS {
                                break;
                            }
                            if !selected.contains(&index) {
                                selected.push(index);
                            }
                        }
                    }
                    let mut candidates: Vec<_> = candidates
                        .into_iter()
                        .enumerate()
                        .filter_map(|(i, c)| selected.contains(&i).then_some(c))
                        .collect();
                    candidates.sort_by_key(|c| (c.score, c.host.node()));
                    if self.inner.current().await?.token != snapshot.token {
                        return Err(FleetCatalogError::Conflict);
                    }
                    Ok((snapshot, class, candidates))
                }),
            )
            .await?;
        if candidates.len() < 3 {
            return Ok(PartitionPlacement {
                class,
                voters: None,
                deferred: Vec::new(),
            });
        }
        // Map/projection metadata has its own bound. Each inspected payload
        // transfers its SDK permit into this retained batch rather than
        // duplicating the full response reservation here.
        let metadata = observer
            .control_budget()
            .acquire(64 * 1024)
            .ok_or(FleetCatalogError::Overloaded)?;
        let mut nodes = BTreeMap::new();
        let mut retained = Vec::with_capacity(PARTITION_TARGETS);
        let mut deferred = Vec::new();
        for candidate in candidates {
            let id = candidate.host.node();
            let (physical, memory) = match observer
                .inspect_peer_physical_inventory_owned(
                    id,
                    snapshot.revision(),
                    deadline.remaining()?,
                )
                .await
            {
                Ok(value) => value,
                Err(_) => {
                    deferred.push(id);
                    continue;
                }
            };
            if capacity::check_observation(&candidate.host, &candidate.capacity, &physical).is_err()
                || physical
                    .inventory
                    .as_ref()
                    .is_none_or(|i| i.initializing().is_some())
            {
                deferred.push(id);
                continue;
            }
            let started = self
                .resource_now()?
                .checked_sub(physical.sample_age_ms)
                .ok_or(FleetCatalogError::Unconfirmed)?;
            self.require_resource_age(started)?;
            nodes.insert(
                id,
                Node {
                    capacity: candidate.capacity,
                    physical,
                    started,
                    known: 0,
                    missing: 0,
                },
            );
            retained.push(memory);
        }
        self.run(
            deadline.remaining()?,
            Box::pin(async move {
                // These permits follow the observations into the owned turn,
                // including cancellation or an abandoned response waiter.
                let _retained = retained;
                let _metadata = metadata;
                if self.inner.current().await?.token != snapshot.token {
                    return Err(FleetCatalogError::Conflict);
                }
                Box::pin(self.charge_transition_births(&snapshot, &mut nodes)).await?;
                let mut inputs = Vec::new();
                for (id, node) in &nodes {
                    if node
                        .physical
                        .inventory
                        .as_ref()
                        .is_none_or(|i| node.known != i.histories().len())
                    {
                        return Err(FleetCatalogError::InventoryIncomplete);
                    }
                    let floor = self
                        .resource_floor(&snapshot, &room::resource_attestation(&node.capacity))
                        .await?;
                    let mut low = 0;
                    let mut high = node.capacity.engine_slots;
                    while low < high {
                        let mid = low + (high - low).div_ceil(2);
                        if room::require_projected_room(node, floor, mid).is_ok() {
                            low = mid;
                        } else {
                            high = mid - 1;
                        }
                    }
                    if low == 0 {
                        deferred.push(*id);
                    }
                    inputs.push((*id, node, floor, low));
                }
                let destinations = inputs
                    .iter()
                    .map(|(id, node, floor, available)| {
                        let profile = &node.physical.view.node.profile;
                        let physical_host = node
                            .physical
                            .view
                            .current_enrollment
                            .as_ref()
                            .ok_or(FleetCatalogError::Unconfirmed)?
                            .physical_host;
                        Ok(cellar_logic::cell_group_placement::PartitionDestination {
                            node: *id,
                            physical_host,
                            region: profile.region.clone(),
                            zone: profile.zone.clone(),
                            occupied: node
                                .physical
                                .view
                                .node
                                .groups
                                .max(floor.engine_slots)
                                .checked_add(node.missing)
                                .ok_or(FleetCatalogError::Limit)?,
                            capacity: node.capacity.engine_slots.min(profile.group_capacity),
                            available: *available,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let voters = cellar_logic::cell_group_placement::select_partition_voters(
                    &destinations,
                    policy,
                    children,
                    snapshot.replica_separation() == FleetReplicaSeparation::PhysicalHosts,
                );
                for input in nodes.values() {
                    self.require_resource_age(input.started)?;
                }
                if self.inner.current().await?.token != snapshot.token {
                    return Err(FleetCatalogError::Conflict);
                }
                Ok(PartitionPlacement {
                    class,
                    voters,
                    deferred,
                })
            }),
        )
        .await
    }
}

fn triples(length: usize) -> impl Iterator<Item = [usize; 3]> {
    (0..length).flat_map(move |a| {
        ((a + 1)..length).flat_map(move |b| ((b + 1)..length).map(move |c| [a, b, c]))
    })
}

fn separated<'a>(
    candidates: impl Iterator<Item = &'a Candidate>,
    separation: FleetReplicaSeparation,
    policy: &cellar_logic::cell_group_placement::PartitionPlacementPolicy,
) -> bool {
    let candidates: Vec<_> = candidates.collect();
    (separation == FleetReplicaSeparation::Nodes
        || candidates
            .iter()
            .map(|c| c.host.physical_host())
            .collect::<BTreeSet<_>>()
            .len()
            == 3)
        && (!policy.distinct_regions
            || candidates
                .iter()
                .map(|c| &c.profile.region)
                .collect::<BTreeSet<_>>()
                .len()
                == 3)
        && (!policy.distinct_zones
            || candidates
                .iter()
                .map(|c| (&c.profile.region, &c.profile.zone))
                .collect::<BTreeSet<_>>()
                .len()
                == 3)
}
