// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Authenticate occupancy/boots and run the bounded pure required-repair choice.
use super::*;
use crate::application::consensus::RaftCells;
use crate::raft::fleet_catalog::deadline::Deadline;
use crate::raft::member_state::{JoiningMember, RepairPlan};
use cellar_logic::consensus::ProposalId;
use cellar_logic::raft_fleet::{
    select_isolated_repair, IsolatedRepairView, LedgerPolicy, LedgerSnapshot, PhysicalSample,
    TopologyNode, TopologyPolicy,
};
use cellar_logic::raft_placement::PlacedMember;
use std::collections::BTreeSet;

pub(in super::super) struct Planning<'a, 'd> {
    pub catalog: &'a FleetCatalog,
    pub cut: &'a FleetSnapshot,
    pub context: &'a FleetIsolatedRepairContext,
    pub group: &'a Group,
    pub observer: &'a RaftCells,
    pub identity: FleetRequestIdentity,
    pub deadline: &'a Deadline<'d>,
}

impl Planning<'_, '_> {
    pub async fn plan(self, inputs: &FleetIsolatedRepairInputs) -> Result<Selected> {
        let Self {
            catalog,
            cut,
            context,
            group,
            observer,
            identity,
            deadline,
        } = self;
        if inputs.nodes.is_empty() || inputs.nodes.len() > 64 || inputs.namespaces.len() > 64 {
            return Err(FleetCatalogError::Limit);
        }
        let bounds = &inputs.history;
        if bounds.group != context.group
            || bounds.resident.engine_slots != 1
            || bounds.peak.engine_slots != 1
            || bounds.resident.memory_bytes == 0
            || bounds.resident.durable_bytes == 0
            || bounds.peak.high_water(bounds.resident) != bounds.peak
        {
            return Err(FleetCatalogError::Invalid("isolated repair history bounds"));
        }
        let rows = catalog
            .run(deadline.remaining()?, async {
                if catalog.inner.current().await?.token != cut.token {
                    return Err(FleetCatalogError::Conflict);
                }
                let book = catalog.resource_book(cut).await?;
                if book.rows.len() >= MAX_MOVES {
                    return Err(FleetCatalogError::Resource(
                        cellar_logic::raft_fleet::LedgerError::MovementLimit,
                    ));
                }
                let rows = catalog.resource_rows(cut, &book).await?;
                catalog.check_resource_permits(cut, &rows).await?;
                let key = group
                    .history
                    .binding()
                    .key()
                    .map_err(|_| FleetCatalogError::Corrupt("isolated repair binding"))?;
                if rows.iter().any(|r| r.request.group == key) {
                    return Err(FleetCatalogError::Topology(
                        cellar_logic::raft_fleet::TopologyError::UnsettledMovement,
                    ));
                }
                Ok(rows)
            })
            .await?;
        let failed = NodeId::new(context.failed_boot.node)
            .map_err(|_| FleetCatalogError::Invalid("isolated repair node"))?;
        let sample = catalog
            .isolated_repair_source(cut, group, failed, observer, deadline)
            .await?;
        // Managed startup binds the complete declared namespace/class/schema
        // set to this digest. Equality to a proven serving source is a
        // conservative capability witness; unknown supersets are ineligible.
        let source_capabilities = sample
            .node()
            .current_enrollment
            .as_ref()
            .ok_or(FleetCatalogError::Unconfirmed)?
            .capability_digest;
        let source_started = catalog
            .resource_now()?
            .checked_sub(sample.sample_age_ms())
            .ok_or(FleetCatalogError::Unconfirmed)?;
        let (failed_host, voters) = catalog
            .run(deadline.remaining()?, async {
                if catalog.inner.current().await?.token != cut.token {
                    return Err(FleetCatalogError::Conflict);
                }
                let old = catalog.host_from_record(cut, failed).await?;
                let proof = catalog.confirm_node_isolation(cut, &old.record).await?;
                if proof.boot != context.failed_boot {
                    return Err(FleetCatalogError::Conflict);
                }
                let mut voters = Vec::new();
                for (node, member) in &group.voters {
                    let host = catalog.host_from_record(cut, *node).await?;
                    voters.push(PlacedMember {
                        member: *member,
                        node: *node,
                        disk: host.disk(),
                    });
                }
                Ok((hex::encode(old.physical_host().bytes()), voters))
            })
            .await?;
        let placement = catalog
            .placement_topology_operation(cut, deadline.remaining()?)
            .await?;
        let mut ids = BTreeSet::new();
        let mut occupied = group.voters.keys().copied().collect::<BTreeSet<_>>();
        let mut nodes = Vec::new();
        for input in &inputs.nodes {
            input.attestation.validate()?;
            let node = NodeId::new(input.attestation.boot.node)
                .map_err(|_| FleetCatalogError::Invalid("isolated repair input node"))?;
            if node == failed || !ids.insert(node) {
                return Err(FleetCatalogError::Invalid(
                    "isolated repair duplicate or failed node",
                ));
            }
            let (host, capacity) = catalog
                .run(deadline.remaining()?, async {
                    if catalog.inner.current().await?.token != cut.token {
                        return Err(FleetCatalogError::Conflict);
                    }
                    let host = catalog.host_from(cut, node).await?;
                    let capacity = catalog.capacity_from(cut, node).await?;
                    if capacity.attestation().boot != input.attestation.boot
                        || capacity.attestation().profile_revision
                            != input.attestation.profile_revision
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                    Ok((host, capacity))
                })
                .await?;
            let physical = match observer
                .inspect_peer_physical_inventory(
                    node,
                    cut.revision(),
                    deadline.remaining()?.min(Duration::from_millis(500)),
                )
                .await
            {
                Ok(p) => p,
                Err(crate::raft::inspection::InspectionError::Overloaded) => {
                    return Err(FleetCatalogError::Overloaded)
                }
                Err(error) if group.voters.contains_key(&node) => {
                    return Err(FleetCatalogError::Observation(error))
                }
                Err(_) => continue,
            };
            super::super::super::capacity::check_observation(
                &host,
                capacity.attestation(),
                &physical,
            )?;
            let inventory = physical
                .inventory
                .as_ref()
                .ok_or(FleetCatalogError::InventoryIncomplete)?;
            if inventory
                .history(&context.group)
                .map_err(|_| FleetCatalogError::Corrupt("isolated repair physical scope"))?
                .is_some()
            {
                occupied.insert(node);
            }
            let observed_ms = catalog
                .resource_now()?
                .checked_sub(physical.sample_age_ms)
                .ok_or(FleetCatalogError::Unconfirmed)?;
            catalog.require_resource_age(observed_ms)?;
            let view = &physical.view;
            let qualified = capacity.attestation();
            let runtime = &view.startup_bounds;
            if bounds.peak.durable_bytes < view.node.profile.bytes_per_group {
                return Err(FleetCatalogError::Invalid(
                    "isolated repair learner byte bound",
                ));
            }
            let charged = catalog
                .run(deadline.remaining()?, async {
                    if catalog.inner.current().await?.token != cut.token {
                        return Err(FleetCatalogError::Conflict);
                    }
                    catalog
                        .charged_resource_inputs(cut, &input.attestation, qualified, view, &rows)
                        .await
                })
                .await?;
            if let Some(initializing) = inventory.initializing() {
                let mut attributed = false;
                for reservation in &charged.reservations {
                    attributed |=
                        crate::raft::physical_inventory::scope_digest(&reservation.group).map_err(
                            |_| FleetCatalogError::Corrupt("isolated repair initializer scope"),
                        )? == initializing;
                }
                if !attributed {
                    if group.voters.contains_key(&node) {
                        return Err(FleetCatalogError::InventoryIncomplete);
                    }
                    continue;
                }
            }
            let scope = charged.scope;
            let pending = charged.reservations.len();
            nodes.push(TopologyNode {
                ledger: LedgerSnapshot {
                    scope,
                    policy: LedgerPolicy {
                        capacity: Resources {
                            engine_slots: u64::from(
                                qualified
                                    .engine_slots
                                    .min(view.node.profile.group_capacity)
                                    .min(runtime.host_groups)
                                    .min(runtime.consensus_groups)
                                    .min(runtime.engine_groups),
                            ),
                            durable_bytes: qualified
                                .durable_bytes
                                .min(view.node.profile.byte_capacity),
                            memory_bytes: qualified.memory_bytes,
                        },
                        reserve: input.reserve.resources(),
                        max_active_moves: MAX_MOVES as u32,
                        max_sample_age_ms: MAX_SAMPLE_AGE_MS,
                    },
                    sample: PhysicalSample {
                        scope,
                        sequence: cut.revision(),
                        observed_ms,
                        physical: charged.physical.resources(),
                    },
                    reservations: charged.reservations,
                    complete: true,
                },
                host: hex::encode(host.physical_host().bytes()),
                region: view.node.profile.region.clone(),
                zone: view.node.profile.zone.clone(),
                keep: placement.as_ref().is_none_or(|p| p.keep.contains(&node)),
                receives_replicas: host.capability_digest() == source_capabilities
                    && host.eligibility().receive_replicas
                    && capacity.receive_replicas()
                    && inventory.initializing().is_none()
                    && super::super::admission::registration_room(view, pending)?
                    && !view.node.profile.draining,
            });
        }
        let policy = placement
            .as_ref()
            .map(|p| p.policy.clone())
            .unwrap_or(FleetTopologyPolicy {
                revision: 1,
                distinct_zones: false,
                distinct_regions: false,
                max_work: 1_000_000,
            });
        let view = IsolatedRepairView {
            scope: context.group.clone(),
            revision: sample.membership().membership_revision(),
            high_water: sample.membership().member_high_water(),
            voters: voters
                .try_into()
                .map_err(|_| FleetCatalogError::InventoryIncomplete)?,
            failed,
            failed_host,
            occupied: occupied.into_iter().collect(),
            peak: bounds.peak.resources(),
        };
        let plan = select_isolated_repair(
            &view,
            &nodes,
            TopologyPolicy {
                revision: policy.revision,
                distinct_hosts: cut.replica_separation() == FleetReplicaSeparation::PhysicalHosts,
                distinct_zones: policy.distinct_zones,
                distinct_regions: policy.distinct_regions,
                max_work: policy.max_work,
            },
            catalog.resource_now()?,
        )
        .map_err(FleetCatalogError::Topology)?;
        let attestation = inputs
            .nodes
            .iter()
            .find(|i| i.attestation.boot.node == plan.replacement.node.bytes())
            .ok_or(FleetCatalogError::InventoryIncomplete)?
            .attestation
            .clone();
        catalog.require_resource_age(source_started)?;
        let mut digest = sha2::Sha256::new();
        use sha2::Digest;
        digest.update(b"cellar-isolated-repair-plan-1");
        digest.update(identity.id);
        let id: [u8; 32] = digest.finalize().into();
        let reserve = inputs
            .nodes
            .iter()
            .find(|i| i.attestation.boot.node == plan.replacement.node.bytes())
            .ok_or(FleetCatalogError::InventoryIncomplete)?
            .reserve;
        Ok(Selected {
            reserve,
            request: FleetMovementRequest {
                resources: FleetResourceRequest {
                    identity,
                    group: context.group.clone(),
                    attestation,
                    peak: bounds.peak,
                },
                source: sample.membership().replica().node,
                plan: RepairPlan {
                    id: ProposalId::new(id[..16].try_into().map_err(|_| FleetCatalogError::Limit)?)
                        .map_err(|_| FleetCatalogError::Limit)?,
                    policy_revision: plan.policy_revision,
                    expected_revision: plan.expected_revision,
                    obsolete: plan.obsolete,
                    replacement: JoiningMember {
                        member: plan.replacement.member,
                        node: plan.replacement.node,
                        disk: plan.replacement.disk,
                    },
                },
            },
        })
    }
}
