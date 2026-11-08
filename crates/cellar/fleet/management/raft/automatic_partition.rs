// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Standard deployment planning; applications provide policy, never child IDs.
use super::*;
use cellar_logic::{cell_group::GroupHistory, cell_group_placement::PartitionPolicy};
use futures_util::future::BoxFuture;
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
};

pub use cellar_logic::cell_group_placement::PartitionPlacementPolicy as RaftFleetPartitionPlacementPolicy;

/// Opt-in startup policy. Complete explicitly authorizes irreversible split /
/// merge execution in the compiled managed namespaces. Physical replay reuses
/// the node's private archive inventory and existing archive/checkpoint pools.
#[derive(Clone, Debug)]
pub struct RaftFleetAutomaticPartitionOptions {
    pub placement: RaftFleetPartitionPlacementPolicy,
    pub sizing: PartitionPolicy,
    pub service: RaftFleetPartitionServiceOptions,
    pub recovery: crate::raft::cell_groups::CellGroupRecoveryLimits,
}

impl RaftFleetAutomaticPartitionOptions {
    pub(crate) fn validate(&self) -> Result<()> {
        self.service.validate()?;
        if !self.placement.valid() {
            return Err(RaftFleetFailure::invalid(
                "automatic partition placement policy",
            ));
        }
        crate::raft::cell_groups::CellGroupRecoveryPool::new(self.recovery)
            .map_err(|_| RaftFleetFailure::invalid("automatic partition recovery limits"))?;
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct ConfiguredPartition {
    pub control: RaftFleetControl,
    pub options: RaftFleetAutomaticPartitionOptions,
}

struct Planner {
    control: RaftFleetControl,
    placement: RaftFleetPartitionPlacementPolicy,
    placement_class: [u8; 32],
    deferred: Mutex<BTreeMap<NodeId, u64>>,
}

impl RaftFleetPartitionPlanner for Planner {
    fn pressure(&self, _: Duration) -> BoxFuture<'_, Result<RaftFleetPartitionPressure>> {
        Box::pin(async {
            // No invented CPU/health observation: zero adds no external pressure
            // hint. The scheduler measures bytes, cells, rates, apply and archive
            // backlog directly; fresh placement uses actual peer inventories.
            Ok(RaftFleetPartitionPressure {
                placement_class: self.placement_class,
                leader_cpu_bps: 0,
                failure_domain_load_bps: 0,
            })
        })
    }

    fn plan(
        &self,
        context: RaftFleetPartitionPlanContext,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<RaftFleetTransitionAdmissionRequest>> {
        Box::pin(async move {
            let turn = Turn::new(&self.control.domain, timeout)?;
            let directory = self
                .control
                .managed_namespace(&context.basis.namespace_id)?;
            let (partition, children) = match context.recommendation {
                RaftFleetPartitionRecommendation::Split { basis, pivot, .. } => (
                    RaftFleetTransitionPartition::Split {
                        parent: group(basis.route.history),
                        pivot_cell: pivot.cell().as_str().into(),
                    },
                    2,
                ),
                RaftFleetPartitionRecommendation::Merge { left, right } => (
                    RaftFleetTransitionPartition::Merge {
                        left: group(left.route.history),
                        right: group(right.route.history),
                    },
                    1,
                ),
                RaftFleetPartitionRecommendation::Wait(_) => {
                    return Err(RaftFleetFailure::invalid("wait is not a placement request"))
                }
            };
            let now = self
                .control
                .domain
                .mono_ms()
                .map_err(|_| FleetCatalogError::Closed)?;
            let excluded: BTreeSet<_> = {
                let mut deferred = self
                    .deferred
                    .lock()
                    .map_err(|_| FleetCatalogError::Closed)?;
                deferred.retain(|_, until| *until > now);
                deferred.keys().copied().collect()
            };
            let placement = self
                .control
                .catalog
                .partition_placement(
                    self.control
                        .node_observer
                        .as_ref()
                        .ok_or_else(RaftFleetFailure::unsupported)?,
                    crate::raft::fleet_catalog::PartitionPlacementRequest {
                        directory,
                        revision: context.basis.fleet_revision,
                        policy: &self.placement,
                        children,
                        excluded: &excluded,
                    },
                    turn.remaining()?,
                )
                .await?;
            let now = self
                .control
                .domain
                .mono_ms()
                .map_err(|_| FleetCatalogError::Closed)?;
            {
                let mut deferred = self
                    .deferred
                    .lock()
                    .map_err(|_| FleetCatalogError::Closed)?;
                for node in placement.deferred {
                    deferred.insert(node, now.saturating_add(1000));
                }
            }
            let voters = placement.voters.ok_or_else(|| RaftFleetFailure {
                code: RaftFleetFailureCode::Unavailable,
                outcome: RaftFleetOutcome::NotSubmitted,
                message:
                    "No RF3 placement satisfies current capacity, separation and recovery reserves."
                        .into(),
            })?;
            self.control
                .domain
                .run(async {
                    let mut rng = crate::asyncrt::rng("cell-group-partition-placement");
                    let mut id = || -> Result<String> {
                        let mut bytes = [0; 16];
                        rng.try_fill_bytes(&mut bytes)
                            .map_err(|_| FleetCatalogError::Unconfirmed)?;
                        ProposalId::new(bytes).map_err(|_| FleetCatalogError::Unconfirmed)?;
                        Ok(hex::encode(bytes))
                    };
                    let request = RaftFleetTransitionAdmissionRequest {
                        catalog_id: context.basis.catalog_id,
                        namespace_id: context.basis.namespace_id,
                        operation_id: id()?,
                        request_id: id()?,
                        expected_fleet_revision: context.basis.fleet_revision,
                        expected_directory_digest: context.basis.directory_digest,
                        class: placement.class,
                        partition,
                        children: voters
                            .into_iter()
                            .map(|nodes| {
                                Ok(RaftFleetTransitionChild {
                                    group: RaftFleetTransitionGroup {
                                        group_id: id()?,
                                        incarnation_id: id()?,
                                    },
                                    request_id: id()?,
                                    creation_id: id()?,
                                    voters: std::array::from_fn(|i| RaftFleetFirstGroupVoter {
                                        node_id: hex::encode(nodes[i].bytes()),
                                        member_id: i as u64 + 1,
                                    }),
                                })
                            })
                            .collect::<Result<_>>()?,
                    };
                    request.validate(&request.namespace_id, &request.operation_id)?;
                    turn.remaining()?;
                    Ok(request)
                })
                .await
                .map_err(|_| FleetCatalogError::Closed)?
        })
    }
}

fn group(history: GroupHistory) -> RaftFleetTransitionGroup {
    RaftFleetTransitionGroup {
        group_id: hex::encode(history.group.bytes()),
        incarnation_id: hex::encode(history.incarnation.bytes()),
    }
}

impl RaftFleetControl {
    /// Start the standard RF3 deployment planner and existing partition executor.
    /// Pool membership comes from rooted active capacity qualification. New nodes
    /// become candidates without application callbacks or controller restarts.
    pub async fn start_automatic_partition_service(
        &self,
        node: &RaftCells,
        sizing: PartitionPolicy,
        placement: RaftFleetPartitionPlacementPolicy,
        options: RaftFleetPartitionServiceOptions,
        timeout: Duration,
    ) -> Result<RaftFleetPartitionService> {
        if !placement.valid() {
            return Err(RaftFleetFailure::invalid(
                "automatic partition placement policy",
            ));
        }
        let turn = Turn::new(&self.domain, timeout)?;
        let host = node.installed_host(turn.remaining()?).await?;
        let mut control = self.clone();
        control.domain = node.handle.domain.clone();
        control.catalog = host.stop_catalog().owned_client(control.domain.clone());
        control.node_observer = Some(node.internal_observer());
        let mut hash = Sha256::new();
        hash.update(b"cellar-partition-placement-1");
        hash.update((placement.pool.len() as u64).to_be_bytes());
        hash.update(placement.pool.as_bytes());
        hash.update([
            u8::from(placement.distinct_regions),
            u8::from(placement.distinct_zones),
        ]);
        hash.update(placement.recovery_slots.to_be_bytes());
        let planner = Planner {
            control: control.clone(),
            placement,
            placement_class: hash.finalize().into(),
            deferred: Mutex::new(BTreeMap::new()),
        };
        control
            .start_partition_service(node, Arc::new(planner), sizing, options, turn.remaining()?)
            .await
    }
}
