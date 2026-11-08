// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Configured whole-fleet placement shares the existing movement owner.
mod evidence;
mod policy;

use super::*;
use crate::raft::catalog::RangeDirectory;
use std::fmt;

pub(super) use evidence::Evidence;
pub(super) use policy::Automatic;

/// Optional permanent-boot fencing and recovery, using the same movement worker
/// as balancing. Installing this explicitly authorizes the platform contract.
#[derive(Clone)]
pub struct FleetAutomaticRecoveryOptions {
    pub platform: Arc<dyn FleetFailureRecoveryPlatform>,
    pub recovery: FleetFailureRecoveryOptions,
}

impl fmt::Debug for FleetAutomaticRecoveryOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FleetAutomaticRecoveryOptions")
            .finish_non_exhaustive()
    }
}

/// Whole-group placement over all enrolled nodes. Destinations and operation
/// identities are generated from rooted metadata, never application callbacks.
///
/// Bounds are conservative deployment contracts for every compiled namespace;
/// physical inventories, rooted floors and exact permits are checked separately.
/// Retained obsolete copies cannot fund moves. An operator's governing topology
/// takes precedence until an explicit automatic intent supersedes that policy.
#[derive(Clone, Debug)]
pub struct FleetAutomaticTopologyOptions {
    pub principal: String,
    pub policy: FleetTopologyPolicy,
    pub resident: FleetResourceBounds,
    pub peak: FleetResourceBounds,
    /// Additional unallocated headroom. Even zero requires the topology
    /// planner's complete RF3 restoration witness for every kept-node loss.
    pub reserve: FleetResourceBounds,
    pub movement: FleetMovementServiceOptions,
    pub collection: FleetTopologyCollectionOptions,
    pub recovery: Option<FleetAutomaticRecoveryOptions>,
}

impl FleetAutomaticTopologyOptions {
    /// Build an explicit handover to automatic policy at a caller-observed
    /// root. Submit through `FleetCatalog::begin_topology`; retain this exact
    /// request for retries after an unknown result. This grants no placement,
    /// membership, convergence or release proof. Ordinary configured selection
    /// discovers the eligible keep-set itself.
    pub fn intent(
        &self,
        catalog: [u8; 32],
        revision: u64,
        mut keep: Vec<cellar_logic::consensus::NodeId>,
    ) -> Result<FleetTopologyIntentRequest> {
        self.validate()?;
        if catalog == [0; 32] || revision == 0 || !(4..=64).contains(&keep.len()) {
            return Err(FleetCatalogError::Invalid("automatic topology intent"));
        }
        keep.sort_unstable();
        if keep.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(FleetCatalogError::Invalid(
                "duplicate automatic topology node",
            ));
        }
        let id = policy::intent_id(catalog, revision, &self.principal, &self.policy, &keep)?;
        let request = FleetTopologyIntentRequest {
            identity: FleetRequestIdentity {
                id,
                operation: id,
                principal: self.principal.clone(),
                expected_revision: revision,
            },
            keep,
            policy: self.policy.clone(),
        };
        crate::raft::fleet_catalog::histories::validate_request(&request.identity)?;
        Ok(request)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        crate::raft::fleet_catalog::histories::validate_request(&FleetRequestIdentity {
            id: [1; 16],
            operation: [1; 16],
            principal: self.principal.clone(),
            expected_revision: 1,
        })?;
        self.movement.validate()?;
        self.collection.validate()?;
        if self.policy.revision == 0
            || !(1..=50_000_000).contains(&self.policy.max_work)
            || self.resident.engine_slots != 1
            || self.peak.engine_slots != 1
            || self.resident.durable_bytes == 0
            || self.resident.memory_bytes == 0
            || self.peak.high_water(self.resident) != self.peak
        {
            return Err(FleetCatalogError::Invalid("automatic topology bounds"));
        }
        if let Some(recovery) = &self.recovery {
            recovery.recovery.validate()?;
        }
        Ok(())
    }
}

/// Local diagnostics only. No phase proves convergence, fencing or release.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FleetAutomaticTopologyWait {
    /// The installed origin boot has not completed capacity activation.
    OriginUnqualified,
    Changed,
    Unchanged,
    OperatorPolicy,
    Disruption,
    InsufficientNodes,
    InventoryPending,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetAutomaticTopologyStatus {
    pub wait: FleetAutomaticTopologyWait,
    pub selected_operations: u64,
}

#[derive(Clone)]
pub(crate) struct ConfiguredAutomaticTopology {
    pub catalog: FleetCatalog,
    pub namespaces: Vec<RangeDirectory>,
    pub options: FleetAutomaticTopologyOptions,
}
