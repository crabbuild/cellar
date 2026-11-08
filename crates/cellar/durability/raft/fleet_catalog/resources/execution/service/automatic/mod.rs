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
/// takes precedence over automatic selection until that policy is cancelled.
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
