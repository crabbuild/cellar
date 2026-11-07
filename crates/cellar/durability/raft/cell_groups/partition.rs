// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Validate advisory partition geometry before any durable operator admission.
use crate::application::consensus::cell_routing_key;
use crate::raft::catalog::CatalogError;
use cellar_logic::cell_group_placement::PartitionDecision;

/// These checks grant no authority. Selected histories, live quorum, capacity
/// and the original directory/Fleet roots still need their own admission proof.
pub(crate) fn validate_recommendation(
    recommendation: &PartitionDecision<'_>,
) -> Result<(), CatalogError> {
    match *recommendation {
        PartitionDecision::Split { basis, pivot, .. } => {
            if cell_routing_key(basis.route.history.namespace, pivot.cell()) != *pivot {
                return Err(CatalogError::Invalid("noncanonical split pivot"));
            }
            basis
                .route
                .range
                .split(pivot.clone())
                .map_err(|_| CatalogError::Invalid("split pivot outside parent interior"))?;
        }
        PartitionDecision::Merge { left, right } => {
            if left.directory_digest != right.directory_digest
                || left.route.history.cluster != right.route.history.cluster
                || left.route.history.namespace != right.route.history.namespace
                || left.route.history.group == right.route.history.group
                || left.route.schema_digest != right.route.schema_digest
                || left.durability != right.durability
                || left.placement_class != right.placement_class
            {
                return Err(CatalogError::Invalid("incompatible merge recommendation"));
            }
            left.route
                .range
                .merge(&right.route.range)
                .map_err(|_| CatalogError::Invalid("merge sources must be adjacent and ordered"))?;
        }
        PartitionDecision::Wait(_) => {
            return Err(CatalogError::Invalid("partition recommendation waits"));
        }
    }
    Ok(())
}
