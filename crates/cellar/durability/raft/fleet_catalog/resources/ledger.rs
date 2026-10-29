// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared scoped charges for selection and independent atomic admission.
use super::admission::capacity_scope;
use super::records::Record;
use super::*;
use crate::raft::host::RaftHostObservation;
use cellar_logic::consensus::ProposalId;
use cellar_logic::raft_fleet::{CapacityScope, Reservation};

pub(super) struct ChargedResources {
    pub scope: CapacityScope,
    pub physical: FleetResourceBounds,
    pub reservations: Vec<Reservation>,
}

impl FleetCatalog {
    /// Called inside the owned catalog turn with every validated rooted book
    /// row. Physical low readings never erase retained floors or old-boot debt.
    /// No materialization credit is inferred from an aggregate observation.
    pub(super) async fn charged_resource_inputs(
        &self,
        snapshot: &FleetSnapshot,
        attestation: &FleetResourceAttestation,
        qualified: &FleetCapacityAttestation,
        view: &RaftHostObservation,
        rows: &[Record],
    ) -> Result<ChargedResources> {
        let scope = capacity_scope(self, attestation)?;
        let mut physical = attestation.used.high_water(FleetResourceBounds {
            engine_slots: view.node.groups,
            durable_bytes: view.node.reserved_bytes.max(qualified.used_durable_bytes),
            memory_bytes: view
                .startup_bounds
                .bounded_buffer_bytes
                .max(qualified.used_memory_bytes),
        });
        physical = physical.high_water(self.resource_floor(snapshot, attestation).await?);
        let mut reservations = Vec::new();
        for row in rows.iter().filter(|row| row.node == scope.node.node) {
            let old_scope = capacity_scope(self, &row.request.attestation)?;
            if old_scope.node.disk != scope.node.disk || old_scope.engine != scope.engine {
                return Err(FleetCatalogError::Corrupt("resource engine rebinding"));
            }
            physical = physical.high_water(row.physical);
            let history = self.history_inner(snapshot, &row.request.group).await?;
            reservations.push(Reservation {
                id: ProposalId::new(row.request.identity.id)
                    .map_err(|_| FleetCatalogError::Corrupt("resource reservation identity"))?,
                group: history
                    .binding()
                    .scope()
                    .map_err(|_| FleetCatalogError::Corrupt("resource reservation scope"))?,
                scope: old_scope,
                peak: row.request.peak.resources(),
                materialized: None,
            });
        }
        Ok(ChargedResources {
            scope,
            physical,
            reservations,
        })
    }
}
