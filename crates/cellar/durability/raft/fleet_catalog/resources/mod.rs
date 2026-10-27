// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Root-CAS resource debits and exact resource-backed membership admission.
//! Physical measurements remain distinct from deployment attestation; release
//! requires a separate terminal proof boundary.
mod admission;
mod bindings;
mod completion;
mod execution;
mod intent;
mod ledger;
mod maintenance;
mod membership;
mod repair;
pub use completion::{
    FleetAbortedMovementCompletion, FleetFencedMovementCompletion, FleetMovementCompletion,
    FleetMovementCompletionRequest,
};
pub(crate) use execution::MovementServiceWork;
pub use execution::{
    FleetIsolatedRepairServiceStatus, FleetMovementEvidence, FleetMovementService,
    FleetMovementServiceEntry, FleetMovementServiceFailure, FleetMovementServiceOptions,
    FleetMovementServiceStatus, FleetTopologyCollectionOptions, FleetTopologyEvidence,
    FleetTopologyServiceInputs, FleetTopologyServiceStatus,
};
pub use execution::{
    FleetMovementExecutionRequest, FleetMovementPhase, FleetMovementProgress, FleetPendingMovement,
    FleetPendingReservation,
};
pub(crate) use execution::{MovementAction, MovementCommand};
pub use intent::{
    FleetTopologyIntentRequest, FleetTopologyMovementEntry, FleetTopologyMovementPage,
    FleetTopologyOperation, FleetTopologyPhase,
};
pub use repair::{
    FleetIsolatedRepairContext, FleetIsolatedRepairEvidence, FleetIsolatedRepairInputs,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Authority {
    None,
    Admission,
    Completion,
    Isolation,
    Topology,
    TopologyAdmission,
    RestartSettlement,
    RestartReadiness,
}
mod movement;
mod records;
mod topology;

pub use topology::{
    FleetTopologyHistoryResources, FleetTopologyNodeResources, FleetTopologyPlan,
    FleetTopologyPolicy, FleetTopologyPreview, FleetTopologyRequest, FleetTopologyVerification,
};

pub use movement::{FleetMovementAdmission, FleetMovementRequest};

use super::*;
use crate::raft::fleet_stop::BootBinding;
use cellar_logic::consensus::{GroupScope, NodeId};
use cellar_logic::raft_fleet::{LedgerError, Resources};
use serde::Deserialize;

// Initial execution profile: two active membership moves fleet-wide and at
// most two on any node. Queued plan entries are not active resource admissions.
const MAX_MOVES: usize = 2;
const MAX_SAMPLE_AGE_MS: u64 = 5000;
const BOOK_KEY: &str = "resource-reservations";
const ROW_PREFIX: &str = "resource/";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetResourceBounds {
    pub engine_slots: u32,
    pub durable_bytes: u64,
    pub memory_bytes: u64,
}

impl FleetResourceBounds {
    fn resources(self) -> Resources {
        Resources {
            engine_slots: u64::from(self.engine_slots),
            durable_bytes: self.durable_bytes,
            memory_bytes: self.memory_bytes,
        }
    }

    fn high_water(self, other: Self) -> Self {
        Self {
            engine_slots: self.engine_slots.max(other.engine_slots),
            durable_bytes: self.durable_bytes.max(other.durable_bytes),
            memory_bytes: self.memory_bytes.max(other.memory_bytes),
        }
    }
}

/// Trusted deployment occupancy attestation for this admission, not an
/// independently measured RSS/disk sample. The submitting operator must retain
/// the referenced evidence. The adapter also owns a fresh authenticated probe;
/// its actual occupancy and installed pools provide additional lower bounds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetResourceAttestation {
    pub boot: BootBinding,
    pub profile_revision: u64,
    pub used: FleetResourceBounds,
    pub evidence_digest: [u8; 32],
    pub evidence_reference: String,
}

impl FleetResourceAttestation {
    fn validate(&self) -> Result<()> {
        self.boot
            .validate()
            .map_err(|_| FleetCatalogError::Invalid("resource boot"))?;
        if self.profile_revision == 0
            || self.used.engine_slots > 32_768
            || self.evidence_digest == [0; 32]
            || self.evidence_reference.is_empty()
            || self.evidence_reference.len() > 256
            || self.evidence_reference.chars().any(char::is_control)
        {
            return Err(FleetCatalogError::Invalid("resource attestation"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetResourceRequest {
    pub identity: FleetRequestIdentity,
    pub group: GroupScope,
    pub attestation: FleetResourceAttestation,
    /// Conservative incremental learner bound, including seed, image, tail
    /// and worker obligations. Actual host admission can still refuse it.
    pub peak: FleetResourceBounds,
}

/// Historical debit acceptance. It grants no current capacity lease, learner
/// initialization, membership permit, cancellation or reservation release.
pub struct FleetResourceAdmission {
    record: records::Record,
    applied: u64,
    observed: u64,
}

impl FleetResourceAdmission {
    pub fn request(&self) -> [u8; 16] {
        self.record.request.identity.id
    }

    pub fn node(&self) -> NodeId {
        // Record validation proves this identity before constructing admission.
        self.record.node
    }

    pub fn peak(&self) -> FleetResourceBounds {
        self.record.request.peak
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied
    }

    pub fn observed_revision(&self) -> u64 {
        self.observed
    }
}

fn row_key(id: [u8; 16]) -> String {
    format!("{ROW_PREFIX}{}", hex::encode(id))
}

fn resource_error(error: LedgerError) -> FleetCatalogError {
    FleetCatalogError::Resource(error)
}
