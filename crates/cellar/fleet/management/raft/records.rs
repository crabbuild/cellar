// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Descriptive operator values. No serialized value grants Raft authority.
use super::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetBegin {
    /// Immutable catalog-generation digest returned by fleet status.
    pub catalog_id: String,
    pub operation_id: String,
    pub expected_revision: u64,
    pub keep: Vec<String>,
    pub policy: FleetTopologyPolicy,
}

impl RaftFleetBegin {
    /// Canonical set ordering also defines the management audit fingerprint.
    pub fn canonicalize(mut self) -> Result<Self> {
        catalog_id(&self.catalog_id)?;
        id(&self.operation_id)?;
        revision(self.expected_revision)?;
        if !(4..=64).contains(&self.keep.len())
            || self.policy.revision == 0
            || !(1..=50_000_000).contains(&self.policy.max_work)
        {
            return Err(RaftFleetFailure::invalid("invalid topology bounds"));
        }
        for node in &self.keep {
            id(node)?;
        }
        self.keep.sort();
        if self.keep.windows(2).any(|w| w[0] == w[1]) {
            return Err(RaftFleetFailure::invalid("duplicate keep node"));
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetCancel {
    pub catalog_id: String,
    pub request_id: String,
    pub expected_revision: u64,
}

impl RaftFleetCancel {
    pub fn validate(&self, operation: &str) -> Result<()> {
        catalog_id(&self.catalog_id)?;
        id(&self.request_id)?;
        id(operation)?;
        revision(self.expected_revision)?;
        if self.request_id == operation {
            return Err(RaftFleetFailure::invalid(
                "cancellation requires a distinct request ID",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetOperation {
    pub catalog_id: String,
    pub operation_id: String,
    pub request_id: String,
    pub principal: String,
    pub expected_revision: u64,
    pub keep: Vec<String>,
    pub policy: FleetTopologyPolicy,
    pub phase: FleetTopologyPhase,
    /// Settled completion or cancellation; it does not authorize node deletion.
    pub terminal: bool,
    /// Historical placement completion at applied_revision. This is not a
    /// fresh health/balance lease; cancellation does not produce this receipt.
    pub converged: bool,
    pub admitted_movements: u64,
    pub applied_revision: u64,
    pub observed_revision: u64,
    pub last_superseded_admission: Option<String>,
}

impl RaftFleetOperation {
    pub(super) fn from_root(value: FleetTopologyOperation, catalog_id: &str) -> Self {
        Self {
            catalog_id: catalog_id.into(),
            operation_id: hex::encode(value.identity.operation),
            request_id: hex::encode(value.identity.id),
            principal: value.identity.principal,
            expected_revision: value.identity.expected_revision,
            keep: value.keep.iter().map(|n| hex::encode(n.bytes())).collect(),
            policy: value.policy,
            phase: value.phase,
            terminal: matches!(
                value.phase,
                FleetTopologyPhase::Complete | FleetTopologyPhase::Cancelled
            ),
            converged: value.phase == FleetTopologyPhase::Complete,
            admitted_movements: value.admitted_movements,
            applied_revision: value.applied_revision,
            observed_revision: value.observed_revision,
            last_superseded_admission: value.last_superseded_admission.map(hex::encode),
        }
    }
}

impl RaftFleetOperation {
    /// Validate descriptive wire data before displaying terminal success.
    pub fn validate(&self) -> Result<()> {
        let canonical = RaftFleetBegin {
            catalog_id: self.catalog_id.clone(),
            operation_id: self.operation_id.clone(),
            expected_revision: self.expected_revision,
            keep: self.keep.clone(),
            policy: self.policy.clone(),
        }
        .canonicalize()?;
        id(&self.request_id)?;
        if let Some(candidate) = &self.last_superseded_admission {
            id(candidate)?;
        }
        if canonical.keep != self.keep
            || self.principal.is_empty()
            || self.principal.len() > 128
            || self.principal.chars().any(char::is_control)
            || self.applied_revision <= self.expected_revision
            || self.observed_revision < self.applied_revision
            || self.terminal
                != matches!(
                    self.phase,
                    FleetTopologyPhase::Complete | FleetTopologyPhase::Cancelled
                )
            || self.converged != (self.phase == FleetTopologyPhase::Complete)
        {
            return Err(RaftFleetFailure::invalid("inconsistent operation response"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetStatus {
    pub catalog_id: String,
    pub observed_revision: u64,
    pub active: Option<RaftFleetOperation>,
    /// The retained keep policy may belong to an already completed operation.
    pub placement: Option<RaftFleetOperation>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetAcceptance {
    pub request_id: String,
    pub operation: RaftFleetOperation,
    /// A repeated audit request was reconciled against the real catalog.
    pub reconciled: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetMovement {
    pub reservation: String,
    pub cluster: String,
    pub namespace: String,
    pub group: String,
    pub incarnation: String,
    pub cell: String,
    pub source: String,
    pub target: String,
    pub admitted_revision: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetMovements {
    pub catalog_id: String,
    pub operation_id: String,
    pub observed_revision: u64,
    pub entries: Vec<RaftFleetMovement>,
    /// A changed movement index invalidates this position: restart pagination.
    pub next_cursor: Option<String>,
}

pub(super) fn id(value: &str) -> Result<[u8; 16]> {
    let mut bytes = [0; 16];
    if value.len() != 32
        || hex::decode_to_slice(value, &mut bytes).is_err()
        || hex::encode(bytes) != value
        || bytes == [0; 16]
    {
        return Err(RaftFleetFailure::invalid(
            "IDs require 32 lowercase hexadecimal digits and must be nonzero",
        ));
    }
    Ok(bytes)
}

pub(super) fn catalog_id(value: &str) -> Result<()> {
    let mut digest = [0; 32];
    if value.len() != 64
        || hex::decode_to_slice(value, &mut digest).is_err()
        || hex::encode(digest) != value
        || digest == [0; 32]
    {
        return Err(RaftFleetFailure::invalid(
            "catalog ID requires a nonzero 64-digit lowercase hexadecimal digest",
        ));
    }
    Ok(())
}

pub(super) fn revision(value: u64) -> Result<()> {
    if value == 0 || value >= u64::MAX - 2 {
        Err(RaftFleetFailure::invalid("invalid expected revision"))
    } else {
        Ok(())
    }
}
