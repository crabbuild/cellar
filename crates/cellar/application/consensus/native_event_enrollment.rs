// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Compiled catalog intent data; only an owned creation response can acknowledge it.

use super::{native_event_rpc, CellEnrollmentRequest, CommandFailure, RaftClass};
use crate::application::event_stream as events;
use crate::raft::cell_groups::CellEnrollmentResponse;
use anyhow::{ensure, Result};
use base64::Engine as _;
use cellar_logic::cell_group::CellHistory;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub(super) type CatalogEnrollments = BTreeMap<(String, String), CatalogEnrollment>;

pub(super) struct CatalogEnrollment {
    scope: String,
    resource: String,
    operation: String,
    fingerprint: [u8; 32],
    wire_bytes: usize,
}

pub(super) fn fingerprint(scope: &str, input: &[u8]) -> Result<(String, [u8; 32])> {
    // Normalize only the ingress nonce. Preserve the complete authority, name,
    // operation and definition wire without decoding another definition graph.
    let (authority, query) =
        native_event_rpc::configuration_receipt_query(scope, input, input.len())?;
    let native_event_rpc::Authority::Event(caller) = authority else {
        anyhow::bail!(CommandFailure::UnsupportedCapability);
    };
    let mut hash = Sha256::new();
    hash.update(b"cellar-catalog-enrollment-intent-1");
    hash.update(u64::try_from(scope.len())?.to_be_bytes());
    hash.update(scope.as_bytes());
    hash.update(query);
    Ok((caller.resource.id, hash.finalize().into()))
}

impl CatalogEnrollment {
    pub(super) fn new(principal: &str, definition: &events::Definition) -> Result<Self> {
        let scope = events::catalog_scope(&definition.resource)?;
        let operation = events::raft::operation(principal, definition)?;
        let args = crate::application::encode_rpc(
            &cellar_native::clone::StructuredValue::from_json(json!([{
                "operation":operation,
                "definition":base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(definition)?),
            }]))?,
        )?;
        let input = native_event_rpc::encode_ingress(
            &scope,
            Some(&events::catalog_name(&definition.resource)?),
            "__catalogConfigure",
            events::Authority::deployment(principal, definition)?.into(),
            &args,
            usize::MAX,
            [1; 16],
        )?;
        Ok(Self {
            fingerprint: fingerprint(&scope, &input)?.1,
            scope,
            resource: definition.resource.id.clone(),
            operation,
            wire_bytes: input.len(),
        })
    }

    pub(super) fn key(&self) -> (String, String) {
        (self.scope.clone(), self.resource.clone())
    }

    pub(super) fn wire_bytes(&self) -> usize {
        self.wire_bytes
    }

    pub(super) fn retained_bytes(&self) -> Option<usize> {
        // Include the map key, descriptor strings and conservative map/Arc
        // overhead in the existing namespace registration bank.
        self.scope
            .len()
            .checked_mul(2)?
            .checked_add(self.resource.len().checked_mul(2)?)?
            .checked_add(self.operation.len())?
            .checked_add(1024)
    }

    /// Data describing the initializer's accepted intent, never current readiness.
    pub(super) fn pending(&self, fingerprint: [u8; 32]) -> Option<Value> {
        if fingerprint != self.fingerprint {
            return None;
        }
        Some(json!({
            "state":"creating", "operation":self.operation, "resource":self.resource,
            "accepted_revision":null, "accepted_digest":null, "last_error":null,
        }))
    }
}

impl RaftClass {
    pub(crate) fn event_enrollment_bytes(&self) -> usize {
        self.event_enrollment_bytes
    }

    /// Acknowledge only the exact intent installed by this request's proven
    /// initializer. The borrowed response retains local/remote minimum proof;
    /// decoded records, booleans and later images cannot stand in for it.
    pub(crate) fn enrolled_event_configuration(
        &self,
        history: &CellHistory,
        request: CellEnrollmentRequest,
        enrollment: &CellEnrollmentResponse,
        input: &[u8],
    ) -> Result<Option<Value>> {
        let Some(intents) = &self.event_enrollments else {
            return Ok(None);
        };
        let (resource, fingerprint) = fingerprint(history.cell.as_str(), input)?;
        let Some(intent) = intents.get(&(history.cell.as_str().to_owned(), resource)) else {
            return Ok(None);
        };
        let record = enrollment.record();
        ensure!(
            record.cell() == history
                && record.creation() == request.creation
                && record.schema() == self.schema_digest()
                && enrollment.durability() >= request.durability,
            CommandFailure::CorruptState
        );
        Ok(intent.pending(fingerprint))
    }
}

#[cfg(test)]
#[path = "native_event_enrollment_tests.rs"]
mod tests;
