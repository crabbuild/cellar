// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use crate::{native, values, Limits, Operation};
use anyhow::{ensure, Result};
use cellar::sdk::containers::{
    BindingIntent, BindingObservation, ExecutionClaim, ExecutionOperation, ExecutionOperationKind,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Version-1 native container request carried by an execution controller.
/// The backend independently checks the claim, immutable resource and operation
/// classification on every request. Payloads carry no caller-selected provider.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub format: u8,
    pub operation: String,
    pub input: Value,
}

impl Request {
    pub(super) fn new(operation: Operation, mut input: Value) -> Self {
        input.sort_all_objects();
        Self {
            format: 1,
            operation: operation.name().into(),
            input,
        }
    }

    /// Validate the exact, bounded wire request before backend I/O. Controller
    /// authentication/fencing remains the backend's separate responsibility.
    pub fn decode(
        claim: &ExecutionClaim,
        operation: &ExecutionOperation,
        payload: &[u8],
        limits: &Limits,
    ) -> Result<Self> {
        ensure!(
            payload.len() <= 1024 * 1024,
            "controller request exceeds bound"
        );
        let request: Self = serde_json::from_slice(payload)?;
        let method = Operation::parse(&request.operation)?;
        ensure!(
            request.format == 1
                && super::OPERATIONS.contains(&method)
                && kind(method)? == operation.kind,
            "invalid controller request contract"
        );
        values::check_depth(&request.input)?;
        native::validate(method.name(), &request.input, limits)?;
        let record = &claim.checkpoint().record;
        match method {
            Operation::Start => ensure!(
                record.intent == BindingIntent::Running
                    && record.observation == BindingObservation::Pending
                    && record.resource_id.is_none()
                    && request.parameters()? == record.parameters_digest,
                "create differs from durable intent"
            ),
            Operation::Destroy => ensure!(
                record.intent == BindingIntent::Deleted
                    && record.observation == BindingObservation::Pending
                    && record.resource_id.is_some(),
                "delete lacks exact durable intent"
            ),
            Operation::Running | Operation::Monitor => ensure!(
                record.resource_id.is_some(),
                "observation lacks exact resource"
            ),
            _ => ensure!(
                record.intent == BindingIntent::Running
                    && record.observation == BindingObservation::Confirmed
                    && record.resource_id.is_some(),
                "operation lacks confirmed running generation"
            ),
        }
        Ok(request)
    }

    pub(super) fn parameters(&self) -> Result<String> {
        Ok(hex_digest(
            &ExecutionOperation::new(
                [1; 16],
                ExecutionOperationKind::Create,
                &serde_json::to_vec(self)?,
                1,
            )?
            .payload_digest,
        ))
    }
}

/// The resource is an immutable provider ID, never a reusable container name.
/// Start's value is the original decimal generation. Other values follow the
/// ordinary container bridge protocol; errors must not become successful replies.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reply {
    pub format: u8,
    pub resource_id: String,
    pub value: Value,
}

impl Reply {
    pub(super) fn decode(
        bytes: &[u8],
        claim: &ExecutionClaim,
        operation: Operation,
    ) -> Result<Self> {
        let reply: Self = serde_json::from_slice(bytes)?;
        ensure!(
            reply.format == 1
                && !reply.resource_id.is_empty()
                && reply.resource_id.len() <= 4096
                && !reply.resource_id.contains('\0'),
            "invalid controller reply resource"
        );
        if operation == Operation::Start {
            ensure!(
                reply.value.as_str()
                    == Some(claim.checkpoint().record.generation.to_string().as_str()),
                "create returned another generation"
            );
        } else {
            ensure!(
                claim.checkpoint().record.resource_id.as_deref()
                    == Some(reply.resource_id.as_str()),
                "controller returned another resource"
            );
        }
        values::check_depth(&reply.value)?;
        Ok(reply)
    }
}

pub(super) fn kind(operation: Operation) -> Result<ExecutionOperationKind> {
    Ok(match operation {
        Operation::Start => ExecutionOperationKind::Create,
        Operation::Destroy => ExecutionOperationKind::Delete,
        Operation::Running
        | Operation::Monitor
        | Operation::ReadFile
        | Operation::StatFile
        | Operation::ListFiles => ExecutionOperationKind::Inspect,
        Operation::ExecOutput => ExecutionOperationKind::Execute,
        Operation::Signal => ExecutionOperationKind::Signal,
        Operation::WriteFile => ExecutionOperationKind::Write,
        Operation::SetInactivityTimeout => ExecutionOperationKind::Configure,
        _ => anyhow::bail!("unsupported controller operation"),
    })
}

pub(super) fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
