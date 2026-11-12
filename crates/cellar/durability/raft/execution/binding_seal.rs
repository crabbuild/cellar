// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Private sequence retirement. This is not a business-command retry or a proof
//! that earlier independent transactions did not execute.

use super::*;
use serde::{Deserialize, Serialize};

pub(crate) const OPERATION: &str = "__sys_raft_binding_seal";
const MAX_INPUT: usize = 4096;

pub(crate) fn contract() -> OperationContract {
    OperationContract {
        operation: OPERATION.into(),
        kind: OperationKind::Command,
        digest: Sha256::digest(b"cellar-binding-sequence-seal-1").into(),
    }
}

/// The owning recovery adapter supplies the immutable original target identity.
/// Its own maintenance deadline/retry horizon is independent of business expiry.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    pub format: u8,
    pub client: [u8; 16],
    pub sequence: u64,
    pub operation: String,
    pub contract: [u8; 32],
    pub request_digest: [u8; 32],
    pub durability: u8,
    pub retry_until_ms: u64,
}

pub(crate) fn execute(
    class: &RaftClass,
    maintenance: CommandId,
    durability: Durability,
    transaction: &CommandTransaction<'_>,
    input: &[u8],
    limits: ExecutionLimits,
) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(input.len() <= MAX_INPUT, CommandFailure::Overloaded);
    let request: Request = serde_json::from_slice(input)?;
    anyhow::ensure!(
        request.format == 1
            && request.client.starts_with(b"RCB1")
            && request.operation.len() <= 128,
        CommandFailure::UnsupportedCapability
    );
    let contract = OperationContract {
        operation: request.operation.clone(),
        kind: OperationKind::Command,
        digest: request.contract,
    };
    class.validate_operation(&contract)?;
    anyhow::ensure!(
        class.is_cell_rpc(&request.operation),
        CommandFailure::UnsupportedCapability
    );
    let minimum = match request.durability {
        1 => Durability::Quorum,
        2 => Durability::S3,
        _ => anyhow::bail!(CommandFailure::CorruptState),
    };
    anyhow::ensure!(durability >= minimum, CommandFailure::UnsupportedCapability);
    let original = CommandRequest {
        id: CommandId::new(ClientId::new(request.client)?, request.sequence)?,
        operation: request.operation,
        contract_digest: request.contract,
        request_digest: request.request_digest,
        durability: minimum,
        retry_until_ms: request.retry_until_ms,
    };
    anyhow::ensure!(
        original.id.client != maintenance.client,
        CommandFailure::CommandConflict
    );
    let connection = transaction.framework()?;
    let state =
        receipts::seal_binding_sequence(&connection, &original, maintenance.client, &limits)?;
    // 0: unused sequence consumed without business execution; 1: an exact
    // complete/partial/sealed receipt already consumed it. Neither is a child
    // business result. 2 reports only a replicated expired-through floor.
    // Normal lookup of a partial/sealed receipt stays unknown.
    Ok(vec![state as u8])
}

#[cfg(test)]
#[path = "binding_seal_tests.rs"]
mod tests;
