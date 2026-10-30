// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Registration and request identity are checked even for a receipt retry.
use super::*;
use cellar_logic::cell_group::CellHistory;

pub(super) fn validate(
    limits: ExecutionLimits,
    schema: [u8; 32],
    history: &CellHistory,
    attempt: &CommandAttempt,
    class: &RaftClass,
    input: &[u8],
    system: bool,
) -> Result<OperationContract, ExecutionError> {
    let contract = OperationContract {
        operation: attempt.request.operation.clone(),
        kind: OperationKind::Command,
        digest: attempt.request.contract_digest,
    };
    if system {
        class.validate_system_operation(&contract)
    } else {
        class.validate_operation(&contract)
    }
    .map_err(ExecutionError::Registration)?;
    if class.schema_digest() != schema {
        return Err(ExecutionError::Registration(
            RegistrationError::ContractMismatch,
        ));
    }
    if input.len() > limits.envelope.max_payload_bytes {
        return Err(ExecutionError::Refused(CommandFailure::Overloaded));
    }
    if command_request_digest(input) != attempt.request.request_digest {
        return Err(ExecutionError::Refused(CommandFailure::CommandConflict));
    }
    if !system {
        class
            .validate_command_identity(
                history,
                &contract,
                attempt.request.id,
                attempt.request.durability,
                input,
            )
            .map_err(|error| {
                ExecutionError::Refused(
                    error
                        .downcast_ref::<CommandFailure>()
                        .cloned()
                        .unwrap_or(CommandFailure::UnsupportedCapability),
                )
            })?;
    } else if attempt.request.id.client
        != crate::raft::effects::system_client(history).map_err(ExecutionError::Refused)?
    {
        return Err(ExecutionError::Refused(
            CommandFailure::UnsupportedCapability,
        ));
    }
    Ok(contract)
}
