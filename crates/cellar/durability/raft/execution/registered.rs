// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Registration and request identity are checked even for a receipt retry.
use super::*;
use cellar_logic::cell_group::CellHistory;

pub(crate) fn validate(
    limits: ExecutionLimits,
    schema: [u8; 32],
    history: &CellHistory,
    request: &CommandRequest,
    class: &RaftClass,
    input: &[u8],
    system: bool,
) -> Result<OperationContract, ExecutionError> {
    validate_admission(
        limits,
        schema,
        history,
        request,
        class,
        input,
        if system {
            CommandAdmission::System
        } else {
            CommandAdmission::Public
        },
    )
}

pub(crate) fn validate_admission(
    limits: ExecutionLimits,
    schema: [u8; 32],
    history: &CellHistory,
    request: &CommandRequest,
    class: &RaftClass,
    input: &[u8],
    admission: CommandAdmission,
) -> Result<OperationContract, ExecutionError> {
    let contract = OperationContract {
        operation: request.operation.clone(),
        kind: OperationKind::Command,
        digest: request.contract_digest,
    };
    class
        .validate_command_admission(&contract, admission)
        .map_err(ExecutionError::Registration)?;
    if class.schema_digest() != schema {
        return Err(ExecutionError::Registration(
            RegistrationError::ContractMismatch,
        ));
    }
    if input.len() > limits.envelope.max_payload_bytes {
        return Err(ExecutionError::Refused(CommandFailure::Overloaded));
    }
    if command_request_digest(input) != request.request_digest {
        return Err(ExecutionError::Refused(CommandFailure::CommandConflict));
    }
    if admission == CommandAdmission::Event {
        let rpc =
            crate::application::consensus::native_event_rpc::decode(history.cell.as_str(), input)
                .map_err(|_| ExecutionError::Refused(CommandFailure::UnsupportedCapability))?;
        class
            .validate_cell_rpc_identity(history.cell.as_str(), rpc.rpc.name.as_deref())
            .map_err(|_| ExecutionError::Refused(CommandFailure::UnsupportedCapability))?;
    }
    if !admission.is_system() {
        class
            .validate_command_identity(history, &contract, request.id, request.durability, input)
            .map_err(|error| {
                ExecutionError::Refused(
                    error
                        .downcast_ref::<CommandFailure>()
                        .cloned()
                        .unwrap_or(CommandFailure::UnsupportedCapability),
                )
            })?;
    } else if request.id.client
        != crate::raft::effects::system_client(history).map_err(ExecutionError::Refused)?
    {
        return Err(ExecutionError::Refused(
            CommandFailure::UnsupportedCapability,
        ));
    }
    Ok(contract)
}

#[cfg(test)]
#[path = "registered_tests.rs"]
mod tests;
