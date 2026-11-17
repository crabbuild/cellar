// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Private immutable EventStream inspection, distinct from public queries and
//! event commands. Only host-minted query admission can invoke this contract.

use super::{native_event_rpc, native_rpc, OperationContract, OperationKind, QueryContext};
use crate::application::{event_stream, Output};
use sha2::{Digest, Sha256};

const OPERATION: &str = "__sys_raft_event_query";

pub(crate) fn is_metadata(method: &str) -> bool {
    matches!(
        method,
        "__catalogStatus"
            | "__catalogConfigureReceipt"
            | "__catalogResolveName"
            | "__catalogResolve"
            | "__streamConfigureReceipt"
            | "__streamResolve"
            | "__streamRead"
            | "__streamSubscriptionStatus"
            | "__streamAckReceipt"
    )
}

/// Only a host provisioning receipt may lazily initialize a broker. Validate
/// its bounded definition, exact operation and target before creation; ordinary
/// metadata reads retain their missing-name refusal.
pub(crate) fn broker_genesis(scope: &str, input: &[u8]) -> anyhow::Result<bool> {
    let request = native_event_rpc::decode(scope, input)?;
    if request.method != "__streamConfigureReceipt" {
        return Ok(false);
    }
    let native_event_rpc::Authority::Event(caller) = request.authority else {
        anyhow::bail!(super::CommandFailure::UnsupportedCapability);
    };
    event_stream::raft::validate_broker_genesis(
        scope,
        &caller,
        &event_stream::broker::body(&request.rpc.args)?,
    )?;
    Ok(true)
}

/// Only a coherent selected receipt's typed absence permits a configuration
/// mutation. Malformed responses and proof/authorization failures remain errors.
pub(crate) fn configuration_operation_absent(
    response: &crate::engine_api::RpcData,
) -> anyhow::Result<bool> {
    let value = crate::application::decode_rpc_result(response)?.to_json()?;
    let Some(error) = value.get("error") else {
        return Ok(false);
    };
    anyhow::ensure!(
        value.as_object().is_some_and(|object| object.len() == 1),
        "malformed configuration probe response"
    );
    let error: event_stream::DeliveryError = serde_json::from_value(error.clone())?;
    Ok(matches!(
        error.code,
        event_stream::ErrorCode::OperationUnknown | event_stream::ErrorCode::NotInitialized
    ))
}

/// A read can discover a missing name before an image exists. The ordinary
/// enrollment path must independently confirm absence and prove creation.
pub(crate) fn configuration_target_missing(error: &anyhow::Error) -> bool {
    use crate::raft::{cell_groups::CellDispatchError, completion::RuntimeError};
    matches!(
        error.downcast_ref::<RuntimeError>(),
        Some(RuntimeError::NotFound)
    ) || matches!(
        error.downcast_ref::<CellDispatchError>(),
        Some(CellDispatchError::Runtime(RuntimeError::NotFound))
    ) || error
        .downcast_ref::<std::sync::Arc<CellDispatchError>>()
        .is_some_and(|error| {
            matches!(
                error.as_ref(),
                CellDispatchError::Runtime(RuntimeError::NotFound)
            )
        })
}

pub(crate) fn contract(class: &str) -> OperationContract {
    let mut hash = Sha256::new();
    hash.update(b"cellar-raft-private-event-query-1");
    hash.update((class.len() as u64).to_be_bytes());
    hash.update(class.as_bytes());
    OperationContract {
        operation: OPERATION.into(),
        kind: OperationKind::Query,
        digest: hash.finalize().into(),
    }
}

pub(crate) fn invoke(context: &QueryContext<'_>, input: &[u8]) -> anyhow::Result<Vec<u8>> {
    let scope = context.identity().cell.as_str();
    let request = native_event_rpc::decode(scope, input)?;
    anyhow::ensure!(
        is_metadata(&request.method),
        super::CommandFailure::UnsupportedCapability
    );
    let result = event_stream::broker::body(&request.rpc.args).and_then(|body| {
        event_stream::metadata::read(
            &context.storage(),
            scope,
            &request.method,
            &request.authority,
            &body,
            context.result_limit(),
        )
    });
    let Output::Rpc(result) = event_stream::broker::reply(result)? else {
        anyhow::bail!("invalid event metadata response");
    };
    native_rpc::encode_response(&result)
}
