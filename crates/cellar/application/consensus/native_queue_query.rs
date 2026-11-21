// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Host-authorized Queue inspection. Ordinary query bytes confer no operator grant.

use super::{native_rpc, CommandFailure, OperationContract, OperationKind, QueryContext};
use crate::application::{decode_rpc, encode_rpc};
use crate::engine_api::RpcData;
use anyhow::{ensure, Context as _, Result};
use cellar_native::clone::StructuredValue;
use serde_json::Value;
use sha2::{Digest, Sha256};

const MAGIC: &[u8; 5] = b"CQQR\x01";

pub(crate) fn contract() -> OperationContract {
    OperationContract {
        operation: "__sys_raft_queue_query".into(),
        kind: OperationKind::Query,
        digest: Sha256::digest(b"cellar-private-queue-inspection-1").into(),
    }
}

fn body(args: &RpcData) -> Result<Value> {
    decode_rpc(args)?
        .to_json()?
        .as_array()
        .and_then(|values| values.first())
        .cloned()
        .context("Queue operator input")
}

pub(crate) fn is_metadata(args: &RpcData) -> Result<bool> {
    Ok(matches!(body(args)?["op"].as_str(), Some("info" | "peek")))
}

pub(super) fn validate_identity(scope: &str, name: Option<&str>) -> Result<()> {
    ensure!(
        name.is_some_and(|name| crate::engine_api::queue_cell_scope(name) == scope),
        CommandFailure::UnsupportedCapability
    );
    Ok(())
}

pub(crate) fn encode(
    scope: &str,
    name: Option<&str>,
    args: &RpcData,
    limit: usize,
) -> Result<Vec<u8>> {
    validate_identity(scope, name)?;
    ensure!(is_metadata(args)?, CommandFailure::UnsupportedCapability);
    let limit = limit
        .checked_sub(MAGIC.len())
        .context(CommandFailure::Overloaded)?;
    let request = native_rpc::encode_request(name, args, limit)?;
    let mut output = MAGIC.to_vec();
    output.extend_from_slice(&request);
    Ok(output)
}

fn decode(scope: &str, input: &[u8]) -> Result<Value> {
    let input = input
        .strip_prefix(MAGIC)
        .context(CommandFailure::UnsupportedCapability)?;
    let request = native_rpc::decode_request(input)?;
    validate_identity(scope, request.name.as_deref())?;
    ensure!(
        is_metadata(&request.args)?,
        CommandFailure::UnsupportedCapability
    );
    body(&request.args)
}

pub(crate) fn invoke(context: &QueryContext<'_>, input: &[u8]) -> Result<Vec<u8>> {
    let body = decode(context.identity().cell.as_str(), input)?;
    let value = crate::application::queue::metadata::read(&context.storage(), &body)?;
    let reply = encode_rpc(&StructuredValue::from_json(value)?)?;
    native_rpc::encode_response(&reply)
}

#[cfg(all(test, not(cellar_internal_tests)))]
#[path = "native_queue_query_tests.rs"]
mod tests;
