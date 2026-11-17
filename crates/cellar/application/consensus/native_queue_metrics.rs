// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Declared Queue producer metrics grant no operator or business-command access.

use super::{native_rpc, CommandFailure, OperationContract, OperationKind, QueryContext};
use crate::application::{decode_rpc, encode_rpc};
use crate::engine_api::RpcData;
use anyhow::{ensure, Context as _, Result};
use cellar_native::clone::StructuredValue;
use sha2::{Digest, Sha256};

const MAGIC: &[u8; 5] = b"CQQM\x01";

pub(crate) fn is_method(scope: &str, method: &str) -> bool {
    scope
        .split_once(':')
        .is_some_and(|(class, _)| class == crate::deploy::QUEUE_CLASS)
        && method == "__queueMetrics"
}

pub(crate) fn contract() -> OperationContract {
    OperationContract {
        operation: "__sys_raft_queue_metrics_query".into(),
        kind: OperationKind::Query,
        digest: Sha256::digest(b"cellar-private-queue-metrics-1").into(),
    }
}

fn arguments(args: &RpcData) -> Result<()> {
    ensure!(
        decode_rpc(args)?
            .to_json()?
            .as_array()
            .is_some_and(Vec::is_empty),
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
    super::native_queue_query::validate_identity(scope, name)?;
    arguments(args)?;
    let limit = limit
        .checked_sub(MAGIC.len())
        .context(CommandFailure::Overloaded)?;
    let request = native_rpc::encode_request(name, args, limit)?;
    let mut output = MAGIC.to_vec();
    output.extend_from_slice(&request);
    Ok(output)
}

pub(crate) fn validate(scope: &str, input: &[u8]) -> Result<()> {
    let input = input
        .strip_prefix(MAGIC)
        .context(CommandFailure::UnsupportedCapability)?;
    let request = native_rpc::decode_request(input)?;
    super::native_queue_query::validate_identity(scope, request.name.as_deref())?;
    arguments(&request.args)
}

pub(crate) fn invoke(context: &QueryContext<'_>, input: &[u8]) -> Result<Vec<u8>> {
    validate(context.identity().cell.as_str(), input)?;
    let value = crate::application::queue::metadata::metrics(&context.storage())?;
    native_rpc::encode_response(&encode_rpc(&StructuredValue::from_json(value)?)?)
}

#[cfg(all(test, not(cellar_internal_tests)))]
#[path = "native_queue_metrics_tests.rs"]
mod tests;
