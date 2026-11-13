// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Immutable large values cross the SDK boundary once. Quorum RPCs retain
//! compact row metadata and verify publication before installing a reference.
use super::*;

pub(super) async fn put(
    context: &Context,
    identity: &str,
    entry: Entry,
    ttl: Option<i64>,
) -> Result<()> {
    let deadline = raft_wire::deadline(context)?;
    let reference =
        raft_wire::publish_bytes(context, identity, entry.value.into(), deadline).await?;
    let args = StructuredValue::from_json(
        json!([{"entries":[{"key":entry.key,"externalValue":reference,
        "value":null,"tag":entry.tag,"metadata":entry.metadata,"expiresAt":entry.expires,"expirationTtlMs":ttl}]}]),
    )?;
    raft_wire::call_at(context, identity, "__kvPut", encode_rpc(&args)?, deadline).await?;
    Ok(())
}

pub(super) async fn read(
    context: &Context,
    identity: &str,
    reply: &raft_wire::Reply,
    row: &CloneValue,
    kind: ReadType,
) -> Result<Option<ReadValue>> {
    if !matches!(wire::field(reply, row, "found"), CloneValue::Boolean(true)) {
        return Ok(None);
    }
    let reference = wire::optional_string(wire::field(reply, row, "externalValue"))?;
    let value = if let Some(reference) = reference {
        ensure!(
            raft_wire::enabled(context),
            "KV_ERROR: unexpected external value"
        );
        let bytes =
            raft_wire::fetch_value(context, identity, &reference, reply.deadline()?).await?;
        // Keep the shared byte reservation until conversion transfers ownership
        // into the application's ordinary bounded result.
        let value = decode_value(bytes.as_ref().to_vec(), kind)?;
        drop(bytes);
        value
    } else {
        let bytes = wire::bytes(reply, wire::field(reply, row, "value"))?;
        decode_value(bytes, kind)?
    };
    reply.check(context)?;
    Ok(Some(value))
}

pub(super) fn check_rows(reply: &raft_wire::Reply, rows: &[CloneValue]) -> Result<()> {
    let mut total = 0;
    for row in rows {
        if !matches!(wire::field(reply, row, "found"), CloneValue::Boolean(true)) {
            continue;
        }
        let length = match wire::optional_string(wire::field(reply, row, "externalValue"))? {
            Some(reference) => raft_wire::value_length(&reference)?,
            None => wire::bytes(reply, wire::field(reply, row, "value"))?.len(),
        };
        charge(&mut total, length)?;
    }
    Ok(())
}

fn charge(total: &mut usize, bytes: usize) -> Result<()> {
    *total = total
        .checked_add(bytes)
        .ok_or_else(|| error("aggregate KV value limit"))?;
    // Compact references cannot bypass the existing native wire's aggregate
    // ceiling and turn a hundred-key read into gigabytes of application memory.
    ensure!(
        *total <= raft_wire::MAX_BYTES - 4096,
        "KV_ERROR: aggregate values exceed the native result limit"
    );
    Ok(())
}

/// Transport only the binary field differently. The quorum service still
/// validates the key, metadata, expiry and conditional revision itself.
pub(super) async fn stage_operator(
    owner: &super::super::consensus::RaftCells,
    scope: &str,
    name: Option<&str>,
    args: RpcData,
    deadline: u64,
) -> Result<RpcData> {
    let value = decode_rpc(&args)?.to_json()?;
    let Value::Array(mut arguments) = value else {
        return Ok(args);
    };
    if arguments.len() != 1 {
        return Ok(args);
    }
    let mut body = arguments.remove(0);
    if !matches!(body["op"].as_str(), Some("put-base64" | "edit")) {
        return Ok(args);
    }
    let Some(encoded) = body["value"].as_str() else {
        return Ok(args);
    };
    if encoded.len() <= policy::MAX_INLINE_VALUE_BYTES {
        return Ok(args);
    }
    let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
        return Ok(args);
    };
    if bytes.len() <= policy::MAX_INLINE_VALUE_BYTES || bytes.len() > policy::MAX_VALUE_BYTES {
        return Ok(args);
    }
    drop(args);
    let reference =
        raft_wire::publish_admin_bytes(owner, scope, name, bytes.into(), deadline).await?;
    body["value"] = Value::Null;
    body["externalValue"] = json!(reference);
    encode_rpc(&StructuredValue::from_json(json!([body]))?)
}

pub(super) async fn expand_operator(
    owner: &super::super::consensus::RaftCells,
    scope: &str,
    mut value: Value,
    deadline: u64,
) -> Result<Value> {
    if let Some(reference) = value["valueReference"].as_str() {
        let bytes = raft_wire::fetch_admin_value(owner, scope, reference, deadline).await?;
        value
            .as_object_mut()
            .ok_or_else(|| error("KV inspection result"))?
            .remove("valueReference");
        value["value"] = json!(base64::engine::general_purpose::STANDARD.encode(bytes.as_ref()));
        value["valueEncoding"] = json!("base64");
    }
    ensure!(
        !owner.is_closed(),
        super::super::consensus::RuntimeError::Closed
    );
    raft_wire::remaining(deadline)?;
    Ok(value)
}
