// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Inline observations of an already initialized, quorum-pinned KV image.
use super::*;
use crate::application::consensus::{native_rpc, CellHistory, CommandFailure};

pub(crate) const OPERATION: &str = "__cellar_kv_inline_read";

/// Null is private eligibility metadata, never a missing-key answer. The
/// existing command handles cold schema, external values and larger replies.
fn fallback() -> Result<RpcData> {
    encode_rpc(&StructuredValue::from_json(Value::Null)?)
}

pub(crate) fn is_fallback(response: &RpcData) -> Result<bool> {
    Ok(matches!(
        decode_rpc_result(response)?.root,
        CloneValue::Null
    ))
}

pub(crate) fn query(
    storage: &Storage,
    history: &CellHistory,
    args: RpcData,
    result_limit: usize,
) -> Result<Vec<u8>> {
    let reply = match inline(storage, history, args, result_limit)? {
        Some(value) => encode_rpc(&value)?,
        None => fallback()?,
    };
    let encoded = native_rpc::encode_response(&reply)?;
    if encoded.len() <= result_limit {
        return Ok(encoded);
    }
    let encoded = native_rpc::encode_response(&fallback()?)?;
    ensure!(encoded.len() <= result_limit, CommandFailure::Overloaded);
    Ok(encoded)
}

fn inline(
    storage: &Storage,
    history: &CellHistory,
    args: RpcData,
    result_limit: usize,
) -> Result<Option<StructuredValue>> {
    // JSON callers retain their original response shaping and command path.
    if !matches!(&args, RpcData::Structured(_)) {
        return Ok(None);
    }
    let request = decode_rpc(&args)?;
    let arguments = wire::array(&request, &request.root)?;
    let body = arguments
        .first()
        .ok_or_else(|| error("RPC has no arguments"))?;
    let keys = names(&request, wire::field(&request, body, "keys"))?;
    check_bulk(&keys)?;
    let metadata = matches!(
        wire::field(&request, body, "withMetadata"),
        CloneValue::Boolean(true)
    );
    let expiration = matches!(
        wire::field(&request, body, "withExpiration"),
        CloneValue::Boolean(true)
    );
    let schema = storage.system_sql_read(
        "SELECT COUNT(*) AS count FROM sqlite_schema WHERE type='table' AND name IN ('__sys_kv','__sys_kv_meta')", &[])?;
    let installed = schema.first().ok_or(CommandFailure::CorruptState)?;
    if integer(installed, "count")? != 2 {
        return Ok(None);
    }
    let at = now();
    let mut remaining = result_limit.saturating_sub(256);
    let mut rows = Vec::new();
    let mut values = Vec::new();
    for key in keys {
        // Inspect lengths before loading value buffers. Both reads use the
        // same immutable image. Conservative codec headroom bounds accumulated
        // rows within the existing result reservation; the final frame is
        // checked exactly. This changes eligibility, never the public limit.
        let row = storage.system_sql_read(
            "SELECT blob_id IS NOT NULL AS external,length(value) AS value_bytes,length(CAST(tag AS BLOB)) AS tag_bytes,length(CAST(metadata AS BLOB)) AS metadata_bytes FROM __sys_kv WHERE name=? AND (expires_at IS NULL OR expires_at>?)",
            &[key.clone().into(), at.into()])?.into_iter().next();
        let cost = key.len().saturating_mul(6).saturating_add(1024);
        let Some(next) = remaining.checked_sub(cost) else {
            return Ok(None);
        };
        remaining = next;
        let Some(row) = row else {
            rows.push(json!({"key":key,"found":false}));
            values.push(None);
            continue;
        };
        // An immutable object locator requires its original publication/pin
        // protocol. This callback neither exposes it nor performs external I/O.
        if integer(&row, "external")? != 0 {
            return Ok(None);
        }
        let length = usize::try_from(integer(&row, "value_bytes")?)?;
        let tag_bytes = usize::try_from(integer(&row, "tag_bytes")?)?;
        let metadata_bytes =
            usize::try_from(optional_integer(&row, "metadata_bytes")?.unwrap_or(0))?;
        let cost = length
            .saturating_mul(4)
            .saturating_add(tag_bytes.saturating_mul(6))
            .saturating_add(metadata_bytes.saturating_mul(6));
        let Some(next) = remaining.checked_sub(cost) else {
            return Ok(None);
        };
        remaining = next;
        let value = storage
            .system_sql_read(
                "SELECT value,tag,metadata,expires_at,revision FROM __sys_kv WHERE name=?",
                &[key.clone().into()],
            )?
            .into_iter()
            .next()
            .ok_or(CommandFailure::CorruptState)?;
        let Some(SqlValue::Blob(bytes)) = value.get("value") else {
            bail!(CommandFailure::CorruptState)
        };
        ensure!(
            bytes.len() == length
                && text(&value, "tag")?.len() == tag_bytes
                && optional_text(&value, "metadata")?.map_or(0, |metadata| metadata.len())
                    == metadata_bytes,
            CommandFailure::CorruptState
        );
        let revision = format!(
            "cg1:{}:{}",
            hex::encode(history.incarnation.bytes()),
            integer(&value, "revision")?
        );
        rows.push(entry(&key, &value, revision, metadata, expiration)?);
        values.push(Some((bytes.clone(), true)));
    }
    Ok(Some(graph(rows, values)?))
}

pub(super) fn entry(
    key: &str,
    row: &SqlRow,
    revision: String,
    metadata: bool,
    expiration: bool,
) -> Result<Value> {
    let mut entry =
        json!({"key":key,"found":true,"tag":text(row,"tag")?,"value":null,"revision":revision});
    if metadata {
        entry["metadata"] = json!(optional_text(row, "metadata")?);
    }
    if expiration {
        entry["expiration"] = json!(optional_integer(row, "expires_at")?);
    }
    Ok(entry)
}

pub(super) fn graph(
    rows: Vec<Value>,
    values: Vec<Option<(Vec<u8>, bool)>>,
) -> Result<StructuredValue> {
    let mut graph = StructuredValue::from_json(Value::Array(rows))?;
    let rows = wire::array(&graph, &graph.root)?.to_vec();
    for (row, bytes) in rows.iter().zip(values) {
        if let Some((bytes, inline)) = bytes {
            if inline {
                wire::set_buffer(&mut graph, row, "value", bytes)?;
            } else {
                wire::set_bytes(&mut graph, row, "value", bytes)?;
            }
        }
    }
    Ok(graph)
}
