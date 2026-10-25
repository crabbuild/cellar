// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded value-only RPC translation. Capability delegates remain refused.
use anyhow::{ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::{engine_api::RpcData, native::clone};
use serde::Deserialize;
use serde_json::{json, Value};

pub(super) fn limits(bytes: usize) -> clone::Limits {
    clone::Limits {
        bytes,
        objects: 100_000,
        values: 200_000,
        depth: 64,
    }
}

pub(super) fn arguments(encoded: &str, maximum: usize) -> Result<clone::StructuredValue> {
    let bytes = STANDARD.decode(encoded)?;
    let value = clone::decode_with_limits(&bytes, limits(maximum))?;
    array(&value)?;
    Ok(value)
}

fn array(value: &clone::StructuredValue) -> Result<()> {
    let clone::Value::Reference(root) = value.root else {
        anyhow::bail!("RPC arguments must be an array");
    };
    ensure!(
        matches!(
            value.objects.get(root as usize),
            Some(clone::Object::DenseArray { .. } | clone::Object::SparseArray { .. })
        ),
        "RPC arguments must be an array"
    );
    Ok(())
}

pub(super) fn encoded(value: &clone::StructuredValue, maximum: usize) -> Result<String> {
    Ok(STANDARD.encode(clone::encode_with_limits(value, limits(maximum))?))
}

/// Translate the actual native CellRpc event, preserving its clone graph.
pub fn request(method: String, args: RpcData, maximum: usize) -> Result<Value> {
    let value = match args {
        RpcData::Json(json) => {
            ensure!(json.len() <= maximum, "RPC arguments exceed limit");
            clone::StructuredValue::from_json(serde_json::from_str(&json)?)?
        }
        RpcData::Structured(bytes) => clone::decode_with_limits(&bytes, limits(maximum))?,
    };
    array(&value)?;
    Ok(json!({"method":method, "args":encoded(&value, maximum)?}))
}

/// Validate a completion after execution. Failure here never proves rollback.
pub fn output(value: Value, maximum: usize) -> Result<RpcData> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Completion {
        value: String,
        error: bool,
    }
    let completion: Completion = serde_json::from_value(value)?;
    let bytes = STANDARD.decode(completion.value)?;
    let mut graph = clone::decode_with_limits(&bytes, limits(maximum))?;
    if completion.error {
        let clone::Value::Reference(root) = graph.root else {
            anyhow::bail!("invalid RPC error envelope")
        };
        ensure!(
            matches!(graph.objects.get(root as usize), Some(clone::Object::DenseArray { elements, .. }) if elements.len() == 2),
            "invalid RPC error envelope"
        );
        for object in &mut graph.objects {
            if let clone::Object::Error { stack, .. } = object {
                *stack = None;
            }
        }
        let bytes = clone::encode_with_limits(&graph, limits(maximum.saturating_sub(1)))?;
        let mut result = Vec::with_capacity(bytes.len() + 1);
        result.push(0);
        result.extend(bytes);
        Ok(RpcData::Structured(result.into()))
    } else {
        Ok(RpcData::Structured(bytes.into()))
    }
}

#[cfg(test)]
#[path = "rpc_tests.rs"]
mod tests;
