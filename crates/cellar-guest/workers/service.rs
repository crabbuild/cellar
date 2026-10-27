// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Host-minted service and loopback routes. Guest data never selects a script.
use super::rpc;
use crate::{failure::Validation, values, Limits};
use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::{engine_api::RpcData, native::clone, sdk::Context, WorkerRpcOperation};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Target {
    Binding { binding: String },
    Export { name: String, props: Option<String> },
}

fn name(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty() && value.len() <= 512 && !value.contains('\0'),
        "invalid service name"
    );
    Ok(())
}

fn graph(bytes: &[u8], maximum: usize) -> Result<clone::StructuredValue> {
    clone::decode_with_limits(bytes, rpc::limits(maximum))
}

fn resolve(
    context: &Context,
    target: Target,
    maximum: usize,
) -> Result<cellar::sdk::service::Fetcher> {
    match target {
        Target::Binding { binding } => {
            name(&binding)?;
            context.service(&binding)
        }
        Target::Export {
            name: export,
            props,
        } => {
            name(&export)?;
            let route = context.exported_entrypoint(export)?;
            match props {
                Some(props) => route.with_props(&graph(&STANDARD.decode(props)?, maximum)?),
                None => Ok(route),
            }
        }
    }
}

pub(super) async fn fetch_response(
    context: &Context,
    input: Value,
    limits: &Limits,
) -> Result<cellar::engine_api::HttpResponse> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Input {
        service: Target,
        request: Value,
    }
    let (route, request) = (|| -> Result<_> {
        let input: Input = serde_json::from_value(input)?;
        Ok((
            resolve(context, input.service, limits.max_body_bytes)?,
            values::input(&input.request, limits.max_body_bytes)?,
        ))
    })()
    .context(Validation)?;
    route.fetch(context, request).await
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Operation {
    Get { path: Vec<String> },
    Call { path: Vec<String>, args: String },
}

fn path(parts: &[String]) -> Result<()> {
    ensure!(
        !parts.is_empty() && parts.len() <= 64,
        "invalid service RPC path"
    );
    for part in parts {
        name(part)?;
    }
    Ok(())
}

pub(super) async fn call(context: &Context, input: Value, limits: &Limits) -> Result<Value> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Input {
        service: Target,
        operation: Operation,
    }
    let (route, operation) = (|| -> Result<_> {
        let input: Input = serde_json::from_value(input)?;
        let route = resolve(context, input.service, limits.max_body_bytes)?;
        let operation = match input.operation {
            Operation::Get { path: parts } => {
                path(&parts)?;
                WorkerRpcOperation::Get { path: parts }
            }
            Operation::Call { path: parts, args } => {
                path(&parts)?;
                let args = rpc::arguments(&args, limits.max_body_bytes)?;
                WorkerRpcOperation::Call {
                    path: parts,
                    args: clone::encode_with_limits(&args, rpc::limits(limits.max_body_bytes))?,
                }
            }
        };
        Ok((route, operation))
    })()
    .context(Validation)?;
    let bytes = route.rpc(context, operation).await?;
    // Conversion follows execution: an invalid result is never a rollback proof.
    super::namespace::reply(&RpcData::Structured(bytes.into()), limits.max_body_bytes)
}

/// Validate native props with the guest's clone limits. An absent native value
/// stays absent, distinct from an explicitly encoded null or undefined.
pub fn props(bytes: &[u8], maximum: usize) -> Result<Option<String>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    rpc::encoded(&graph(bytes, maximum)?, maximum).map(Some)
}

/// Encode a native entrypoint RPC without executing it or weakening its limits.
pub fn request(
    entrypoint: String,
    operation: WorkerRpcOperation,
    properties: Vec<u8>,
    maximum: usize,
) -> Result<Value> {
    name(&entrypoint)?;
    let operation = match operation {
        WorkerRpcOperation::Get { path: parts } => {
            path(&parts)?;
            json!({"kind":"get", "path":parts})
        }
        WorkerRpcOperation::Call { path: parts, args } => {
            path(&parts)?;
            let encoded = STANDARD.encode(args);
            rpc::arguments(&encoded, maximum)?;
            json!({"kind":"call", "path":parts, "args":encoded})
        }
    };
    Ok(
        json!({"entrypoint":entrypoint, "props":props(&properties, maximum)?, "operation":operation}),
    )
}

/// Validate the executed result, preserving the native remote-error envelope
/// and stack stripping. Conversion failure cannot prove rollback.
pub fn output(value: Value, maximum: usize) -> Result<Vec<u8>> {
    let RpcData::Structured(bytes) = rpc::output(value, maximum)? else {
        anyhow::bail!("invalid Worker RPC completion");
    };
    Ok(bytes.to_vec())
}
