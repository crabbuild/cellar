// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::sdk::Request;
use futures_util::StreamExt;
use serde_json::{json, Value};

pub fn input(value: &Value, limit: usize) -> Result<Request> {
    let bytes = STANDARD.decode(field(value, "body")?)?;
    input_body(value, bytes, limit)
}

/// Apply the shared request limits to already decoded, owned body bytes.
pub fn input_body(value: &Value, bytes: Vec<u8>, limit: usize) -> Result<Request> {
    ensure!(bytes.len() <= limit, "container request body exceeds limit");
    let request_headers: Vec<(String, String)> = serde_json::from_value(value["headers"].clone())?;
    headers(&request_headers)?;
    Ok(Request {
        url: field(value, "url")?.into(),
        method: field(value, "method")?.into(),
        headers: request_headers,
        body: bytes.into(),
        entrypoint: None,
    })
}

pub async fn response(response: cellar::engine_api::HttpResponse, limit: usize) -> Result<Value> {
    let (mut metadata, bytes) = response_parts(response, limit).await?;
    metadata["body"] = json!(STANDARD.encode(bytes));
    Ok(metadata)
}

/// Collect a bounded response while preserving raw bytes for binary transports.
pub async fn response_parts(
    mut response: cellar::engine_api::HttpResponse,
    limit: usize,
) -> Result<(Value, Vec<u8>)> {
    // Refuse known-invalid replies before polling a potentially pending stream.
    // These are response failures: the underlying mutation may have committed.
    ensure!(
        response.websocket.is_none(),
        "guest adapter does not support WebSocket upgrades"
    );
    ensure!(
        response.body.len() <= limit,
        "guest bridged response exceeds limit"
    );
    headers(&response.headers)?;
    if let Some(mut stream) = response.stream.take() {
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(anyhow::Error::msg)?;
            ensure!(
                response.body.len().saturating_add(chunk.len()) <= limit,
                "guest bridged response exceeds limit"
            );
            response.body.extend(chunk);
        }
    }
    Ok((
        json!({"status":response.status,"headers":response.headers,"body":""}),
        response.body,
    ))
}

pub(crate) fn headers(headers: &[(String, String)]) -> Result<()> {
    ensure!(headers.len() <= 128, "too many guest HTTP headers");
    let bytes = headers.iter().fold(0usize, |bytes, (key, value)| {
        bytes.saturating_add(key.len()).saturating_add(value.len())
    });
    ensure!(bytes <= 32768, "guest HTTP headers exceed byte budget");
    Ok(())
}

pub fn field<'a>(value: &'a Value, name: &str) -> Result<&'a str> {
    value[name]
        .as_str()
        .with_context(|| format!("{name} must be a string"))
}

/// Match the facade's depth limit at the native effect boundary too. The JSON
/// parser's own recursion bound is deliberately not this SDK value contract.
pub fn check_depth(value: &Value) -> Result<()> {
    fn visit(value: &Value, depth: usize) -> Result<()> {
        ensure!(depth <= 64, "guest SDK value exceeds nesting limit");
        match value {
            Value::Array(values) => {
                for value in values {
                    visit(value, depth + 1)?;
                }
            }
            Value::Object(values) => {
                for value in values.values() {
                    visit(value, depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    visit(value, 0)
}
