// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded response metadata accompanies a real native RPC stream lease.
use anyhow::{bail, ensure, Context as _, Result};
use cellar::native::clone::{Object, StructuredValue, Value};
use std::collections::BTreeMap;

fn object<'a>(graph: &'a StructuredValue, value: &Value) -> Result<&'a Object> {
    let Value::Reference(id) = value else {
        bail!("RPC response metadata requires an object")
    };
    graph
        .objects
        .get(*id as usize)
        .context("invalid response reference")
}

fn array<'a>(graph: &'a StructuredValue, value: &Value) -> Result<&'a [Value]> {
    match object(graph, value)? {
        Object::DenseArray {
            elements,
            properties,
        } if properties.is_empty() => Ok(elements),
        _ => bail!("RPC response metadata requires a dense array"),
    }
}

fn string(value: &Value, maximum: usize) -> Result<String> {
    let Value::String(value) = value else {
        bail!("RPC response metadata requires a string")
    };
    ensure!(
        value.len() <= maximum,
        "RPC response metadata exceeds limit"
    );
    let value = String::from_utf16(value)?;
    ensure!(
        value.len() <= maximum,
        "RPC response metadata exceeds limit"
    );
    Ok(value)
}

pub(super) fn validate(graph: &StructuredValue, value: &Value) -> Result<()> {
    let Object::Object(properties) = object(graph, value)? else {
        bail!("invalid RPC response marker")
    };
    ensure!(
        properties.len() == 7,
        "invalid RPC response metadata fields"
    );
    let mut fields = BTreeMap::new();
    for (key, value) in properties {
        ensure!(
            fields.insert(string(key, 16)?, value).is_none(),
            "duplicate RPC response field"
        );
    }
    let field = |name: &str| {
        fields
            .get(name)
            .copied()
            .context("missing RPC response field")
    };
    let Value::Number(status) = field("status")? else {
        bail!("invalid RPC response status")
    };
    let Value::Boolean(body) = field("body")? else {
        bail!("invalid RPC response body flag")
    };
    let status_text = string(field("statusText")?, 512)?;
    ensure!(
        status_text
            .chars()
            .all(|c| c <= '\u{ff}' && c != '\r' && c != '\n'),
        "invalid RPC response status text"
    );
    string(field("url")?, 8192)?;
    ensure!(
        matches!(field("redirected")?, Value::Boolean(_)),
        "invalid RPC redirect flag"
    );
    let kind = string(field("type")?, 16)?;
    let headers = array(graph, field("headers")?)?;
    ensure!(headers.len() <= 128, "RPC response headers exceed limit");
    let checked = cellar::sdk::http::Headers::new();
    let mut bytes = 0;
    for header in headers {
        let pair = array(graph, header)?;
        ensure!(pair.len() == 2, "invalid RPC response header pair");
        let name = string(&pair[0], 32768)?;
        let value = string(&pair[1], 32768)?;
        bytes += name.len() + value.len();
        ensure!(bytes <= 32768, "RPC response headers exceed limit");
        checked.append(&name, &value)?;
    }
    if kind == "error" {
        ensure!(
            *status == 0.0 && !body && headers.is_empty() && status_text.is_empty(),
            "invalid RPC error response"
        );
    } else {
        ensure!(
            matches!(kind.as_str(), "default" | "basic" | "cors"),
            "unsupported RPC response type"
        );
        ensure!(
            status.fract() == 0.0 && (200.0..=599.0).contains(status),
            "invalid RPC response status"
        );
        ensure!(
            !body || ![204.0, 205.0, 304.0].contains(status),
            "bodyless RPC response contains a body"
        );
    }
    Ok(())
}
