// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Response metadata accompanies a real native RPC stream lease.
use super::http_metadata::Head;
use anyhow::{bail, ensure, Result};
use cellar::native::clone::{StructuredValue, Value};

pub(super) fn validate(graph: &StructuredValue, value: &Value) -> Result<()> {
    let head = Head::new(
        graph,
        value,
        &[
            "status",
            "statusText",
            "headers",
            "body",
            "url",
            "redirected",
            "type",
        ],
    )?;
    let Value::Number(status) = head.field("status")? else {
        bail!("invalid RPC response status")
    };
    let body = head.boolean("body")?;
    let status_text = head.text("statusText", 512)?;
    ensure!(
        status_text
            .chars()
            .all(|c| c == '\t' || ('\u{20}'..='\u{ff}').contains(&c) && c != '\u{7f}'),
        "invalid RPC response status text"
    );
    head.text("url", 8192)?;
    head.boolean("redirected")?;
    let kind = head.text("type", 16)?;
    let headers = head.headers()?;
    if kind == "error" {
        ensure!(
            *status == 0.0 && !body && headers == 0 && status_text.is_empty(),
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
