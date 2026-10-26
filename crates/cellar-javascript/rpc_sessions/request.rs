// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Request arguments are validated before the native RPC route has effects.
use super::http_metadata::Head;
use anyhow::{ensure, Result};
use cellar::native::{
    clone::{StructuredValue, Value},
    web::Url,
};

pub(super) fn validate(graph: &StructuredValue, value: &Value) -> Result<()> {
    let head = Head::new(
        graph,
        value,
        &[
            "method",
            "url",
            "headers",
            "body",
            "redirect",
            "cache",
            "credentials",
            "integrity",
            "keepalive",
            "mode",
            "referrer",
            "referrerPolicy",
            "destination",
        ],
    )?;
    let method = head.text("method", 64)?;
    ensure!(
        !method.is_empty()
            && method
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)),
        "invalid RPC request method"
    );
    ensure!(
        !["CONNECT", "TRACE", "TRACK"]
            .iter()
            .any(|forbidden| method.eq_ignore_ascii_case(forbidden)),
        "forbidden RPC request method"
    );
    let url = Url::new(&head.text("url", 8192)?, None)?;
    ensure!(
        matches!(url.protocol().as_str(), "http:" | "https:")
            && !url.host().is_empty()
            && url.username().is_empty()
            && url.password().is_empty(),
        "RPC requests require an HTTP URL without credentials"
    );
    let body = head.boolean("body")?;
    ensure!(
        !body
            || !["GET", "HEAD"]
                .iter()
                .any(|m| method.eq_ignore_ascii_case(m)),
        "bodyless RPC request method contains a body"
    );
    ensure!(
        !head.boolean("keepalive")? || !body,
        "streamed RPC request cannot use keepalive"
    );
    let mode = head.text("mode", 16)?;
    ensure!(
        matches!(mode.as_str(), "cors" | "no-cors" | "same-origin"),
        "unsupported RPC request mode"
    );
    ensure!(
        mode != "no-cors" || ["GET", "HEAD", "POST"].contains(&method.as_str()),
        "invalid no-cors RPC request method"
    );
    let cache = head.text("cache", 16)?;
    ensure!(
        matches!(
            cache.as_str(),
            "default" | "no-store" | "reload" | "no-cache" | "force-cache" | "only-if-cached"
        ),
        "invalid RPC request cache mode"
    );
    ensure!(
        cache != "only-if-cached" || mode == "same-origin",
        "invalid RPC request cache policy"
    );
    ensure!(
        matches!(
            head.text("credentials", 16)?.as_str(),
            "omit" | "same-origin" | "include"
        ),
        "invalid RPC request credentials"
    );
    ensure!(
        matches!(
            head.text("redirect", 16)?.as_str(),
            "error" | "follow" | "manual"
        ),
        "invalid RPC request redirect mode"
    );
    ensure!(
        matches!(
            head.text("referrerPolicy", 32)?.as_str(),
            "" | "no-referrer"
                | "no-referrer-when-downgrade"
                | "origin"
                | "origin-when-cross-origin"
                | "same-origin"
                | "strict-origin"
                | "strict-origin-when-cross-origin"
                | "unsafe-url"
        ),
        "invalid RPC request referrer policy"
    );
    let referrer = head.text("referrer", 8192)?;
    if !referrer.is_empty() && referrer != "about:client" {
        Url::new(&referrer, None)?;
    }
    head.text("destination", 64)?;
    head.text("integrity", 8192)?;
    head.headers()?;
    Ok(())
}
