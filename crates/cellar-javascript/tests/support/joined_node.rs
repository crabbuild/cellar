// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Final bridge accounting belongs after the owning process/transport has joined.
use anyhow::{ensure, Context as _, Result};
use cellar_javascript::{JavaScriptApplication, NodeOptions, NodeRuntime, Options, Source};
use std::sync::Arc;

pub fn run<T>(
    options: Options,
    node: NodeOptions,
    source: Source,
    host: impl FnOnce(Arc<JavaScriptApplication>) -> Result<T>,
) -> Result<T> {
    let mut retained = None;
    let result = NodeRuntime::run(options, node, source, |application| {
        // Keep observations alive after the runtime's stop/join. Native host
        // drain alone can still leave bounded retirement frames with the live
        // transport; each fixture checks native invocation drain inside `host`.
        retained = Some(application.clone());
        host(application)
    });
    let usage = retained
        .as_ref()
        .map(|application| application.bridge_usage());
    let value = result.with_context(|| format!("joined Node runtime usage={usage:?}"))?;
    let usage = usage.context("Node runtime returned without admitting its host")?;
    ensure!(
        usage.stopped && usage.active_invocations == 0 && usage.retained_bytes == 0,
        "joined Node runtime retained bridge resources: {usage:?}"
    );
    Ok(value)
}
