// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![cfg(any(feature = "node", feature = "embed"))]
#![allow(clippy::disallowed_methods)]
use anyhow::Result;
use cellar_javascript::{EventKind, Options, Source};
use std::{sync::Arc, time::Duration};
#[path = "../../cellar-container/tests/support/named_language.rs"]
mod qualification;

fn source() -> Result<Source> {
    let code = include_str!("support/sandboxes.mjs").replace("export ", "");
    Source::new(
        format!("var CellarApp = (() => {{{code}; return {{cells, fetch}}; }})();"),
        "sandboxes.mjs",
    )
    .events([EventKind::Fetch])
    .cell("Counter")
    .sandbox_executor(
        "Sandbox",
        Arc::new(cellar_container::ChildExecutor::new(
            Arc::new(cellar_container::Native),
            "c".repeat(64),
        )?),
    )
}

#[cfg(feature = "node")]
#[test]
fn node_named_sandbox_allocation() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let entry = directory.path().join("sandboxes.mjs");
    std::fs::write(&entry, include_str!("support/sandboxes.mjs"))?;
    cellar_javascript::NodeRuntime::run(
        Options {
            turn_timeout: Duration::from_secs(2),
            ..Default::default()
        },
        cellar_javascript::NodeOptions::new(entry),
        source()?,
        |application| qualification::exercise(application),
    )
}

#[cfg(feature = "embed")]
#[test]
fn embedded_named_sandbox_allocation() -> Result<()> {
    cellar_javascript::JavaScriptRuntime::run(
        Options {
            turn_timeout: Duration::from_secs(2),
            ..Default::default()
        },
        source()?,
        |application| qualification::exercise(application),
    )
}
