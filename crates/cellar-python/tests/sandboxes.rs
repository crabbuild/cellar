// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! CPython initialization belongs to the main thread of a dedicated process.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use cellar_python::{EventKind, Options, PythonRuntime, Source};
use std::sync::Arc;
#[path = "../../cellar-container/tests/support/named_language.rs"]
mod qualification;

fn main() -> Result<()> {
    let source = Source::new(include_str!("support/sandboxes.py"), "sandboxes.py")
        .events([EventKind::Fetch])
        .cell("Counter")
        .sandbox_executor(
            "Sandbox",
            Arc::new(cellar_container::ChildExecutor::new(
                Arc::new(cellar_container::Native),
                "c".repeat(64),
            )?),
        )?;
    // SAFETY: this main owns interpreter initialization; the runtime joins all
    // native work before returning and no foreign Python API is called here.
    let report = unsafe {
        PythonRuntime::run(Options::default(), source, |application, _| {
            qualification::exercise(application)
        })
    }?;
    ensure!(report.complete());
    Ok(())
}
