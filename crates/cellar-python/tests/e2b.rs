// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Standalone interpreter test: CPython initialization belongs to process main.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use cellar_python::{EventKind, Options, PythonRuntime, Source};
#[path = "../../cellar-container/tests/support/provider_language.rs"]
mod acceptance;
#[path = "../../cellar-container/tests/support/e2b.rs"]
mod peer;

fn main() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let peer = runtime.block_on(peer::Peer::start())?;
    let source = Source::new(include_str!("support/provider.py"), "e2b.py")
        .events([EventKind::Fetch])
        .cell("Counter")
        .container_provider(peer.provider()?)
        .container_binding("backup", peer.provider_binding("backup")?)?;
    // SAFETY: this standalone main owns interpreter initialization and calls
    // no foreign Python APIs. Runtime shutdown joins all native guest work.
    let report = unsafe {
        PythonRuntime::run(Options::default(), source, move |application, _monitor| {
            runtime.block_on(acceptance::exercise(application, peer, "e2b", 5))
        })
    }?;
    ensure!(report.complete());
    Ok(())
}
