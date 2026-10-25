// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{Context as _, Result};
use cellar::sdk::Host;
use cellar_javascript::{EventKind, NodeOptions, NodeRuntime, Options, Source};

fn main() -> Result<()> {
    let entry = std::env::var_os("CELLAR_NODE_ENTRY")
        .context("set CELLAR_NODE_ENTRY to the absolute app.js or dist/app.js path")?;
    let mut node = NodeOptions::new(std::path::PathBuf::from(entry));
    if let Some(executable) = std::env::var_os("CELLAR_NODE_EXECUTABLE") {
        node.executable = executable.into();
    }
    NodeRuntime::run(
        Options::default(),
        node,
        Source::module().events([EventKind::Fetch]).cell_events(
            "Room",
            [
                EventKind::Fetch,
                EventKind::WebSocketOpen,
                EventKind::WebSocketMessage,
                EventKind::WebSocketClose,
            ],
        ),
        |application| {
            let stop = application.clone();
            Host::fleet()
                .register("streaming-example", "v1", move || Ok(application.clone()))?
                .run_until(async move { stop.stopped().await })
        },
    )
}
