// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::Result;
use cellar::sdk::Host;
use cellar_python::{EventKind, Options, PythonRuntime, Source};

fn main() -> Result<()> {
    // SAFETY: this main thread owns CPython initialization; no Python code or
    // threads have run before entering the managed runtime.
    let report = unsafe {
        PythonRuntime::run(
            Options::default(),
            Source::new(include_str!("app.py"), "app.py")
                .events([EventKind::Fetch])
                .cell_events(
                    "Room",
                    [
                        EventKind::Fetch,
                        EventKind::WebSocketOpen,
                        EventKind::WebSocketMessage,
                        EventKind::WebSocketClose,
                    ],
                ),
            |application, monitor| {
                let stopped = monitor.clone();
                Host::fleet()
                    .register("streaming-example", "v1", move || Ok(application.clone()))?
                    .run_until_observed(stopped.cancelled(), move |outcome| {
                        monitor.record_node(outcome)
                    })
            },
        )
    }?;
    anyhow::ensure!(report.complete(), "Python example shutdown was incomplete");
    Ok(())
}
