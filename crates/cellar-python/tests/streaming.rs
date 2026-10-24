// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::Result;
use cellar::sdk::{NodeOutcome, ShutdownMode};
use cellar_python::{EventKind, Options, PythonRuntime, Source};
#[path = "../../cellar-javascript/tests/support/native.rs"]
mod native;
#[path = "../../cellar/tests/support/language_streaming.rs"]
mod streaming;

fn main() -> Result<()> {
    let source = Source::new(include_str!("streaming.py"), "streaming.py")
        .events([EventKind::Fetch])
        .cell_events(
            "Counter",
            [
                EventKind::Fetch,
                EventKind::WebSocketOpen,
                EventKind::WebSocketMessage,
                EventKind::WebSocketClose,
            ],
        );
    // SAFETY: this standalone test initializes CPython once on its main thread.
    let report = unsafe {
        PythonRuntime::run(
            Options {
                max_body_bytes: 4096,
                ..Default::default()
            },
            source,
            |application, monitor| {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?
                    .block_on(async {
                        let directory = tempfile::tempdir()?;
                        let (node, address) = native::start(directory.path(), application).await?;
                        let result = streaming::check(&address).await;
                        let outcome = node.shutdown(ShutdownMode::Preserve).await?;
                        monitor.record_node(outcome.clone());
                        let NodeOutcome::Stopped(report) = outcome else {
                            anyhow::bail!("node failed")
                        };
                        anyhow::ensure!(
                            report.application_work_drained && !report.drain_timed_out,
                            "streaming work leaked: {report:?}"
                        );
                        result
                    })
            },
        )
    }?;
    anyhow::ensure!(report.complete(), "Python streaming teardown incomplete");
    Ok(())
}
