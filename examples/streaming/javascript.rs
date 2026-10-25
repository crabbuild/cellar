// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::Result;
use cellar::sdk::Host;
use cellar_javascript::{EventKind, JavaScriptRuntime, Options, Source};

fn main() -> Result<()> {
    JavaScriptRuntime::run(
        Options::default(),
        Source::new(include_str!("app.bundle.js"), "app.ts")
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
        |application| {
            let stop = application.clone();
            Host::fleet()
                .register("streaming-example", "v1", move || Ok(application.clone()))?
                .run_until(async move { stop.stopped().await })
        },
    )
}
