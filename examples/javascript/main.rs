// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::Result;
use cellar::sdk::Host;
use cellar_javascript::{EventKind, JavaScriptRuntime, Options, Source};

const APPLICATION_VERSION: &str = match option_env!("CELLAR_APPLICATION_VERSION") {
    Some(version) => version,
    None => "v1",
};

fn main() -> Result<()> {
    JavaScriptRuntime::run(
        Options::default(),
        Source::new(include_str!("app.bundle.js"), "app.ts")
            .events([EventKind::Fetch])
            .cell_events("Counter", [EventKind::Fetch, EventKind::Alarm]),
        |application| {
            let stop = application.clone();
            Host::fleet()
                .register("javascript-example", APPLICATION_VERSION, move || {
                    Ok(application.clone())
                })?
                .run_until(async move { stop.stopped().await })
        },
    )
}
