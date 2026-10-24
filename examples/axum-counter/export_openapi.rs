// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Offline CLI boundary. Builds declarations without a node or listener.
// File export is process-owned CLI I/O, outside any node execution domain.
#![allow(clippy::disallowed_methods)]

fn main() -> anyhow::Result<()> {
    let json = cellar_axum_counter::definition()?.export_openapi()?;
    match std::env::args().nth(1) {
        Some(path) => std::fs::write(path, json)?,
        None => cellar::cli_output::Output::new(cellar::cli_output::Format::Text)
            .bytes(json.as_bytes())?,
    }
    Ok(())
}
