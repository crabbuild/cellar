// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Disposable Node workers fail independently of every surviving RPC caller.
#[cfg(unix)]
mod application;
#[cfg(unix)]
mod matrix;
#[cfg(unix)]
mod trace;

#[cfg(unix)]
pub use matrix::{run, shutdown, shutdown_smoke, smoke};

#[cfg(not(unix))]
pub async fn run(
    _: std::sync::Arc<crate::application::Composite>,
) -> anyhow::Result<serde_json::Value> {
    anyhow::bail!("owned Node worker failure qualification requires Unix")
}

#[cfg(not(unix))]
pub async fn smoke(
    base: std::sync::Arc<crate::application::Composite>,
) -> anyhow::Result<serde_json::Value> {
    run(base).await
}

#[cfg(not(unix))]
pub use run as shutdown;
#[cfg(not(unix))]
pub use smoke as shutdown_smoke;
