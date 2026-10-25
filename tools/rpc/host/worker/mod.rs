// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Disposable engines fail independently of every surviving RPC caller.
#[cfg(unix)]
mod application;
#[cfg(unix)]
mod backend;
#[cfg(unix)]
mod matrix;
#[cfg(unix)]
mod trace;

#[cfg(unix)]
pub use matrix::{
    lifecycle, run, shutdown, shutdown_smoke, smoke, v8_shutdown, v8_shutdown_smoke,
    v8_termination, v8_termination_smoke,
};

#[cfg(not(unix))]
pub async fn run(
    _: std::sync::Arc<crate::application::Composite>,
) -> anyhow::Result<serde_json::Value> {
    anyhow::bail!("owned engine lifecycle qualification requires Unix")
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

#[cfg(not(unix))]
pub use run as v8_termination;
#[cfg(not(unix))]
pub use run as v8_shutdown;
#[cfg(not(unix))]
pub use run as lifecycle;
#[cfg(not(unix))]
pub use smoke as v8_termination_smoke;
#[cfg(not(unix))]
pub use smoke as v8_shutdown_smoke;
