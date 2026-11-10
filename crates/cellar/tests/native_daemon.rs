// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Process-level native ingress, routing, bucket restore and reload coverage.
#![allow(clippy::disallowed_methods)]

#[path = "support/native_daemon_counter.rs"]
pub mod counter;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compiled_daemon_routes_persists_reloads_and_hands_off() -> anyhow::Result<()> {
    Box::pin(counter::run(counter::DurabilityMode::Bucket)).await
}
