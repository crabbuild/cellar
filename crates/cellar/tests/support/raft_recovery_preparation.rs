// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Retire the recovery fixture's private prepared input after every initial
//! native marker is synced and its physical preload owner has joined.

use super::cluster::PreloadReplicas;
use anyhow::{Context as _, Result};
use cellar::sdk::ExecutionDomain;
use std::path::PathBuf;

#[cfg(test)]
#[path = "raft_recovery_preparation_tests.rs"]
mod tests;

pub(super) fn retire_after_preload(template: PathBuf, preload: PreloadReplicas) -> PreloadReplicas {
    Box::new(move |logs, store| {
        Box::pin(async move {
            // A failed or unfinished native preload still owns its input.
            // The caller retains it for diagnosis under its existing policy.
            preload(logs, store).await?;
            let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
            let result: Result<()> = match domain.blocking(move || std::fs::remove_file(template)) {
                Ok(task) => task
                    .await
                    .context("join prepared-input retirement")
                    .and_then(|result| result.context("retire prepared recovery input")),
                Err(error) => Err(error.into()),
            };
            domain.shutdown().await;
            // Refuse startup if retirement failed. Replica runtimes have not
            // started yet, so no archive publisher overlaps the full template.
            result
        })
    })
}
