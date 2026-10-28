// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Filesystem metrics use a signed read-only RPC to the current cell owner.
use super::*;
pub use crate::filesystem_state::{FilesystemQuotas, FilesystemState, FilesystemUsage};

impl FleetClient {
    pub async fn filesystem(&self, scope: &str) -> Result<FilesystemState> {
        ensure!(
            cellar_logic::cell::valid_cell_scope(scope) && scope.contains(':'),
            "invalid filesystem observation scope"
        );
        let result = Reachable::from_entrances_route(
            &self.bucket,
            Subject {
                noun: "cell filesystem",
                source: "dashboard",
                timeout: Duration::from_secs(30),
            },
            scope.into(),
            None,
            self.routing_entrances(scope).await?,
            "filesystem-state",
        )
        .await?
        .call(json!({}))
        .await?;
        let sample: FilesystemState = serde_json::from_value(result)?;
        sample.validate(scope)?;
        Ok(sample)
    }
}
