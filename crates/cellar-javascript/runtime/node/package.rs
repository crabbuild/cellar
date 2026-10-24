// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::NodeOptions;
use anyhow::{ensure, Result};
use sha2::{Digest, Sha256};

impl NodeOptions {
    /// Verify the build-bound runtime closure before Node starts. Pair the
    /// returned SHA-256 with `Host::runtime_inventory(FEATURE_NODE_V1, hash)`.
    /// None means Cargo was not given an explicit Node environment project.
    pub fn packaged() -> Result<Option<(Self, String)>> {
        let raw = include_bytes!(concat!(env!("OUT_DIR"), "/node_inventory.json"));
        if raw.is_empty() {
            return Ok(None);
        }
        let inventory: cellar::protocol::NodeInventory = serde_json::from_slice(raw)?;
        ensure!(
            inventory.target == cellar::native_deploy::current_target(),
            "compiled Node inventory target mismatch"
        );
        // Verify the build-bound executable and package files before any import.
        cellar::native_deploy::verify_node_inventory_files(&inventory)?;
        let mut options = Self::new(inventory.entry);
        options.executable = inventory.executable;
        options.working_directory = Some(inventory.working_directory);
        Ok(Some((options, format!("{:x}", Sha256::digest(raw)))))
    }
}
