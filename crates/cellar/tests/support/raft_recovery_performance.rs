// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Host-lifetime observations beside the controller's recovery clock. Keep
//! closed handles so removing a voter cannot silently remove its final work.

use anyhow::{ensure, Result};
use cellar::sdk::consensus::RaftCells;
use std::collections::BTreeMap;

pub(super) const PHASES: &[&str] = &[
    "before_fault",
    "seed_started",
    "seed_installed",
    "first_write_verified",
    "payloads_verified",
    "before_cleanup",
    "after_cleanup",
];

#[derive(Default)]
pub(super) struct Sampler {
    hosts: BTreeMap<String, RaftCells>,
}

impl Sampler {
    pub fn capture(&mut self, services: &[RaftCells]) -> Result<serde_json::Value> {
        let mut introduced = Vec::new();
        for service in services {
            let source = service.performance().source;
            if let std::collections::btree_map::Entry::Vacant(entry) = self.hosts.entry(source) {
                introduced.push(entry.key().clone());
                entry.insert(service.clone());
            }
        }
        let mut snapshots = Vec::with_capacity(self.hosts.len());
        for (source, service) in &self.hosts {
            let snapshot = service.performance();
            ensure!(
                snapshot.source == *source,
                "performance host identity changed"
            );
            snapshots.push(snapshot);
        }
        introduced.sort();
        Ok(serde_json::json!({
            "new_sources": introduced,
            "hosts": snapshots,
        }))
    }
}
