// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit automatic-placement policy. Runtime integration belongs to groups.
use super::RaftHostOptions;
use cellar_logic::{raft_leadership::LeaderPolicy, raft_placement::PlacementPolicy};

const PROBE_MEMORY: usize = 1024 * 1024;

#[derive(Clone, Debug)]
pub struct RaftControllerOptions {
    pub placement: PlacementPolicy,
    pub leadership: Option<LeaderPolicy>,
    pub start_paused: bool,
    pub interval_ms: u64,
    pub probe_timeout_ms: u64,
    pub operation_timeout_ms: u64,
    pub seed_timeout_ms: u64,
    pub unavailable_after_ms: u64,
    pub learner_timeout_ms: u64,
    /// Includes the local sample and all current voters/active learner.
    pub probes_per_group: usize,
    pub probe_concurrency: usize,
    /// Retained decoded observations, separate from transport peak memory.
    pub metadata_bytes: usize,
}

impl RaftControllerOptions {
    pub fn new(placement: PlacementPolicy) -> Self {
        Self {
            placement,
            leadership: None,
            start_paused: false,
            interval_ms: 100,
            probe_timeout_ms: 500,
            operation_timeout_ms: 5000,
            seed_timeout_ms: 30_000,
            unavailable_after_ms: 5000,
            learner_timeout_ms: 60_000,
            probes_per_group: 8,
            probe_concurrency: 4,
            metadata_bytes: 8 * PROBE_MEMORY,
        }
    }

    pub(super) fn validate(&self, host: &RaftHostOptions) -> anyhow::Result<()> {
        self.placement.validate()?;
        if let Some(policy) = self.leadership {
            policy.validate()?;
        }
        let profile = host
            .node_profile
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("controller requires a node profile"))?;
        anyhow::ensure!(host.node_directory.is_some() && host.enable_snapshots
            && profile.bytes_per_group == self.placement.required_bytes
            && host.max_control_requests >= 8,
            "controller requires directory, snapshots, matching reservations and reserved control capacity");
        anyhow::ensure!(
            (10..=60_000).contains(&self.interval_ms)
                && (1..=60_000).contains(&self.probe_timeout_ms)
                && (1..=60_000).contains(&self.operation_timeout_ms)
                && (1..=3_600_000).contains(&self.seed_timeout_ms)
                && (self.interval_ms..=3_600_000).contains(&self.unavailable_after_ms)
                && (self.seed_timeout_ms..=3_600_000).contains(&self.learner_timeout_ms)
                && (4..=32).contains(&self.probes_per_group)
                && (1..=4).contains(&self.probe_concurrency)
                && self.metadata_bytes >= self.probes_per_group * PROBE_MEMORY
                && self.metadata_bytes <= 128 * PROBE_MEMORY,
            "invalid controller bounds"
        );
        Ok(())
    }
}

/// Local service observations. Counters record attempts, never quorum proof or
/// a promise that an admitted transfer won an election. They reset on node boot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftControllerStatus {
    pub enabled: bool,
    pub recovering: bool,
    pub probe_rounds: u64,
    pub repair_requests: u64,
    pub activation_requests: u64,
    pub transfer_attempts: u64,
    pub completed_repairs: u64,
}
