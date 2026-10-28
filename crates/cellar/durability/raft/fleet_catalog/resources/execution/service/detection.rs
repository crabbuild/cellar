// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded exact-boot observations. This service never admits evacuation or
//! isolation; its classifications remain ephemeral inputs to recovery policy.
use super::*;
use crate::raft::fleet_catalog::deadline::Deadline;
use crate::raft::fleet_stop::BootBinding;
use crate::raft::inspection::InspectionError;
use cellar_logic::consensus::NodeId;
pub use cellar_logic::raft_fleet::NodeHealth as FleetNodeHealth;
use cellar_logic::raft_fleet::{HealthObservation, HealthPolicy, HealthTracker};
use futures_util::{stream, StreamExt};
use std::collections::BTreeMap;

const MAX_AGE_MS: u64 = 5000;
const PROBES_PER_ROUND: usize = 4;

#[derive(Clone, Debug)]
pub struct FleetFailureDetectionOptions {
    /// Direct failures must recur across this interval; elapsed time alone is
    /// insufficient. Discovery gaps and unknown outcomes restart the interval.
    pub unavailability_grace: Duration,
    /// One original budget includes local admission, connection and reply.
    pub probe_timeout: Duration,
}

impl Default for FleetFailureDetectionOptions {
    fn default() -> Self {
        Self {
            unavailability_grace: Duration::from_secs(5),
            probe_timeout: Duration::from_millis(500),
        }
    }
}

impl FleetFailureDetectionOptions {
    pub(super) fn validate(&self) -> Result<()> {
        if self.unavailability_grace < Duration::from_secs(5)
            || self.unavailability_grace > Duration::from_secs(300)
            || self.probe_timeout < Duration::from_millis(1)
            || self.probe_timeout > Duration::from_millis(500)
        {
            return Err(FleetCatalogError::Invalid("failure detection bounds"));
        }
        Ok(())
    }

    fn policy(&self) -> HealthPolicy {
        // Validated before constructing the worker; no external clock values.
        HealthPolicy::new(self.unavailability_grace.as_millis() as u64, MAX_AGE_MS)
            .expect("validated failure detection bounds")
    }
}

/// Diagnostic connectivity of one exact boot, never a membership, physical
/// closure or eviction proof. Unavailable still requires platform isolation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetNodeFailureStatus {
    pub boot: BootBinding,
    pub health: FleetNodeHealth,
    pub consecutive_failures: u32,
    pub last_error: Option<InspectionError>,
    pub sample_age_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetFailureDetectionStatus {
    pub observed_revision: u64,
    /// Other enrolled boots. This monitor never diagnoses its own process.
    pub nodes: Vec<FleetNodeFailureStatus>,
    captured_at: u64,
}

impl FleetFailureDetectionStatus {
    pub(super) fn age(&mut self, now: Option<u64>) {
        let elapsed = now.and_then(|now| now.checked_sub(self.captured_at));
        for node in &mut self.nodes {
            node.sample_age_ms = node.sample_age_ms.and_then(|age| age.checked_add(elapsed?));
            if node.sample_age_ms.is_none_or(|age| age > MAX_AGE_MS) {
                node.health = FleetNodeHealth::Unknown;
                node.consecutive_failures = 0;
            }
        }
    }
}

struct Node {
    boot: BootBinding,
    tracker: HealthTracker,
    error: Option<InspectionError>,
}

pub(super) struct Detection {
    options: FleetFailureDetectionOptions,
    nodes: BTreeMap<NodeId, Node>,
    after: Option<NodeId>,
    revision: u64,
    origin: Option<NodeId>,
}

impl Detection {
    pub fn new(options: FleetFailureDetectionOptions) -> Self {
        Self {
            options,
            nodes: BTreeMap::new(),
            after: None,
            revision: 0,
            origin: None,
        }
    }

    pub fn set_origin(&mut self, node: NodeId) {
        self.origin = Some(node);
    }

    pub fn invalidate(&mut self) {
        for node in self.nodes.values_mut() {
            node.tracker = HealthTracker::default();
            node.error = None;
        }
    }

    pub fn view(&self, now: u64) -> FleetFailureDetectionStatus {
        let policy = self.options.policy();
        FleetFailureDetectionStatus {
            observed_revision: self.revision,
            captured_at: now,
            nodes: self
                .nodes
                .values()
                .map(|node| FleetNodeFailureStatus {
                    boot: node.boot.clone(),
                    health: node.tracker.state(policy, now),
                    consecutive_failures: node.tracker.consecutive_failures(policy, now),
                    last_error: node.error.clone(),
                    sample_age_ms: node.tracker.age_ms(now),
                })
                .collect(),
        }
    }

    fn refresh_inventory(&mut self, current: BTreeMap<NodeId, BootBinding>) {
        self.nodes.retain(|id, _| current.contains_key(id));
        for (id, boot) in current {
            let old = self.nodes.entry(id).or_insert_with(|| Node {
                boot: boot.clone(),
                tracker: HealthTracker::default(),
                error: None,
            });
            if old.boot != boot {
                *old = Node {
                    boot,
                    tracker: HealthTracker::default(),
                    error: None,
                };
            }
        }
    }

    pub async fn round(
        &mut self,
        catalog: &FleetCatalog,
        observer: &RaftCells,
        cut: &FleetSnapshot,
        deadline: &Deadline<'_>,
    ) -> Result<()> {
        let records = catalog
            .run(deadline.remaining()?, catalog.node_records(cut))
            .await?;
        let owner = catalog.birth_owner()?;
        let origin = self
            .origin
            .ok_or(FleetCatalogError::Invalid("failure detection origin"))?;
        let mut current = BTreeMap::new();
        for record in records {
            if record.boot.is_none()
                || record.isolation.is_some()
                || record.state == FleetNodeState::Removed
            {
                continue;
            }
            let boot = record.boot_binding(owner.catalog)?;
            let id = NodeId::new(boot.node)
                .map_err(|_| FleetCatalogError::Corrupt("failure detection node"))?;
            if id == origin {
                continue;
            }
            current.insert(id, boot);
        }
        self.refresh_inventory(current);
        let ids = self.nodes.keys().copied().collect::<Vec<_>>();
        let start = self
            .after
            .and_then(|after| ids.iter().position(|id| *id > after))
            .unwrap_or(0);
        let batch = ids
            .iter()
            .cycle()
            .skip(start)
            .take(ids.len().min(PROBES_PER_ROUND))
            .copied()
            .collect::<Vec<_>>();
        let timeout = self.options.probe_timeout.min(deadline.remaining()?);
        let domain = &catalog.inner.domain;
        let mut probes = stream::iter(batch.iter().copied().map(|id| async move {
            let started = domain.mono_ms().map_err(|_| FleetCatalogError::Closed)?;
            let result = observer
                .inspect_peer_node(id, cut.revision(), timeout)
                .await;
            let finished = domain.mono_ms().map_err(|_| FleetCatalogError::Closed)?;
            Ok::<_, FleetCatalogError>((id, started, finished, result))
        }))
        .buffer_unordered(PROBES_PER_ROUND);
        while let Some(result) = probes.next().await {
            let (id, started, finished, result) = result?;
            let node = self.nodes.get_mut(&id).ok_or(FleetCatalogError::Conflict)?;
            let age = finished
                .checked_sub(started)
                .ok_or(FleetCatalogError::Closed)?;
            let (observation, age, error) = match result {
                Ok(sample) if sample.view.fleet_boot.as_ref() == Some(&node.boot) => (
                    HealthObservation::Responsive,
                    age.max(sample.sample_age_ms),
                    None,
                ),
                Ok(_) => (
                    HealthObservation::Unknown,
                    age,
                    Some(InspectionError::StalePeer),
                ),
                Err(error) => (classify(&error), age, Some(error)),
            };
            node.tracker
                .observe(self.options.policy(), finished, age, observation);
            node.error = error;
        }
        // Root changes can replace the observed boot or quarantine it. Never
        // publish this batch against a different authoritative node inventory.
        catalog
            .run(deadline.remaining()?, async {
                if catalog.inner.current().await?.token != cut.token {
                    return Err(FleetCatalogError::Conflict);
                }
                Ok(())
            })
            .await?;
        self.after = batch.last().copied();
        self.revision = cut.revision();
        Ok(())
    }
}

fn classify(error: &InspectionError) -> HealthObservation {
    match error {
        InspectionError::Unreachable => HealthObservation::DirectFailure,
        InspectionError::Overloaded
        | InspectionError::Unavailable
        | InspectionError::Refused
        | InspectionError::StalePeer => HealthObservation::Unknown,
    }
}

#[cfg(test)]
#[path = "detection_tests.rs"]
mod tests;
