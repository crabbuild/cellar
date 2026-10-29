// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Recovery inputs from exact containers and authenticated native observations.
use super::*;
use crate::application::consensus::RaftCells;
use crate::raft::fleet_catalog::deadline::Deadline;
use serde::Deserialize;

/// Explicit conservative bounds for a homogeneous deployment. No disk/RSS
/// estimate is inferred from a test fixture. The deployment must enforce the
/// non-group disk ceiling and the per-history seed/copy/worker ceilings through
/// its storage, image, log and worker configuration. Recreate this provider if
/// those limits change; a policy for one workload is not safe for another.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetDockerResourcePolicy {
    /// Upper bound on all durable data outside the host's per-group charge,
    /// including shared log, scratch, retained files and container writable data.
    pub durable_overhead_bytes: u64,
    pub resident: FleetResourceBounds,
    pub peak: FleetResourceBounds,
    pub reserve: FleetResourceBounds,
}

impl FleetDockerResourcePolicy {
    fn validate(&self) -> Result<()> {
        if self.durable_overhead_bytes == 0
            || self.resident.engine_slots != 1
            || self.peak.engine_slots != 1
            || self.resident.durable_bytes == 0
            || self.resident.memory_bytes == 0
            || self.peak.durable_bytes < self.resident.durable_bytes
            || self.peak.memory_bytes < self.resident.memory_bytes
            || self.reserve.engine_slots > 32_768
        {
            return Err(FleetCatalogError::Invalid("Docker resource policy"));
        }
        Ok(())
    }
}

/// Opt-in movement and isolated-repair evidence for a bound Docker fleet.
/// Memory includes the complete cgroup usage (including cache); durable usage
/// is a conservative configured ceiling plus authenticated per-group charges,
/// not a filesystem scan. Immutable evidence is retained in the catalog bucket.
///
/// Every callback obtains fresh capacity, native and Engine observations under
/// one deadline. Missing, changed or unbound nodes block the attempt. Ready and
/// Active nodes are supported; departure/return settlement needs its separately
/// retained evidence provider. This provider does not grant capacity or activate
/// nodes. Pass it as both evidence arguments to the recovery service.
#[derive(Clone)]
pub struct FleetDockerResources {
    isolation: FleetDockerIsolation,
    observer: RaftCells,
    policy: FleetDockerResourcePolicy,
}

#[derive(Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Evidence {
    format: u8,
    binding: Binding,
    policy: FleetDockerResourcePolicy,
    profile_revision: u64,
    catalog_revision: u64,
    memory: engine::MemorySample,
    used: FleetResourceBounds,
}

impl FleetDockerResources {
    pub fn new(
        isolation: FleetDockerIsolation,
        observer: RaftCells,
        policy: FleetDockerResourcePolicy,
    ) -> Result<Self> {
        policy.validate()?;
        Ok(Self {
            isolation,
            observer,
            policy,
        })
    }

    /// Fresh retained occupancy for capacity/movement integration. This does
    /// not qualify platform capacity: that rooted qualification must exist.
    pub async fn observe(
        &self,
        node: NodeId,
        timeout: Duration,
    ) -> Result<FleetResourceAttestation> {
        let timeout = timeout.min(Duration::from_secs(5));
        self.isolation
            .run(timeout, async {
                let catalog = &self.isolation.inner.catalog;
                let deadline = Deadline::new(catalog, timeout)?;
                let cut = catalog.read(deadline.remaining()?).await?;
                self.sample(&cut, node, &deadline).await
            })
            .await
    }

    async fn sample(
        &self,
        cut: &FleetSnapshot,
        node: NodeId,
        deadline: &Deadline<'_>,
    ) -> Result<FleetResourceAttestation> {
        let adapter = &self.isolation.inner;
        let catalog = &adapter.catalog;
        let host = catalog
            .authorize_host(cut, node, deadline.remaining()?)
            .await?;
        let capacity = catalog
            .authorize_capacity(cut, node, deadline.remaining()?)
            .await?;
        let capacity = capacity.attestation();
        let observed = self
            .observer
            .inspect_peer_node(node, cut.revision(), deadline.remaining()?)
            .await
            .map_err(FleetCatalogError::Observation)?;
        super::super::capacity::check_observation(&host, capacity, &observed)?;
        let binding = self.isolation.binding(&capacity.boot).await?;
        let memory = adapter.engine.memory(&binding, capacity).await?;
        let view = observed.view;
        if self.policy.resident.durable_bytes < view.node.profile.bytes_per_group {
            return Err(FleetCatalogError::Invalid("Docker resident disk bound"));
        }
        let used = FleetResourceBounds {
            engine_slots: view.node.groups,
            durable_bytes: view
                .node
                .reserved_bytes
                .checked_add(self.policy.durable_overhead_bytes)
                .ok_or(FleetCatalogError::Limit)?
                .max(capacity.used_durable_bytes),
            memory_bytes: memory
                .used
                .max(view.startup_bounds.bounded_buffer_bytes)
                .max(capacity.used_memory_bytes),
        };
        if used.durable_bytes > capacity.durable_bytes || used.memory_bytes > capacity.memory_bytes
        {
            return Err(FleetCatalogError::Overloaded);
        }
        let record = Evidence {
            format: 1,
            binding,
            policy: self.policy.clone(),
            profile_revision: capacity.profile_revision,
            catalog_revision: cut.revision(),
            memory,
            used,
        };
        let digest = records::digest(&record)?;
        adapter
            .persist(&format!("resources/{}", hex::encode(digest)), &record)
            .await?;
        deadline.remaining()?;
        // A measurement made across a capacity/boot mutation cannot be handed
        // back as current evidence. Admission independently checks again.
        if catalog.read(deadline.remaining()?).await?.token != cut.token {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(FleetResourceAttestation {
            boot: capacity.boot.clone(),
            profile_revision: capacity.profile_revision,
            used,
            evidence_digest: digest,
            evidence_reference: format!("docker-resources-1:{}", hex::encode(digest)),
        })
    }
}

impl FleetMovementEvidence for FleetDockerResources {
    fn refresh<'a>(
        &'a self,
        movement: &'a FleetPendingMovement,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<FleetMovementExecutionRequest>> + Send + 'a>> {
        Box::pin(async move {
            Ok(FleetMovementExecutionRequest {
                reservation: movement.reservation,
                attestation: self
                    .observe(movement.plan.replacement.node, timeout)
                    .await?,
                from_s3: true,
            })
        })
    }
}

impl FleetIsolatedRepairEvidence for FleetDockerResources {
    fn refresh<'a>(
        &'a self,
        context: &'a FleetIsolatedRepairContext,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<FleetIsolatedRepairInputs>> + Send + 'a>> {
        Box::pin(async move {
            let timeout = timeout.min(Duration::from_secs(5));
            self.isolation
                .run(timeout, async {
                    context
                        .group
                        .validate()
                        .map_err(|_| FleetCatalogError::Invalid("Docker repair scope"))?;
                    context
                        .failed_boot
                        .validate()
                        .map_err(|_| FleetCatalogError::Invalid("Docker repair boot"))?;
                    let catalog = &self.isolation.inner.catalog;
                    let deadline = Deadline::new(catalog, timeout)?;
                    let cut = catalog.read(deadline.remaining()?).await?;
                    let nodes = catalog
                        .run(deadline.remaining()?, catalog.node_records(&cut))
                        .await?;
                    let mut inputs = Vec::new();
                    for node in nodes {
                        if node.node == context.failed_boot.node
                            || !matches!(node.state, FleetNodeState::Ready | FleetNodeState::Active)
                        {
                            continue;
                        }
                        let node = NodeId::new(node.node)
                            .map_err(|_| FleetCatalogError::Corrupt("Docker resource node"))?;
                        inputs.push(FleetTopologyNodeResources {
                            attestation: self.sample(&cut, node, &deadline).await?,
                            reserve: self.policy.reserve,
                        });
                    }
                    if inputs.is_empty() {
                        return Err(FleetCatalogError::InventoryIncomplete);
                    }
                    Ok(FleetIsolatedRepairInputs {
                        history: FleetTopologyHistoryResources {
                            group: context.group.clone(),
                            resident: self.policy.resident,
                            peak: self.policy.peak,
                        },
                        nodes: inputs,
                        namespaces: self.isolation.inner.namespaces.clone(),
                    })
                })
                .await
        })
    }
}
