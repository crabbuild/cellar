// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Rooted deployment bounds supplement the executor's fresh physical proofs.
use super::*;
use crate::raft::fleet_catalog::{histories::FleetHistory, managed_transition_capacity};
use cellar_logic::consensus::NodeId;
use sha2::{Digest, Sha256};

pub(in super::super) struct Evidence {
    pub catalog: FleetCatalog,
    pub namespaces: Vec<RangeDirectory>,
    pub options: FleetAutomaticTopologyOptions,
    pub memory: Arc<crate::raft::budget::Budget>,
}

impl Evidence {
    /// Do not seize topology ownership while a first group or another birth
    /// still needs admission. One bounded metadata page is retained at a time.
    pub async fn ready(&self, cut: &FleetSnapshot, timeout: Duration) -> Result<bool> {
        if cut.head.history_count == 0 {
            return Ok(false);
        }
        let _memory = self
            .memory
            .acquire(1024 * 1024)
            .ok_or(FleetCatalogError::Overloaded)?;
        let deadline = crate::raft::fleet_catalog::deadline::Deadline::new(&self.catalog, timeout)?;
        let mut after = None;
        let mut live = false;
        loop {
            let page = self
                .catalog
                .page(
                    cut,
                    FleetTable::Histories,
                    after.as_ref(),
                    128,
                    deadline.remaining()?,
                )
                .await?;
            for row in page.entries {
                let history = FleetHistory::decode(&row.value)?;
                match *history.state() {
                    FleetHistoryState::Live => live = true,
                    FleetHistoryState::Cancelled { .. } | FleetHistoryState::Retired { .. } => (),
                    _ => return Ok(false),
                }
            }
            after = page.next;
            if after.is_none() {
                return Ok(live);
            }
        }
    }

    pub async fn nodes(
        &self,
        cut: &FleetSnapshot,
        timeout: Duration,
    ) -> Result<(Vec<NodeId>, Vec<FleetTopologyNodeResources>)> {
        self.catalog
            .run(
                timeout,
                Box::pin(async {
                    if self.catalog.inner.current().await?.token != cut.token {
                        return Err(FleetCatalogError::Conflict);
                    }
                    let mut keep = Vec::new();
                    let mut nodes = Vec::new();
                    for node in self.catalog.node_records(cut).await? {
                        if node.state == FleetNodeState::Removed || node.isolation.is_some() {
                            continue;
                        }
                        let id = NodeId::new(node.node)
                            .map_err(|_| FleetCatalogError::Corrupt("automatic topology node"))?;
                        let capacity = match self.catalog.capacity_from(cut, id).await {
                            Ok(capacity) => capacity,
                            Err(FleetCatalogError::Conflict)
                                if node.state == FleetNodeState::Registered =>
                            {
                                continue
                            }
                            Err(error) => return Err(error),
                        };
                        let attested = capacity.attestation();
                        let profile = self.catalog.capacity_profile(attested).await?;
                        if capacity.receive_replicas() && !profile.draining {
                            keep.push(id);
                        }
                        let mut attestation =
                            managed_transition_capacity::resource_attestation(attested);
                        // These are retained, rooted deployment lower bounds, not
                        // a new measurement or a zero-occupancy assumption. The
                        // executor independently probes complete physical inventory.
                        attestation.used = self.catalog.resource_floor(cut, &attestation).await?;
                        nodes.push(FleetTopologyNodeResources {
                            attestation,
                            reserve: self.options.reserve,
                        });
                    }
                    keep.sort();
                    if self.catalog.inner.current().await?.token != cut.token {
                        return Err(FleetCatalogError::Conflict);
                    }
                    Ok((keep, nodes))
                }),
            )
            .await
    }

    pub async fn signature(&self, cut: &FleetSnapshot, timeout: Duration) -> Result<[u8; 32]> {
        self.catalog
            .run(timeout, async {
                if self.catalog.inner.current().await?.token != cut.token {
                    return Err(FleetCatalogError::Conflict);
                }
                let mut digest = Sha256::new();
                digest.update(b"cellar-automatic-topology-observation-1");
                digest.update(self.catalog.catalog_id()?);
                // Policy/operation journal changes made by this controller do
                // not trigger another operation. Node qualification, births and
                // membership admission do. Business-cell writes touch none of
                // these indexes. A fresh controller revalidates once.
                for table in [
                    FleetTable::Nodes,
                    FleetTable::Histories,
                    FleetTable::Admissions,
                ] {
                    digest.update(
                        serde_json::to_vec(&cut.head.tables[table.slot()])
                            .map_err(|_| FleetCatalogError::Limit)?,
                    );
                }
                Ok(digest.finalize().into())
            })
            .await
    }

    async fn histories(
        &self,
        cut: &FleetSnapshot,
        timeout: Duration,
    ) -> Result<Vec<FleetTopologyHistoryResources>> {
        let deadline = crate::raft::fleet_catalog::deadline::Deadline::new(&self.catalog, timeout)?;
        let mut after = None;
        let mut histories = Vec::new();
        loop {
            let page = self
                .catalog
                .page(
                    cut,
                    FleetTable::Histories,
                    after.as_ref(),
                    128,
                    deadline.remaining()?,
                )
                .await?;
            for row in page.entries {
                let history = FleetHistory::decode(&row.value)?;
                if history
                    .binding()
                    .key()
                    .map_err(|_| FleetCatalogError::Corrupt("automatic history binding"))?
                    != row.key
                {
                    return Err(FleetCatalogError::Corrupt("automatic history key"));
                }
                match *history.state() {
                    FleetHistoryState::Cancelled { .. } | FleetHistoryState::Retired { .. } => (),
                    FleetHistoryState::Live => {
                        histories.push(self.bounds(history.binding().scope().map_err(|_| {
                            FleetCatalogError::Corrupt("automatic topology history")
                        })?))
                    }
                    _ => return Err(FleetCatalogError::InventoryIncomplete),
                }
            }
            after = page.next;
            if after.is_none() {
                return Ok(histories);
            }
        }
    }

    fn bounds(&self, group: GroupHistory) -> FleetTopologyHistoryResources {
        FleetTopologyHistoryResources {
            group,
            resident: self.options.resident,
            peak: self.options.peak,
        }
    }
}

impl FleetTopologyEvidence for Evidence {
    fn refresh<'a>(
        &'a self,
        operation: &'a FleetTopologyOperation,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<FleetTopologyServiceInputs>> + Send + 'a>> {
        Box::pin(async move {
            let deadline =
                crate::raft::fleet_catalog::deadline::Deadline::new(&self.catalog, timeout)?;
            let cut = self.catalog.read(deadline.remaining()?).await?;
            let (_, nodes) = self.nodes(&cut, deadline.remaining()?).await?;
            let histories = self.histories(&cut, deadline.remaining()?).await?;
            Ok(FleetTopologyServiceInputs {
                request: FleetTopologyRequest {
                    keep: operation.keep.clone(),
                    policy: operation.policy.clone(),
                    nodes,
                    histories,
                },
                namespaces: self.namespaces.clone(),
            })
        })
    }
}

impl FleetMovementEvidence for Evidence {
    fn refresh<'a>(
        &'a self,
        movement: &'a FleetPendingMovement,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<FleetMovementExecutionRequest>> + Send + 'a>> {
        Box::pin(async move {
            let deadline =
                crate::raft::fleet_catalog::deadline::Deadline::new(&self.catalog, timeout)?;
            let cut = self.catalog.read(deadline.remaining()?).await?;
            let (_, nodes) = self.nodes(&cut, deadline.remaining()?).await?;
            let attestation = nodes
                .into_iter()
                .find(|n| n.attestation.boot.node == movement.plan.replacement.node.bytes())
                .ok_or(FleetCatalogError::InventoryIncomplete)?
                .attestation;
            Ok(FleetMovementExecutionRequest {
                reservation: movement.reservation,
                attestation,
                from_s3: true,
            })
        })
    }
}

impl FleetIsolatedRepairEvidence for Evidence {
    fn refresh<'a>(
        &'a self,
        context: &'a FleetIsolatedRepairContext,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<FleetIsolatedRepairInputs>> + Send + 'a>> {
        Box::pin(async move {
            let deadline =
                crate::raft::fleet_catalog::deadline::Deadline::new(&self.catalog, timeout)?;
            let cut = self.catalog.read(deadline.remaining()?).await?;
            let history = self
                .catalog
                .history(&cut, &context.group, deadline.remaining()?)
                .await?;
            if *history.state() != FleetHistoryState::Live {
                return Err(FleetCatalogError::Conflict);
            }
            let (_, nodes) = self.nodes(&cut, deadline.remaining()?).await?;
            Ok(FleetIsolatedRepairInputs {
                history: self.bounds(context.group),
                nodes,
                namespaces: self.namespaces.clone(),
            })
        })
    }
}
