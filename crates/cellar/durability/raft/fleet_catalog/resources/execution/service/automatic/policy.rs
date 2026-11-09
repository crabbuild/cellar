// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Select one durable topology intent; the existing executor owns every move.
use super::*;
use crate::raft::fleet_catalog::deadline::Deadline;
use sha2::{Digest, Sha256};

pub(in super::super) struct Automatic {
    pub evidence: Arc<Evidence>,
    last_complete: Option<[u8; 32]>,
    checked_nodes: Option<([u8; 16], [u8; 32])>,
}

impl Automatic {
    fn owns(&self, operation: &FleetTopologyOperation) -> Result<bool> {
        // A principal identifies the caller, not the operation's mode. The
        // immutable generated request distinguishes automatic from operator
        // intent, including when both use the same authenticated principal.
        if operation.identity.principal != self.evidence.options.principal {
            return Ok(false);
        }
        let id = intent_id(
            self.evidence.catalog.catalog_id()?,
            operation.identity.expected_revision,
            &operation.identity.principal,
            &operation.policy,
            &operation.keep,
        )?;
        Ok(operation.identity.id == id && operation.identity.operation == id)
    }

    async fn operator_policy(&self, cut: &FleetSnapshot, deadline: &Deadline<'_>) -> Result<bool> {
        match self
            .evidence
            .catalog
            .placement_topology_operation(cut, deadline.remaining()?)
            .await?
        {
            Some(operation) => Ok(!self.owns(&operation)?),
            None => Ok(false),
        }
    }

    pub fn new(evidence: Arc<Evidence>) -> Self {
        Self {
            evidence,
            last_complete: None,
            checked_nodes: None,
        }
    }

    pub fn completed(&mut self, signature: [u8; 32]) {
        self.last_complete = Some(signature);
    }

    /// Failure observations may fence automatic planning, never host execution.
    /// Admitted movements retain their owner and debt. After durable cancellation,
    /// the failure service may acquire isolation; cancellation settles only when
    /// the movement book is empty. Explicit operator intents are never seized.
    pub async fn yield_for_recovery(
        &self,
        cut: &FleetSnapshot,
        unavailable: bool,
        idle: bool,
        deadline: &Deadline<'_>,
    ) -> Result<bool> {
        let catalog = &self.evidence.catalog;
        let Some(operation) = catalog
            .active_topology_operation(cut, deadline.remaining()?)
            .await?
        else {
            return Ok(false);
        };
        if !self.owns(&operation)? {
            return Ok(false);
        }
        match operation.phase {
            FleetTopologyPhase::Running if unavailable => {
                let request = super::super::topology::service_request(
                    catalog,
                    &operation,
                    cut,
                    b"failure-yield",
                )?;
                catalog
                    .cancel_topology(cut, &request, deadline.remaining()?)
                    .await?;
            }
            FleetTopologyPhase::Cancelling if idle => {
                let request = super::super::topology::service_request(
                    catalog,
                    &operation,
                    cut,
                    b"cancel-complete",
                )?;
                catalog
                    .complete_topology_cancellation(cut, &request, deadline.remaining()?)
                    .await?;
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    /// Eligibility changes cancel an automatic operation through its ordinary
    /// durable fence before selecting a new immutable definition.
    pub async fn reconcile(
        &mut self,
        operation: &FleetTopologyOperation,
        cut: &FleetSnapshot,
        deadline: &Deadline<'_>,
    ) -> Result<bool> {
        if !self.owns(operation)? || operation.phase != FleetTopologyPhase::Running {
            return Ok(false);
        }
        let nodes: [u8; 32] = Sha256::digest(
            serde_json::to_vec(&cut.head.tables[FleetTable::Nodes.slot()])
                .map_err(|_| FleetCatalogError::Limit)?,
        )
        .into();
        if self.checked_nodes == Some((operation.identity.operation, nodes)) {
            return Ok(false);
        }
        let (keep, _) = self.evidence.nodes(cut, deadline.remaining()?).await?;
        if keep == operation.keep && operation.policy == self.evidence.options.policy {
            self.checked_nodes = Some((operation.identity.operation, nodes));
            return Ok(false);
        }
        let request = super::super::topology::service_request(
            &self.evidence.catalog,
            operation,
            cut,
            b"automatic-replan",
        )?;
        self.evidence
            .catalog
            .cancel_topology(cut, &request, deadline.remaining()?)
            .await?;
        Ok(true)
    }

    pub async fn select(
        &self,
        cut: &FleetSnapshot,
        deadline: &Deadline<'_>,
        status: &watch::Sender<FleetMovementServiceStatus>,
    ) -> Result<()> {
        let catalog = &self.evidence.catalog;
        let wait = if cut.head.history_count == 0 {
            FleetAutomaticTopologyWait::InventoryPending
        } else if cut.head.disruption.is_some() {
            FleetAutomaticTopologyWait::Disruption
        } else if self.operator_policy(cut, deadline).await? {
            FleetAutomaticTopologyWait::OperatorPolicy
        } else if self.last_complete
            == Some(self.evidence.signature(cut, deadline.remaining()?).await?)
        {
            FleetAutomaticTopologyWait::Unchanged
        } else {
            FleetAutomaticTopologyWait::Changed
        };
        status.send_modify(|s| {
            s.automatic_topology
                .get_or_insert(FleetAutomaticTopologyStatus {
                    wait,
                    selected_operations: 0,
                })
                .wait = wait
        });
        if wait != FleetAutomaticTopologyWait::Changed {
            return Ok(());
        }
        let (keep, _) = self.evidence.nodes(cut, deadline.remaining()?).await?;
        if keep.len() < 4 {
            status.send_modify(|s| {
                if let Some(a) = &mut s.automatic_topology {
                    a.wait = FleetAutomaticTopologyWait::InsufficientNodes;
                }
            });
            return Ok(());
        }
        if !self.evidence.ready(cut, deadline.remaining()?).await? {
            status.send_modify(|s| {
                if let Some(a) = &mut s.automatic_topology {
                    a.wait = FleetAutomaticTopologyWait::InventoryPending;
                }
            });
            return Ok(());
        }
        let request = self
            .evidence
            .options
            .intent(catalog.catalog_id()?, cut.revision(), keep)?;
        catalog
            .begin_topology(
                cut,
                &request,
                &self.evidence.namespaces,
                deadline.remaining()?,
            )
            .await?;
        status.send_modify(|s| {
            if let Some(a) = &mut s.automatic_topology {
                a.selected_operations = a.selected_operations.saturating_add(1);
            }
        });
        Ok(())
    }
}

pub(super) fn intent_id(
    catalog: [u8; 32],
    revision: u64,
    principal: &str,
    policy: &FleetTopologyPolicy,
    keep: &[cellar_logic::consensus::NodeId],
) -> Result<[u8; 16]> {
    let mut hash = Sha256::new();
    hash.update(b"cellar-automatic-topology-intent-1");
    hash.update(catalog);
    hash.update(revision.to_be_bytes());
    let encoded_keep: Vec<_> = keep.iter().map(|node| node.bytes()).collect();
    hash.update(
        serde_json::to_vec(&(principal, policy, &encoded_keep))
            .map_err(|_| FleetCatalogError::Limit)?,
    );
    let digest: [u8; 32] = hash.finalize().into();
    digest[..16]
        .try_into()
        .map_err(|_| FleetCatalogError::Limit)
}
