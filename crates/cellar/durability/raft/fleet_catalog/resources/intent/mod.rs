// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Durable desired placement. Exclusion gates new admission at the root CAS;
//! existing permits and physical owners keep their independent obligations.
mod admission;
mod guard;
mod lifecycle;
mod progress;
mod records;

use super::*;
use crate::application::consensus::RaftCells;
use crate::raft::catalog::NamespaceCatalog;
use cellar_logic::consensus::ProposalId;
use records::{Definition, IntentRecord, Policy};
use std::collections::BTreeSet;

const POLICY_KEY: &str = "topology-placement";
const ACTIVE_KEY: &str = "active-topology";
const ROW_PREFIX: &str = "topology/";
const POLICY_KIND: u8 = 67;
const REQUEST_KIND: u8 = 68;
const ROW_KIND: u8 = 69;
const MOVE_KIND: u8 = 70;

/// Desired placement, not a capacity or convergence certificate. The explicit
/// keep-set and policy remain canonical across retries and controller restarts.
#[derive(Clone, Debug)]
pub struct FleetTopologyIntentRequest {
    pub identity: FleetRequestIdentity,
    pub keep: Vec<NodeId>,
    pub policy: FleetTopologyPolicy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FleetTopologyPhase {
    Running,
    Cancelling,
    Complete,
    Cancelled,
}

/// Rooted historical status. Completion denotes checked placement only; it
/// never proves node closure, releases storage or authorizes volume deletion.
#[derive(Clone, Debug)]
pub struct FleetTopologyOperation {
    pub identity: FleetRequestIdentity,
    pub keep: Vec<NodeId>,
    pub policy: FleetTopologyPolicy,
    pub phase: FleetTopologyPhase,
    pub admitted_movements: u64,
    pub applied_revision: u64,
    pub observed_revision: u64,
    /// Last unconfirmed prospective admission fenced by a later root CAS.
    /// This is not a movement, settlement or physical-release certificate.
    pub last_superseded_admission: Option<[u8; 16]>,
}

#[derive(Clone, Debug)]
pub struct FleetTopologyMovementEntry {
    pub reservation: [u8; 16],
    pub group: GroupScope,
    pub source: NodeId,
    pub target: NodeId,
    pub admitted_revision: u64,
}

pub struct FleetTopologyMovementPage {
    pub entries: Vec<FleetTopologyMovementEntry>,
    pub next: Option<FleetCursor>,
}

fn row_key(operation: [u8; 16]) -> String {
    format!("{ROW_PREFIX}{}", hex::encode(operation))
}

fn active_change(operation: Option<[u8; 16]>) -> FleetChange {
    FleetChange {
        table: FleetTable::Policy,
        key: ACTIVE_KEY.into(),
        value: operation.map(|id| id.to_vec()),
    }
}

fn mutation(request: &FleetRequestIdentity, changes: Vec<FleetChange>) -> FleetMutation {
    FleetMutation {
        id: request.id,
        operation: request.operation,
        expected_revision: request.expected_revision,
        principal: request.principal.clone(),
        changes,
    }
}

impl FleetCatalog {
    /// Rooted optional lookup for operator status. Absence is an observation
    /// at this snapshot, not proof that a concurrent request cannot commit.
    pub async fn find_topology_operation(
        &self,
        snapshot: &FleetSnapshot,
        operation: ProposalId,
        timeout: Duration,
    ) -> Result<Option<FleetTopologyOperation>> {
        self.check_snapshot(snapshot)?;
        self.run(timeout, async {
            if self
                .inner
                .lookup(
                    snapshot.head.tables[FleetTable::Operations.slot()],
                    &row_key(operation.bytes()),
                )
                .await?
                .is_none()
            {
                return Ok(None);
            }
            let record = self.intent_record(snapshot, operation.bytes()).await?;
            self.intent_observation(snapshot, &record).await.map(Some)
        })
        .await
    }

    /// Operation whose keep policy currently governs new placement, including
    /// after successful convergence or restoration of an older policy.
    pub async fn placement_topology_operation(
        &self,
        snapshot: &FleetSnapshot,
        timeout: Duration,
    ) -> Result<Option<FleetTopologyOperation>> {
        self.check_snapshot(snapshot)?;
        self.run(timeout, async {
            let Some(policy) = self.placement_policy(snapshot).await? else {
                return Ok(None);
            };
            let record = self.intent_record(snapshot, policy.operation).await?;
            let view = self.intent_observation(snapshot, &record).await?;
            if Definition::new(&view.keep.iter().copied().collect(), &view.policy)?
                != policy.definition
            {
                return Err(FleetCatalogError::Corrupt("placement operation policy"));
            }
            Ok(Some(view))
        })
        .await
    }

    pub async fn topology_operation(
        &self,
        snapshot: &FleetSnapshot,
        operation: ProposalId,
        timeout: Duration,
    ) -> Result<FleetTopologyOperation> {
        self.check_snapshot(snapshot)?;
        self.run(timeout, async {
            let record = self.intent_record(snapshot, operation.bytes()).await?;
            self.intent_observation(snapshot, &record).await
        })
        .await
    }

    pub async fn active_topology_operation(
        &self,
        snapshot: &FleetSnapshot,
        timeout: Duration,
    ) -> Result<Option<FleetTopologyOperation>> {
        self.check_snapshot(snapshot)?;
        self.run(timeout, async {
            let Some(record) = self.active_intent(snapshot).await? else {
                return Ok(None);
            };
            self.intent_observation(snapshot, &record).await.map(Some)
        })
        .await
    }

    async fn commit_intent(
        &self,
        snapshot: &FleetSnapshot,
        mutation: &FleetMutation,
        check: impl FnOnce() -> Result<()>,
    ) -> Result<FleetCommit> {
        mutation.validate()?;
        self.commit_with_disruption(snapshot, mutation, None, Authority::Topology, check)
            .await
    }

    async fn require_intent_current(&self, snapshot: &FleetSnapshot) -> Result<()> {
        if self.inner.current().await?.token != snapshot.token {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(())
    }

    async fn require_intent_settled(&self, snapshot: &FleetSnapshot) -> Result<()> {
        if !self.resource_book(snapshot).await?.rows.is_empty() {
            return Err(resource_error(LedgerError::MovementLimit));
        }
        self.check_resource_permits(snapshot, &[]).await
    }

    async fn require_intent_preview(
        &self,
        snapshot: &FleetSnapshot,
        record: &IntentRecord,
        preview: &FleetTopologyPreview,
    ) -> Result<()> {
        self.check_snapshot(&preview.snapshot)?;
        if preview.snapshot.token != snapshot.token {
            return Err(FleetCatalogError::Conflict);
        }
        self.require_resource_age(preview.oldest)?;
        let definition = Definition::new(&preview.keep, &preview.policy)?;
        if self.read_intent_policy(record.desired).await?.definition != definition {
            return Err(FleetCatalogError::Conflict);
        }
        self.require_intent_current(snapshot).await
    }
}
