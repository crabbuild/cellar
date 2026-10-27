// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Typed operator access to rooted topology and node operations. Configure
//! namespace handles and any live observer on the trusted server. Placement
//! proofs remain host-owned; capacity-granted operators explicitly attest limits.
mod connection;
mod failure;
mod lifecycle;
mod nodes;
mod records;
use crate::application::consensus::RaftCells;
use crate::asyncrt::{DomainToken, ExecutionDomain};
use crate::raft::catalog::NamespaceCatalog;
use crate::raft::fleet_catalog::{
    FleetCatalog, FleetCatalogError, FleetCursor, FleetRequestIdentity, FleetTopologyIntentRequest,
    FleetTopologyOperation, FleetTopologyPhase, FleetTopologyPolicy,
};
use cellar_logic::consensus::{NodeId, ProposalId};
pub use connection::{RaftFleetConnection, RaftFleetNamespace, RaftFleetNamespaceScope};
pub use failure::{RaftFleetFailure, RaftFleetFailureCode, RaftFleetOutcome};
pub use lifecycle::{
    RaftFleetEvacuationAcceptance, RaftFleetEvacuationDebt, RaftFleetEvacuationPhase,
    RaftFleetEvacuationProgress, RaftFleetEvacuationStatus, RaftFleetNodeCancel,
    RaftFleetNodeOperation, RaftFleetNodeRemove, RaftFleetNodeStop, RaftFleetVerificationBlocker,
    RaftFleetVerificationReason, RaftFleetVerificationStage,
};
pub use nodes::{
    RaftFleetCapacityAttestation, RaftFleetNodeAcceptance, RaftFleetNodeAction,
    RaftFleetNodeAdmission, RaftFleetNodeCapacity, RaftFleetNodeDescriptor, RaftFleetNodeState,
    RaftFleetNodeStatus, RaftFleetRegisterNode,
};
pub use records::{
    RaftFleetAcceptance, RaftFleetBegin, RaftFleetCancel, RaftFleetMovement, RaftFleetMovements,
    RaftFleetOperation, RaftFleetStatus,
};
use std::{sync::Arc, time::Duration};

type Result<T> = std::result::Result<T, RaftFleetFailure>;

/// Management adapter, independent of HTTP and human authentication. A separate
/// explicitly started node-owned topology service executes admitted work. This
/// handle's existence says nothing about controller or node availability.
#[derive(Clone)]
pub struct RaftFleetControl {
    catalog: FleetCatalog,
    catalog_id: String,
    namespaces: Arc<[NamespaceCatalog]>,
    domain: DomainToken,
    node_observer: Option<RaftCells>,
    removal_verification: Option<Arc<lifecycle::verification::Verification>>,
}

impl RaftFleetControl {
    pub fn new(
        domain: &ExecutionDomain,
        catalog: FleetCatalog,
        namespaces: Vec<NamespaceCatalog>,
    ) -> Result<Self> {
        if namespaces.len() > 64 {
            return Err(RaftFleetFailure::invalid("too many managed namespaces"));
        }
        let domain = domain
            .services()
            .domain_token()
            .ok_or(FleetCatalogError::Closed)?;
        let catalog_id = hex::encode(catalog.catalog_id()?);
        Ok(Self {
            catalog,
            catalog_id,
            namespaces: namespaces.into(),
            domain,
            node_observer: None,
            removal_verification: None,
        })
    }

    pub fn catalog_id(&self) -> &str {
        &self.catalog_id
    }

    fn check_catalog(&self, expected: &str) -> Result<()> {
        if expected != self.catalog_id {
            return Err(RaftFleetFailure {
                code: RaftFleetFailureCode::Conflict,
                outcome: RaftFleetOutcome::NotSubmitted,
                message: "The catalog generation differs from the request; it was not dispatched."
                    .into(),
            });
        }
        Ok(())
    }

    pub async fn status(&self, timeout: Duration) -> Result<RaftFleetStatus> {
        let turn = Turn::new(&self.domain, timeout)?;
        let root = self.catalog.read(turn.remaining()?).await?;
        let active = self
            .catalog
            .active_topology_operation(&root, turn.remaining()?)
            .await?
            .map(|v| RaftFleetOperation::from_root(v, &self.catalog_id));
        let placement = self
            .catalog
            .placement_topology_operation(&root, turn.remaining()?)
            .await?
            .map(|v| RaftFleetOperation::from_root(v, &self.catalog_id));
        Ok(RaftFleetStatus {
            catalog_id: self.catalog_id.clone(),
            observed_revision: root.revision(),
            active,
            placement,
        })
    }

    pub async fn operation(
        &self,
        operation: &str,
        timeout: Duration,
    ) -> Result<RaftFleetOperation> {
        let operation = ProposalId::new(records::id(operation)?)
            .map_err(|_| RaftFleetFailure::invalid("operation ID"))?;
        let turn = Turn::new(&self.domain, timeout)?;
        let root = self.catalog.read(turn.remaining()?).await?;
        self.catalog.find_topology_operation(&root, operation, turn.remaining()?).await?.map(|v| RaftFleetOperation::from_root(v, &self.catalog_id))
            .ok_or(RaftFleetFailure { code: RaftFleetFailureCode::NotFound, outcome: RaftFleetOutcome::Unknown,
                message: "The operation is absent at this observed catalog revision; an in-flight request may still commit.".into() })
    }

    pub async fn begin(
        &self,
        principal: &str,
        request: RaftFleetBegin,
        timeout: Duration,
    ) -> Result<RaftFleetOperation> {
        let request = request.canonicalize()?;
        self.check_catalog(&request.catalog_id)?;
        let operation = records::id(&request.operation_id)?;
        let keep = request
            .keep
            .iter()
            .map(|s| NodeId::new(records::id(s)?).map_err(|_| RaftFleetFailure::invalid("node ID")))
            .collect::<Result<Vec<_>>>()?;
        // Never reject an old expected revision before the core's canonical
        // retry lookup: a lost reply may already have advanced the root.
        let turn = Turn::new(&self.domain, timeout)?;
        let root = self.catalog.read(turn.remaining()?).await?;
        self.catalog
            .begin_topology(
                &root,
                &FleetTopologyIntentRequest {
                    identity: FleetRequestIdentity {
                        id: operation,
                        operation,
                        principal: principal.into(),
                        expected_revision: request.expected_revision,
                    },
                    keep,
                    policy: request.policy,
                },
                &self.namespaces,
                turn.remaining()?,
            )
            .await
            .map(|v| RaftFleetOperation::from_root(v, &self.catalog_id))
            .map_err(Into::into)
    }

    pub async fn cancel(
        &self,
        principal: &str,
        operation: &str,
        request: RaftFleetCancel,
        timeout: Duration,
    ) -> Result<RaftFleetOperation> {
        request.validate(operation)?;
        self.check_catalog(&request.catalog_id)?;
        let turn = Turn::new(&self.domain, timeout)?;
        let root = self.catalog.read(turn.remaining()?).await?;
        self.catalog
            .cancel_topology(
                &root,
                &FleetRequestIdentity {
                    id: records::id(&request.request_id)?,
                    operation: records::id(operation)?,
                    principal: principal.into(),
                    expected_revision: request.expected_revision,
                },
                turn.remaining()?,
            )
            .await
            .map(|v| RaftFleetOperation::from_root(v, &self.catalog_id))
            .map_err(Into::into)
    }

    pub async fn movements(
        &self,
        operation: &str,
        cursor: Option<&str>,
        limit: usize,
        timeout: Duration,
    ) -> Result<RaftFleetMovements> {
        let operation_id = operation.to_string();
        let operation = ProposalId::new(records::id(operation)?)
            .map_err(|_| RaftFleetFailure::invalid("operation ID"))?;
        if !(1..=128).contains(&limit) {
            return Err(RaftFleetFailure::invalid(
                "movement page limit must be 1–128",
            ));
        }
        let cursor = cursor
            .map(FleetCursor::from_token)
            .transpose()
            .map_err(|_| RaftFleetFailure::invalid("invalid movement cursor"))?;
        let turn = Turn::new(&self.domain, timeout)?;
        let root = self.catalog.read(turn.remaining()?).await?;
        let page = self
            .catalog
            .topology_movements(&root, operation, cursor.as_ref(), limit, turn.remaining()?)
            .await?;
        let next_cursor = page
            .next
            .as_ref()
            .map(FleetCursor::to_token)
            .transpose()
            .map_err(FleetCatalogError::from)?;
        let entries = page
            .entries
            .into_iter()
            .map(|m| RaftFleetMovement {
                reservation: hex::encode(m.reservation),
                cluster: hex::encode(m.group.cluster.bytes()),
                namespace: hex::encode(m.group.namespace.bytes()),
                group: hex::encode(m.group.group.bytes()),
                incarnation: hex::encode(m.group.incarnation.bytes()),
                cell: m.group.cell,
                source: hex::encode(m.source.bytes()),
                target: hex::encode(m.target.bytes()),
                admitted_revision: m.admitted_revision,
            })
            .collect();
        Ok(RaftFleetMovements {
            catalog_id: self.catalog_id.clone(),
            operation_id,
            observed_revision: root.revision(),
            entries,
            next_cursor,
        })
    }

    /// Observe a terminal operation without mutating it. Timeout never cancels
    /// the operation; callers retain its exact ID and can resume observation.
    pub async fn wait(
        &self,
        operation: &str,
        timeout: Duration,
        interval: Duration,
    ) -> Result<RaftFleetOperation> {
        records::id(operation)?;
        if timeout.is_zero()
            || timeout > Duration::from_secs(86_400)
            || interval < Duration::from_millis(50)
            || interval > Duration::from_secs(5)
        {
            return Err(RaftFleetFailure::invalid("invalid wait bounds"));
        }
        let now = self
            .domain
            .mono_ms()
            .map_err(|_| FleetCatalogError::Closed)?;
        let deadline = now
            .checked_add(timeout.as_millis() as u64)
            .ok_or_else(|| RaftFleetFailure::invalid("wait deadline"))?;
        loop {
            let now = self
                .domain
                .mono_ms()
                .map_err(|_| FleetCatalogError::Closed)?;
            if now >= deadline {
                return Err(RaftFleetFailure {
                    code: RaftFleetFailureCode::WaitTimeout,
                    outcome: RaftFleetOutcome::Unknown,
                    message: "Observation timed out; the fleet operation remains unchanged.".into(),
                });
            }
            match self
                .operation(
                    operation,
                    Duration::from_millis((deadline - now).min(5_000)),
                )
                .await
            {
                Ok(view) if view.terminal => return Ok(view),
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.code,
                        RaftFleetFailureCode::NotFound | RaftFleetFailureCode::Busy
                    ) => {}
                Err(error) => return Err(error),
            }
            let now = self
                .domain
                .mono_ms()
                .map_err(|_| FleetCatalogError::Closed)?;
            self.domain
                .sleep_until(deadline.min(now.saturating_add(interval.as_millis() as u64)))
                .map_err(|_| FleetCatalogError::Closed)?
                .await;
        }
    }
}

// One deadline covers all reads and admission phases in an operator call.
struct Turn<'a> {
    domain: &'a DomainToken,
    deadline: u64,
}

impl<'a> Turn<'a> {
    fn new(domain: &'a DomainToken, timeout: Duration) -> Result<Self> {
        if timeout.as_millis() == 0 || timeout > Duration::from_secs(60) {
            return Err(RaftFleetFailure::invalid(
                "operator timeout must be 1–60000 ms",
            ));
        }
        let now = domain.mono_ms().map_err(|_| FleetCatalogError::Closed)?;
        let deadline = now
            .checked_add(timeout.as_millis() as u64)
            .ok_or_else(|| RaftFleetFailure::invalid("operator deadline"))?;
        Ok(Self { domain, deadline })
    }

    async fn run<F: std::future::Future>(&self, future: F) -> Result<F::Output> {
        self.remaining()?;
        let timer = self
            .domain
            .sleep_until(self.deadline)
            .map_err(|_| FleetCatalogError::Closed)?;
        crate::asyncrt::select! {
            _ = timer => Err(FleetCatalogError::Unconfirmed.into()),
            result = self.domain.run(future) => result.map_err(|_| RaftFleetFailure::from(FleetCatalogError::Closed)),
        }
    }

    fn remaining(&self) -> Result<Duration> {
        let now = self
            .domain
            .mono_ms()
            .map_err(|_| FleetCatalogError::Closed)?;
        let ms = self
            .deadline
            .checked_sub(now)
            .filter(|ms| *ms > 0)
            .ok_or(FleetCatalogError::Unconfirmed)?;
        Ok(Duration::from_millis(ms))
    }
}
