// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Human-controlled departure, bounded proof walks and exact joined stop.
mod records;
pub(super) mod verification;
use super::*;
use crate::raft::archive::{ArchiveLimits, ArchiveVerifier};
use crate::raft::fleet_catalog::{
    FleetFinalizationAdmission, FleetRemovalAdmission, FleetRemovalRequest, FleetSnapshot,
};
use cellar_logic::consensus::ProcessGeneration;
use cellar_ltx::LtxHost;
pub use records::*;
use verification::{Key, Session, Verification};

type Phase = RaftFleetEvacuationPhase;

fn node_id(value: &str) -> Result<NodeId> {
    NodeId::new(super::records::id(value)?).map_err(|_| RaftFleetFailure::invalid("node ID"))
}

fn operation_id(value: &str) -> Result<ProposalId> {
    ProposalId::new(super::records::id(value)?)
        .map_err(|_| RaftFleetFailure::invalid("operation ID"))
}

fn progress(
    control: &RaftFleetControl,
    node: &str,
    request: &RaftFleetNodeOperation,
    phase: Phase,
    checked: u64,
    total: u64,
    observed: u64,
) -> RaftFleetEvacuationProgress {
    RaftFleetEvacuationProgress {
        catalog_id: control.catalog_id.clone(),
        node_id: node.into(),
        request_id: request.request_id.clone(),
        operation_id: request.operation_id.clone(),
        expected_revision: request.expected_revision,
        phase,
        checked_histories: checked,
        total_histories: total,
        applied_revision: None,
        observed_revision: observed,
        candidate_digest: None,
        closure_digest: None,
        process: None,
        stop_id: None,
        terminal: phase == Phase::Removed,
    }
}

fn fenced(
    control: &RaftFleetControl,
    node: &str,
    request: &RaftFleetNodeOperation,
    admission: &FleetFinalizationAdmission,
    total: u64,
) -> Result<RaftFleetEvacuationProgress> {
    let mut value = progress(
        control,
        node,
        request,
        Phase::Finalizing,
        total,
        total,
        admission.observed_revision(),
    );
    value.applied_revision = Some(admission.applied_revision());
    value.candidate_digest = Some(hex::encode(admission.candidate_digest()));
    value.process = Some(hex::encode(admission.process()?.bytes()));
    Ok(value)
}

fn removed(
    control: &RaftFleetControl,
    node: &str,
    request: &RaftFleetNodeOperation,
    admission: &FleetRemovalAdmission,
    total: u64,
) -> RaftFleetEvacuationProgress {
    let mut value = progress(
        control,
        node,
        request,
        Phase::Removed,
        total,
        total,
        admission.observed_revision(),
    );
    value.applied_revision = Some(admission.applied_revision());
    value.candidate_digest = Some(hex::encode(admission.candidate_digest()));
    value.closure_digest = Some(hex::encode(admission.closure_digest()));
    value.stop_id = Some(hex::encode(admission.stop_id()));
    value
}

fn request_from(identity: &FleetRequestIdentity, catalog: &str) -> RaftFleetNodeOperation {
    RaftFleetNodeOperation {
        catalog_id: catalog.into(),
        operation_id: hex::encode(identity.operation),
        request_id: hex::encode(identity.id),
        expected_revision: identity.expected_revision,
    }
}

impl RaftFleetControl {
    /// Explicit replay services for one bounded lifecycle proof session. The
    /// caller owns the private GenerationStore and its configured disk/memory
    /// limits. This neither creates a node nor starts a background controller.
    pub fn with_removal_verifier(
        mut self,
        host: LtxHost,
        verifier: ArchiveVerifier,
        limits: ArchiveLimits,
    ) -> Result<Self> {
        self.removal_verification = Some(Arc::new(Verification::new(host, verifier, limits)?));
        Ok(self)
    }

    fn lifecycle_observer(&self) -> Result<&RaftCells> {
        self.node_observer.as_ref().ok_or_else(|| RaftFleetFailure {
            code: RaftFleetFailureCode::Unsupported, outcome: RaftFleetOutcome::NotSubmitted,
            message: "Lifecycle verification/stop requires a trusted running observer on the management server.".into(),
        })
    }

    pub async fn evacuate_node(
        &self,
        principal: &str,
        node: &str,
        request: RaftFleetNodeOperation,
        timeout: Duration,
    ) -> Result<RaftFleetEvacuationProgress> {
        request.validate()?;
        self.check_catalog(&request.catalog_id)?;
        let identity = request.identity(principal)?;
        let node_value = node_id(node)?;
        let turn = Turn::new(&self.domain, timeout)?;
        let root = self.catalog.read(turn.remaining()?).await?;
        let commit = self
            .catalog
            .begin_evacuation_after_placement(
                &root,
                &identity,
                node_value,
                &self.namespaces,
                turn.remaining()?,
            )
            .await?;
        let admission = self
            .catalog
            .evacuation_admission(
                &commit.snapshot,
                operation_id(&request.operation_id)?,
                turn.remaining()?,
            )
            .await?;
        let mut value = progress(
            self,
            node,
            &request,
            Phase::Excluded,
            0,
            admission.histories(),
            commit.snapshot.revision(),
        );
        value.applied_revision = Some(commit.applied_revision);
        Ok(value)
    }

    /// Advance at most one history. The request/cut remains exact across
    /// successful progress calls. On server restart the ephemeral walk repeats;
    /// already committed fences reconcile before any new live proof.
    pub async fn prepare_node_removal(
        &self,
        principal: &str,
        node: &str,
        request: RaftFleetNodeOperation,
        timeout: Duration,
    ) -> Result<RaftFleetEvacuationProgress> {
        request.validate()?;
        self.check_catalog(&request.catalog_id)?;
        let identity = request.identity(principal)?;
        let node_value = node_id(node)?;
        let operation = operation_id(&request.operation_id)?;
        let turn = Turn::new(&self.domain, timeout)?;
        let root = self.catalog.read(turn.remaining()?).await?;
        let admission = self
            .catalog
            .evacuation_admission(&root, operation, turn.remaining()?)
            .await?;
        if admission.node() != node_value {
            return Err(FleetCatalogError::PayloadConflict.into());
        }
        if let Some(receipt) = self
            .catalog
            .finalization_receipt(&root, operation, turn.remaining()?)
            .await?
        {
            if receipt.identity() != &identity {
                return Err(FleetCatalogError::PayloadConflict.into());
            }
            let count = self
                .catalog
                .evacuation_candidate_histories(receipt.candidate_digest(), turn.remaining()?)
                .await?;
            return fenced(self, node, &request, &receipt, count);
        }
        self.check_lifecycle_cut(&root, &request)?;
        let observer = self.lifecycle_observer()?;
        if observer
            .observe_node(turn.remaining()?)
            .await
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .node
            .identity
            .node
            == node_value
        {
            return Err(RaftFleetFailure::invalid(
                "use a surviving observer for departure verification",
            ));
        }
        let runtime = self
            .removal_verification
            .as_ref()
            .ok_or_else(RaftFleetFailure::unsupported)?;
        let key = Key {
            identity: identity.clone(),
            node: node_value,
            stop: None,
        };
        let mut lease = runtime.lease(&key)?;
        if lease.session.is_none() {
            let walk = self
                .catalog
                .prepare_evacuation_verification(
                    &root,
                    operation,
                    &self.namespaces,
                    turn.remaining()?,
                )
                .await?;
            let total = walk.histories();
            lease.session = Some(Session {
                key,
                walk,
                checked: 0,
                total,
            });
        }
        let session = lease.session.as_mut().ok_or(FleetCatalogError::Closed)?;
        if runtime.advance(self, session, observer, &turn).await? {
            return Ok(progress(
                self,
                node,
                &request,
                Phase::VerifyingBeforeStop,
                session.checked,
                session.total,
                root.revision(),
            ));
        }
        let candidate = session.walk.publish_candidate(turn.remaining()?).await?;
        let receipt = candidate
            .begin_finalization(
                &root,
                &identity,
                observer,
                &self.namespaces,
                turn.remaining()?,
            )
            .await?;
        lease.session = None;
        fenced(self, node, &request, &receipt, candidate.histories())
    }

    fn check_lifecycle_cut(
        &self,
        root: &FleetSnapshot,
        request: &RaftFleetNodeOperation,
    ) -> Result<()> {
        if root.revision() != request.expected_revision {
            return Err(FleetCatalogError::Conflict.into());
        }
        Ok(())
    }

    /// Exact stop consent. A positive acceptance does not certify joined work;
    /// only the separately signed closure produces Joined. No infrastructure is
    /// deleted. The receiving node rechecks its current lifecycle fence.
    pub async fn stop_evacuated_node(
        &self,
        principal: &str,
        node: &str,
        request: RaftFleetNodeStop,
        timeout: Duration,
    ) -> Result<RaftFleetEvacuationProgress> {
        request.validate()?;
        self.check_catalog(&request.operation.catalog_id)?;
        let identity = request.operation.identity(principal)?;
        let node_value = node_id(node)?;
        let operation = operation_id(&request.operation.operation_id)?;
        let turn = Turn::new(&self.domain, timeout)?;
        let root = self.catalog.read(turn.remaining()?).await?;
        let admission = self
            .catalog
            .evacuation_admission(&root, operation, turn.remaining()?)
            .await?;
        if admission.node() != node_value {
            return Err(FleetCatalogError::PayloadConflict.into());
        }
        let fence = self
            .catalog
            .finalization_receipt(&root, operation, turn.remaining()?)
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        if hex::encode(fence.process()?.bytes()) != request.expected_process {
            return Err(FleetCatalogError::Conflict.into());
        }
        let count = self
            .catalog
            .evacuation_candidate_histories(fence.candidate_digest(), turn.remaining()?)
            .await?;
        let stop =
            ProposalId::new(identity.id).map_err(|_| RaftFleetFailure::invalid("stop ID"))?;
        let expected = ProcessGeneration::new(super::records::id(&request.expected_process)?)
            .map_err(|_| RaftFleetFailure::invalid("process ID"))?;
        let dispatch = self
            .catalog
            .admit_operator_stop(&root, &identity, node_value, expected, turn.remaining()?)
            .await?;
        if let Some(removed) = self
            .catalog
            .removal_receipt(&root, operation, turn.remaining()?)
            .await?
        {
            if removed.stop_id() != identity.id {
                return Err(FleetCatalogError::PayloadConflict.into());
            }
            let mut value = progress(
                self,
                node,
                &request.operation,
                Phase::Joined,
                count,
                count,
                root.revision(),
            );
            value.closure_digest = Some(hex::encode(removed.closure_digest()));
            value.process = Some(request.expected_process);
            value.stop_id = Some(request.operation.request_id.clone());
            return Ok(value);
        }
        // A signed closure survives lost stop responses; no fresh physical stop
        // is dispatched for an already joined exact selector.
        match self
            .catalog
            .verify_physical_closure(&root, operation, stop, turn.remaining()?)
            .await
        {
            Ok(closure) => {
                let mut value = progress(
                    self,
                    node,
                    &request.operation,
                    Phase::Joined,
                    count,
                    count,
                    root.revision(),
                );
                value.closure_digest = Some(hex::encode(closure.digest()));
                value.process = Some(request.expected_process);
                value.stop_id = Some(request.operation.request_id.clone());
                return Ok(value);
            }
            Err(FleetCatalogError::Unconfirmed) => (),
            Err(error) => return Err(error.into()),
        }
        self.check_lifecycle_cut(&root, &request.operation)?;
        let accepted = self
            .lifecycle_observer()?
            .request_peer_fleet_stop(dispatch, root.revision(), turn.remaining()?)
            .await?;
        if accepted.request() != identity.id {
            return Err(FleetCatalogError::Unconfirmed.into());
        }
        let mut value = progress(
            self,
            node,
            &request.operation,
            Phase::StopRequested,
            count,
            count,
            root.revision(),
        );
        value.process = Some(request.expected_process);
        value.stop_id = Some(request.operation.request_id.clone());
        Ok(value)
    }

    /// One post-fence history per successful call. Terminal CAS still owns fresh
    /// survivor/physical revalidation and the exact joined-closure signature.
    pub async fn remove_node(
        &self,
        principal: &str,
        node: &str,
        request: RaftFleetNodeRemove,
        timeout: Duration,
    ) -> Result<RaftFleetEvacuationProgress> {
        request.validate()?;
        self.check_catalog(&request.operation.catalog_id)?;
        let identity = request.operation.identity(principal)?;
        let node_value = node_id(node)?;
        let operation = operation_id(&request.operation.operation_id)?;
        let stop_id = super::records::id(&request.stop_id)?;
        let turn = Turn::new(&self.domain, timeout)?;
        let root = self.catalog.read(turn.remaining()?).await?;
        let admission = self
            .catalog
            .evacuation_admission(&root, operation, turn.remaining()?)
            .await?;
        if admission.node() != node_value {
            return Err(FleetCatalogError::PayloadConflict.into());
        }
        let intent = FleetRemovalRequest {
            identity: identity.clone(),
            stop: ProposalId::new(stop_id).map_err(|_| RaftFleetFailure::invalid("stop ID"))?,
        };
        // Exact durable retries need no observer, replay service or proof cache.
        match self
            .catalog
            .complete_evacuation(&root, &intent, None, &[], turn.remaining()?)
            .await
        {
            Ok(receipt) => {
                let count = self
                    .catalog
                    .evacuation_candidate_histories(receipt.candidate_digest(), turn.remaining()?)
                    .await?;
                return Ok(removed(self, node, &request.operation, &receipt, count));
            }
            Err(FleetCatalogError::InventoryIncomplete) => (),
            Err(error) => return Err(error.into()),
        }
        self.check_lifecycle_cut(&root, &request.operation)?;
        self.catalog
            .verify_physical_closure(&root, operation, intent.stop, turn.remaining()?)
            .await?;
        let observer = self.lifecycle_observer()?;
        let runtime = self
            .removal_verification
            .as_ref()
            .ok_or_else(RaftFleetFailure::unsupported)?;
        let key = Key {
            identity,
            node: node_value,
            stop: Some(stop_id),
        };
        let mut lease = runtime.lease(&key)?;
        if lease.session.is_none() {
            let walk = self
                .catalog
                .prepare_evacuation_verification(
                    &root,
                    operation,
                    &self.namespaces,
                    turn.remaining()?,
                )
                .await?;
            let total = walk.histories();
            lease.session = Some(Session {
                key,
                walk,
                checked: 0,
                total,
            });
        }
        let session = lease.session.as_mut().ok_or(FleetCatalogError::Closed)?;
        if runtime.advance(self, session, observer, &turn).await? {
            let mut value = progress(
                self,
                node,
                &request.operation,
                Phase::VerifyingRemoval,
                session.checked,
                session.total,
                root.revision(),
            );
            value.stop_id = Some(request.stop_id.clone());
            return Ok(value);
        }
        let receipt = self
            .catalog
            .complete_evacuation(
                &root,
                &intent,
                Some(&mut session.walk),
                &self.namespaces,
                turn.remaining()?,
            )
            .await?;
        let count = session.checked;
        lease.session = None;
        Ok(removed(self, node, &request.operation, &receipt, count))
    }

    pub async fn evacuation_status(
        &self,
        operation: &str,
        timeout: Duration,
    ) -> Result<RaftFleetEvacuationStatus> {
        let op = operation_id(operation)?;
        let turn = Turn::new(&self.domain, timeout)?;
        let root = self.catalog.read(turn.remaining()?).await?;
        let admission = self
            .catalog
            .evacuation_admission(&root, op, turn.remaining()?)
            .await?;
        let node = hex::encode(admission.node().bytes());
        let current = self
            .catalog
            .describe_node(&root, admission.node(), turn.remaining()?)
            .await?;
        let finalization = match self
            .catalog
            .finalization_receipt(&root, op, turn.remaining()?)
            .await?
        {
            Some(r) => {
                let request = request_from(r.identity(), &self.catalog_id);
                let count = self
                    .catalog
                    .evacuation_candidate_histories(r.candidate_digest(), turn.remaining()?)
                    .await?;
                Some(fenced(self, &node, &request, &r, count)?)
            }
            None => None,
        };
        let removal = match self
            .catalog
            .removal_receipt(&root, op, turn.remaining()?)
            .await?
        {
            Some(r) => {
                let request = request_from(r.identity(), &self.catalog_id);
                let count = self
                    .catalog
                    .evacuation_candidate_histories(r.candidate_digest(), turn.remaining()?)
                    .await?;
                Some(removed(self, &node, &request, &r, count))
            }
            None => None,
        };
        let pending_movements = self
            .catalog
            .pending_reservations(&root, turn.remaining()?)
            .await?
            .into_iter()
            .map(|m| RaftFleetEvacuationDebt {
                reservation: hex::encode(m.reservation),
                cluster: hex::encode(m.group.cluster.bytes()),
                namespace: hex::encode(m.group.namespace.bytes()),
                group: hex::encode(m.group.group.bytes()),
                incarnation: hex::encode(m.group.incarnation.bytes()),
                cell: m.group.cell,
                target: hex::encode(m.target.bytes()),
                peak_engine_slots: m.peak.engine_slots,
                peak_durable_bytes: m.peak.durable_bytes,
                peak_memory_bytes: m.peak.memory_bytes,
                boot: hex::encode(m.boot.boot),
                executable_movement: m.executable_movement,
            })
            .collect();
        if self.catalog.read(turn.remaining()?).await?.revision() != root.revision() {
            return Err(FleetCatalogError::Conflict.into());
        }
        Ok(RaftFleetEvacuationStatus {
            catalog_id: self.catalog_id.clone(),
            operation_id: operation.into(),
            node_id: node,
            state: current.state,
            observed_revision: root.revision(),
            exclusion_revision: admission.applied_revision(),
            finalization,
            removal,
            pending_movements,
        })
    }
}
