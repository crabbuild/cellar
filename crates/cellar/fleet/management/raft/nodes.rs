// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Operator node enrollment and explicit deployment capacity attestations.
use super::*;
use crate::raft::fleet_catalog::{FleetCapacityAction, FleetCapacityRequest, FleetNodeSpec};
use cellar_logic::consensus::{DiskLineage, EngineInstanceId, IncarnationId};
use serde::{Deserialize, Serialize};

pub use crate::raft::fleet_catalog::{
    FleetCapacityAttestation as RaftFleetCapacityAttestation, FleetNodeState as RaftFleetNodeState,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetNodeDescriptor {
    pub node_id: String,
    pub disk_id: String,
    pub physical_host: String,
    pub label: String,
    pub server_name: String,
    pub capability_digest: String,
    pub storage_instance: Option<String>,
}

impl RaftFleetNodeDescriptor {
    fn spec(&self) -> Result<FleetNodeSpec> {
        records::catalog_id(&self.capability_digest)?;
        let mut capability_digest = [0; 32];
        hex::decode_to_slice(&self.capability_digest, &mut capability_digest)
            .map_err(|_| RaftFleetFailure::invalid("capability digest"))?;
        if self.label.len() > 128
            || crate::machine::validate_node_name(&self.label).is_err()
            || self.server_name.is_empty()
            || self.server_name.len() > 255
            || rustls::pki_types::ServerName::try_from(self.server_name.clone()).is_err()
        {
            return Err(RaftFleetFailure::invalid("invalid node descriptor"));
        }
        Ok(FleetNodeSpec {
            node: NodeId::new(records::id(&self.node_id)?)
                .map_err(|_| RaftFleetFailure::invalid("node ID"))?,
            disk: DiskLineage::new(records::id(&self.disk_id)?)
                .map_err(|_| RaftFleetFailure::invalid("disk ID"))?,
            physical_host: IncarnationId::new(records::id(&self.physical_host)?)
                .map_err(|_| RaftFleetFailure::invalid("physical host ID"))?,
            label: self.label.clone(),
            server_name: self.server_name.clone(),
            capability_digest,
            storage_instance: self
                .storage_instance
                .as_deref()
                .map(|value| {
                    EngineInstanceId::new(records::id(value)?)
                        .map_err(|_| RaftFleetFailure::invalid("engine instance ID"))
                })
                .transpose()?,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetRegisterNode {
    pub catalog_id: String,
    pub request_id: String,
    pub expected_revision: u64,
    pub descriptor: RaftFleetNodeDescriptor,
}

impl RaftFleetRegisterNode {
    pub fn validate(&self) -> Result<()> {
        validate_request(&self.catalog_id, &self.request_id, self.expected_revision)?;
        self.descriptor.spec()?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetNodeCapacity {
    pub catalog_id: String,
    pub request_id: String,
    pub expected_revision: u64,
    /// Explicit assertion by the capacity-granted deployment operator. The
    /// server still owns the independent authenticated observation/inventory.
    /// Retain these exact bytes/evidence for ambiguous retries after a reboot.
    pub attestation: RaftFleetCapacityAttestation,
}

impl RaftFleetNodeCapacity {
    pub fn validate(&self, node: &str) -> Result<()> {
        validate_request(&self.catalog_id, &self.request_id, self.expected_revision)?;
        if records::id(node)? != self.attestation.boot.node
            || hex::encode(self.attestation.boot.catalog) != self.catalog_id
        {
            return Err(RaftFleetFailure::invalid("capacity node/catalog binding"));
        }
        Ok(())
    }
}

fn validate_request(catalog: &str, request: &str, revision: u64) -> Result<()> {
    records::catalog_id(catalog)?;
    records::id(request)?;
    records::revision(revision)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RaftFleetNodeAction {
    Register,
    Qualify,
    Activate,
}

impl RaftFleetNodeAction {
    fn state(self) -> RaftFleetNodeState {
        match self {
            Self::Register => RaftFleetNodeState::Registered,
            Self::Qualify => RaftFleetNodeState::Ready,
            Self::Activate => RaftFleetNodeState::Active,
        }
    }
}

/// Historical admission. Later node state, boot or capacity may differ. This
/// receipt grants no membership, future health lease or storage-release proof.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetNodeAdmission {
    pub catalog_id: String,
    pub request_id: String,
    pub principal: String,
    pub expected_revision: u64,
    pub node_id: String,
    pub action: RaftFleetNodeAction,
    pub admitted_state: RaftFleetNodeState,
    pub applied_revision: u64,
    pub observed_revision: u64,
}

impl RaftFleetNodeAdmission {
    pub fn validate(&self) -> Result<()> {
        validate_request(&self.catalog_id, &self.request_id, self.expected_revision)?;
        records::id(&self.node_id)?;
        if self.principal.is_empty()
            || self.principal.len() > 128
            || self.principal.chars().any(char::is_control)
            || self.admitted_state != self.action.state()
            || self.applied_revision <= self.expected_revision
            || self.observed_revision < self.applied_revision
        {
            return Err(RaftFleetFailure::invalid("inconsistent node admission"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetNodeAcceptance {
    pub admission: RaftFleetNodeAdmission,
    pub reconciled: bool,
}

/// Descriptive state at one catalog cut, without a liveness or health claim.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetNodeStatus {
    pub catalog_id: String,
    pub observed_revision: u64,
    pub descriptor: RaftFleetNodeDescriptor,
    pub state: RaftFleetNodeState,
    /// Present only if the exact current rooted boot/epoch has a valid typed
    /// capacity record. Ready is qualified but cannot receive a new replica.
    pub capacity: Option<RaftFleetCapacityAttestation>,
}

impl RaftFleetNodeStatus {
    pub fn validate(&self) -> Result<()> {
        records::catalog_id(&self.catalog_id)?;
        records::revision(self.observed_revision)?;
        self.descriptor.spec()?;
        if let Some(capacity) = &self.capacity {
            if capacity.boot.node != records::id(&self.descriptor.node_id)?
                || hex::encode(capacity.boot.catalog) != self.catalog_id
                || !matches!(
                    self.state,
                    RaftFleetNodeState::Ready | RaftFleetNodeState::Active
                )
            {
                return Err(RaftFleetFailure::invalid("inconsistent node capacity"));
            }
        }
        Ok(())
    }
}

fn identity(principal: &str, request: &str, revision: u64) -> Result<FleetRequestIdentity> {
    if principal.is_empty() || principal.len() > 128 || principal.chars().any(char::is_control) {
        return Err(RaftFleetFailure::invalid("invalid operator principal"));
    }
    let id = records::id(request)?;
    Ok(FleetRequestIdentity {
        id,
        operation: id,
        principal: principal.into(),
        expected_revision: revision,
    })
}

impl RaftFleetControl {
    /// Install a trusted running observer for capacity probes. This starts no
    /// node, transport or controller and does not infer any deployment limits.
    /// Existing-root standalone connections retain no observer by default.
    pub fn with_node_observer(mut self, observer: RaftCells) -> Self {
        self.node_observer = Some(observer);
        self
    }

    pub async fn node(&self, node: &str, timeout: Duration) -> Result<RaftFleetNodeStatus> {
        let node =
            NodeId::new(records::id(node)?).map_err(|_| RaftFleetFailure::invalid("node ID"))?;
        let turn = Turn::new(&self.domain, timeout)?;
        let root = self.catalog.read(turn.remaining()?).await?;
        let host = self
            .catalog
            .describe_node(&root, node, turn.remaining()?)
            .await?;
        let capacity = match self
            .catalog
            .authorize_capacity(&root, node, turn.remaining()?)
            .await
        {
            Ok(value) => Some(value.attestation().clone()),
            Err(FleetCatalogError::Conflict) => None,
            Err(error) => return Err(error.into()),
        };
        // A conflict may mean concurrent root movement, not absent capacity.
        // Refuse a mixed-cut response even when the optional proof was absent.
        if self.catalog.read(turn.remaining()?).await?.revision() != root.revision() {
            return Err(FleetCatalogError::Conflict.into());
        }
        Ok(RaftFleetNodeStatus {
            catalog_id: self.catalog_id.clone(),
            observed_revision: root.revision(),
            descriptor: RaftFleetNodeDescriptor {
                node_id: hex::encode(host.spec.node.bytes()),
                disk_id: hex::encode(host.spec.disk.bytes()),
                physical_host: hex::encode(host.spec.physical_host.bytes()),
                label: host.spec.label,
                server_name: host.spec.server_name,
                capability_digest: hex::encode(host.spec.capability_digest),
                storage_instance: host.spec.storage_instance.map(|v| hex::encode(v.bytes())),
            },
            state: host.state,
            capacity,
        })
    }

    pub async fn register_node(
        &self,
        principal: &str,
        request: RaftFleetRegisterNode,
        timeout: Duration,
    ) -> Result<RaftFleetNodeAdmission> {
        request.validate()?;
        self.check_catalog(&request.catalog_id)?;
        let identity = identity(principal, &request.request_id, request.expected_revision)?;
        let turn = Turn::new(&self.domain, timeout)?;
        let root = self.catalog.read(turn.remaining()?).await?;
        let commit = self
            .catalog
            .register_node(
                &root,
                &identity,
                &request.descriptor.spec()?,
                turn.remaining()?,
            )
            .await?;
        Ok(RaftFleetNodeAdmission {
            catalog_id: self.catalog_id.clone(),
            request_id: request.request_id,
            principal: principal.into(),
            expected_revision: request.expected_revision,
            node_id: request.descriptor.node_id,
            action: RaftFleetNodeAction::Register,
            admitted_state: RaftFleetNodeState::Registered,
            applied_revision: commit.applied_revision,
            observed_revision: commit.snapshot.revision(),
        })
    }

    pub async fn qualify_node(
        &self,
        principal: &str,
        node: &str,
        request: RaftFleetNodeCapacity,
        timeout: Duration,
    ) -> Result<RaftFleetNodeAdmission> {
        self.node_capacity(
            principal,
            node,
            request,
            RaftFleetNodeAction::Qualify,
            timeout,
        )
        .await
    }

    pub async fn activate_node(
        &self,
        principal: &str,
        node: &str,
        request: RaftFleetNodeCapacity,
        timeout: Duration,
    ) -> Result<RaftFleetNodeAdmission> {
        self.node_capacity(
            principal,
            node,
            request,
            RaftFleetNodeAction::Activate,
            timeout,
        )
        .await
    }

    async fn node_capacity(
        &self,
        principal: &str,
        node: &str,
        request: RaftFleetNodeCapacity,
        action: RaftFleetNodeAction,
        timeout: Duration,
    ) -> Result<RaftFleetNodeAdmission> {
        request.validate(node)?;
        self.check_catalog(&request.catalog_id)?;
        let observer = self.node_observer.as_ref().ok_or_else(|| RaftFleetFailure {
            code: RaftFleetFailureCode::Unsupported, outcome: RaftFleetOutcome::NotSubmitted,
            message: "Node qualification requires a trusted live observer configured on the management server.".into(),
        })?;
        let identity = identity(principal, &request.request_id, request.expected_revision)?;
        let turn = Turn::new(&self.domain, timeout)?;
        let root = self.catalog.read(turn.remaining()?).await?;
        let admitted = self
            .catalog
            .admit_node_capacity(
                &root,
                &FleetCapacityRequest {
                    identity,
                    attestation: request.attestation,
                    action: match action {
                        RaftFleetNodeAction::Qualify => FleetCapacityAction::Qualify,
                        RaftFleetNodeAction::Activate => FleetCapacityAction::Activate,
                        RaftFleetNodeAction::Register => {
                            return Err(RaftFleetFailure::invalid("capacity action"))
                        }
                    },
                },
                observer,
                &self.namespaces,
                turn.remaining()?,
            )
            .await?;
        Ok(RaftFleetNodeAdmission {
            catalog_id: self.catalog_id.clone(),
            request_id: request.request_id,
            principal: principal.into(),
            expected_revision: request.expected_revision,
            node_id: node.into(),
            action,
            admitted_state: admitted.state(),
            applied_revision: admitted.applied_revision(),
            observed_revision: admitted.observed_revision(),
        })
    }
}
