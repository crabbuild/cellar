// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Canonical SDK handles bind logical cell histories. Physical group controls
//! are separate values; no cell handle owns a group, worker or permanent map entry.
use super::{CommandId, Durability, OperationContract};
use crate::raft::{
    cell_groups::*,
    storage::{LogError, ReplicaLog},
    version::{CellVersionObservation, CellVersionToken},
};
use cellar_logic::{
    cell_group::{CellHistory, CellVersion, CommitWitness, GroupHistory},
    consensus::{EngineInstanceId, MemberId, NodeIdentity},
};
use std::sync::atomic::Ordering;
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

pub use super::client::CommandOptions;
pub use crate::raft::cell_groups::CellQueryRouting as QueryRouting;

#[derive(Clone, Debug)]
pub enum QueryConsistency {
    Stale,
    AtLeast(CellVersionToken),
    Linearizable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StaleReadPolicy {
    RecentSource,
    DisconnectedHistorical,
}

#[derive(Clone, Debug)]
pub struct QueryOptions {
    /// Defaults to a fresh quorum read. Historical reads require explicit opt-in.
    pub consistency: QueryConsistency,
    pub stale_policy: StaleReadPolicy,
    pub timeout: Duration,
    pub max_attempts: u8,
    pub routing: QueryRouting,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            consistency: QueryConsistency::Linearizable,
            stale_policy: StaleReadPolicy::RecentSource,
            timeout: Duration::from_secs(5),
            max_attempts: 4,
            routing: QueryRouting::Automatic,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandReceipt {
    pub id: CommandId,
    pub result: Vec<u8>,
    /// Selected logical cell version covering this exact stored receipt.
    pub version: CellVersion,
    pub witness: CommitWitness,
    pub version_token: CellVersionToken,
    pub durability: Durability,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryResult {
    pub result: Vec<u8>,
    pub version: CellVersion,
    pub witness: CommitWitness,
    pub version_token: CellVersionToken,
    pub served_by: MemberId,
    pub served_by_node: NodeIdentity,
}

mod actions;
mod close;
mod drain;
mod fleet;
mod hibernation;
mod learner;
mod membership;
mod node;
mod observation;
mod origins;
mod recovery;
mod repair;
mod services;
mod startup;
mod stop;
mod transition;
pub use membership::CellGroupControlError;
pub(crate) use services::{ControlServiceCompletion, ControlServiceKind, ControlServiceLease};
pub use startup::CellGroupStartupError;
pub(crate) mod resources;
use close::Close;
pub use close::{MutationOutcomes, RaftCloseError, RaftCloseReport, RaftGroupCloseReport};
pub use resources::{
    CellGroupCollectionFailure, CellGroupCollectionUsage, CellGroupImageUsage,
    CellGroupResourceUsage,
};

struct Core {
    host: CellGroupHost,
    controller: CellGroupController,
    runtime: CellGroupRuntime,
    log: ReplicaLog,
    node: NodeIdentity,
    engine: EngineInstanceId,
    gateways: Mutex<Vec<(CellQueryGateway, CellCommandGateway)>>,
    resources: resources::Resources,
    close_limits: crate::raft::operations::RaftCloseReportLimits,
    services: Arc<services::Services>,
    managed: Option<crate::raft::fleet_catalog::FleetHostAuthorization>,
    fleet_inspection: Option<crate::raft::fleet_catalog::FleetTrustReader>,
    transport: crate::raft::transport::RaftTransport,
    archive_io: Option<crate::raft::cell_groups::CellGroupArchivePool>,
    node_profile: Option<crate::raft::inspection::RaftNodeProfile>,
    startup_bounds: crate::raft::host::RaftHostStartupBounds,
    stale_source_timeout_ms: u64,
}

pub(crate) struct Handle {
    pub(crate) domain: crate::asyncrt::DomainToken,
    pub(crate) admission: Arc<super::admission::Admission>,
    core: Arc<Core>,
    close: Arc<Close>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.close.request();
    }
}

struct ExternalLifetime {
    close: Arc<Close>,
}

impl Drop for ExternalLifetime {
    fn drop(&mut self) {
        self.close.request();
    }
}

pub struct RaftCells {
    pub(crate) handle: Arc<Handle>,
    // Internal service observers retain metadata without keeping the last
    // external SDK owner alive. They cannot indefinitely self-own this node.
    lifetime: Option<Arc<ExternalLifetime>>,
}

impl Clone for RaftCells {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            lifetime: self.lifetime.clone(),
        }
    }
}

/// Physical services transferred together into the SDK's joined close owner.
pub(crate) struct Infrastructure {
    pub runtime: CellGroupRuntime,
    pub log: ReplicaLog,
    pub transport: crate::raft::transport::RaftTransport,
    pub archive_io: Option<crate::raft::cell_groups::CellGroupArchivePool>,
}

impl RaftCells {
    pub(crate) fn from_shared(
        host: CellGroupHost,
        controller: CellGroupController,
        infrastructure: Infrastructure,
        admission: Arc<super::admission::Admission>,
        resources: resources::Resources,
        options: &crate::raft::host::RaftHostOptions,
    ) -> Result<Self, CellGroupControllerError> {
        let Infrastructure {
            runtime,
            log,
            transport,
            archive_io,
        } = infrastructure;
        let domain = log.domain();
        let node = NodeIdentity {
            node: log.limits().node,
            disk: log.limits().disk,
            process: log.limits().process,
        };
        let engine = log
            .storage_instance()
            .map_err(|_| CellGroupControllerError::Invalid("SDK engine identity"))?;
        controller.bind_public_admission(
            admission.clone(),
            crate::asyncrt::services().application_work_registry(),
        )?;
        let close = Close::new(admission.clone());
        let startup_bounds = node::startup_bounds(&controller, &runtime, &log, options)?;
        let core = Arc::new(Core {
            host,
            controller,
            runtime,
            log,
            node,
            engine,
            gateways: Mutex::new(Vec::new()),
            resources,
            close_limits: options.close_report,
            services: services::Services::new(
                options.max_control_bytes,
                options.max_control_requests,
            ),
            managed: options
                .managed
                .as_ref()
                .map(|managed| managed.authorization.clone()),
            fleet_inspection: options
                .managed
                .as_ref()
                .map(|managed| managed.authorization.trust_reader()),
            transport,
            archive_io,
            node_profile: options.node_profile.clone(),
            startup_bounds,
            stale_source_timeout_ms: options.stale_source_timeout_ms,
        });
        let service = Self {
            handle: Arc::new(Handle {
                domain: domain.clone(),
                admission,
                core: core.clone(),
                close: close.clone(),
            }),
            lifetime: Some(Arc::new(ExternalLifetime {
                close: close.clone(),
            })),
        };
        close::start(core, close, domain)?;
        service
            .handle
            .core
            .transport
            .install_group_fleet_gateway(&service)
            .map_err(|_| CellGroupControllerError::Invalid("fleet metadata gateway"))?;
        Ok(service)
    }

    pub(crate) fn from_internal_handle(handle: Arc<Handle>) -> Self {
        Self {
            handle,
            lifetime: None,
        }
    }

    pub(crate) fn uses_transport(&self, transport: &crate::raft::transport::RaftTransport) -> bool {
        transport.uses_shared_runtime(&self.handle.core.runtime)
    }

    pub(crate) fn retain_gateways(
        &self,
        query: CellQueryGateway,
        command: CellCommandGateway,
    ) -> Result<(), CellGroupControllerError> {
        self.handle
            .core
            .gateways
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?
            .push((query, command));
        Ok(())
    }

    pub fn cell(&self, history: CellHistory) -> Result<RaftCellHandle, CellDispatchError> {
        self.check_open()?;
        let cell = self.handle.core.host.cell(history)?;
        Ok(RaftCellHandle {
            owner: self.clone(),
            cell,
        })
    }

    /// Administrative identity is an entire physical group, never a cell alias.
    pub fn group(
        &self,
        history: GroupHistory,
    ) -> Result<RaftGroupHandle, crate::raft::completion::RuntimeError> {
        self.handle
            .core
            .log
            .cell_group(&history)
            .map_err(|_| crate::raft::completion::RuntimeError::NotFound)?;
        Ok(RaftGroupHandle {
            owner: self.clone(),
            history,
        })
    }

    fn check_open(&self) -> Result<(), CellDispatchError> {
        if self.is_closed() {
            return Err(CellDispatchError::Runtime(
                crate::raft::completion::RuntimeError::Closed,
            ));
        }
        Ok(())
    }

    /// Immutable identity of this node's log owner. This is metadata, not a live
    /// membership, serving, fencing or restart authorization.
    pub fn identity(&self) -> NodeIdentity {
        self.handle.core.node
    }

    pub fn is_closed(&self) -> bool {
        self.handle.close.closing.load(Ordering::Acquire) || self.handle.core.host.is_closed()
    }

    pub fn resource_usage(&self) -> CellGroupResourceUsage {
        self.handle.core.resources.capture(&self.handle.core)
    }

    pub fn performance(&self) -> super::RaftPerformanceSnapshot {
        self.handle.core.log.performance_snapshot()
    }

    pub(crate) fn begin_node_drain(&self) {
        let _ = self.handle.admission.seal();
    }

    /// Request joined closure before returning the waiter. Cancellation cannot
    /// abandon the physical close or turn unknown mutations into failed writes.
    pub fn close(
        &self,
    ) -> impl Future<Output = Result<RaftCloseReport, Arc<RaftCloseError>>> + Send + 'static {
        self.handle.close.request();
        let close = self.handle.close.clone();
        async move { close.wait().await }
    }
}

#[derive(Clone)]
pub struct RaftCellHandle {
    owner: RaftCells,
    cell: CellGroupCellHandle,
}

/// Effect routing cannot keep its own host alive. Recreate a disposable logical
/// handle from the same immutable namespace registry while that owner exists.
#[derive(Clone)]
pub(crate) struct WeakCellHandle {
    owner: std::sync::Weak<Handle>,
    history: CellHistory,
}

impl WeakCellHandle {
    pub(crate) fn upgrade(&self) -> Result<Option<RaftCellHandle>, CellDispatchError> {
        let Some(handle) = self.owner.upgrade() else {
            return Ok(None);
        };
        RaftCells {
            handle,
            lifetime: None,
        }
        .cell(self.history.clone())
        .map(Some)
    }
}

impl RaftCellHandle {
    pub(crate) fn downgrade(&self) -> WeakCellHandle {
        WeakCellHandle {
            owner: Arc::downgrade(&self.owner.handle),
            history: self.history().clone(),
        }
    }

    pub(crate) fn definition_digest(&self) -> [u8; 32] {
        self.cell.definition_digest()
    }

    pub fn history(&self) -> &CellHistory {
        self.cell.history()
    }

    pub fn contract(&self, operation: &str) -> Result<OperationContract, super::RegistrationError> {
        self.cell.contract(operation)
    }

    pub fn command(
        &self,
        contract: &OperationContract,
        input: Vec<u8>,
        options: CommandOptions,
    ) -> impl Future<Output = Result<CommandReceipt, Arc<CellCommandError>>> + Send + 'static {
        let id = options.id;
        let accepted = self
            .owner
            .check_open()
            .map_err(|cause| CellCommandError {
                command: id,
                ambiguous: false,
                cause: Box::new(cause),
            })
            .and_then(|()| {
                self.cell.command(
                    CellCommandRequest {
                        id,
                        contract: contract.clone(),
                        input,
                        durability: options.durability,
                        retry_until_ms: options.retry_until_ms,
                    },
                    CellRoutingLimits {
                        timeout: options.timeout,
                        max_attempts: 4,
                    },
                )
            });
        let owner = self.owner.clone();
        async move {
            let _owner = owner;
            let response = accepted.map_err(Arc::new)?.response().await?;
            let CellVersionObservation { version, witness } = response.observation();
            Ok(CommandReceipt {
                id,
                result: response.result().to_vec(),
                version,
                witness,
                version_token: response.token().clone(),
                durability: response.durability(),
            })
        }
    }

    pub fn query(
        &self,
        invocation: super::ProposalId,
        contract: &OperationContract,
        input: Vec<u8>,
        options: QueryOptions,
    ) -> impl Future<Output = Result<QueryResult, Arc<CellDispatchError>>> + Send + 'static {
        let accepted = self.owner.check_open().and_then(|()| {
            if !matches!(options.consistency, QueryConsistency::Stale)
                && options.stale_policy != StaleReadPolicy::RecentSource
            {
                return Err(CellDispatchError::Invalid(
                    "stale policy requires stale consistency",
                ));
            }
            let consistency = match options.consistency {
                QueryConsistency::Stale => match options.stale_policy {
                    StaleReadPolicy::RecentSource => CellQueryConsistency::RecentSource {
                        max_age_ms: self.owner.handle.core.stale_source_timeout_ms,
                    },
                    StaleReadPolicy::DisconnectedHistorical => CellQueryConsistency::Stale,
                },
                QueryConsistency::AtLeast(token) => CellQueryConsistency::AtLeast(token),
                QueryConsistency::Linearizable => CellQueryConsistency::Linearizable,
            };
            self.cell.query_routed(
                CellQueryRequest {
                    invocation,
                    contract: contract.clone(),
                    input,
                    consistency,
                },
                CellRoutingLimits {
                    timeout: options.timeout,
                    max_attempts: options.max_attempts,
                },
                options.routing,
            )
        });
        let owner = self.owner.clone();
        async move {
            let _owner = owner;
            let response = accepted.map_err(Arc::new)?.response().await?;
            let CellVersionObservation { version, witness } = response.observation().clone();
            Ok(QueryResult {
                result: response.result().to_vec(),
                version,
                witness,
                version_token: response.token().clone(),
                served_by: response.replica().member,
                served_by_node: response.replica().node_identity(),
            })
        }
    }

    pub fn enroll(
        &self,
        request: CellEnrollmentRequest,
        timeout: Duration,
    ) -> impl Future<Output = Result<CellEnrollmentResponse, Arc<CellEnrollmentError>>> + Send + 'static
    {
        let response = self.cell.enroll(
            request,
            CellRoutingLimits {
                timeout,
                max_attempts: 4,
            },
        );
        let owner = self.owner.clone();
        async move {
            let _owner = owner;
            response.await
        }
    }

    pub fn receipt(
        &self,
        request: CellReceiptRequest,
        timeout: Duration,
    ) -> impl Future<Output = Result<CellReceiptResponse, Arc<CellDispatchError>>> + Send + 'static
    {
        let response = self.cell.receipt(
            request,
            CellRoutingLimits {
                timeout,
                max_attempts: 4,
            },
        );
        let owner = self.owner.clone();
        async move {
            let _owner = owner;
            response.await
        }
    }
}

#[derive(Clone)]
pub struct RaftGroupHandle {
    owner: RaftCells,
    history: GroupHistory,
}

impl RaftGroupHandle {
    pub fn history(&self) -> GroupHistory {
        self.history
    }

    pub async fn status(&self) -> Result<CellGroupStatus, crate::raft::completion::RuntimeError> {
        self.owner.handle.core.runtime.status(self.history).await
    }

    /// A fresh quorum observation of the complete physical configuration. The
    /// deadline starts at invocation, including time before this future polls.
    /// The returned metadata is not a lease or permission to release fleet debt.
    pub fn quorum_membership(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<CellGroupQuorumMembership, crate::raft::completion::RuntimeError>>
           + Send
           + 'static {
        use crate::raft::completion::RuntimeError;
        let deadline = (|| {
            if self.owner.is_closed() {
                return Err(RuntimeError::Closed);
            }
            if timeout.is_zero() || timeout > Duration::from_secs(60) {
                return Err(RuntimeError::Invalid("group observation timeout".into()));
            }
            self.owner
                .handle
                .domain
                .mono_ms()
                .map_err(|_| RuntimeError::Closed)?
                .checked_add(timeout.as_millis() as u64)
                .ok_or(RuntimeError::Overloaded)
        })();
        let owner = self.owner.clone();
        let history = self.history;
        async move {
            owner
                .handle
                .core
                .runtime
                .quorum_membership(history, deadline?)
                .await
        }
    }

    /// Admission only; observe subsequent election status independently.
    pub async fn transfer_leader(
        &self,
        member: MemberId,
    ) -> Result<(), crate::raft::completion::RuntimeError> {
        self.owner
            .handle
            .core
            .runtime
            .transfer_leader(self.history, member.get())
            .await
    }
}
