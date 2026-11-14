// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Namespace registrations over one shared node controller. Cell handles are
//! disposable values; obtaining or cloning one neither enrolls nor caches a cell.
mod bindings;
mod sdk_dispatch;
mod storage;
pub use storage::CellGroupNamespaceStorage;

use super::*;
use crate::application::consensus::{OperationContract, RaftClass, RegistrationError};
use crate::raft::{
    budget::{Budget, Permit},
    completion::RuntimeError,
    transport::RaftTransport,
};
use cellar_logic::{
    cell_group::CellHistory,
    consensus::{ClusterId, NamespaceId},
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

pub struct CellGroupHostRegistration {
    pub dispatcher: CellGroupDispatcher,
    pub storage: CellGroupNamespaceStorage,
}

#[derive(Clone, Copy, Debug)]
pub struct CellGroupHostLimits {
    pub max_namespaces: usize,
    /// Includes each registration's configured invocation, route-cache and
    /// directory-cache capacities, not only their current occupancy.
    pub namespace_bytes: usize,
}

struct Binding {
    dispatcher: CellGroupDispatcher,
    class: Arc<RaftClass>,
    storage: Mutex<Option<CellGroupNamespaceStorage>>,
    _admission: Permit,
}

struct Inner {
    controller: CellGroupController,
    domain: crate::asyncrt::DomainToken,
    namespaces: Arc<BTreeMap<(ClusterId, NamespaceId), Arc<Binding>>>,
    budget: Arc<Budget>,
    payloads: Option<CellGroupPayloadPool>,
    closed: AtomicBool,
}

/// Bind compiled namespaces to an already initialized shared node controller.
/// Group placement and physical bootstrap remain node-startup responsibilities;
/// these registrations grant no voting, leader or serving authority.
#[derive(Clone)]
pub struct CellGroupHost {
    inner: Arc<Inner>,
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct CellGroupHostUsage {
    pub namespaces: usize,
    pub namespace_bytes: usize,
    pub namespace_byte_limit: usize,
    pub payloads: Option<CellGroupPayloadUsage>,
    pub payload_byte_limit: usize,
    pub controller: CellGroupControllerUsage,
}

impl CellGroupHost {
    pub fn bind(
        runtime: &CellGroupRuntime,
        sql: &CellGroupSqlPool,
        transport: &RaftTransport,
        controller: CellGroupController,
        registrations: Vec<CellGroupHostRegistration>,
        limits: CellGroupHostLimits,
    ) -> Result<Self, CellDispatchError> {
        Self::bind_with_payloads(
            runtime,
            sql,
            transport,
            controller,
            registrations,
            limits,
            None,
        )
    }

    pub(crate) fn bind_with_payloads(
        runtime: &CellGroupRuntime,
        sql: &CellGroupSqlPool,
        transport: &RaftTransport,
        controller: CellGroupController,
        registrations: Vec<CellGroupHostRegistration>,
        limits: CellGroupHostLimits,
        payloads: Option<CellGroupPayloadPool>,
    ) -> Result<Self, CellDispatchError> {
        let domain = runtime.log().domain();
        if !controller.belongs_to(runtime)
            || !transport.uses_shared_runtime(runtime)
            || !sql.belongs_to_engine(&runtime.log().owner())
            || !sql.uses_domain(&domain)
        {
            return Err(CellDispatchError::Invalid("shared host resource owner"));
        }
        if !(1..=4096).contains(&limits.max_namespaces)
            || registrations.is_empty()
            || registrations.len() > limits.max_namespaces
            || limits.namespace_bytes < 8192
        {
            return Err(CellDispatchError::Invalid("shared host namespace limits"));
        }
        let budget = Budget::new(limits.namespace_bytes, limits.max_namespaces);
        let mut namespaces = BTreeMap::new();
        for registration in registrations {
            let dispatcher = registration.dispatcher;
            if !dispatcher.uses_domain(&domain) {
                return Err(CellDispatchError::Invalid("shared host namespace domain"));
            }
            let (config, class, bytes) = dispatcher.host_binding()?;
            if class.name() == crate::deploy::KV_CLASS && payloads.is_none() {
                return Err(CellDispatchError::Invalid(
                    "KV requires node-owned payload admission",
                ));
            }
            controller.require_host_effects(&dispatcher)?;
            registration.storage.validate_namespace(&config)?;
            let bytes = bytes
                .checked_add(registration.storage.bytes())
                .ok_or(CellDispatchError::Runtime(RuntimeError::Overloaded))?;
            let admission = budget
                .acquire(bytes)
                .ok_or(CellDispatchError::Runtime(RuntimeError::Overloaded))?;
            let key = (config.cluster, config.namespace);
            if namespaces.contains_key(&key) {
                return Err(CellDispatchError::Invalid("duplicate host namespace"));
            }
            namespaces.insert(
                key,
                Arc::new(Binding {
                    dispatcher,
                    class,
                    storage: Mutex::new(Some(registration.storage)),
                    _admission: admission,
                }),
            );
        }
        let namespaces = Arc::new(namespaces);
        bindings::validate(&namespaces)?;
        controller
            .own_host_resources(Arc::new(HostResources {
                namespaces: namespaces.clone(),
            }))
            .map_err(CellDispatchError::Runtime)?;
        Ok(Self {
            inner: Arc::new(Inner {
                controller,
                domain,
                namespaces,
                budget,
                payloads,
                closed: AtomicBool::new(false),
            }),
        })
    }

    /// Selecting a history allocates no permanent binding and opens no SQL.
    /// Each invocation still obtains exact runtime/catalog authority.
    pub fn cell(&self, history: CellHistory) -> Result<CellGroupCellHandle, CellDispatchError> {
        self.inner.check_open()?;
        self.cell_metadata(history)
    }

    /// The SDK validates the original ordinary RPC before metadata-only
    /// construction. Actual dispatch still requires its proven source permit.
    pub(crate) fn cell_metadata(
        &self,
        history: CellHistory,
    ) -> Result<CellGroupCellHandle, CellDispatchError> {
        self.inner.check_lifecycle()?;
        let binding = self
            .inner
            .namespaces
            .get(&(history.cluster, history.namespace))
            .ok_or(CellDispatchError::Runtime(RuntimeError::NotFound))?
            .clone();
        Ok(CellGroupCellHandle {
            host: self.inner.clone(),
            binding,
            history,
        })
    }

    pub(crate) fn name_directory(
        &self,
        cluster: ClusterId,
        namespace: NamespaceId,
    ) -> Result<RangeDirectory, CellDispatchError> {
        self.inner.check_lifecycle()?;
        let binding = self
            .inner
            .namespaces
            .get(&(cluster, namespace))
            .ok_or(CellDispatchError::Runtime(RuntimeError::NotFound))?;
        Ok(binding.dispatcher.directory())
    }

    pub(crate) async fn refresh_namespace_routes(
        &self,
        history: cellar_logic::cell_group::GroupHistory,
    ) -> Result<u64, CellDispatchError> {
        self.inner.check_open()?;
        let binding = self
            .inner
            .namespaces
            .get(&(history.cluster, history.namespace))
            .ok_or(CellDispatchError::Runtime(RuntimeError::NotFound))?;
        binding.dispatcher.refresh_routes().await
    }

    /// Namespace capability lookup only; supplies no group placement or startup authority.
    pub(crate) fn directory(
        &self,
        history: cellar_logic::cell_group::GroupHistory,
    ) -> Result<RangeDirectory, CellDispatchError> {
        self.namespace_directory(history.cluster, history.namespace)
    }

    pub(crate) fn namespace_directory(
        &self,
        cluster: cellar_logic::consensus::ClusterId,
        namespace: cellar_logic::consensus::NamespaceId,
    ) -> Result<RangeDirectory, CellDispatchError> {
        self.inner.check_open()?;
        self.inner
            .namespaces
            .get(&(cluster, namespace))
            .map(|binding| binding.dispatcher.directory())
            .ok_or(CellDispatchError::Runtime(RuntimeError::NotFound))
    }

    pub(crate) fn cron_cells(
        &self,
    ) -> Result<Vec<(CellHistory, cellar_logic::consensus::ProposalId)>, CellDispatchError> {
        use sha2::{Digest, Sha256};
        self.inner.check_lifecycle()?;
        self.inner
            .namespaces
            .iter()
            .filter_map(|(&(cluster, namespace), binding)| {
                binding.class.cron_scope().map(|scope| {
                    let mut hash = Sha256::new();
                    hash.update(b"cellar-cron-enrollment-1");
                    hash.update(cluster.bytes());
                    hash.update(namespace.bytes());
                    hash.update(binding.class.schema_digest());
                    hash.update(scope.as_bytes());
                    let bytes: [u8; 32] = hash.finalize().into();

                    fn invalid<E>(_: E) -> CellDispatchError {
                        CellDispatchError::Invalid("cron enrollment identity")
                    }
                    Ok((
                        CellHistory {
                            cluster,
                            namespace,
                            cell: cellar_logic::cell_group::CellId::new(scope).map_err(invalid)?,
                            incarnation: cellar_logic::consensus::IncarnationId::new(
                                bytes[..16].try_into().map_err(invalid)?,
                            )
                            .map_err(invalid)?,
                        },
                        cellar_logic::consensus::ProposalId::new(
                            bytes[16..].try_into().map_err(invalid)?,
                        )
                        .map_err(invalid)?,
                    ))
                })
            })
            .collect()
    }

    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
            || self.inner.domain.mono_ms().is_err()
            || self.inner.controller.usage().closing
    }

    pub fn usage(&self) -> CellGroupHostUsage {
        CellGroupHostUsage {
            namespaces: self.inner.namespaces.len(),
            namespace_bytes: self.inner.budget.usage().0,
            namespace_byte_limit: self.inner.budget.capacity().0,
            payloads: self
                .inner
                .payloads
                .as_ref()
                .map(CellGroupPayloadPool::usage),
            payload_byte_limit: self
                .inner
                .payloads
                .as_ref()
                .map_or(0, |pool| pool.limits().max_retained_bytes),
            controller: self.inner.controller.usage(),
        }
    }

    /// Seal all handles, then join the node controller's physical owners.
    /// Cancellation cannot reopen admission or cancel those physical joins.
    pub async fn close(&self) -> Result<(), CellGroupControllerError> {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.controller.close().await
    }
}

/// Registered exactly once with the controller's physical join owner. This
/// retains no public controller handle and cannot prevent last-handle shutdown.
pub(in crate::raft::cell_groups) struct HostResources {
    namespaces: Arc<BTreeMap<(ClusterId, NamespaceId), Arc<Binding>>>,
}

impl HostResources {
    pub(in crate::raft::cell_groups) fn release(&self) -> Result<(), CellGroupControllerError> {
        let mut released = Ok(());
        for binding in self.namespaces.values() {
            match binding.storage.lock() {
                Ok(mut storage) => {
                    storage.take();
                }
                Err(error) => {
                    error.into_inner().take();
                    released = Err(CellGroupControllerError::Invalid(
                        "namespace resource owner poisoned",
                    ));
                }
            }
        }
        released
    }
}

impl Binding {
    fn options(&self, history: &CellHistory) -> Result<CellSqlOptions, CellDispatchError> {
        self.storage
            .lock()
            .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?
            .as_ref()
            .map(|storage| storage.for_cell(history))
            .ok_or(CellDispatchError::Runtime(RuntimeError::Closed))
    }
}

impl Inner {
    fn check_open(&self) -> Result<(), CellDispatchError> {
        if crate::application::consensus::callback_active() {
            return Err(CellDispatchError::Execution(
                crate::raft::execution::ExecutionError::Refused(
                    cellar_logic::consensus::CommandFailure::UnsupportedCapability,
                ),
            ));
        }
        self.check_lifecycle()
    }

    fn check_lifecycle(&self) -> Result<(), CellDispatchError> {
        if self.closed.load(Ordering::Acquire)
            || self.domain.mono_ms().is_err()
            || self.controller.usage().closing
        {
            return Err(CellDispatchError::Runtime(RuntimeError::Closed));
        }
        Ok(())
    }
}

/// A logical cell history, independent of its current physical group binding.
#[derive(Clone)]
pub struct CellGroupCellHandle {
    host: Arc<Inner>,
    binding: Arc<Binding>,
    history: CellHistory,
}

impl CellGroupCellHandle {
    pub(crate) fn definition_digest(&self) -> [u8; 32] {
        self.binding.class.definition_digest()
    }

    pub fn history(&self) -> &CellHistory {
        &self.history
    }

    pub fn contract(&self, operation: &str) -> Result<OperationContract, RegistrationError> {
        if operation.len() > 128 {
            return Err(RegistrationError::Invalid("operation bounds"));
        }
        self.binding
            .class
            .contract(operation)
            .ok_or_else(|| RegistrationError::UnknownOperation(operation.into()))
    }

    /// Queue before returning the response waiter. No group ID is retained by
    /// this handle, and cancellation cannot release an admitted SQL producer.
    pub fn command(
        &self,
        request: CellCommandRequest,
        limits: CellRoutingLimits,
    ) -> Result<CellCommandCall, CellCommandError> {
        self.command_retaining(request, limits, None)
    }

    pub(crate) fn command_retaining(
        &self,
        request: CellCommandRequest,
        limits: CellRoutingLimits,
        reservation: Option<crate::rpc_reservation::Reservation>,
    ) -> Result<CellCommandCall, CellCommandError> {
        self.host.check_open().map_err(|cause| CellCommandError {
            command: request.id,
            ambiguous: false,
            cause: Box::new(cause),
        })?;
        let options = self
            .binding
            .options(&self.history)
            .map_err(|cause| CellCommandError {
                command: request.id,
                ambiguous: false,
                cause: Box::new(cause),
            })?;
        let mut pending = self.binding.dispatcher.prepare_command(
            self.history.clone(),
            request,
            limits,
            None,
            None,
            false,
        )?;
        pending.retain_submission(reservation);
        self.host
            .controller
            .dispatch_prepared_command(pending, options)
    }

    /// Queue a read with its explicit consistency and original invocation deadline.
    pub fn query(
        &self,
        request: CellQueryRequest,
        limits: CellRoutingLimits,
    ) -> Result<CellQueryCall, CellDispatchError> {
        self.query_routed(request, limits, CellQueryRouting::Local)
    }

    pub fn query_routed(
        &self,
        request: CellQueryRequest,
        limits: CellRoutingLimits,
        routing: CellQueryRouting,
    ) -> Result<CellQueryCall, CellDispatchError> {
        self.host.check_open()?;
        let invocation = self.binding.dispatcher.query_routed(
            self.history.clone(),
            request,
            limits,
            None,
            routing,
        )?;
        self.host
            .controller
            .query(invocation, self.binding.options(&self.history)?)
    }

    pub fn enroll(
        &self,
        request: CellEnrollmentRequest,
        limits: CellRoutingLimits,
    ) -> impl std::future::Future<Output = Result<CellEnrollmentResponse, Arc<CellEnrollmentError>>>
           + Send
           + 'static {
        let opened = self.host.check_open().map_err(|cause| {
            Arc::new(CellEnrollmentError {
                creation: request.creation,
                ambiguous: false,
                cause: Box::new(cause),
            })
        });
        let creation = request.creation;
        let invocation =
            self.binding
                .dispatcher
                .enroll_routed(self.history.clone(), request, limits);
        let host = self.host.clone();
        let binding = self.binding.clone();
        let history = self.history.clone();
        async move {
            opened?;
            let invocation = invocation.await.map_err(Arc::new)?;
            let options = binding.options(&history).map_err(|cause| {
                Arc::new(CellEnrollmentError {
                    creation,
                    ambiguous: false,
                    cause: Box::new(cause),
                })
            })?;
            let call = host
                .controller
                .enroll(invocation, options)
                .map_err(|e| Arc::new(e.error))?;
            call.response().await
        }
    }

    /// Inspect a locally attached group. The canonical SDK supplies remote routing.
    /// Absence remains ambiguous and never releases an original producer.
    pub fn receipt(
        &self,
        request: CellReceiptRequest,
        limits: CellRoutingLimits,
    ) -> impl std::future::Future<Output = Result<CellReceiptResponse, Arc<CellDispatchError>>>
           + Send
           + 'static {
        self.receipt_with_transport(request, limits, None)
    }

    pub(crate) fn receipt_routed(
        &self,
        request: CellReceiptRequest,
        limits: CellRoutingLimits,
        transport: RaftTransport,
    ) -> impl std::future::Future<Output = Result<CellReceiptResponse, Arc<CellDispatchError>>>
           + Send
           + 'static {
        self.receipt_with_transport(request, limits, Some(transport))
    }

    fn receipt_with_transport(
        &self,
        request: CellReceiptRequest,
        limits: CellRoutingLimits,
        transport: Option<RaftTransport>,
    ) -> impl std::future::Future<Output = Result<CellReceiptResponse, Arc<CellDispatchError>>>
           + Send
           + 'static {
        let invocation = self.host.check_open().and_then(|()| {
            self.binding
                .dispatcher
                .lookup_receipt(self.history.clone(), request, limits, None)
        });
        let host = self.host.clone();
        let binding = self.binding.clone();
        let history = self.history.clone();
        async move {
            let options = binding.options(&history).map_err(Arc::new)?;
            host.controller
                .lookup_receipt(invocation.map_err(Arc::new)?, options, transport.as_ref())
                .await
                .map_err(Arc::new)
        }
    }
}
