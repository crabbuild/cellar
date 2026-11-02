// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Namespace registrations over one shared node controller. Cell handles are
//! disposable values; obtaining or cloning one neither enrolls nor caches a cell.
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
        Arc,
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
    storage: CellGroupNamespaceStorage,
    _admission: Permit,
}

struct Inner {
    controller: CellGroupController,
    domain: crate::asyncrt::DomainToken,
    namespaces: BTreeMap<(ClusterId, NamespaceId), Arc<Binding>>,
    budget: Arc<Budget>,
    closed: AtomicBool,
}

/// Bind compiled namespaces to an already initialized shared node controller.
/// Group placement and physical bootstrap remain node-startup responsibilities;
/// these registrations grant no voting, leader or serving authority.
#[derive(Clone)]
pub struct CellGroupHost {
    inner: Arc<Inner>,
}

#[derive(Clone, Copy, Debug)]
pub struct CellGroupHostUsage {
    pub namespaces: usize,
    pub namespace_bytes: usize,
    pub namespace_byte_limit: usize,
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
                    storage: registration.storage,
                    _admission: admission,
                }),
            );
        }
        Ok(Self {
            inner: Arc::new(Inner {
                controller,
                domain,
                namespaces,
                budget,
                closed: AtomicBool::new(false),
            }),
        })
    }

    /// Selecting a history allocates no permanent binding and opens no SQL.
    /// Each invocation still obtains exact runtime/catalog authority.
    pub fn cell(&self, history: CellHistory) -> Result<CellGroupCellHandle, CellDispatchError> {
        self.inner.check_open()?;
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

impl Inner {
    fn check_open(&self) -> Result<(), CellDispatchError> {
        if crate::application::consensus::callback_active() {
            return Err(CellDispatchError::Execution(
                crate::raft::execution::ExecutionError::Refused(
                    cellar_logic::consensus::CommandFailure::UnsupportedCapability,
                ),
            ));
        }
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
        self.host.check_open().map_err(|cause| CellCommandError {
            command: request.id,
            ambiguous: false,
            cause: Box::new(cause),
        })?;
        self.host.controller.dispatch_command(
            &self.binding.dispatcher,
            self.history.clone(),
            request,
            limits,
            self.binding.storage.for_cell(&self.history),
        )
    }

    /// Queue a read with its explicit consistency and original invocation deadline.
    pub fn query(
        &self,
        request: CellQueryRequest,
        limits: CellRoutingLimits,
    ) -> Result<CellQueryCall, CellDispatchError> {
        self.host.check_open()?;
        let invocation =
            self.binding
                .dispatcher
                .query(self.history.clone(), request, limits, None)?;
        self.host
            .controller
            .query(invocation, self.binding.storage.for_cell(&self.history))
    }

    pub fn enroll(
        &self,
        request: CellEnrollmentRequest,
        limits: CellRoutingLimits,
    ) -> impl std::future::Future<Output = Result<CellSqlEnrollment, Arc<CellEnrollmentError>>>
           + Send
           + 'static {
        let opened = self.host.check_open().map_err(|cause| {
            Arc::new(CellEnrollmentError {
                creation: request.creation,
                ambiguous: false,
                cause: Box::new(cause),
            })
        });
        let invocation =
            self.binding
                .dispatcher
                .enroll(self.history.clone(), request, limits, None);
        let host = self.host.clone();
        let options = self.binding.storage.for_cell(&self.history);
        async move {
            opened?;
            let invocation = invocation.await.map_err(Arc::new)?;
            let call = host
                .controller
                .enroll(invocation, options)
                .map_err(|e| Arc::new(e.error))?;
            call.response().await
        }
    }

    /// Absence remains ambiguous and never releases an original producer.
    pub fn receipt(
        &self,
        request: CellReceiptRequest,
        limits: CellRoutingLimits,
    ) -> impl std::future::Future<Output = Result<CellReceiptResponse, Arc<CellDispatchError>>>
           + Send
           + 'static {
        let invocation = self.host.check_open().and_then(|()| {
            self.binding
                .dispatcher
                .lookup_receipt(self.history.clone(), request, limits, None)
        });
        let host = self.host.clone();
        let options = self.binding.storage.for_cell(&self.history);
        async move {
            host.controller
                .lookup_receipt(invocation.map_err(Arc::new)?, options)
                .await
                .map_err(Arc::new)
        }
    }
}
