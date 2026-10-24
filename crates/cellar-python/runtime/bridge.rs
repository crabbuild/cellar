// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::{PyTraverseError, PyVisit};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::{mpsc, watch};

pub(crate) type Life = Arc<AtomicBool>;

/// Resident Python objects observe retirement without retaining an invocation's
/// mailbox, caller scope or event loop. This token grants no SDK authority.
#[pyclass(frozen)]
struct OwnerLifetime {
    active: Life,
}

#[pymethods]
impl OwnerLifetime {
    fn active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }
}

pub(crate) enum Event {
    Operation {
        id: u64,
        name: String,
        payload: String,
        body: Option<Vec<u8>>,
        mutation: Option<Mutation>,
        reservation: super::Reservation,
        observation: super::telemetry::Operation,
    },
    Complete {
        queued_at: std::time::Instant,
        payload: String,
        body: Option<Vec<u8>>,
        failed: bool,
        reservation: super::Reservation,
    },
}

/// Admission lasts until the native future completes or is dropped, including
/// queued/cancelled operations. No Python callback is needed to release it.
pub(crate) struct Mutation(Arc<AtomicBool>);

impl Drop for Mutation {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

pub(crate) struct Start {
    pub queued_at: std::time::Instant,
    pub id: u64,
    pub payload: String,
    pub body: Vec<u8>,
    pub bridge: InvocationBridge,
    pub reservation: super::Reservation,
}

#[pyclass]
pub(crate) struct InvocationBridge {
    pub sender: mpsc::Sender<Event>,
    pub active: Arc<super::InvocationState>,
    pub lifecycle: Arc<std::sync::atomic::AtomicU8>,
    pub cancelled: watch::Sender<bool>,
    pub completion_enqueued: AtomicBool,
    pub owner: Life,
    pub operations: AtomicUsize,
    pub mutation: Arc<AtomicBool>,
    pub max_operations: usize,
    pub max_bytes: usize,
    pub budget: Arc<super::Budget>,
    pub event_loop: Option<Py<PyAny>>,
    pub scope: Option<super::scope::CallerScope>,
    pub clock_origin: std::time::Instant,
}

#[pymethods]
impl InvocationBridge {
    // The loop can hold tasks/contexts which refer back to this bridge. Expose
    // the owned edge to CPython's cyclic collector instead of leaking cycles.
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit.call(&self.event_loop)?;
        if let Some(scope) = &self.scope {
            scope.traverse(&visit)?;
        }
        Ok(())
    }

    fn __clear__(&mut self) {
        self.expire(false);
        self.event_loop = None;
        self.scope = None;
    }

    fn active(&self) -> bool {
        self.active.active() && self.owner.load(Ordering::Acquire)
    }

    #[pyo3(signature = (finished=false))]
    fn expire(&self, finished: bool) {
        self.active.expire(finished);
        if !finished {
            // Direct Python expiration is visible even if the native driver
            // has not yet consumed cancellation. This flag owns no descriptor
            // or runtime and cannot keep either alive through a retained context.
            self.lifecycle
                .fetch_or(super::wakeup::INVALIDATED, Ordering::AcqRel);
        }
        // Completion remains in the ordered mailbox. Cancellation needs a
        // separate wakeup: application code can retain this sender after the
        // task ends, and a full mailbox must not delay revocation.
        if !self.completion_enqueued.load(Ordering::Acquire) {
            self.cancelled.send_replace(true);
        }
    }

    fn owner_active(&self) -> bool {
        self.owner.load(Ordering::Acquire)
    }

    fn owner_lifetime(&self) -> OwnerLifetime {
        OwnerLifetime {
            active: self.owner.clone(),
        }
    }

    #[pyo3(signature = (id, name, payload, body=None))]
    fn submit(
        &self,
        py: Python<'_>,
        id: u64,
        name: &str,
        payload: &str,
        body: Option<&Bound<'_, PyBytes>>,
    ) -> PyResult<()> {
        if !self.active() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err("ContextExpired"));
        }
        self.ensure_scope(py)?;
        if name.is_empty() || name.len() > 64 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Python SDK operation name must be 1..64 bytes",
            ));
        }
        let body = body.map(|body| body.as_bytes());
        if body.is_some() != (name == "cell.fetch") {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Only cell.fetch requires a byte body",
            ));
        }
        let charge = crate::values::envelope_size(payload.len(), body.map_or(0, <[u8]>::len))
            .map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("Python operation size overflow")
            })?;
        if charge > self.max_bytes {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Python SDK value budget exceeded",
            ));
        }
        if self.operations.fetch_add(1, Ordering::Relaxed) >= self.max_operations {
            return Err(pyo3::exceptions::PyBlockingIOError::new_err(
                "Python SDK operation budget exceeded",
            ));
        }
        let reservation = self.budget.reserve(charge).map_err(|_| {
            pyo3::exceptions::PyBlockingIOError::new_err(
                "Python aggregate bridge byte budget exhausted",
            )
        })?;
        let mutation = if crate::operations::is_read(name) {
            None
        } else {
            self.mutation
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|_| {
                    pyo3::exceptions::PyBlockingIOError::new_err(
                        "Only one outstanding Cellar mutation is allowed per invocation",
                    )
                })?;
            Some(Mutation(self.mutation.clone()))
        };
        // Reserve mailbox capacity before counting admission. The observation
        // then follows every accepted event even if the receiver retires.
        let permit = self.sender.try_reserve().map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => {
                pyo3::exceptions::PyBlockingIOError::new_err("Python SDK operation queue is full")
            }
            mpsc::error::TrySendError::Closed(_) => {
                pyo3::exceptions::PyRuntimeError::new_err("Python SDK operation queue is closed")
            }
        })?;
        permit.send(Event::Operation {
            id,
            name: name.to_owned(),
            payload: payload.to_owned(),
            body: body.map(<[u8]>::to_vec),
            mutation,
            reservation,
            observation: self.budget.metrics.admit(self.clock_origin),
        });
        self.budget
            .copied_input(name.len() + payload.len() + body.map_or(0, <[u8]>::len));
        Ok(())
    }

    #[pyo3(signature = (payload, failed, body=None))]
    fn complete(
        &self,
        py: Python<'_>,
        payload: &str,
        failed: bool,
        body: Option<&Bound<'_, PyBytes>>,
    ) -> PyResult<()> {
        if !self.active() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err("ContextExpired"));
        }
        self.ensure_scope(py)?;
        let body = body.map(|body| body.as_bytes());
        let charge = crate::values::envelope_size(payload.len(), body.map_or(0, <[u8]>::len))
            .map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("Python response size overflow")
            })?;
        if charge > self.max_bytes {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Python response exceeds value budget",
            ));
        }
        let reservation = self.budget.reserve(charge).map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "Python aggregate bridge byte budget exhausted",
            )
        })?;
        let permit = self.sender.try_reserve().map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err("Python completion queue is full or closed")
        })?;
        // Reserve both byte and mailbox capacity before owning copies. The
        // encoded-equivalent charge covers the smaller raw body allocation.
        let payload = payload.to_owned();
        let body = body.map(<[u8]>::to_vec);
        self.budget
            .copied_input(payload.len() + body.as_ref().map_or(0, Vec::len));
        permit.send(Event::Complete {
            queued_at: self.clock_origin + self.clock_origin.elapsed(),
            reservation,
            payload,
            body,
            failed,
        });
        self.completion_enqueued.store(true, Ordering::Release);
        Ok(())
    }
}

impl InvocationBridge {
    fn ensure_scope(&self, py: Python<'_>) -> PyResult<()> {
        let valid = (|| -> PyResult<bool> {
            let Some(scope) = &self.scope else {
                return Ok(false);
            };
            let current_loop = scope.running_loop(py)?;
            if !self
                .event_loop
                .as_ref()
                .is_some_and(|owner| current_loop.is(owner.bind(py)))
            {
                return Ok(false);
            }
            let current = scope.invocation(py)?;
            Ok(current
                .extract::<PyRef<'_, Self>>()
                .is_ok_and(|owner| std::ptr::eq(&*owner, self)))
        })()
        .unwrap_or(false);
        if valid {
            Ok(())
        } else {
            Err(pyo3::exceptions::PyRuntimeError::new_err("ContextMismatch"))
        }
    }
}
