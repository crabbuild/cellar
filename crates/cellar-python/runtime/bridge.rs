use pyo3::prelude::*;
use pyo3::{PyTraverseError, PyVisit};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::mpsc;

pub(crate) type Life = Arc<AtomicBool>;

pub(crate) enum Event {
    Operation {
        id: u64,
        name: String,
        payload: String,
        mutation: Option<Mutation>,
        reservation: super::Reservation,
    },
    Complete {
        payload: String,
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
    pub id: u64,
    pub payload: String,
    pub bridge: InvocationBridge,
    pub reservation: super::Reservation,
}

#[pyclass]
pub(crate) struct InvocationBridge {
    pub sender: mpsc::Sender<Event>,
    pub active: Life,
    pub owner: Life,
    pub operations: AtomicUsize,
    pub mutation: Arc<AtomicBool>,
    pub max_operations: usize,
    pub max_bytes: usize,
    pub budget: Arc<super::Budget>,
    pub event_loop: Option<Py<PyAny>>,
}

#[pymethods]
impl InvocationBridge {
    // The loop can hold tasks/contexts which refer back to this bridge. Expose
    // the owned edge to CPython's cyclic collector instead of leaking cycles.
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit.call(&self.event_loop)
    }

    fn __clear__(&mut self) {
        self.active.store(false, Ordering::Release);
        self.event_loop = None;
    }

    fn active(&self) -> bool {
        self.active.load(Ordering::Acquire) && self.owner.load(Ordering::Acquire)
    }

    fn expire(&self) {
        self.active.store(false, Ordering::Release);
    }

    fn owner_active(&self) -> bool {
        self.owner.load(Ordering::Acquire)
    }

    fn submit(&self, py: Python<'_>, id: u64, name: String, payload: String) -> PyResult<()> {
        if !self.active() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err("ContextExpired"));
        }
        self.ensure_scope(py)?;
        if payload.len() > self.max_bytes
            || self.operations.fetch_add(1, Ordering::Relaxed) >= self.max_operations
        {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Python SDK operation budget exceeded",
            ));
        }
        let reservation = self.budget.reserve(payload.len()).map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "Python aggregate bridge byte budget exhausted",
            )
        })?;
        let mutation = if crate::operations::is_read(&name) {
            None
        } else {
            self.mutation
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|_| {
                    pyo3::exceptions::PyRuntimeError::new_err(
                        "Only one outstanding Cellar mutation is allowed per invocation",
                    )
                })?;
            Some(Mutation(self.mutation.clone()))
        };
        self.sender
            .try_send(Event::Operation {
                id,
                name,
                payload,
                mutation,
                reservation,
            })
            .map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "Python SDK operation queue is full or closed",
                )
            })
    }

    fn complete(&self, py: Python<'_>, payload: String, failed: bool) -> PyResult<()> {
        if !self.active() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err("ContextExpired"));
        }
        self.ensure_scope(py)?;
        if payload.len() > self.max_bytes {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Python response exceeds value budget",
            ));
        }
        self.sender
            .try_send(Event::Complete {
                reservation: self.budget.reserve(payload.len()).map_err(|_| {
                    pyo3::exceptions::PyRuntimeError::new_err(
                        "Python aggregate bridge byte budget exhausted",
                    )
                })?,
                payload,
                failed,
            })
            .map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "Python completion queue is full or closed",
                )
            })
    }
}

impl InvocationBridge {
    fn ensure_scope(&self, py: Python<'_>) -> PyResult<()> {
        let valid = (|| -> PyResult<bool> {
            let current_loop = py.import("asyncio")?.call_method0("get_running_loop")?;
            if !self
                .event_loop
                .as_ref()
                .is_some_and(|owner| current_loop.is(owner.bind(py)))
            {
                return Ok(false);
            }
            let current = py
                .import("cellar")?
                .getattr("_current_invocation")?
                .call_method0("get")?;
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
