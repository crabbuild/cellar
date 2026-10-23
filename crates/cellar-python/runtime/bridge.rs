use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString, PyTuple};
use pyo3::{PyTraverseError, PyVisit};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::{mpsc, watch};

pub(crate) type Life = Arc<AtomicBool>;

pub(crate) enum Event {
    Operation {
        id: u64,
        name: String,
        payload: String,
        mutation: Option<Mutation>,
        reservation: super::Reservation,
        observation: super::telemetry::Operation,
    },
    Complete {
        payload: String,
        body: Option<Vec<u8>>,
        failed: bool,
        reservation: super::Reservation,
    },
    CompleteHttp {
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
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
    pub body: Vec<u8>,
    pub bridge: InvocationBridge,
    pub reservation: super::Reservation,
}

#[pyclass]
pub(crate) struct InvocationBridge {
    pub sender: mpsc::Sender<Event>,
    pub active: Life,
    pub cancelled: watch::Sender<bool>,
    pub completion_enqueued: AtomicBool,
    pub owner: Life,
    pub operations: AtomicUsize,
    pub mutation: Arc<AtomicBool>,
    pub max_operations: usize,
    pub max_bytes: usize,
    pub budget: Arc<super::Budget>,
    pub event_loop: Option<Py<PyAny>>,
    pub clock_origin: std::time::Instant,
}

#[pymethods]
impl InvocationBridge {
    // The loop can hold tasks/contexts which refer back to this bridge. Expose
    // the owned edge to CPython's cyclic collector instead of leaking cycles.
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit.call(&self.event_loop)
    }

    fn __clear__(&mut self) {
        self.expire();
        self.event_loop = None;
    }

    fn active(&self) -> bool {
        self.active.load(Ordering::Acquire) && self.owner.load(Ordering::Acquire)
    }

    fn expire(&self) {
        self.active.store(false, Ordering::Release);
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

    fn submit(&self, py: Python<'_>, id: u64, name: &str, payload: &str) -> PyResult<()> {
        if !self.active() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err("ContextExpired"));
        }
        self.ensure_scope(py)?;
        if name.is_empty() || name.len() > 64 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Python SDK operation name must be 1..64 bytes",
            ));
        }
        if payload.len() > self.max_bytes {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Python SDK value budget exceeded",
            ));
        }
        if self.operations.fetch_add(1, Ordering::Relaxed) >= self.max_operations {
            return Err(pyo3::exceptions::PyBlockingIOError::new_err(
                "Python SDK operation budget exceeded",
            ));
        }
        let reservation = self.budget.reserve(payload.len()).map_err(|_| {
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
            mutation,
            reservation,
            observation: self.budget.metrics.admit(self.clock_origin),
        });
        self.budget.copied_input(name.len() + payload.len());
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
        let (reservation, permit) =
            self.reserve_completion(payload.len(), body.map_or(0, <[u8]>::len))?;
        // Reserve both byte and mailbox capacity before owning copies. The
        // encoded-equivalent charge covers the smaller raw body allocation.
        let payload = payload.to_owned();
        let body = body.map(<[u8]>::to_vec);
        self.budget
            .copied_input(payload.len() + body.as_ref().map_or(0, Vec::len));
        permit.send(Event::Complete {
            reservation,
            payload,
            body,
            failed,
        });
        self.completion_enqueued.store(true, Ordering::Release);
        Ok(())
    }

    fn complete_response(
        &self,
        py: Python<'_>,
        status: u16,
        headers: &Bound<'_, PyTuple>,
        body: &Bound<'_, PyBytes>,
    ) -> PyResult<()> {
        if !self.active() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err("ContextExpired"));
        }
        self.ensure_scope(py)?;
        if !(200..=599).contains(&status) || headers.len() > 128 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Invalid Python HTTP metadata",
            ));
        }
        let mut metadata =
            crate::values::HTTP_METADATA_BASE_BYTES + headers.len().saturating_sub(1);
        let mut copied = body.as_bytes().len();
        // The facade snapshots all containers into tuples. Inspect borrowed,
        // immutable strings before reserving capacity or allocating owned data.
        for row in headers.iter() {
            let row = row.cast::<PyTuple>()?;
            if row.len() != 2 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "Invalid Python HTTP header",
                ));
            }
            let name = row.get_item(0)?;
            let value = row.get_item(1)?;
            let name = name.cast::<PyString>()?.to_str()?;
            let value = value.cast::<PyString>()?.to_str()?;
            copied = copied
                .checked_add(name.len())
                .and_then(|bytes| bytes.checked_add(value.len()))
                .ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err("Python response size overflow")
                })?;
            // ASCII-escaped JSON and base64 cannot be smaller than these raw
            // bytes. Refuse an obviously oversized value before scanning it.
            if copied > self.max_bytes {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "Python response exceeds value budget",
                ));
            }
            let bytes = crate::values::response_header_size(name, value).map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("Python response size overflow")
            })?;
            metadata = metadata.checked_add(bytes).ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err("Python response size overflow")
            })?;
        }
        let (reservation, permit) = self.reserve_completion(metadata, body.as_bytes().len())?;
        let mut owned = Vec::with_capacity(headers.len());
        for row in headers.iter() {
            let row = row.cast::<PyTuple>()?;
            owned.push((
                row.get_item(0)?.extract::<String>()?,
                row.get_item(1)?.extract::<String>()?,
            ));
        }
        let body = body.as_bytes().to_vec();
        self.budget.copied_input(copied);
        permit.send(Event::CompleteHttp {
            status,
            headers: owned,
            body,
            reservation,
        });
        self.completion_enqueued.store(true, Ordering::Release);
        Ok(())
    }
}

impl InvocationBridge {
    fn reserve_completion(
        &self,
        metadata: usize,
        body: usize,
    ) -> PyResult<(super::Reservation, mpsc::Permit<'_, Event>)> {
        let charge = crate::values::envelope_size(metadata, body).map_err(|_| {
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
        Ok((reservation, permit))
    }

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
