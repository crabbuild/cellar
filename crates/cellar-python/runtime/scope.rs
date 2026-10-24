// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Resolve caller identity through the bootstrap's original Python accessors.
use pyo3::{prelude::*, PyTraverseError, PyVisit};

pub(crate) struct CallerScope {
    running_loop: Py<PyAny>,
    invocation: Py<PyAny>,
}

impl CallerScope {
    pub(super) fn capture(py: Python<'_>, facade: &Bound<'_, PyModule>) -> PyResult<Self> {
        Ok(Self {
            running_loop: py.import("asyncio")?.getattr("get_running_loop")?.unbind(),
            invocation: facade
                .getattr("_current_invocation")?
                .getattr("get")?
                .unbind(),
        })
    }

    pub(super) fn clone_ref(&self, py: Python<'_>) -> Self {
        Self {
            running_loop: self.running_loop.clone_ref(py),
            invocation: self.invocation.clone_ref(py),
        }
    }

    pub(super) fn running_loop<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.running_loop.bind(py).call0()
    }

    pub(super) fn invocation<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.invocation.bind(py).call0()
    }

    pub(super) fn traverse(&self, visit: &PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit.call(&self.running_loop)?;
        visit.call(&self.invocation)
    }
}
