//! An opaque deadline follows one Python call through codecs and native I/O.
use cellar::sdk::rpc::{CallBudget, Failure};
use pyo3::prelude::*;

#[pyclass(frozen)]
pub(super) struct RpcBudget {
    pub budget: CallBudget,
}

pub(super) fn failure(error: Failure) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(format!("{:?}", error.code()))
}

#[pymethods]
impl RpcBudget {
    fn limit(&self, timeout_ms: u32) -> Self {
        Self {
            budget: self.budget.limit(timeout_ms),
        }
    }

    fn remaining_ms(&self) -> PyResult<u64> {
        self.budget.remaining_ms().map_err(failure)
    }
}
