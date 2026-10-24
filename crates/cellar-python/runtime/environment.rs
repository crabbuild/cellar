// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit Python environment view, independent of Rust's process environment.
use anyhow::{ensure, Result};
use pyo3::{prelude::*, types::PyDict};
use std::collections::BTreeMap;

/// Application environment values visible through Python `os.environ`.
/// Interpreter configuration uses `Interpreter`, never environment overrides.
/// Debug output deliberately omits values and names.
#[derive(Clone, Default)]
pub struct Environment(BTreeMap<String, String>);

impl Environment {
    pub fn new(values: impl IntoIterator<Item = (String, String)>) -> Result<Self> {
        let mut result = BTreeMap::new();
        for (key, value) in values {
            ensure!(
                !key.is_empty() && !key.contains(['=', '\0']),
                "invalid Python environment key"
            );
            ensure!(
                !key.starts_with("PYTHON") && !key.starts_with("_PYTHON"),
                "Python interpreter environment overrides are not permitted"
            );
            ensure!(
                !value.contains('\0'),
                "Python environment values cannot contain NUL"
            );
            ensure!(
                result.insert(key, value).is_none(),
                "duplicate Python environment key"
            );
        }
        Ok(Self(result))
    }

    pub(super) fn install(&self, py: Python<'_>) -> Result<()> {
        let os = py.import("os")?;
        let original = os.getattr("environ")?;
        let encode_key = original.getattr("encodekey")?;
        let encode_value = original.getattr("encodevalue")?;
        let data = PyDict::new(py);
        for (key, value) in &self.0 {
            data.set_item(encode_key.call1((key,))?, encode_value.call1((value,))?)?;
        }
        // A separate mapping avoids mutating libc's environment while the Rust
        // host parses its own explicit process configuration. The standard
        // _Environ implementation retains normal Python set/delete semantics.
        let view = original.get_type().call1((
            &data,
            encode_key,
            original.getattr("decodekey")?,
            encode_value,
            original.getattr("decodevalue")?,
        ))?;
        os.setattr("environ", view)?;
        if os.getattr("supports_bytes_environ")?.is_truthy()? {
            let binary = os.getattr("environb")?;
            let view = binary.get_type().call1((
                &data,
                binary.getattr("encodekey")?,
                binary.getattr("decodekey")?,
                binary.getattr("encodevalue")?,
                binary.getattr("decodevalue")?,
            ))?;
            os.setattr("environb", view)?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for Environment {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Environment")
            .field("entries", &self.0.len())
            .finish_non_exhaustive()
    }
}
