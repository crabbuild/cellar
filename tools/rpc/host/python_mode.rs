// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Observe the loaded interpreter after user/codec imports, never a subprocess.
use anyhow::{ensure, Context as _, Result};
use pyo3::{prelude::*, types::PyModule};
use serde::Serialize;
use serde_json::{json, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum Mode {
    Gil,
    FreeThreaded,
}

impl Mode {
    pub(crate) fn parse(text: &str) -> Result<Self> {
        match text {
            "gil" => Ok(Self::Gil),
            "free-threaded" => Ok(Self::FreeThreaded),
            _ => anyhow::bail!("--python-mode must be gil or free-threaded"),
        }
    }

    pub(crate) fn compiled() -> Self {
        if cellar_python::Interpreter::default()
            .soabi
            .starts_with("cpython-314t-")
        {
            Self::FreeThreaded
        } else {
            Self::Gil
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct Snapshot {
    version: [u32; 3],
    implementation: String,
    soabi: String,
    free_threaded: bool,
    gil_enabled: bool,
    protobuf_version: String,
    protobuf_backend: String,
}

impl Snapshot {
    fn validate(&self, mode: Mode) -> Result<()> {
        let free_threaded = mode == Mode::FreeThreaded;
        ensure!(
            self.implementation == "cpython" && self.version[..2] == [3, 14],
            "RPC qualification requires loaded CPython 3.14"
        );
        ensure!(
            self.free_threaded == free_threaded && self.gil_enabled != free_threaded,
            "loaded Python runtime does not match the requested threading mode"
        );
        let prefixes = if free_threaded {
            ["cpython-314t-", "cp314t-"]
        } else {
            ["cpython-314-", "cp314-"]
        };
        ensure!(
            prefixes.iter().any(|prefix| self.soabi.starts_with(prefix)),
            "loaded Python SOABI differs from its threading mode"
        );
        ensure!(
            self.protobuf_version == "7.34.2"
                && matches!(self.protobuf_backend.as_str(), "python" | "upb"),
            "unqualified Protobuf runtime"
        );
        Ok(())
    }
}

fn capture() -> Result<Snapshot> {
    // The owning PythonRuntime has initialized CPython and is pumping its loop.
    // Only owned Rust values leave the attachment, before any async host work.
    Python::attach(|python| -> PyResult<Snapshot> {
        let sys = PyModule::import(python, "sys")?;
        let version = sys.getattr("version_info")?;
        let config = PyModule::import(python, "sysconfig")?;
        let disabled: Option<u32> = config
            .call_method1("get_config_var", ("Py_GIL_DISABLED",))?
            .extract()?;
        let protobuf = PyModule::import(python, "google.protobuf")?;
        let backend = PyModule::import(python, "google.protobuf.internal.api_implementation")?;
        Ok(Snapshot {
            version: [
                version.get_item(0)?.extract()?,
                version.get_item(1)?.extract()?,
                version.get_item(2)?.extract()?,
            ],
            implementation: sys.getattr("implementation")?.getattr("name")?.extract()?,
            soabi: config
                .call_method1("get_config_var", ("SOABI",))?
                .extract()?,
            free_threaded: disabled == Some(1),
            gil_enabled: sys.call_method0("_is_gil_enabled")?.extract()?,
            protobuf_version: protobuf.getattr("__version__")?.extract()?,
            protobuf_backend: backend.call_method0("Type")?.extract()?,
        })
    })
    .map_err(|_| anyhow::anyhow!("could not observe loaded Python runtime"))
}

pub(crate) struct Check {
    expected: Mode,
    startup: Snapshot,
}

impl Check {
    pub(crate) fn begin(expected: Mode) -> Result<Self> {
        let startup = capture()?;
        startup.validate(expected)?;
        Ok(Self { expected, startup })
    }

    pub(crate) fn record(self, report: &mut Value, ordinary: bool) -> Result<()> {
        let after = if ordinary {
            let after = capture()?;
            after.validate(self.expected)?;
            ensure!(
                after == self.startup,
                "Python runtime identity changed during the matrix"
            );
            Some(after)
        } else {
            // Private crash/supervision children deliberately terminate work.
            // Their startup observation cannot claim a completed mode matrix.
            None
        };
        report
            .as_object_mut()
            .context("qualification report must be an object")?
            .insert(
                "python_runtime".into(),
                json!({"expected_mode":self.expected,"startup":self.startup,"after_matrix":after}),
            );
        Ok(())
    }
}

#[cfg(test)]
#[path = "python_mode_tests.rs"]
mod tests;
