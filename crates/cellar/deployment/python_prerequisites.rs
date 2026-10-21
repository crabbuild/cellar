//! Data-only contract for the native libraries outside a Python bundle.
use super::python_environment::{
    normal_absolute, valid_digest, PythonFile, MAX_PYTHON_EXPANDED_BYTES,
};
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

pub const MAX_PYTHON_NATIVE_LIBRARIES: usize = 4096;
pub const MAX_PYTHON_NATIVE_EDGES: usize = 65_536;

/// A qualified host cohort. OS updates require qualification and recapture.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PythonPlatform {
    Linux {
        machine: String,
        kernel_release: String,
        os_release_sha256: String,
    },
    Macos {
        machine: String,
        build: String,
        shared_cache_uuid: String,
    },
}

/// System cache entries are supplied by the attested macOS build. Other
/// external libraries have exact file identities and are never archived.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PythonNativeLibrary {
    File { bytes: u64, sha256: String },
    MacosSharedCache,
}

/// Resolved load-time dependency graph. Each reference lists canonical providers
/// observed in its load contexts, including inherited native search paths.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PythonPrerequisites {
    pub platform: PythonPlatform,
    pub libraries: BTreeMap<PathBuf, PythonNativeLibrary>,
    pub dependencies: BTreeMap<PathBuf, BTreeMap<String, Vec<PathBuf>>>,
    /// Dependencies of the compiled host, without binding its build path.
    pub host_dependencies: BTreeMap<String, Vec<PathBuf>>,
}

impl PythonPrerequisites {
    pub(crate) fn validate(
        &self,
        target: &str,
        roots: &[PathBuf],
        files: &BTreeMap<PathBuf, PythonFile>,
    ) -> Result<()> {
        let architecture = target.split('-').next().unwrap_or_default();
        let macos = match &self.platform {
            PythonPlatform::Macos {
                machine,
                build,
                shared_cache_uuid,
            } => {
                ensure!(
                    target.ends_with("-apple-darwin") && bounded(build),
                    "invalid Python macOS prerequisite cohort"
                );
                ensure!(
                    shared_cache_uuid.len() == 32
                        && shared_cache_uuid
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                        && shared_cache_uuid.bytes().any(|byte| byte != b'0'),
                    "invalid Python shared-cache UUID"
                );
                ensure!(
                    (architecture == "aarch64" && machine == "arm64")
                        || (architecture == "x86_64" && machine == "x86_64"),
                    "Python prerequisite architecture mismatch"
                );
                true
            }
            PythonPlatform::Linux {
                machine,
                kernel_release,
                os_release_sha256,
            } => {
                ensure!(
                    target.ends_with("-linux-gnu")
                        && matches!(architecture, "x86_64" | "aarch64")
                        && machine == architecture,
                    "invalid Python Linux prerequisite target"
                );
                ensure!(
                    bounded(kernel_release) && valid_digest(os_release_sha256),
                    "invalid Python Linux prerequisite cohort"
                );
                false
            }
        };
        ensure!(
            !self.libraries.is_empty()
                && self.libraries.len() <= MAX_PYTHON_NATIVE_LIBRARIES
                && !self.dependencies.is_empty()
                && self.dependencies.len() <= MAX_PYTHON_NATIVE_EDGES
                && !self.host_dependencies.is_empty(),
            "Python native prerequisite limits"
        );
        let mut bytes = 0u64;
        for (path, library) in &self.libraries {
            ensure!(
                normal_absolute(path) && roots.iter().all(|root| !path.starts_with(root)),
                "external Python prerequisite overlaps its bundle"
            );
            match library {
                PythonNativeLibrary::File {
                    bytes: size,
                    sha256,
                } => {
                    ensure!(
                        *size > 0 && valid_digest(sha256),
                        "invalid Python prerequisite file identity"
                    );
                    bytes = bytes
                        .checked_add(*size)
                        .ok_or_else(|| anyhow::anyhow!("Python prerequisite byte overflow"))?;
                    ensure!(
                        bytes <= MAX_PYTHON_EXPANDED_BYTES,
                        "Python prerequisite byte limit"
                    );
                }
                PythonNativeLibrary::MacosSharedCache => ensure!(
                    macos && system_cache_path(path),
                    "invalid Python shared-cache prerequisite"
                ),
            }
        }
        let available = |path: &Path| {
            matches!(files.get(path), Some(PythonFile::File { .. }))
                || self.libraries.contains_key(path)
        };
        let mut edges = 0usize;
        for (path, dependencies) in &self.dependencies {
            ensure!(
                matches!(files.get(path), Some(PythonFile::File { .. }))
                    || matches!(
                        self.libraries.get(path),
                        Some(PythonNativeLibrary::File { .. })
                    ),
                "Python dependency graph has an unqualified source"
            );
            validate_edges(dependencies, &available, &mut edges)?;
        }
        validate_edges(&self.host_dependencies, &available, &mut edges)?;
        Ok(())
    }
}

fn validate_edges(
    edges: &BTreeMap<String, Vec<PathBuf>>,
    available: &impl Fn(&Path) -> bool,
    count: &mut usize,
) -> Result<()> {
    *count = edges
        .values()
        .try_fold(*count, |total, providers| {
            total.checked_add(providers.len())
        })
        .ok_or_else(|| anyhow::anyhow!("Python native edge overflow"))?;
    ensure!(
        *count <= MAX_PYTHON_NATIVE_EDGES,
        "Python native dependency edge limit"
    );
    for (reference, providers) in edges {
        ensure!(
            !reference.is_empty()
                && reference.len() <= 4096
                && !reference.contains('\0')
                && !providers.is_empty()
                && providers.windows(2).all(|pair| pair[0] < pair[1]),
            "invalid Python native dependency alternatives"
        );
        ensure!(
            providers
                .iter()
                .all(|provider| normal_absolute(provider) && available(provider)),
            "unresolved Python native dependency"
        );
    }
    Ok(())
}

pub(crate) fn system_cache_path(path: &Path) -> bool {
    path.starts_with("/usr/lib") || path.starts_with("/System/Library")
}

fn bounded(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && value.bytes().all(|byte| byte.is_ascii_graphic())
}

#[cfg(test)]
#[path = "python_prerequisites_tests.rs"]
mod tests;
